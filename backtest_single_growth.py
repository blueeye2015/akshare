#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
单增策略季度回测程序
========================
回测规则：
1. 调仓频率：每季度最后一个交易日
2. 选股逻辑：复用 double_single_growth_selector.py 的 select_single_growth
3. 持仓方式：等权分配资金，买入目标池中所有入选股票
4. 价格数据：后复权收盘价（adjust_type='hfq'），消除分红除权影响
5. 交易成本：
   - 买入佣金：0.025%（万分之2.5）
   - 卖出佣金：0.025%
   - 卖出印花税：0.1%（千分之一）
   - 合计买入成本 ≈ 0.025%，卖出成本 ≈ 0.125%
6. 停牌处理：停牌期间价格前向填充（ffill），调仓日停牌则跳过买卖
7. 交易单位：100股整数倍（A股最小交易单位）

作者：AI Assistant
日期：2026-05-11
"""

import pandas as pd
import numpy as np
import psycopg2
from datetime import datetime
from collections import defaultdict
import logging
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import os
import sys

# 导入选股器（复用现有逻辑）
from double_single_growth_selector import (
    DoubleSingleGrowthSelector,
    GrowthCriteria,
    load_db_config
)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# 回测引擎
# ---------------------------------------------------------------------------
class SingleGrowthBacktest:
    def __init__(
        self,
        selector: DoubleSingleGrowthSelector,
        criteria: GrowthCriteria,
        start_date: str,
        end_date: str,
        initial_cash: float = 1_000_000.0,
        buy_commission: float = 0.00025,
        sell_commission: float = 0.00025,
        stamp_duty: float = 0.001,
    ):
        self.selector = selector
        self.criteria = criteria
        self.start_date = start_date
        self.end_date = end_date
        self.initial_cash = initial_cash
        self.buy_commission = buy_commission
        self.sell_commission = sell_commission
        self.stamp_duty = stamp_duty

        # 状态
        self.cash = initial_cash
        self.positions = {}  # symbol -> shares (整数)
        self.daily_records = []
        self.trade_records = []
        self.rebalance_records = []

    # --- 数据获取 ---
    def _get_trade_dates(self) -> pd.DatetimeIndex:
        """获取回测区间内所有交易日（以上证指数/沪深300有数据的日期为准）"""
        query = """
        SELECT DISTINCT trade_date 
        FROM index_daily 
        WHERE ts_code = '000300.SH'
          AND trade_date >= %s AND trade_date <= %s
        ORDER BY trade_date
        """
        df = pd.read_sql_query(
            query, self.selector.conn,
            params=[self.start_date, self.end_date]
        )
        return pd.to_datetime(df['trade_date']).sort_values()

    def _get_rebalance_dates(self, trade_dates: pd.DatetimeIndex) -> list:
        """每季度最后一个交易日作为调仓日"""
        df = pd.DataFrame({'trade_date': trade_dates})
        df['year'] = df['trade_date'].dt.year
        df['quarter'] = df['trade_date'].dt.quarter
        rebalance = df.groupby(['year', 'quarter'])['trade_date'].last()
        return rebalance.tolist()

    def _get_prices(self, symbols: list, start_date: str, end_date: str) -> pd.DataFrame:
        """
        获取指定股票在区间内的后复权收盘价。
        返回宽格式 DataFrame，index=trade_date，columns=symbol。
        缺失值用前向填充（处理停牌）。
        """
        if not symbols:
            return pd.DataFrame()

        placeholders = ','.join(['%s'] * len(symbols))
        query = f"""
        SELECT trade_date, symbol, close 
        FROM stock_history 
        WHERE symbol IN ({placeholders}) 
          AND trade_date >= %s AND trade_date <= %s
          AND adjust_type = 'hfq'
        """
        df = pd.read_sql_query(
            query, self.selector.conn,
            params=[*symbols, start_date, end_date]
        )

        if df.empty:
            return pd.DataFrame()

        df['trade_date'] = pd.to_datetime(df['trade_date'])
        df = df.pivot(index='trade_date', columns='symbol', values='close').sort_index()

        # 对齐到完整交易日序列（停牌日用前一天价格填充）
        all_dates = self._get_trade_dates()
        all_dates = all_dates[(all_dates >= pd.Timestamp(start_date)) & (all_dates <= pd.Timestamp(end_date))]
        df = df.reindex(all_dates)
        df = df.ffill()  # 停牌日前向填充
        return df

    def _get_benchmark(self) -> pd.DataFrame:
        """获取沪深300收盘价作为基准"""
        query = """
        SELECT trade_date, close 
        FROM index_daily 
        WHERE ts_code = '000300.SH'
          AND trade_date >= %s AND trade_date <= %s
        ORDER BY trade_date
        """
        df = pd.read_sql_query(
            query, self.selector.conn,
            params=[self.start_date, self.end_date]
        )
        df['trade_date'] = pd.to_datetime(df['trade_date'])
        df = df.set_index('trade_date').sort_index()
        df.rename(columns={'close': 'benchmark'}, inplace=True)
        return df

    # --- 核心回测逻辑 ---
    def run(self) -> pd.DataFrame:
        """执行回测，返回每日净值 DataFrame"""
        trade_dates = self._get_trade_dates()
        rebalance_dates = self._get_rebalance_dates(trade_dates)
        logger.info(f"回测区间: {self.start_date} ~ {self.end_date}")
        logger.info(f"调仓日数量: {len(rebalance_dates)}")

        for i, rebalance_dt in enumerate(rebalance_dates):
            rd_str = rebalance_dt.strftime('%Y-%m-%d')
            logger.info(f"\n{'='*70}")
            logger.info(f"【调仓 {i+1}/{len(rebalance_dates)}】日期: {rd_str}")

            # ---------- 1. 选股 ----------
            try:
                df_selected = self.selector.select_single_growth_fast(self.criteria, trade_date=rd_str)
            except Exception as e:
                logger.error(f"选股异常: {e}")
                df_selected = pd.DataFrame()

            if df_selected.empty:
                logger.warning("本季度未选出股票，全部清仓")
                target_symbols = []
            else:
                target_symbols = df_selected['symbol'].tolist()
                logger.info(f"本季度选出 {len(target_symbols)} 只股票")

            self.rebalance_records.append({
                'date': rd_str,
                'selected_count': len(target_symbols),
                'selected_symbols': target_symbols.copy(),
            })

            # ---------- 2. 确定持仓期 ----------
            if i < len(rebalance_dates) - 1:
                next_dt = rebalance_dates[i + 1]
            else:
                next_dt = trade_dates.iloc[-1]
            next_str = next_dt.strftime('%Y-%m-%d')

            # ---------- 3. 获取持仓期价格数据 ----------
            current_holdings = list(self.positions.keys())
            all_needed = list(set(target_symbols + current_holdings))
            df_prices = self._get_prices(all_needed, rd_str, next_str)

            if df_prices.empty:
                logger.warning(f"[{rd_str}] 未获取到价格数据，跳过本季度")
                continue

            # 确保调仓日本身在价格表中有记录
            if rebalance_dt not in df_prices.index:
                logger.warning(f"[{rd_str}] 调仓日不在价格数据中，跳过")
                continue

            current_prices = df_prices.loc[rebalance_dt]

            # ---------- 4. 卖出逻辑（调仓日收盘价） ----------
            sold_symbols = []
            for symbol in list(self.positions.keys()):
                if symbol in target_symbols:
                    continue  # 保留在目标池中的股票

                if symbol not in current_prices.index or pd.isna(current_prices[symbol]):
                    logger.debug(f"[{rd_str}] {symbol} 停牌/无价格，暂不卖出")
                    continue

                price = float(current_prices[symbol])
                shares = self.positions[symbol]
                gross = shares * price
                fee = gross * (self.sell_commission + self.stamp_duty)
                net_proceeds = gross - fee
                self.cash += net_proceeds

                self.trade_records.append({
                    'date': rd_str,
                    'symbol': symbol,
                    'action': 'SELL',
                    'price': price,
                    'shares': shares,
                    'gross_amount': gross,
                    'fee': fee,
                    'net_amount': net_proceeds,
                })
                sold_symbols.append(symbol)
                del self.positions[symbol]

            if sold_symbols:
                logger.info(f"卖出 {len(sold_symbols)} 只，现金: {self.cash:,.2f}")

            # ---------- 5. 买入逻辑（等权分配，100股整数倍） ----------
            # 只买入：在目标池中、且当前无持仓（已在持仓的不动，避免反复交易）
            # 但如果目标池中已有持仓的股票，保持不动
            symbols_to_buy = [s for s in target_symbols if s not in self.positions]

            # 过滤掉停牌或无价格的
            active_buy_targets = []
            for s in symbols_to_buy:
                if s in current_prices.index and pd.notna(current_prices[s]) and current_prices[s] > 0:
                    active_buy_targets.append(s)

            if active_buy_targets and self.cash > 0:
                # 等权分配：给每只股票分配相同的预算
                budget_per_stock = self.cash / len(active_buy_targets)
                bought_count = 0

                for symbol in active_buy_targets:
                    price = float(current_prices[symbol])
                    max_shares = int(budget_per_stock / price)
                    shares = (max_shares // 100) * 100  # 100股整数倍

                    if shares <= 0:
                        continue

                    gross = shares * price
                    fee = gross * self.buy_commission
                    total_cost = gross + fee

                    if self.cash >= total_cost:
                        self.cash -= total_cost
                        self.positions[symbol] = self.positions.get(symbol, 0) + shares
                        bought_count += 1

                        self.trade_records.append({
                            'date': rd_str,
                            'symbol': symbol,
                            'action': 'BUY',
                            'price': price,
                            'shares': shares,
                            'gross_amount': gross,
                            'fee': fee,
                            'net_amount': total_cost,
                        })

                logger.info(
                    f"买入 {bought_count}/{len(active_buy_targets)} 只，"
                    f"剩余现金: {self.cash:,.2f}"
                )

            # ---------- 6. 记录持仓期每日净值 ----------
            for date in df_prices.index:
                portfolio_value = self.cash
                for symbol, shares in self.positions.items():
                    if symbol in df_prices.columns and pd.notna(df_prices.loc[date, symbol]):
                        portfolio_value += shares * float(df_prices.loc[date, symbol])

                self.daily_records.append({
                    'date': date,
                    'portfolio_value': portfolio_value,
                    'cash': self.cash,
                    'holdings_count': len(self.positions),
                })

        logger.info(f"\n{'='*70}")
        logger.info("回测结束")
        return pd.DataFrame(self.daily_records)


# ---------------------------------------------------------------------------
# 回测分析器
# ---------------------------------------------------------------------------
class BacktestAnalyzer:
    def __init__(self, df_daily: pd.DataFrame, df_benchmark: pd.DataFrame, initial_cash: float):
        self.initial_cash = initial_cash
        self.df = df_daily.copy()
        self.df['date'] = pd.to_datetime(self.df['date'])
        self.df = self.df.set_index('date').sort_index()
        self.df['strategy_return'] = self.df['portfolio_value'] / initial_cash - 1

        self.bm = df_benchmark.copy()
        self.bm['benchmark_return'] = self.bm['benchmark'] / self.bm['benchmark'].iloc[0] - 1

        # 合并
        self.df = self.df.join(self.bm[['benchmark', 'benchmark_return']], how='left')

    def calculate_metrics(self) -> dict:
        """计算回测指标"""
        sr = self.df['portfolio_value']
        br = self.bm['benchmark']

        # 年化收益率
        total_return = sr.iloc[-1] / self.initial_cash - 1
        years = (sr.index[-1] - sr.index[0]).days / 365.25
        annual_return = (1 + total_return) ** (1 / years) - 1 if years > 0 else 0

        # 基准年化
        bm_total = br.iloc[-1] / br.iloc[0] - 1
        bm_annual = (1 + bm_total) ** (1 / years) - 1 if years > 0 else 0

        # 日收益率
        daily_ret = sr.pct_change().dropna()
        bm_daily_ret = br.pct_change().dropna()

        # 波动率（年化）
        volatility = daily_ret.std() * np.sqrt(252)
        bm_volatility = bm_daily_ret.std() * np.sqrt(252)

        # 最大回撤
        cummax = sr.cummax()
        drawdown = (sr - cummax) / cummax
        max_drawdown = drawdown.min()

        # 夏普比率（假设无风险利率 3%）
        risk_free = 0.03
        sharpe = (annual_return - risk_free) / volatility if volatility > 0 else 0

        # Calmar
        calmar = annual_return / abs(max_drawdown) if max_drawdown != 0 else 0

        # 胜率（按日）
        win_rate = (daily_ret > 0).mean()

        # 与基准对比
        excess_return = annual_return - bm_annual

        return {
            'total_return': total_return,
            'annual_return': annual_return,
            'benchmark_return': bm_total,
            'benchmark_annual': bm_annual,
            'excess_annual': excess_return,
            'volatility': volatility,
            'benchmark_volatility': bm_volatility,
            'max_drawdown': max_drawdown,
            'sharpe_ratio': sharpe,
            'calmar_ratio': calmar,
            'win_rate': win_rate,
            'trading_days': len(daily_ret),
            'years': years,
        }

    def plot(self, output_path: str = 'backtest_single_growth.png'):
        """绘制收益曲线"""
        fig, axes = plt.subplots(3, 1, figsize=(14, 12), sharex=True,
                                 gridspec_kw={'height_ratios': [3, 1, 1]})

        # 1. 净值曲线
        ax1 = axes[0]
        ax1.plot(self.df.index, self.df['strategy_return'],
                 label='双增策略', color='#e74c3c', linewidth=1.5)
        ax1.plot(self.df.index, self.df['benchmark_return'],
                 label='沪深300', color='#95a5a6', linewidth=1.0, linestyle='--')

        ax1.axhline(0, color='black', linewidth=0.5)
        ax1.fill_between(self.df.index, self.df['strategy_return'], 0,
                         where=(self.df['strategy_return'] >= 0),
                         color='#e74c3c', alpha=0.1)
        ax1.fill_between(self.df.index, self.df['strategy_return'], 0,
                         where=(self.df['strategy_return'] < 0),
                         color='#27ae60', alpha=0.1)

        ax1.set_title('单增策略季度回测 — 累计收益率', fontsize=14)
        ax1.set_ylabel('累计收益率')
        ax1.legend(loc='upper left')
        ax1.grid(True, alpha=0.3)

        # 2. 相对强弱（策略 - 基准）
        ax2 = axes[1]
        relative = self.df['strategy_return'] - self.df['benchmark_return']
        ax2.fill_between(self.df.index, relative, 0,
                         where=(relative >= 0), color='#e74c3c', alpha=0.3, label='跑赢基准')
        ax2.fill_between(self.df.index, relative, 0,
                         where=(relative < 0), color='#27ae60', alpha=0.3, label='跑输基准')
        ax2.axhline(0, color='black', linewidth=0.5)
        ax2.set_ylabel('超额收益')
        ax2.legend(loc='upper left')
        ax2.grid(True, alpha=0.3)

        # 3. 最大回撤
        ax3 = axes[2]
        cummax = self.df['portfolio_value'].cummax()
        drawdown = (self.df['portfolio_value'] - cummax) / cummax
        ax3.fill_between(self.df.index, drawdown, 0, color='#3498db', alpha=0.4)
        ax3.set_ylabel('回撤')
        ax3.set_xlabel('日期')
        ax3.set_title(f'最大回撤: {drawdown.min():.2%}')
        ax3.grid(True, alpha=0.3)

        plt.tight_layout()
        plt.savefig(output_path, dpi=200)
        logger.info(f"图表已保存: {output_path}")

    def print_report(self, metrics: dict, trade_records: list):
        """打印回测报告"""
        print("\n" + "=" * 70)
        print("单增策略季度回测报告")
        print("=" * 70)
        print(f"回测区间:     {self.df.index[0].strftime('%Y-%m-%d')} ~ {self.df.index[-1].strftime('%Y-%m-%d')} ({metrics['years']:.2f} 年)")
        print(f"初始资金:     {self.initial_cash:,.0f}")
        print(f"期末资金:     {self.df['portfolio_value'].iloc[-1]:,.2f}")
        print(f"\n策略收益:")
        print(f"  总收益率:   {metrics['total_return']:>10.2%}")
        print(f"  年化收益:   {metrics['annual_return']:>10.2%}")
        print(f"  年化波动:   {metrics['volatility']:>10.2%}")
        print(f"  最大回撤:   {metrics['max_drawdown']:>10.2%}")
        print(f"  夏普比率:   {metrics['sharpe_ratio']:>10.2f}")
        print(f"  Calmar:     {metrics['calmar_ratio']:>10.2f}")
        print(f"  日胜率:     {metrics['win_rate']:>10.2%}")
        print(f"\n基准对比 (沪深300):")
        print(f"  基准总收益: {metrics['benchmark_return']:>10.2%}")
        print(f"  基准年化:   {metrics['benchmark_annual']:>10.2%}")
        print(f"  超额年化:   {metrics['excess_annual']:>10.2%}")

        if trade_records:
            df_trades = pd.DataFrame(trade_records)
            buy_count = len(df_trades[df_trades['action'] == 'BUY'])
            sell_count = len(df_trades[df_trades['action'] == 'SELL'])
            total_fee = df_trades['fee'].sum()
            print(f"\n交易统计:")
            print(f"  买入次数:   {buy_count}")
            print(f"  卖出次数:   {sell_count}")
            print(f"  总交易成本: {total_fee:>10,.2f}")
        print("=" * 70)


# ---------------------------------------------------------------------------
# 主程序
# ---------------------------------------------------------------------------
def main():
    # 配置参数
    START_DATE = '2010-01-01'
    END_DATE = '2026-05-08'  # 数据库最新日期，也可设为 None 自动获取
    INITIAL_CASH = 1_000_000.0

    # 单增选股参数（与 double_single_growth_selector.py 默认一致）
    criteria = GrowthCriteria(
        strategy='single',
        max_peg=0.5,
        min_profit_growth=0.50,
        max_deduct_pe=50.0,
        top_n=None,  # 不限制数量，买入所有入选股票
    )

    # 加载数据库配置并连接
    logger.info("正在加载数据库配置...")
    db_config = load_db_config()
    selector = DoubleSingleGrowthSelector(db_config)
    selector.connect()

    # 如果 END_DATE 为 None，取最新交易日期
    if END_DATE is None:
        END_DATE = selector.get_latest_trade_date()
        logger.info(f"自动设置结束日期: {END_DATE}")

    try:
        # 执行回测
        backtest = SingleGrowthBacktest(
            selector=selector,
            criteria=criteria,
            start_date=START_DATE,
            end_date=END_DATE,
            initial_cash=INITIAL_CASH,
        )
        df_daily = backtest.run()

        if df_daily.empty:
            logger.error("回测未产生任何记录，请检查数据范围或选股条件")
            return

        # 获取基准
        df_benchmark = backtest._get_benchmark()

        # 分析结果
        analyzer = BacktestAnalyzer(df_daily, df_benchmark, INITIAL_CASH)
        metrics = analyzer.calculate_metrics()
        analyzer.print_report(metrics, backtest.trade_records)

        # 保存结果
        df_daily.to_csv('backtest_single_growth_daily.csv', encoding='utf-8-sig')
        logger.info("每日净值已保存: backtest_single_growth_daily.csv")

        if backtest.trade_records:
            pd.DataFrame(backtest.trade_records).to_csv(
                'backtest_single_growth_trades.csv', index=False, encoding='utf-8-sig'
            )
            logger.info("交易记录已保存: backtest_single_growth_trades.csv")

        # 保存每季度选股结果
        if backtest.rebalance_records:
            df_rebalance = pd.DataFrame([
                {
                    'date': r['date'],
                    'selected_count': r['selected_count'],
                    'selected_symbols': ','.join(r['selected_symbols'])
                }
                for r in backtest.rebalance_records
            ])
            df_rebalance.to_csv('backtest_single_growth_rebalance.csv', index=False, encoding='utf-8-sig')
            logger.info("调仓记录已保存: backtest_single_growth_rebalance.csv")

        # 绘图
        analyzer.plot('backtest_single_growth.png')

    finally:
        selector.disconnect()
        logger.info("数据库连接已关闭")


if __name__ == '__main__':
    main()
