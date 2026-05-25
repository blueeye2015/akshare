#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
煤炭股指数编制程序
==================
编制规则：
1. 成分股：stock_basic 中 industry LIKE '%煤%' 的全部股票（目前25只）
2. 基期：2010-01-04（或成分股最早有数据的日期）
3. 基点：1000点
4. 两种加权方式：
   - 等权指数：每日收益率 = 成分股日收益率的简单平均
   - 流通市值加权指数：按每日流通市值加权
5. 调仓频率：季度调仓（每季度最后一个交易日重新平衡权重）
   - 等权：每季度重新等权
   - 市值加权：每季度按最新流通市值重新加权
6. 停牌处理：停牌股票当日收益率为0（价格沿用前一天）
7. 退市处理：退市股票从名单中剔除，指数做连续性调整

输出：
- coal_index.csv：每日指数数据
- coal_index.png：指数走势图
"""

import pandas as pd
import numpy as np
import psycopg2
import os
import logging
from dotenv import load_dotenv
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

load_dotenv('.env')
DSN = os.getenv('DB_DSN1')

# 基期和基点
BASE_DATE = '2010-01-04'
BASE_POINT = 1000.0


def get_coal_symbols(conn) -> pd.DataFrame:
    """获取煤炭行业股票列表"""
    query = """
    SELECT symbol, name, industry, list_date
    FROM stock_basic
    WHERE industry LIKE '%%煤%%'
    ORDER BY symbol
    """
    df = pd.read_sql_query(query, conn)
    df['list_date'] = pd.to_datetime(df['list_date'])
    logger.info(f"煤炭成分股数量: {len(df)}")
    return df


def get_trade_dates(conn, start_date: str, end_date: str) -> pd.DatetimeIndex:
    """获取交易日序列"""
    query = """
    SELECT DISTINCT trade_date
    FROM index_daily
    WHERE ts_code = '000300.SH'
      AND trade_date >= %s AND trade_date <= %s
    ORDER BY trade_date
    """
    df = pd.read_sql_query(query, conn, params=[start_date, end_date])
    return pd.to_datetime(df['trade_date']).sort_values()


def get_stock_prices(conn, symbols: list, start_date: str, end_date: str) -> pd.DataFrame:
    """获取成分股后复权收盘价（停牌日ffill）"""
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
    df = pd.read_sql_query(query, conn, params=[*symbols, start_date, end_date])
    df['trade_date'] = pd.to_datetime(df['trade_date'])
    df = df.pivot(index='trade_date', columns='symbol', values='close').sort_index()
    # 停牌日ffill
    trade_dates = get_trade_dates(conn, start_date, end_date)
    df = df.reindex(trade_dates)
    df = df.ffill()
    return df


def get_circ_mv(conn, symbols: list, start_date: str, end_date: str) -> pd.DataFrame:
    """获取每日流通市值（亿元）用于市值加权"""
    if not symbols:
        return pd.DataFrame()
    placeholders = ','.join(['%s'] * len(symbols))
    # daily_basic 的 circ_mv 单位是万元，转为亿元
    query = f"""
    SELECT trade_date, LEFT(ts_code, 6) as symbol, circ_mv / 10000.0 as circ_mv
    FROM daily_basic
    WHERE LEFT(ts_code, 6) IN ({placeholders})
      AND trade_date >= %s AND trade_date <= %s
    """
    df = pd.read_sql_query(query, conn, params=[*symbols, start_date, end_date])
    df['trade_date'] = pd.to_datetime(df['trade_date'])
    df = df.pivot(index='trade_date', columns='symbol', values='circ_mv').sort_index()
    # 停牌日用前一天市值填充
    trade_dates = get_trade_dates(conn, start_date, end_date)
    df = df.reindex(trade_dates)
    df = df.ffill()
    return df


def get_benchmark(conn, start_date: str, end_date: str) -> pd.Series:
    """获取沪深300收盘价作为基准"""
    query = """
    SELECT trade_date, close
    FROM index_daily
    WHERE ts_code = '000300.SH'
      AND trade_date >= %s AND trade_date <= %s
    ORDER BY trade_date
    """
    df = pd.read_sql_query(query, conn, params=[start_date, end_date])
    df['trade_date'] = pd.to_datetime(df['trade_date'])
    df = df.set_index('trade_date')['close'].sort_index()
    return df


def calc_equal_weight_index(df_prices: pd.DataFrame, rebalance_dates: pd.DatetimeIndex) -> pd.Series:
    """
    计算等权指数
    每季度末重新等权，非调仓日按各自涨跌自然漂移
    """
    dates = df_prices.index
    index_vals = pd.Series(index=dates, dtype=float)
    current_index = BASE_POINT

    # 标记调仓日
    rebalance_set = set(rebalance_dates)

    # 初始化：第一个调仓日之前的等权配置
    # 找到第一个有数据的日期
    first_valid = df_prices.first_valid_index()
    if first_valid is None:
        return index_vals

    # 从第一个交易日开始
    positions = pd.Series(0.0, index=df_prices.columns)  # 每只股票的持仓数量（以基点计）

    for i, date in enumerate(dates):
        prices_today = df_prices.loc[date]

        if i == 0:
            # 首日：等权买入所有有价格的股票
            valid_mask = prices_today.notna() & (prices_today > 0)
            n = valid_mask.sum()
            if n == 0:
                index_vals.iloc[i] = current_index
                continue
            weight = current_index / n
            positions[valid_mask] = weight / prices_today[valid_mask]
            index_vals.iloc[i] = current_index
            continue

        # 计算今日指数值 = sum(持仓数量 * 今日价格)
        portfolio_value = (positions * prices_today).sum()
        index_vals.iloc[i] = portfolio_value
        current_index = portfolio_value

        # 如果是调仓日，重新等权
        if date in rebalance_set:
            valid_mask = prices_today.notna() & (prices_today > 0)
            n = valid_mask.sum()
            if n > 0:
                weight = current_index / n
                positions[:] = 0.0
                positions[valid_mask] = weight / prices_today[valid_mask]

    return index_vals


def calc_mv_weight_index(df_prices: pd.DataFrame, df_mv: pd.DataFrame, rebalance_dates: pd.DatetimeIndex) -> pd.Series:
    """
    计算流通市值加权指数
    每季度末按流通市值重新加权，非调仓日按各自涨跌自然漂移
    """
    dates = df_prices.index
    index_vals = pd.Series(index=dates, dtype=float)
    positions = pd.Series(0.0, index=df_prices.columns)

    for i, date in enumerate(dates):
        prices_today = df_prices.loc[date]

        if i == 0:
            mv_today = df_mv.loc[date] if date in df_mv.index else pd.Series(0.0, index=df_prices.columns)
            valid_mask = prices_today.notna() & (prices_today > 0) & mv_today.notna() & (mv_today > 0)
            total_mv = mv_today[valid_mask].sum()
            if total_mv <= 0:
                index_vals.iloc[i] = BASE_POINT
                continue
            current_index = BASE_POINT
            # 按市值加权分配
            for sym in df_prices.columns:
                if valid_mask.get(sym, False):
                    weight = current_index * (mv_today[sym] / total_mv)
                    positions[sym] = weight / prices_today[sym]
            index_vals.iloc[i] = current_index
            continue

        portfolio_value = (positions * prices_today).sum()
        index_vals.iloc[i] = portfolio_value
        current_index = portfolio_value

        # 调仓日重新按市值加权
        if date in rebalance_dates:
            mv_today = df_mv.loc[date] if date in df_mv.index else pd.Series(0.0, index=df_prices.columns)
            valid_mask = prices_today.notna() & (prices_today > 0) & mv_today.notna() & (mv_today > 0)
            total_mv = mv_today[valid_mask].sum()
            if total_mv > 0:
                positions[:] = 0.0
                for sym in df_prices.columns:
                    if valid_mask.get(sym, False):
                        weight = current_index * (mv_today[sym] / total_mv)
                        positions[sym] = weight / prices_today[sym]

    return index_vals


def main():
    logger.info("=" * 60)
    logger.info("开始编制煤炭股指数")
    logger.info("=" * 60)

    conn = psycopg2.connect(DSN)

    # 1. 获取成分股
    df_coal = get_coal_symbols(conn)
    symbols = df_coal['symbol'].tolist()
    logger.info(f"成分股: {symbols}")

    # 2. 获取最新交易日
    cur = conn.cursor()
    cur.execute("SELECT MAX(trade_date) FROM index_daily WHERE ts_code = '000300.SH'")
    end_date = cur.fetchone()[0].strftime('%Y-%m-%d')
    cur.close()

    # 3. 获取交易日和调仓日
    trade_dates = get_trade_dates(conn, BASE_DATE, end_date)
    rebalance_dates = trade_dates[pd.DatetimeIndex(trade_dates).is_quarter_end]
    logger.info(f"交易日数量: {len(trade_dates)}, 调仓日数量: {len(rebalance_dates)}")

    # 4. 获取价格数据
    logger.info("正在获取成分股价格数据...")
    df_prices = get_stock_prices(conn, symbols, BASE_DATE, end_date)
    logger.info(f"价格数据: {df_prices.shape}")

    # 5. 获取流通市值数据
    logger.info("正在获取流通市值数据...")
    df_mv = get_circ_mv(conn, symbols, BASE_DATE, end_date)
    logger.info(f"市值数据: {df_mv.shape}")

    # 6. 获取基准
    benchmark = get_benchmark(conn, BASE_DATE, end_date)

    conn.close()

    # 7. 计算指数
    logger.info("计算等权指数...")
    eq_index = calc_equal_weight_index(df_prices, rebalance_dates)

    logger.info("计算市值加权指数...")
    mv_index = calc_mv_weight_index(df_prices, df_mv, rebalance_dates)

    # 8. 合并结果
    df_result = pd.DataFrame({
        'trade_date': trade_dates,
        'equal_weight_index': eq_index.values,
        'mv_weight_index': mv_index.values,
        'hs300': benchmark.reindex(trade_dates).values,
    })
    df_result = df_result.set_index('trade_date')

    # 删除全NaN的行（早期可能无数据）
    df_result = df_result.dropna(subset=['equal_weight_index', 'mv_weight_index'], how='all')

    # 归一化基准到1000点
    first_valid = df_result['equal_weight_index'].first_valid_index()
    if first_valid is not None:
        base_hs300 = df_result.loc[first_valid, 'hs300']
        df_result['hs300_norm'] = df_result['hs300'] / base_hs300 * BASE_POINT

    # 9. 保存CSV
    df_result.to_csv('coal_index.csv', encoding='utf-8-sig')
    logger.info("指数数据已保存: coal_index.csv")

    # 10. 统计指标
    def calc_metrics(series: pd.Series, name: str):
        series = series.dropna()
        if len(series) < 2:
            return {}
        total_ret = series.iloc[-1] / series.iloc[0] - 1
        years = (series.index[-1] - series.index[0]).days / 365.25
        annual_ret = (1 + total_ret) ** (1 / years) - 1 if years > 0 else 0
        daily_ret = series.pct_change().dropna()
        vol = daily_ret.std() * np.sqrt(252)
        cummax = series.cummax()
        max_dd = ((series - cummax) / cummax).min()
        sharpe = (annual_ret - 0.03) / vol if vol > 0 else 0
        return {
            'name': name,
            'total_return': total_ret,
            'annual_return': annual_ret,
            'volatility': vol,
            'max_drawdown': max_dd,
            'sharpe': sharpe,
        }

    metrics = [
        calc_metrics(df_result['equal_weight_index'], '煤炭等权'),
        calc_metrics(df_result['mv_weight_index'], '煤炭市值加权'),
        calc_metrics(df_result['hs300_norm'], '沪深300'),
    ]

    print("\n" + "=" * 70)
    print("煤炭股指数回测报告")
    print("=" * 70)
    print(f"基期: {first_valid.strftime('%Y-%m-%d') if first_valid else 'N/A'}")
    print(f"期末: {df_result.index[-1].strftime('%Y-%m-%d')}")
    print(f"成分股数量: {len(symbols)}")
    print()
    for m in metrics:
        if not m:
            continue
        print(f"【{m['name']}】")
        print(f"  总收益率:  {m['total_return']:>10.2%}")
        print(f"  年化收益:  {m['annual_return']:>10.2%}")
        print(f"  年化波动:  {m['volatility']:>10.2%}")
        print(f"  最大回撤:  {m['max_drawdown']:>10.2%}")
        print(f"  夏普比率:  {m['sharpe']:>10.2f}")
        print()
    print("=" * 70)

    # 11. 绘图
    plt.figure(figsize=(14, 8))
    plt.plot(df_result.index, df_result['equal_weight_index'], label='Coal Equal-Weight', color='#e74c3c', linewidth=1.5)
    plt.plot(df_result.index, df_result['mv_weight_index'], label='Coal MV-Weight', color='#f39c12', linewidth=1.5)
    plt.plot(df_result.index, df_result['hs300_norm'], label='CSI 300', color='#95a5a6', linewidth=1.0, linestyle='--')

    plt.axhline(BASE_POINT, color='black', linewidth=0.5, alpha=0.3)
    plt.title('Coal Stock Index vs CSI 300 (Base=1000)', fontsize=14)
    plt.xlabel('Date')
    plt.ylabel('Index Point')
    plt.legend(loc='upper left')
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig('coal_index.png', dpi=200)
    logger.info("图表已保存: coal_index.png")


if __name__ == '__main__':
    main()
