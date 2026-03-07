#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
增强版回测框架
- 市场择时（熊市降仓）
- 因子分数加权
- 波动率自适应止盈止损
- 真实交易成本
- 增加notify_order函数，添加止损逻辑
"""
import backtrader as bt
import pandas as pd
import os
import numpy as np
import psycopg2
import logging
import glob
from datetime import datetime
from dotenv import load_dotenv
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import seaborn as sns
from collections import defaultdict
import time
import gc
import csv

load_dotenv('.env')
POSTGRES_CONFIG = os.getenv("DB_DSN1")
BENCHMARK_SYMBOL = '000300.SH'
ADJUST_TYPE = 'hfq'
CACHE_DIR = 'factor_cache_global_short'
INITIAL_CASH = 1000000.0

# 自定义数据加载器
class PandasDataWithFactor(bt.feeds.PandasData):
    lines = ('factor',)
    params = (
        ('factor', 'factor'),
        ('datetime', None),
        ('open', 'open'),
        ('high', 'high'),
        ('low', 'low'),
        ('close', 'close'),
        ('volume', 'volume'),
        ('openinterest', -1),
    )

def calculate_limit_price(symbol, close_price, trade_date, direction='up', debug=False):
    """
    计算 T+1 的涨跌停挂单价 (适配创业板历史规则)
    
    :param symbol: 股票代码
    :param close_price: 昨日收盘价
    :param trade_date: 当前交易日期
    :param direction: 'up' (买入/涨停) / 'down' (卖出/跌停)
    :param debug: 是否打印调试信息
    """
    # 转换为 Timestamp 以支持比较
    current_dt = pd.Timestamp(trade_date)
    START_DATE_CHINEXT_20PCT = pd.Timestamp('2020-08-24')
    
    # --- 1. 判定板块与涨跌幅比例 ---
    ratio = 0.10      # 默认主板 10%
    board_name = "主板"
    
    if symbol.startswith('688'):
        ratio = 0.20
        board_name = "科创板"
        
    elif symbol.startswith('300'):
        if current_dt >= START_DATE_CHINEXT_20PCT:
            ratio = 0.20
            board_name = "创业板(注册制)"
        else:
            ratio = 0.10
            board_name = "创业板(核准制)"
            
    elif symbol.startswith(('8', '4')):
        ratio = 0.30
        board_name = "北交所"
    
    # --- 2. 计算挂单价格 ---
    if direction == 'up':
        # 涨停买入：挂单价 = 涨停价 - 1分钱 (防止一字板成交)
        limit_val = close_price * (1 + ratio)
        # 简单的四舍五入
        exec_price = round(limit_val, 2) - 0.01
        arrow = "🔺"
    else:
        # 跌停卖出：挂单价 = 跌停价 + 1分钱 (防止一字跌停成交)
        limit_val = close_price * (1 - ratio)
        exec_price = round(limit_val, 2) + 0.01
        arrow = "nm" # unicode for arrow down is usually 🔻 but let's stick to safe chars or green
        arrow = "🔻"

    # --- 3. 打印详细提示 (你想要的部分) ---
    if debug:
        print(f"[{current_dt.date()}] {arrow} {direction.upper()} | {symbol} | {board_name} | "
              f"昨收:{close_price:.2f} x (1±{ratio:.0%}) = 计算价:{limit_val:.2f} -> 挂单:{exec_price:.2f}")

    return exec_price

# 增强版策略
class MLFactorStrategy(bt.Strategy):
    params = dict(
        top_n_pct=0.03,  # 改善：从2%提高到3%，更分散
        rebalance_monthday=1,
        debug=True,
        stop_loss_base=0.08,
        take_profit_base=0.25,
        volatility_lookback=20,
        # 🔥 新增参数，默认为空字典
        ipo_mapping={},
        stock_names={}, # 静态字典(备用)
        name_history={},   # 🔥 动态历史字典
    )

    def __init__(self):
        print("\n" + "!"*50)
        print("【启动确认】增强版策略已加载！")
        print("!"*50 + "\n")

        # 市场择时指标
        self.market_ma120 = bt.indicators.SMA(self.datas[0].close, period=120)
        self.market_ma250 = bt.indicators.SMA(self.datas[0].close, period=250)
        
        # 动态仓位控制
        self.target_position_ratio = 1.0
            
        # 1. 打印 Data0 的身份信息 (最关键的一步)
        data0 = self.datas[0]
        print(f"【Data0 身份核查】名称: {data0._name}")
        
        # 股票列表（排除Data0）
        self.stocks = self.datas[1:]
        self.add_timer(when=bt.timer.SESSION_END, monthdays=[self.p.rebalance_monthday], cheat=False)
        self.last_rebalance_month = -1
        
        # 交易记录
        self.closed_trades = []
        self.stock_entry_price = defaultdict(lambda: None)
        self.first_bar = True
        self.trade_max_size = defaultdict(float)

        # 🔥🔥🔥 1. 新增：动作日志列表 (用于生成实战指令单)
        self.action_log = []
        # 🔥🔥🔥 新增：用于画图的净值记录列表
        self.net_value_history = []

    def get_current_stock_name(self, symbol, current_date):
        """
        根据当前日期，获取股票当时的真实名称 (判断 ST 的关键)
        """
        # 1. 优先查历史变更表
        history = self.p.name_history.get(symbol)
        
        if history:
            # history 是一个按时间排序的 list: [(date1, name1), (date2, name2)...]
            # 我们需要找到 start_date <= current_date 的最后一条记录
            
            # 简单遍历法 (因为变更记录通常很少，遍历很快)
            found_name = None
            for start_date, name in history:
                if start_date <= current_date:
                    found_name = name
                else:
                    # 因为是按时间排序的，如果 start_date 超过了当前日期，后面的都不用看了
                    break
            
            if found_name:
                return found_name
        
        # 2. 如果查不到历史 (比如新股或者数据缺失)，回退到静态字典
        return self.p.stock_names.get(symbol, "Unknown")
    
    def log_action(self, date, symbol, action, price, weight, reason):
        """记录单步操作到日志"""
        # 打印到控制台 (可选)
        if self.p.debug:
            print(f"[{date}] {action:<4} | {symbol} | 价:{price:.2f} | 仓:{weight:.1%} | 因:{reason}")
            
        # 存入列表
        self.action_log.append({
            'date': date,
            'symbol': symbol,
            'name': self.p.stock_names.get(symbol, symbol),
            'action': action, # BUY, SELL, HOLD
            'price': price,
            'target_weight': weight,
            'reason': reason
        })

    def next(self):
        # =================================================================
        # 🎯 核心修复：显式调用调仓 + 保险兜底
        # =================================================================
        current_date = self.datetime.date(0)
        
        # --- 每日持仓巡检 (告诉你现在手里有什么) ---
        # 只在有操作的那天打印，或者每月1号打印，避免日志爆炸
        # 这里设置为：只要有持仓，每天都记录一条 "HOLD" 状态，方便画图或核对
        # (为了节省CSV体积，这里我设置为每月1号记录一次持仓快照)
        if current_date.day == 1:
            total_val = self.broker.getvalue()
            cash = self.broker.getcash()

            for data, pos in self.getpositions().items():
                if pos.size != 0 and data._name != BENCHMARK_SYMBOL:
                    val = pos.size * data.close[0]
                    #这里直接用外面算好的 total_val，不用再重复获取
                    if total_val > 0:
                        weight = val / total_val
                    else:
                        weight = 0
                    self.log_action(current_date, data._name, "HOLD", data.close[0], weight, "月初持仓快照")

            # 2. 记录现金仓位
            if total_val > 0:
                cash_weight = cash / total_val
                if cash_weight > 0.001: # 现金占比 > 0.1% 才记录
                    self.log_action(current_date, "CASH", "HOLD", 1.0, cash_weight, "闲置现金/避险资金")

        # 首根K线立即调仓（确保有初始持仓）
        # if self.first_bar:
        #     print(f"[{current_date}] 首根K线，强制调仓！")
        #     self.rebalance_portfolio()
        #     self.first_bar = False
        #     self.last_rebalance_month = current_date.month
        #     return
        
        # 每月1号调仓（主逻辑）
        # =================================================================
        # 🎯 修复：更稳健的月度调仓逻辑 (防止跳过假期)
        # =================================================================
        # 逻辑：只要当前月份 != 上次调仓月份，说明这是本月的第一个交易日
        if current_date.month != self.last_rebalance_month:
            print(f"[{current_date}] 新月份首个交易日 -> 触发调仓")
            self.rebalance_portfolio()
            self.last_rebalance_month = current_date.month
        
        # =================================================================
        # 市场择时
        # =================================================================
        if len(self.datas[0]) > 250:
            market_price = self.datas[0].close[0]
            market_ma250 = self.market_ma250[0]
            
            if market_price < market_ma250 * 0.95:  # 熊市
                self.target_position_ratio = 0.3
            elif market_price < self.market_ma120[0]:  # 震荡
                self.target_position_ratio = 0.7
            else:  # 牛市
                self.target_position_ratio = 1.0
        
        # =================================================================
        # 动态止盈止损
        # =================================================================
        for data, pos in self.getpositions().items():
            if pos.size == 0 or data._name == BENCHMARK_SYMBOL:
                continue
            
            entry = self.stock_entry_price.get(data._name)
            if not entry:
                continue
            
            # ✅ 修复：多取一根K线，确保close_hist[:-1]与high/low长度一致
            lookback = self.p.volatility_lookback + 1  # 21 instead of 20
            close_hist = np.array(data.close.get(size=lookback))
            if len(close_hist) < lookback:
                continue  # 数据不足
            
            # high/low保持原长度
            high = np.array(data.high.get(size=self.p.volatility_lookback))
            low = np.array(data.low.get(size=self.p.volatility_lookback))
            
            # 现在close_hist[:-1]也是20个元素，可以正常广播
            tr = np.maximum(
                high - low,
                np.maximum(
                    abs(high - close_hist[:-1]),
                    abs(low - close_hist[:-1])
                )
            )
            atr = np.mean(tr[-10:])
            vol_ratio = atr / close_hist[-1]
            
            # 动态阈值
            dynamic_stop = max(0.05, self.p.stop_loss_base * vol_ratio * 10)
            dynamic_profit = self.p.take_profit_base * (1 + vol_ratio * 5)
            
            # 执行
            ret = data.close[0] / entry - 1
            if ret < -dynamic_stop or ret > dynamic_profit:
                reason = "止损" if ret < 0 else "止盈"
                # 🔥🔥🔥 记录风控操作
                self.log_action(current_date, data._name, "SELL", data.close[0], 0.0, f"{reason}({ret:.2%})")
                print(f"[{current_date}] {data._name} 止盈止损平仓: {ret:.2%}")
                self.order_target_percent(data=data, target=0.0)
                self.stock_entry_price[data._name] = None

        # 🔥🔥🔥 新增：每天收盘前记录日期和总资产
        # 注意：放在 next 的最后一行
        self.net_value_history.append({
            'date': self.datetime.date(0),
            'value': self.broker.getvalue(),
            'cash': self.broker.getcash()
        })    

        

    def notify_order(self, order):
        if order.status in [order.Submitted, order.Accepted]:
            # 订单已提交/被接受，无需操作
            return

        # 检查订单是否完成（全部成交）
        if order.status == order.Completed:
            symbol = order.data._name
            
            if order.isbuy():
                # 买入成交：记录实际成交均价
                self.stock_entry_price[symbol] = order.executed.price
                if self.p.debug:
                    print(f"[{self.datetime.date(0)}] ✅ 买入成交: {symbol} | 价格: {order.executed.price:.2f} | 数量: {order.executed.size}")
                    
            elif order.issell():
                # 卖出成交：清空成本记录（可选）
                if symbol in self.stock_entry_price:
                    del self.stock_entry_price[symbol]
                if self.p.debug:
                    print(f"[{self.datetime.date(0)}] ✅ 卖出成交: {symbol} | 价格: {order.executed.price:.2f} | 数量: {order.executed.size}")

        elif order.status in [order.Canceled, order.Margin, order.Rejected]:
            if self.p.debug:
                print(f"[{self.datetime.date(0)}] ⚠️ 订单异常: {order.data._name} | 状态: {order.status}")

    def notify_trade(self, trade):
        if not trade.isclosed or trade.data._name == BENCHMARK_SYMBOL:
            return
        
        symbol = trade.data._name
            
        # 2. 获取价格信息
        entry_price = trade.price  # 开仓均价 (BT自带，非常准确)
        exit_price = trade.data.close[0]  # 平仓时的市价 (近似值)
        
        # 3. 计算成本 (Plan A)
        max_size = self.trade_max_size.get(trade.ref, 0)
        position_cost = entry_price * max_size
        
        # 4. 计算收益率 (含 Plan B 兜底)
        pct_ret = 0.0
        
        if position_cost > 0:
            # Plan A: 标准计算 (净利润 / 总成本)
            pct_ret = trade.pnlcomm / position_cost
        else:
            # Plan B: 兜底计算 (如果成本为0，说明是超短线交易没抓到 Size)
            # 直接用 (卖出价 - 买入价) / 买入价
            # 这种情况下无法精确扣除手续费占比，但比 0 准确得多
            if entry_price > 0:
                pct_ret = (exit_price - entry_price) / entry_price
            
        self.closed_trades.append({
            'symbol': symbol,
            'open_date': bt.num2date(trade.dtopen).date(),
            'close_date': bt.num2date(trade.dtclose).date(),
            'pnl_net': trade.pnlcomm,
            'return': pct_ret,
            'price_entry': entry_price,  # 买入价
            'price_exit': exit_price     # 卖出价
        })

    def notify_timer(self, timer, when, *args, **kwargs):
        # 保留定时器作为备用，但不再依赖它
        pass

    # def _is_limit_up(self, data):
    #     if len(data) < 2: return False
    #     limit = get_price_limit(data._name)
    #     return data.high[0] >= round(data.close[-1] * (1 + limit), 2) - 0.01

    # def _is_limit_down(self, data):
    #     if len(data) < 2: return False
    #     limit = get_price_limit(data._name)
    #     return data.low[0] <= round(data.close[-1] * (1 - limit), 2) + 0.01

    def rebalance_portfolio(self):
        is_debug_day = (self.datetime.date(0).month == 6 and self.datetime.date(0).day <= 5)
        # 获取当前日期
        current_date = self.datetime.date(0)
        # 筛选有效股票
        valid_stocks = []
        reject_counts = {'nan_close': 0, 'nan_factor': 0, 'low_factor': 0, 'limit_up': 0, 'ST': 0, 'ok': 0}
        
        for d in self.stocks:
            stock_name = self.p.stock_names.get(d._name, "Unknown")
            # 🔥🔥🔥 使用动态名称查询 🔥🔥🔥
            # 这一步至关重要！在 2020 年它会返回 '*ST同洲'，在 2025 年返回 '同洲电子'
            stock_name = self.get_current_stock_name(d._name, current_date)
            # 过滤条件
            if len(d) == 0 or np.isnan(d.close[0]) or d.close[0] < 0.01:
                reject_counts['nan_close'] += 1
                continue
            
            if np.isnan(d.factor[0]):
                reject_counts['nan_factor'] += 1
                continue
                
            if d.factor[0] <= -0.99:
                reject_counts['low_factor'] += 1
                continue
            
            # if self._is_limit_up(d):
            #     reject_counts['limit_up'] += 1
            #     continue
            # if self._is_limit_up(d):
            #     reject_counts['limit_up'] += 1
            #     continue

            # if 'ST' in stock_name:
            #     reject_counts['ST'] += 1
            #     continue
            
            reject_counts['ok'] += 1
            valid_stocks.append((d.factor[0], d))
        
        # 打印诊断
        if is_debug_day:
            print(f"\n[{self.datetime.date(0)}] 选股漏斗:")
            print(f"  - 无效数据: {reject_counts['nan_close']}")
            print(f"  - 因子缺失: {reject_counts['nan_factor']}")
            print(f"  - 因子无效: {reject_counts['low_factor']}")
            print(f"  - 涨停不可买: {reject_counts['limit_up']}")
            print(f"  - ST不可买: {reject_counts['ST']}")
            print(f"  - ✅ 最终入选: {reject_counts['ok']}")
        
        if not valid_stocks:
            return
        
        # 排序并选择
        valid_stocks.sort(key=lambda x: x[0], reverse=True)
        top_n = int(len(self.stocks) * self.p.top_n_pct)
        if top_n == 0 and len(valid_stocks) > 0: top_n = 5
        
        target_stocks = [d for score, d in valid_stocks[:top_n]]
        
        # 因子分数加权
        factor_scores = np.array([d.factor[0] for d in target_stocks])
        
        # 去极值和归一化
        p10, p90 = np.percentile(factor_scores, 10), np.percentile(factor_scores, 90)
        
        # ✅ 修复：使用max防止分母为0
        denom = max(p90 - p10, 1e-8)
        factor_scores = (factor_scores - p10) / denom
        factor_scores = np.clip(factor_scores, 0, 1)
        
        # 权重分配（归一化到目标仓位）
        if factor_scores.sum() > 0:
            weights = factor_scores / factor_scores.sum() * self.target_position_ratio
        else:
            weights = np.ones(len(target_stocks)) / len(target_stocks) * self.target_position_ratio
        
        # 计算权重 (简化为等权，也可保留原本的归一化逻辑)
        weight_per_stock = self.target_position_ratio / len(target_stocks)

        # 调仓执行
        target_names = {d._name for d in target_stocks}
                
        # 1. 卖出逻辑 (不在目标池的股票)
        for data, pos in self.getpositions().items():
            if pos.size != 0 and data._name not in target_names:
                # 🔥🔥🔥 记录卖出操作
                self.log_action(current_date, data._name, "SELL", data.close[0], 0.0, "换仓移出")

                # 检查是否跌停：如果 T 日已经跌停，T+1 大概率跑不掉，但这里我们尝试挂单
                # 计算 T+1 的跌停保护价
                limit_down_price = calculate_limit_price(data._name, data.close[0], current_date, direction='down', debug=True)
                
                # 使用 Limit 单卖出：只有价格 >= 跌停价+0.01 时才成交
                # 如果 T+1 开盘死封跌停，价格会低于 limit_down_price，订单不会成交 -> 这种被闷杀更真实
                self.order_target_percent(
                    data=data, 
                    target=0.0, 
                    exectype=bt.Order.Limit, 
                    price=limit_down_price
                )
                self.stock_entry_price[data._name] = None
        
        # 2. 买入逻辑 (目标池中的股票)
        for i, d in enumerate(target_stocks):
            current_pos = self.getposition(d).size
            
            # 如果当前没有持仓，且 T 日没有涨停 (T日涨停买入是允许的，只要T+1能买进)
            if current_pos == 0:
                # 🔥🔥🔥 记录买入操作
                self.log_action(current_date, d._name, "BUY", d.close[0], weight_per_stock, "月度轮动")
                # 计算 T+1 涨停价的"一分钱下方"
                limit_buy_price = calculate_limit_price(d._name, d.close[0], current_date, direction='up', debug=True)
                
                # 发送限价买单
                self.order_target_percent(
                    data=d, 
                    target=weights[i],
                    exectype=bt.Order.Limit, # 指定为限价单
                    price=limit_buy_price    # 设定价格上限
                )
    
    def stop(self):
        # 回测结束时，保存指令单
        print(f"\n正在导出实战指令单 -> strategy_actions.csv ...")
        
        fieldnames = ['date', 'symbol', 'name', 'action', 'price', 'target_weight', 'reason']
        with open('strategy_actions.csv', 'w', newline='', encoding='utf-8-sig') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(self.action_log)
        
        print(f"✅ 导出完成！请用 Excel 打开查看详细操作记录。")

# 印花税成本模型
class StampDutyCommissionScheme(bt.CommInfoBase):
    params = (
        ('stamp_duty', 0.001),
        ('commission', 0.00025),
        ('stocklike', True),
        ('commtype', bt.CommInfoBase.COMM_PERC),
    )
    
    def _getcommission(self, size, price, pseudoexec):
        if size > 0:  # 买入
            return abs(size) * price * self.p.commission
        else:  # 卖出
            return abs(size) * price * (self.p.commission + self.p.stamp_duty)

# 主程序
if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s: %(message)s')
    
    # 加载因子
    logging.info("1. 加载因子...")
    chunk_files = glob.glob(os.path.join(CACHE_DIR, '*.parquet'))
    if not chunk_files:
        logging.error("未找到因子文件")
        exit(1)
        
    df_factor = pd.concat([pd.read_parquet(f) for f in chunk_files], ignore_index=True)
    # 2. 【关键】删除因子文件自带的 'close' 列
    # 原因：我们的训练脚本为了方便检查保存了 close，但数据库行情里也有 close。
    #如果不删掉，合并时会出现 close_x, close_y，导致回测报错 KeyError: 'close'。
    if 'close' in df_factor.columns:
        df_factor = df_factor.drop(columns=['close'])
    df_factor['trade_date'] = pd.to_datetime(df_factor['trade_date'])
    
    min_date, max_date = df_factor['trade_date'].min(), df_factor['trade_date'].max()
    logging.info(f"因子覆盖时间: {min_date.date()} -> {max_date.date()}")
    
    # 在加载因子后检查
    print("\n" + "="*50)
    print("📊 因子数据合法性检查")
    print("="*50)

    # 因子分布统计
    factor_stats = df_factor['factor'].describe()
    print("因子统计:")
    print(factor_stats)

    # 检查因子是否在合理范围 [0,1]
    out_of_range = df_factor[(df_factor['factor'] < -1) | (df_factor['factor'] > 1)]
    if len(out_of_range) > 0:
        print(f"❌ 发现 {len(out_of_range)} 条因子超出[-1,1]范围")
    else:
        print("✅ 因子范围正常")

    # 检查因子是否全为-1（无效）
    all_invalid = (df_factor['factor'] == -1).all()
    if all_invalid:
        print("❌ 因子全为-1，无效因子！")
    else:
        valid_factor_rate = (df_factor['factor'] > -0.99).mean()
        print(f"✅ 有效因子占比: {valid_factor_rate:.2%}")

    print("="*50 + "\n")

    # 股票池
    all_symbols = df_factor['symbol'].unique().tolist()
    symbols_to_run = all_symbols
    if BENCHMARK_SYMBOL not in symbols_to_run:
        symbols_to_run.append(BENCHMARK_SYMBOL)
    
    # 连接数据库
    logging.info("2. 加载数据...")
    conn = psycopg2.connect(POSTGRES_CONFIG)
    
    # 加载基准
    df_bench = pd.read_sql_query(
        "SELECT trade_date, open, high, low, close FROM index_daily WHERE ts_code=%s AND trade_date BETWEEN %s AND %s ORDER BY trade_date",
        conn, params=[BENCHMARK_SYMBOL, min_date, max_date]
    )
    df_bench['trade_date'] = pd.to_datetime(df_bench['trade_date'])
    df_bench['volume'] = 0
    df_bench['factor'] = -1
    
    # 加载个股数据
    stock_syms = [s for s in symbols_to_run if s != BENCHMARK_SYMBOL]
    placeholders = ','.join(['%s'] * len(stock_syms))
    df_stocks = pd.read_sql_query(
        f"SELECT trade_date, symbol, open, high, low, close, volume FROM stock_history WHERE symbol IN ({placeholders}) AND trade_date BETWEEN %s AND %s AND adjust_type=%s",
        conn, params=[*stock_syms, min_date, max_date, ADJUST_TYPE]
    )

    # -----------------------------------------------------------
    # 🔥 新增：加载 IPO 上市日期数据
    # -----------------------------------------------------------
    logging.info("2.1 加载证券基础信息 (IPO日期)...")
    df_basic = pd.read_sql_query(
        "SELECT symbol, list_date, name FROM stock_basic", 
        conn
    )
    # 转换为字典: {'000001': datetime.date(1991, 4, 3), ...}
    # 注意处理可能的 None/Nat，如果有空值，默认给一个很早的日期
    df_basic['list_date'] = pd.to_datetime(df_basic['list_date']).dt.date
    ipo_dict = df_basic.set_index('symbol')['list_date'].to_dict()

    # 🔥 新增：生成 name 字典
    name_dict = df_basic.set_index('symbol')['name'].to_dict()
    
    # -----------------------------------------------------------
    # 🔥🔥🔥 新增：加载股票曾用名历史 (解决 ST 状态回溯问题)
    # -----------------------------------------------------------
    logging.info("2.2 加载股票曾用名历史...")
    
    # 1. 读取数据 (按代码和开始时间排序)
    sql_name = """
    SELECT security_code as symbol, start_date, name 
    FROM public.stock_namechange 
    ORDER BY security_code, start_date
    """
    df_name_change = pd.read_sql_query(sql_name, conn)

    df_name_change['start_date'] = pd.to_datetime(df_name_change['start_date']).dt.date

    # 3. 构建时间轴字典
    # 格式: { '002052': [ (date(2010,1,1), '同洲电子'), (date(2019,1,1), '*ST同洲')... ] }
    name_history_dict = defaultdict(list)
    for _, row in df_name_change.iterrows():
        name_history_dict[row['symbol']].append( (row['start_date'], row['name']) )
        
    conn.close()
    
    df_stocks['trade_date'] = pd.to_datetime(df_stocks['trade_date'])
    
    # 合并因子
    logging.info("3. 合并因子...")
    df_all = pd.merge(df_stocks, df_factor, on=['trade_date', 'symbol'], how='left')
    df_all['factor'].fillna(-1, inplace=True)
    
    # 在 visual_backtest_v4.py 合并数据后添加诊断
    print("\n" + "="*50)
    print("🔍 价格数据合法性检查")
    print("="*50)

    # 检查后复权价格是否异常
    price_stats = df_all.groupby('symbol')['close'].agg(['min', 'max', 'mean'])
    print("价格极值统计:")
    print(f"  最低价格: {price_stats['min'].min():.4f}")
    print(f"  最高价格: {price_stats['max'].max():.4f}")
    print(f"  平均价格: {price_stats['mean'].mean():.4f}")

    # 检查是否有价格<=0的幽灵数据
    invalid_prices = df_all[df_all['close'] <= 0]
    if len(invalid_prices) > 0:
        print(f"❌ 发现 {len(invalid_prices)} 条价格<=0的异常数据！")
        print(invalid_prices[['trade_date', 'symbol', 'close']].head())
    else:
        print("✅ 价格数据无负值或零值")

    # 检查是否有价格日涨幅超过20%（非新股）
    df_all['return'] = df_all.groupby('symbol')['close'].pct_change()
    extreme_moves = df_all[(df_all['return'].abs() > 0.2) & (df_all['close'] > 10)]
    if len(extreme_moves) > 0:
        print(f"⚠️ 发现 {len(extreme_moves)} 条涨幅超20%的异常波动")
        print(extreme_moves[['trade_date', 'symbol', 'return', 'close']].head())
    else:
        print("✅ 价格波动正常")
    print("="*50 + "\n")
    # 基准时间轴
    FULL_TIMELINE = pd.to_datetime(df_bench['trade_date']).sort_values()
    logging.info(f"基准时间轴: {len(FULL_TIMELINE)} 天")
    
    # 初始化Cerebro
    cerebro = bt.Cerebro()
    cerebro.broker.setcash(INITIAL_CASH)
    cerebro.addstrategy(MLFactorStrategy, ipo_mapping=ipo_dict, name_history=name_history_dict, # 🔥 名字历史参数
                        stock_names=name_dict)
    
    # 添加Benchmark
    start_dt = FULL_TIMELINE[0].to_pydatetime()
    cerebro.adddata(PandasDataWithFactor(dataname=df_bench.set_index('trade_date'), fromdate=start_dt), name=BENCHMARK_SYMBOL)
    
    # 添加成本
    cerebro.broker.setcommission(commission=0.00025, stocklike=True, commtype=bt.CommInfoBase.COMM_PERC)
    cerebro.broker.addcommissioninfo(StampDutyCommissionScheme())
    cerebro.broker.set_slippage_perc(perc=0.0005, slip_open=True, slip_match=True, slip_out=False)
    
    # 个股对齐
    logging.info("4. 对齐个股数据...")
    grouped = df_all.groupby('symbol')
    add_count = 0
    price_columns = ['open', 'high', 'low', 'close']
    
    for symbol, df_s in grouped:
        df_s.set_index('trade_date', inplace=True)
        df_s = df_s[~df_s.index.duplicated(keep='first')]
        df_aligned = df_s.reindex(FULL_TIMELINE)
        
        # 填充价格（停牌ffill，上市前0）
        df_aligned[price_columns] = df_aligned[price_columns].fillna(method='ffill').fillna(0.0)
        df_aligned['volume'] = df_aligned['volume'].fillna(0)
        df_aligned['factor'] = df_aligned['factor'].fillna(-1)
        
        cerebro.adddata(PandasDataWithFactor(dataname=df_aligned), name=symbol)
        add_count += 1
    
    logging.info(f"已添加 {add_count} 只股票")
    
    # 运行回测
    logging.info("5. 开始回测...")
    results = cerebro.run(preload=False, runonce=False)
    
    # 结果分析
    if results:
        strat = results[0]
        final_value = cerebro.broker.getvalue()
        print(f"\n{'='*50}")
        print(f"最终资金: {final_value:,.2f}")
        print(f"收益率: {(final_value/INITIAL_CASH-1)*100:.2f}%")
        
        if hasattr(strat, 'closed_trades') and strat.closed_trades:
            df_res = pd.DataFrame(strat.closed_trades)
            print(f"交易笔数: {len(df_res)}")
            print(f"胜率: {(df_res['pnl_net'] > 0).mean():.2%}")
            print(f"平均每笔收益: {df_res['pnl_net'].mean():.2f}")
            df_res.to_csv('trade_log_enhanced.csv', index=False)
            print(f"\n交易记录已保存至 trade_log_enhanced.csv")
        else:
            print("⚠️ 无交易记录")
    else:
        logging.error("回测返回空结果")

    print("\n正在绘制收益曲线...")
    
    # 1. 提取策略净值数据
    df_equity = pd.DataFrame(strat.net_value_history)
    df_equity['date'] = pd.to_datetime(df_equity['date'])
    df_equity.set_index('date', inplace=True)
    
    # 计算策略收益率 (净值 / 初始资金 - 1)
    # 假设初始资金是 10,000,000 (或者从 broker 获取初始值)
    real_initial_cash = df_equity['value'].iloc[0] 
    df_equity['strategy_return'] = df_equity['value'] / real_initial_cash - 1
    
    # 2. 获取基准收益率 (Benchmark)
    # 假设 data0 是沪深300
    benchmark_data = strat.datas[0]
    # 提取基准的时间和收盘价
    bm_dates = [bt.num2date(d) for d in benchmark_data.datetime.array]
    bm_close = list(benchmark_data.close.array)
    df_benchmark = pd.DataFrame({'close': bm_close}, index=bm_dates)
    
    # 截取与回测区间相同的时间段
    df_benchmark = df_benchmark.loc[df_equity.index[0]:df_equity.index[-1]]
    # 计算累计收益率 (归一化)
    df_benchmark['benchmark_return'] = df_benchmark['close'] / df_benchmark['close'].iloc[0] - 1
    
    # 3. 开始画图
    plt.figure(figsize=(12, 8))
    
    # 上半部分：收益率曲线
    ax1 = plt.subplot(2, 1, 1)
    ax1.plot(df_equity.index, df_equity['strategy_return'], label='Strategy (策略)', color='red', linewidth=2)
    ax1.plot(df_benchmark.index, df_benchmark['benchmark_return'], label='Benchmark (沪深300)', color='gray', linestyle='--', alpha=0.7)
    
    # 填充正收益和负收益区域
    ax1.fill_between(df_equity.index, df_equity['strategy_return'], 0, where=(df_equity['strategy_return']>=0), color='red', alpha=0.1)
    ax1.fill_between(df_equity.index, df_equity['strategy_return'], 0, where=(df_equity['strategy_return']<0), color='green', alpha=0.1)
    
    ax1.set_title(f'Strategy vs Benchmark Equity Curve (Total Return: {df_equity["strategy_return"].iloc[-1]:.2%})', fontsize=14)
    ax1.grid(True, which='both', linestyle='--', alpha=0.5)
    ax1.legend(loc='upper left')
    
    # 下半部分：最大回撤 (Drawdown)
    ax2 = plt.subplot(2, 1, 2, sharex=ax1)
    
    # 计算回撤
    running_max = df_equity['value'].cummax()
    drawdown = (df_equity['value'] - running_max) / running_max
    
    ax2.fill_between(df_equity.index, drawdown, 0, color='blue', alpha=0.3)
    ax2.set_title(f'Max Drawdown (最大回撤: {drawdown.min():.2%})', fontsize=12)
    ax2.grid(True, linestyle='--', alpha=0.5)
    ax2.set_ylabel('Drawdown')
    
    # 保存图片
    plt.tight_layout()
    plt.savefig('backtest_result_chart.png', dpi=300)
    print(f"✅ 图表已保存为 backtest_result_chart.png，请打开查看！")