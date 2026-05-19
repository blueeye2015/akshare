import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

load_dotenv('.env')
DSN = os.getenv('DB_DSN1')

def get_db_engine():
    return create_engine(DSN)

# 2023-08-28 著名墓碑线测试
REBALANCE_DATE = '2023-08-28'

# 假设的旧持仓
old_positions = {
    '000001': 1000,  # 平安银行
    '000002': 500,   # 万科A
    '000858': 100,   # 五粮液
    '600000': 800,   # 浦发银行
    '600519': 50,    # 贵州茅台
}

# 假设的新持仓
new_symbols = ['300059', '300750', '002594', '000725', '002415']

COST_CONFIG = {
    'commission': 0.00025,
    'stamp_tax': 0.001,
    'slippage': 0.005,
    'min_commission': 5,
}

INITIAL_BUDGET = 1000000.0

def get_stock_price(symbol, date, price_type='open'):
    """获取股票价格（从stock_history）"""
    engine = get_db_engine()
    sql = f"SELECT {price_type} FROM stock_history WHERE symbol='{symbol}' AND trade_date = '{date}'"
    df = pd.read_sql(sql, engine)
    if df.empty:
        return None
    return float(df.iloc[0][price_type])

def get_index_price(ts_code, date, price_type='open'):
    """获取指数价格（从index_daily）"""
    engine = get_db_engine()
    sql = f"SELECT {price_type} FROM index_daily WHERE ts_code='{ts_code}' AND trade_date = '{date}'"
    df = pd.read_sql(sql, engine)
    if df.empty:
        return None
    return float(df.iloc[0][price_type])

def get_execution_price(open_price, direction='buy', slippage=COST_CONFIG['slippage']):
    if direction == 'buy':
        return open_price * (1 + slippage)
    else:
        return open_price * (1 - slippage)

print("="*70)
print(f"🪦 2023-08-28 著名墓碑线 - 换仓测试")
print(f"📅 日期: {REBALANCE_DATE}")
print("="*70)

# 获取上证指数正确数据
index_open = get_index_price('000001.SH', REBALANCE_DATE, 'open')
index_high = get_index_price('000001.SH', REBALANCE_DATE, 'high')
index_low = get_index_price('000001.SH', REBALANCE_DATE, 'low')
index_close = get_index_price('000001.SH', REBALANCE_DATE, 'close')

if index_open:
    index_change = (index_close - index_open) / index_open * 100
    gap = (index_open - get_index_price('000001.SH', '2023-08-25', 'close')) / get_index_price('000001.SH', '2023-08-25', 'close') * 100
    
    print(f"\n📊 上证指数 {REBALANCE_DATE}:")
    print(f"   开盘: {index_open:.2f} (较前收高开 {gap:+.2f}%)")
    print(f"   最高: {index_high:.2f}")
    print(f"   最低: {index_low:.2f}")
    print(f"   收盘: {index_close:.2f} ({index_change:+.2f}%)")
    print(f"   日内振幅: {(index_high-index_low)/index_open*100:.2f}%")
    
    if index_open == index_high and index_close < index_open:
        print(f"   ⚠️  典型墓碑线形态: 开盘即最高，一路下跌")

# 第一步：卖出旧持仓
print(f"\n📉 步骤1: 卖出旧持仓 (共{len(old_positions)}只)")
print("-" * 60)
print(f"{'代码':<10} {'股数':>8} {'开盘':>8} {'滑点卖出价':>10} {'毛收入':>12} {'税费':>10} {'净收入':>12}")
print("-" * 60)

total_cash = 0
sell_details = []

for symbol, shares in old_positions.items():
    open_p = get_stock_price(symbol, REBALANCE_DATE, 'open')
    close_p = get_stock_price(symbol, REBALANCE_DATE, 'close')
    if open_p is None:
        print(f"{symbol:<10} 无数据")
        continue
    
    exec_price = get_execution_price(open_p, 'sell')
    gross = shares * exec_price
    commission = max(gross * COST_CONFIG['commission'], COST_CONFIG['min_commission'])
    stamp_tax = gross * COST_CONFIG['stamp_tax']
    net = gross - commission - stamp_tax
    total_cash += net
    
    day_change = (close_p - open_p) / open_p * 100 if close_p else 0
    sell_details.append({
        'symbol': symbol, 'shares': shares, 'open': open_p, 'exec': exec_price,
        'gross': gross, 'commission': commission, 'tax': stamp_tax, 'net': net,
        'day_change': day_change
    })
    
    print(f"{symbol:<10} {shares:>8} {open_p:>8.2f} {exec_price:>10.2f} {gross:>12,.2f} {commission+stamp_tax:>10.2f} {net:>12,.2f}")

print("-" * 60)
print(f"{'合计':<10} {'':>8} {'':>8} {'':>10} {'':>12} {'':>10} {total_cash:>12,.2f}")

# 计算如果持有到收盘不卖，会亏多少
print(f"\n📊 旧持仓如果持有不卖:")
hold_value = 0
for d in sell_details:
    close_p = get_stock_price(d['symbol'], REBALANCE_DATE, 'close')
    if close_p:
        value = d['shares'] * close_p
        hold_value += value
        change = (close_p - d['open']) / d['open'] * 100
        print(f"   {d['symbol']}: 开盘{d['open']:.2f} -> 收盘{close_p:.2f} ({change:+.2f}%)")

print(f"\n💰 卖出所得现金: {total_cash:,.2f}")

# 第二步：买入新持仓
print(f"\n📈 步骤2: 买入新持仓 (共{len(new_symbols)}只，等权分配)")
print("-" * 60)
cash_per_stock = total_cash / len(new_symbols)
print(f"每只可用资金: {cash_per_stock:,.2f}")
print(f"{'代码':<10} {'开盘':>8} {'滑点买入价':>10} {'可买股数':>8} {'买入成本':>12} {'收盘':>8} {'当天盈亏':>10}")
print("-" * 60)

new_positions = {}
total_buy_cost = 0
buy_details = []

for symbol in new_symbols:
    open_p = get_stock_price(symbol, REBALANCE_DATE, 'open')
    close_p = get_stock_price(symbol, REBALANCE_DATE, 'close')
    if open_p is None:
        print(f"{symbol:<10} 无数据")
        continue
    
    exec_price = get_execution_price(open_p, 'buy')
    shares = int(cash_per_stock / exec_price / 100) * 100
    
    if shares == 0:
        print(f"{symbol:<10} {open_p:>8.2f} {exec_price:>10.2f} {'资金不足':>8}")
        continue
    
    gross = shares * exec_price
    commission = max(gross * COST_CONFIG['commission'], COST_CONFIG['min_commission'])
    total_cost = gross + commission
    
    new_positions[symbol] = shares
    total_buy_cost += total_cost
    
    day_change = (close_p - exec_price) / exec_price * 100 if close_p else 0
    buy_details.append({
        'symbol': symbol, 'shares': shares, 'exec_price': exec_price,
        'total_cost': total_cost, 'close': close_p, 'day_change': day_change
    })
    
    print(f"{symbol:<10} {open_p:>8.2f} {exec_price:>10.2f} {shares:>8} {total_cost:>12,.2f} {close_p:>8.2f} {day_change:>+9.2f}%")

cash_left = total_cash - total_buy_cost
print("-" * 60)
print(f"买入总成本: {total_buy_cost:,.2f}, 剩余现金: {cash_left:,.2f}")

# 第三步：收盘后估值
print(f"\n📊 步骤3: 收盘后总资产估值")
print("-" * 60)
end_value = cash_left
print(f"剩余现金: {cash_left:,.2f}")

for d in buy_details:
    value = d['shares'] * d['close']
    end_value += value
    profit = (d['close'] - d['exec_price']) / d['exec_price'] * 100
    print(f"{d['symbol']}: {d['shares']}股 × {d['close']:.2f} = {value:,.2f} (买入后{profit:+.2f}%)")

print("-" * 60)
print(f"收盘总资产: {end_value:,.2f}")

# 最终汇总
print(f"\n{'='*70}")
print(f"📈 换仓日结果汇总:")
print(f"{'='*70}")
print(f"   初始总资产:     {INITIAL_BUDGET:>15,.2f}")
print(f"   卖出旧持仓得:   {total_cash:>15,.2f}")
print(f"   买入新持仓花:   {total_buy_cost:>15,.2f}")
print(f"   剩余现金:       {cash_left:>15,.2f}")
print(f"   收盘总资产:     {end_value:>15,.2f}")
print(f"   当日盈亏:       {(end_value-INITIAL_BUDGET)/INITIAL_BUDGET*100:>15.2f}%")
print(f"{'='*70}")

print(f"\n✅ 墓碑线换仓逻辑验证:")
print(f"   1. 当天大幅高开 → 卖出旧持仓价格极高（非常有利）✓")
print(f"   2. 随后一路下跌 → 买入新持仓后价格下跌（不利）")
print(f"   3. 关键在于：新持仓是否比旧持仓跌得少")

# 比较新旧持仓当日表现
print(f"\n📊 新旧持仓对比:")
old_avg_change = np.mean([d['day_change'] for d in sell_details])
new_avg_change = np.mean([d['day_change'] for d in buy_details])
print(f"   旧持仓平均涨跌: {old_avg_change:+.2f}%")
print(f"   新持仓平均涨跌: {new_avg_change:+.2f}%")
if new_avg_change > old_avg_change:
    print(f"   ✅ 换仓正确：新持仓表现优于旧持仓")
else:
    print(f"   ⚠️  换仓失误：新持仓表现差于旧持仓")
    print(f"   💡 在墓碑线行情中，如果新持仓也大跌，换仓反而加剧亏损")
