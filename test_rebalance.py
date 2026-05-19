import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

load_dotenv('.env')
DSN = os.getenv('DB_DSN1')

def get_db_engine():
    return create_engine(DSN)

# 模拟换仓日：假设3月9日是换仓日
# 旧持仓（假设是一些股票）
old_positions = {
    '000001': 1000,  # 平安银行
    '000002': 500,   # 万科A
    '000858': 200,   # 五粮液
}

# 新持仓（换仓后的股票）
new_symbols = ['300059', '300750', '002594']  # 东方财富、宁德时代、比亚迪

REBALANCE_DATE = '2026-03-09'

COST_CONFIG = {
    'commission': 0.00025,
    'stamp_tax': 0.001,
    'slippage': 0.005,
    'min_commission': 5,
}

def get_price(symbol, date, price_type='open'):
    engine = get_db_engine()
    sql = f"SELECT {price_type} FROM stock_history WHERE symbol='{symbol}' AND trade_date='{date}'"
    df = pd.read_sql(sql, engine)
    if df.empty:
        return None
    return float(df.iloc[0][price_type])

print("="*70)
print(f"🔄 换仓日模拟: {REBALANCE_DATE}")
print(f"📉 假设场景: 低开高走 (卖出时价格低，买入后价格上涨)")
print("="*70)

# 获取指数判断走势
engine = get_db_engine()
df_index = pd.read_sql(f"SELECT open, close FROM stock_history WHERE symbol='000001' AND trade_date='{REBALANCE_DATE}'", engine)
if not df_index.empty:
    index_open = float(df_index.iloc[0]['open'])
    index_close = float(df_index.iloc[0]['close'])
    index_change = (index_close - index_open) / index_open * 100
    print(f"\n📊 上证指数 {REBALANCE_DATE}: 开盘{index_open:.2f} -> 收盘{index_close:.2f} ({index_change:+.2f}%)")

# 第一步：卖出旧持仓（开盘价 - 滑点）
print(f"\n📉 步骤1: 卖出旧持仓 (共{len(old_positions)}只)")
print("-" * 50)
total_cash = 0
for symbol, shares in old_positions.items():
    open_p = get_price(symbol, REBALANCE_DATE, 'open')
    close_p = get_price(symbol, REBALANCE_DATE, 'close')
    if open_p is None:
        continue
    
    # 卖出用开盘价 - 滑点（假设低开，卖出价更低）
    exec_price = open_p * (1 - COST_CONFIG['slippage'])
    gross = shares * exec_price
    commission = max(gross * COST_CONFIG['commission'], COST_CONFIG['min_commission'])
    stamp_tax = gross * COST_CONFIG['stamp_tax']
    net = gross - commission - stamp_tax
    total_cash += net
    
    day_change = (close_p - open_p) / open_p * 100 if close_p else 0
    print(f"  {symbol}: 开盘{open_p:.2f}/收盘{close_p:.2f}({day_change:+.1f}%) | "
          f"卖出价{exec_price:.2f} | 净得{net:,.2f}")

print(f"\n  💰 卖出后总现金: {total_cash:,.2f}")

# 第二步：买入新持仓（开盘价 + 滑点）
print(f"\n📈 步骤2: 买入新持仓 (共{len(new_symbols)}只)")
print("-" * 50)
cash_per_stock = total_cash / len(new_symbols)
new_positions = {}
total_cost = 0

for symbol in new_symbols:
    open_p = get_price(symbol, REBALANCE_DATE, 'open')
    close_p = get_price(symbol, REBALANCE_DATE, 'close')
    if open_p is None:
        print(f"  {symbol}: 无数据")
        continue
    
    # 买入用开盘价 + 滑点（买入成本更高）
    exec_price = open_p * (1 + COST_CONFIG['slippage'])
    shares = int(cash_per_stock / exec_price / 100) * 100
    
    if shares == 0:
        print(f"  {symbol}: 资金不足")
        continue
    
    gross = shares * exec_price
    commission = max(gross * COST_CONFIG['commission'], COST_CONFIG['min_commission'])
    cost = gross + commission
    
    new_positions[symbol] = shares
    total_cost += cost
    
    day_change = (close_p - open_p) / open_p * 100 if close_p else 0
    print(f"  {symbol}: 开盘{open_p:.2f}/收盘{close_p:.2f}({day_change:+.1f}%) | "
          f"买入价{exec_price:.2f} | 买入{shares}股 | 成本{cost:,.2f}")

cash_left = total_cash - total_cost
print(f"\n  💵 买入总成本: {total_cost:,.2f}")
print(f"  💵 剩余现金: {cash_left:,.2f}")

# 计算收盘后的总资产
print(f"\n📊 步骤3: 收盘后估值")
print("-" * 50)
end_value = cash_left
for symbol, shares in new_positions.items():
    close_p = get_price(symbol, REBALANCE_DATE, 'close')
    if close_p:
        value = shares * close_p
        end_value += value
        buy_price = get_price(symbol, REBALANCE_DATE, 'open') * (1 + COST_CONFIG['slippage'])
        profit = (close_p - buy_price) / buy_price * 100
        print(f"  {symbol}: 持仓{shares}股 × 收盘{close_p:.2f} = {value:,.2f} (买入后{profit:+.2f}%)")

print(f"\n{'='*70}")
print(f"📈 换仓日结果:")
print(f"   卖出所得: {total_cash:,.2f}")
print(f"   买入成本: {total_cost:,.2f}")
print(f"   收盘总资产: {end_value:,.2f}")
print(f"   当日盈亏: {(end_value - total_cash)/total_cash*100:+.2f}%")
print(f"{'='*70}")

# 验证：如果当天是先跌（卖出亏）后涨（买入赚），应该体现这种切换优势
print(f"\n✅ 验证逻辑:")
print(f"   - 卖出旧持仓用'开盘价-滑点'，如果低开则卖出价更低（不利）")
print(f"   - 买入新持仓用'开盘价+滑点'，如果高走则买入后涨价（有利）")
print(f"   - 整体效果取决于旧持仓和新持仓当天的相对强弱")
