import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os
from datetime import timedelta

# --- 配置 ---
load_dotenv('.env')
DSN = os.getenv('DB_DSN1')

SRC_TABLE = "strategy_positions"             # 当前持仓表 (2/6 调仓后的新名单)
DST_TABLE = "strategy_performance"   # 收益记录表
INITIAL_BUDGET = 1000000.0           # 初始总预算 (仅当数据库无历史记录时使用)

# 设定本次回测区间
START_DATE_STR = '2026-03-07'
END_DATE_STR = '2026-04-07'

# 交易成本配置
COST_CONFIG = {
    'commission_buy': 0.00025,   # 万2.5
    'commission_sell': 0.00025,  # 万2.5
    'stamp_tax': 0.001,          # 千1，仅卖出
    'slippage': 0.005,           # 0.5% 双边滑点
    'min_commission': 5,         # 最低佣金5元
}

def get_db_engine():
    return create_engine(DSN)

def get_execution_price(open_price, direction='buy', slippage=COST_CONFIG['slippage']):
    """获取实际成交价：开盘价 ± 滑点"""
    if direction == 'buy':
        return open_price * (1 + slippage)  # 买的时候价格更高
    else:
        return open_price * (1 - slippage)  # 卖的时候价格更低

def get_open_price(symbol, date, engine):
    """获取某只股票某日的开盘价"""
    price_sql = f"""
    SELECT open FROM stock_history 
    WHERE symbol = '{symbol}' AND trade_date = '{date}'
    """
    df_price = pd.read_sql(price_sql, engine)
    if df_price.empty:
        return None
    return float(df_price.iloc[0]['open'])

def rebalance_portfolio(old_positions, new_symbols, rebalance_date, engine, config=COST_CONFIG):
    """
    换仓逻辑：先卖旧持仓，再算可用资金，最后买新持仓
    返回: (新持仓dict, 剩余现金, 详细记录)
    """
    print(f"\n{'='*60}")
    print(f"📅 换仓日: {rebalance_date}")
    print(f"{'='*60}")
    
    # 1️⃣ 卖出旧持仓（考虑滑点和税费）
    total_cash = 0
    sell_details = []
    
    print(f"\n📉 步骤1: 卖出旧持仓 (共{len(old_positions)}只)")
    for symbol, shares in old_positions.items():
        open_price = get_open_price(symbol, rebalance_date, engine)
        if open_price is None:
            print(f"   ⚠️ {symbol} 无开盘价数据，跳过")
            continue
            
        exec_price = get_execution_price(open_price, 'sell', config['slippage'])
        
        # 卖出金额
        gross_amount = shares * exec_price
        # 扣除佣金（最低5元）和印花税
        commission = max(gross_amount * config['commission_sell'], config['min_commission'])
        stamp_tax = gross_amount * config['stamp_tax']
        net_amount = gross_amount - commission - stamp_tax
        
        total_cash += net_amount
        sell_details.append({
            'symbol': symbol, 'shares': shares, 'open_price': open_price,
            'exec_price': exec_price, 'gross': gross_amount, 
            'commission': commission, 'stamp_tax': stamp_tax, 'net': net_amount
        })
    
    print(f"   💰 卖出后总现金: {total_cash:,.2f}")
    
    # 2️⃣ 买入新持仓（等权分配，考虑滑点和佣金）
    if not new_symbols:
        print("   ⚠️ 无新持仓，全部保留现金")
        return {}, total_cash, {'sell': sell_details, 'buy': []}
    
    # 检查新持仓在当天是否有数据
    valid_new_symbols = []
    for symbol in new_symbols:
        open_price = get_open_price(symbol, rebalance_date, engine)
        if open_price is not None:
            valid_new_symbols.append(symbol)
        else:
            print(f"   ⚠️ {symbol} 无开盘价数据，从买入列表中移除")
    
    if not valid_new_symbols:
        print("   ⚠️ 所有新持仓均无数据，全部保留现金")
        return {}, total_cash, {'sell': sell_details, 'buy': []}
    
    cash_per_stock = total_cash / len(valid_new_symbols)
    new_positions = {}
    buy_details = []
    total_buy_cost = 0
    
    print(f"\n📈 步骤2: 买入新持仓 (共{len(valid_new_symbols)}只，每只约{cash_per_stock:,.2f}元)")
    for symbol in valid_new_symbols:
        open_price = get_open_price(symbol, rebalance_date, engine)
        exec_price = get_execution_price(open_price, 'buy', config['slippage'])
        
        # 计算可买股数（100股整数）
        max_shares = int(cash_per_stock / exec_price / 100) * 100
        
        if max_shares == 0:
            print(f"   ⚠️ {symbol} 资金不足，无法买入 (每股{exec_price:.2f}, 可用{cash_per_stock:.2f})")
            continue
        
        # 买入成本
        gross_cost = max_shares * exec_price
        commission = max(gross_cost * config['commission_buy'], config['min_commission'])
        total_cost = gross_cost + commission
        
        # 如果超出预算，减少股数
        while total_cost > cash_per_stock and max_shares >= 100:
            max_shares -= 100
            gross_cost = max_shares * exec_price
            commission = max(gross_cost * config['commission_buy'], config['min_commission'])
            total_cost = gross_cost + commission
        
        if max_shares > 0:
            new_positions[symbol] = max_shares
            total_buy_cost += total_cost
            buy_details.append({
                'symbol': symbol, 'shares': max_shares, 'open_price': open_price,
                'exec_price': exec_price, 'gross': gross_cost,
                'commission': commission, 'total_cost': total_cost
            })
    
    # 3️⃣ 剩余现金
    cash_left = total_cash - total_buy_cost
    print(f"   💵 买入总成本: {total_buy_cost:,.2f}")
    print(f"   💵 剩余现金: {cash_left:,.2f}")
    
    return new_positions, cash_left, {'sell': sell_details, 'buy': buy_details}

def backfill_history():
    engine = get_db_engine()
    start_date = pd.to_datetime(START_DATE_STR).date()
    end_date = pd.to_datetime(END_DATE_STR).date()
    
    # 1. 💰 资产继承逻辑：获取上一个交易日的总资产
    print(f"1. 🔍 正在查询上期资产余额...")
    last_day = start_date - timedelta(days=1)
    inherited_balance = INITIAL_BUDGET
    
    balance_sql = text(f"SELECT total_account_value FROM {DST_TABLE} WHERE date < :start_date ORDER BY date DESC LIMIT 1")
    
    with engine.connect() as conn:
        res = conn.execute(balance_sql, {"start_date": start_date}).fetchone()
        if res:
            inherited_balance = float(res[0])
            print(f"   ✅ 继承成功！上期末总资产: {inherited_balance:,.2f}")
        else:
            print(f"   ℹ️ 未找到历史记录，将使用初始预算: {INITIAL_BUDGET:,.2f}")

    # 2. 📂 读取当前持仓
    print(f"\n2. 📖 读取 {SRC_TABLE} 中的持仓配置...")
    pos_sql = f"SELECT symbol, volume FROM {SRC_TABLE}"
    df_pos = pd.read_sql(pos_sql, engine)
    
    if df_pos.empty:
        print("❌ 持仓表为空，请检查数据。")
        return

    # 格式化代码并建立映射
    df_pos['symbol'] = df_pos['symbol'].astype(str).str.zfill(6)
    # 去重（如果有重复symbol，取平均或求和）
    df_pos = df_pos.groupby('symbol')['volume'].sum().reset_index()
    volume_map = dict(zip(df_pos['symbol'], df_pos['volume']))
    symbols_str = "'" + "','".join(volume_map.keys()) + "'"

    print(f"   读取到 {len(volume_map)} 只持仓股票")

    # 3. 📈 获取行情
    print(f"\n3. 📊 获取 {start_date} 至 {end_date} 的行情数据...")
    price_sql = f"""
    SELECT trade_date, symbol, open, close 
    FROM stock_history 
    WHERE symbol IN ({symbols_str}) 
      AND trade_date >= '{start_date}' AND trade_date <= '{end_date}'
    ORDER BY trade_date, symbol
    """
    df_prices = pd.read_sql(price_sql, engine)
    df_prices['symbol'] = df_prices['symbol'].astype(str).str.zfill(6)
    
    if df_prices.empty:
        print("❌ 未获取到行情数据")
        return
    
    # 透视表并处理停牌/缺失值
    df_pivot = df_prices.pivot(index='trade_date', columns='symbol', values='close').ffill()
    df_open = df_prices.pivot(index='trade_date', columns='symbol', values='open')

    print(f"   获取到 {len(df_pivot)} 个交易日的数据")

    # 4. 🧮 计算每日资产变化
    print(f"\n4. 🧮 计算每日资产变化...")
    
    # 找到第一个交易日，计算买入成本
    first_date = df_pivot.index[0]
    first_day_prices = df_pivot.iloc[0]
    first_day_opens = df_open.iloc[0] if first_date in df_open.index else first_day_prices
    
    # 使用开盘价（含滑点）计算初始买入成本，考虑资金限制
    # 等权分配资金
    cash_per_stock = inherited_balance / len(volume_map)
    initial_market_value = 0
    actual_positions = {}  # 实际能买到的持仓
    
    for symbol in volume_map:
        if symbol in first_day_opens.index and pd.notna(first_day_opens[symbol]):
            exec_price = get_execution_price(first_day_opens[symbol], 'buy')
            # 计算可买股数（100股整数）
            max_shares = int(cash_per_stock / exec_price / 100) * 100
            if max_shares > 0:
                actual_positions[symbol] = max_shares
                initial_market_value += exec_price * max_shares
    
    remaining_cash = inherited_balance - initial_market_value
    print(f"   目标持仓: {len(volume_map)}只, 实际买入: {len(actual_positions)}只")
    print(f"   初始买入市值: {initial_market_value:,.2f}, 剩余现金: {remaining_cash:,.2f}")
    
    # 使用实际持仓计算每日市值
    total_market_values = pd.Series(0.0, index=df_pivot.index)
    for symbol in df_pivot.columns:
        if symbol in actual_positions:
            total_market_values += df_pivot[symbol] * actual_positions[symbol]
    
    # 构建结果表
    df_result = pd.DataFrame({
        'date': total_market_values.index,
        'market_value': total_market_values.values,
        'cash': remaining_cash,
        'total_account_value': total_market_values.values + remaining_cash,
        'note': f"Inherited from prev period"
    })

    # 计算收益率 (衔接上期末)
    df_result['daily_return'] = df_result['total_account_value'].pct_change()
    df_result.loc[df_result.index[0], 'daily_return'] = (df_result.iloc[0]['total_account_value'] / inherited_balance) - 1

    # 5. 💾 写入数据库
    print(f"\n5. 💾 写入 {DST_TABLE}...")
    delete_sql = text(f"DELETE FROM {DST_TABLE} WHERE date >= :sd AND date <= :ed")
    
    with engine.connect() as conn:
        conn.execute(delete_sql, {"sd": start_date, "ed": end_date})
        conn.commit()
        df_result.to_sql(DST_TABLE, con=conn, if_exists='append', index=False)
        conn.commit()

    print(f"\n✅ 处理完成！起始总资产: {inherited_balance:,.2f} -> 期末总资产: {df_result.iloc[-1]['total_account_value']:,.2f}")
    print(f"   区间收益率: {(df_result.iloc[-1]['total_account_value'] / inherited_balance - 1) * 100:.2f}%")
    
    # 打印每日收益率摘要
    print(f"\n📊 每日收益率摘要:")
    for idx, row in df_result.head(10).iterrows():
        print(f"   {row['date']}: {row['daily_return']*100:+.2f}% (总资产: {row['total_account_value']:,.2f})")

if __name__ == '__main__':
    backfill_history()
