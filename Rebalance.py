import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os
from datetime import timedelta

# --- 配置扩展 ---
load_dotenv('.env')
DSN = os.getenv('DB_DSN1')

SRC_TABLE = "strategy_positions"      
DST_TABLE = "strategy_performance"   
INITIAL_BUDGET = 1000000.0           

# 交易成本设定 (模拟 A 股)
COMMISSION_RATE = 0.0003  # 万三
TAX_RATE = 0.001          # 千一 (仅卖出)

START_DATE_STR = '2026-03-08'
END_DATE_STR = '2026-03-12'

def get_db_engine():
    return create_engine(DSN)

def backfill_history_v2():
    engine = get_db_engine()
    start_date = pd.to_datetime(START_DATE_STR).date()
    end_date = pd.to_datetime(END_DATE_STR).date()
    
    # 1. 获取上期末结余与旧持仓
    # 假设我们从数据库获取 T-1 日的持仓明细，如果没有，则视为全现金进入
    print(f"1. 🔍 正在初始化调仓日数据...")
    inherited_balance = INITIAL_BUDGET
    with engine.connect() as conn:
        res = conn.execute(text(f"SELECT total_account_value FROM {DST_TABLE} WHERE date < :sd ORDER BY date DESC LIMIT 1"), {"sd": start_date}).fetchone()
        if res: inherited_balance = float(res[0])

   
    # 2. 读取“目标”新持仓 (增加 status 过滤)
    print(f"2. 📖 读取 {SRC_TABLE} 中的有效持仓...")
    # 重点在这里：只取当前活跃的名单
    pos_sql = text(f"SELECT symbol, volume FROM {SRC_TABLE} WHERE status = 'active'")

    with engine.connect() as conn:
        df_target_pos = pd.read_sql(pos_sql, conn)

    if df_target_pos.empty:
        print("❌ 未找到状态为 'active' 的持仓，请检查数据库状态字段。")
        return

    df_target_pos['symbol'] = df_target_pos['symbol'].astype(str).str.zfill(6)
    target_symbols = df_target_pos['symbol'].tolist()

    # 3. 获取行情 (包含 Open 以模拟调仓，Close 以计算市值)
    price_sql = f"""
    SELECT trade_date, symbol, open, close 
    FROM stock_history 
    WHERE symbol IN ({str(target_symbols)[1:-1]}) 
      AND trade_date >= '{start_date}' AND trade_date <= '{end_date}'
    ORDER BY trade_date, symbol
    """
    df_raw = pd.read_sql(price_sql, engine)
    df_raw['symbol'] = df_raw['symbol'].astype(str).str.zfill(6)

    # 透视行情数据
    df_open = df_raw.pivot(index='trade_date', columns='symbol', values='open').ffill()
    df_close = df_raw.pivot(index='trade_date', columns='symbol', values='close').ffill()

    # 4. 模拟调仓瞬间 (T=0)
    print(f"4. ⚖️ 模拟调仓日摩擦成本...")
    rebalance_date = df_open.index[0]
    
    # 计算初始买入成本 (以开盘价计算)
    # 逻辑：假设上期末资产全部转为现金，今日开盘买入新仓位
    buy_prices = df_open.loc[rebalance_date]
    volume_map = dict(zip(df_target_pos['symbol'], df_target_pos['volume']))
    
    execution_cost = 0.0
    total_spent = 0.0
    for sym, vol in volume_map.items():
        if sym in buy_prices:
            px = buy_prices[sym]
            cost = px * vol
            total_spent += cost
            # 买入手续费
            execution_cost += cost * COMMISSION_RATE

    # 调仓后的初始现金 = 继承资产 - 买入花费 - 交易成本
    remaining_cash = inherited_balance - total_spent - execution_cost
    
    if remaining_cash < 0:
        print(f"⚠️ 警告：预算不足以覆盖新仓位！缺口: {abs(remaining_cash):,.2f}")

    # 5. 计算每日净值
    print(f"5. 📈 生成收益序列...")
    results = []
    for date in df_close.index:
        day_prices = df_close.loc[date]
        market_value = sum(day_prices[s] * volume_map[s] for s in volume_map if s in day_prices)
        total_value = market_value + remaining_cash
        
        results.append({
            'date': date,
            'market_value': market_value,
            'cash': remaining_cash,
            'total_account_value': total_value,
            'note': "Rebalanced" if date == rebalance_date else "Holding"
        })

    df_res = pd.DataFrame(results)
    df_res['daily_return'] = df_res['total_account_value'].pct_change()
    # 第一天收益率计算需对比 inherited_balance
    df_res.loc[0, 'daily_return'] = (df_res.iloc[0]['total_account_value'] / inherited_balance) - 1

    # 6. 写入
    # (此处省略 Delete 逻辑，参考原代码)
    print(f"✅ 完成！调仓日损耗(含手续费): {execution_cost:,.2f}")
    return df_res

if __name__ == '__main__':
    backfill_history_v2()