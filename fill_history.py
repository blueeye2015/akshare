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
START_DATE_STR = '2026-03-08'
END_DATE_STR = '2026-05-08'

def get_db_engine():
    return create_engine(DSN)

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
    print(f"2. 📖 读取 {SRC_TABLE} 中的持仓配置...")
    pos_sql = f"SELECT symbol, volume FROM {SRC_TABLE} WHERE status = 'active' "
    df_pos = pd.read_sql(pos_sql, engine)
    
    if df_pos.empty:
        print("❌ 持仓表为空，请检查数据。")
        return

    # 格式化代码并建立映射
    df_pos['symbol'] = df_pos['symbol'].astype(str).str.zfill(6)
    volume_map = dict(zip(df_pos['symbol'], df_pos['volume']))
    symbols_str = "'" + "','".join(volume_map.keys()) + "'"

    # 3. 📈 获取行情
    print(f"3. 📊 获取 {start_date} 至 {end_date} 的行情数据...")
    price_sql = f"""
    SELECT trade_date, symbol, close 
    FROM stock_history 
    WHERE symbol IN ({symbols_str}) 
      AND trade_date >= '{start_date}' AND trade_date <= '{end_date}'
    ORDER BY trade_date, symbol
    """
    df_prices = pd.read_sql(price_sql, engine)
    df_prices['symbol'] = df_prices['symbol'].astype(str).str.zfill(6)
    
    # 透视表并处理停牌/缺失值
    df_pivot = df_prices.pivot(index='trade_date', columns='symbol', values='close').ffill()

    # 4. 🧮 计算核心价值
    print(f"4. 🧮 计算每日资产变化...")
    
    # 计算第一天的买入成本，从而得出剩余现金
    first_day_prices = df_pivot.iloc[0]
    initial_market_value = sum(first_day_prices[s] * volume_map[s] for s in df_pivot.columns if s in volume_map)
    remaining_cash = inherited_balance - initial_market_value
    
    # 计算每日持仓市值序列
    total_market_values = pd.Series(0.0, index=df_pivot.index)
    for symbol in df_pivot.columns:
        if symbol in volume_map:
            total_market_values += df_pivot[symbol] * volume_map[symbol]
    
    # 构建结果表
    df_result = pd.DataFrame({
        'date': total_market_values.index,
        'market_value': total_market_values.values,
        'cash': remaining_cash,
        'total_account_value': total_market_values.values + remaining_cash,
        'note': f"Inherited from prev period"
    })

    # 计算收益率 (衔接上期末)
    # 第一天的收益率 = (当天总资产 / 上期末总资产) - 1
    df_result['daily_return'] = df_result['total_account_value'].pct_change()
    df_result.loc[df_result.index[0], 'daily_return'] = (df_result.iloc[0]['total_account_value'] / inherited_balance) - 1

    # 5. 💾 写入数据库
    print(f"5. 💾 写入 {DST_TABLE}...")
    delete_sql = text(f"DELETE FROM {DST_TABLE} WHERE date >= :sd AND date <= :ed")
    
    with engine.connect() as conn:
        conn.execute(delete_sql, {"sd": start_date, "ed": end_date})
        conn.commit()
        df_result.to_sql(DST_TABLE, con=conn, if_exists='append', index=False)
        conn.commit()

    print(f"✅ 处理完成！起始总资产: {inherited_balance:,.2f} -> 期末总资产: {df_result.iloc[-1]['total_account_value']:,.2f}")

if __name__ == '__main__':
    backfill_history()