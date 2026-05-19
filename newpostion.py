import pandas as pd
import numpy as np
import os
from sqlalchemy import create_engine, text
import datetime

# --- 配置 ---
from dotenv import load_dotenv
load_dotenv('.env')
DSN = os.getenv('DB_DSN1')
HOLDINGS_PLAN_FILE = "my_holdings.csv" 
TBL_POSITIONS = "strategy_positions"    
TBL_PERFORMANCE = "strategy_performance" 

INITIAL_CAPITAL = 1000000.0
FRICTION_RATE = 0.003  # 调仓摩擦成本

def get_db_engine():
    return create_engine(DSN)

def run_strategy():
    engine = get_db_engine()
    
    # 1. 获取当前最新的账户总价值 (total_value)
    # 如果 performance 表为空，则使用初始资金
    val_sql = f"SELECT total_value FROM {TBL_PERFORMANCE} ORDER BY date DESC LIMIT 1"
    try:
        res = pd.read_sql(val_sql, engine)
        if not res.empty:
            current_total_value = float(res.iloc[0, 0])
        else:
            current_total_value = INITIAL_CAPITAL
    except Exception:
        current_total_value = INITIAL_CAPITAL

    print(f"📈 当前账户基准价值: {current_total_value:,.2f}")

    # 2. 读取调仓计划 (my_holdings.csv)
    if not os.path.exists(HOLDINGS_PLAN_FILE):
        print(f"❌ 错误: 未找到计划文件 {HOLDINGS_PLAN_FILE}")
        return

    df_plan = pd.read_csv(HOLDINGS_PLAN_FILE)
    if df_plan.empty:
        print("⚠️ 调仓计划为空，跳过执行")
        return

    # 预处理数据
    df_plan['symbol'] = df_plan['symbol'].astype(str).str.zfill(6)
    num_stocks = len(df_plan)
    
    # 3. 计算可用资金并分配 (扣除摩擦成本)
    available_capital = current_total_value * (1 - FRICTION_RATE)
    per_stock_budget = available_capital / num_stocks

    print(f"📝 正在为 {num_stocks} 只个股分配资金 (每只预算: {per_stock_budget:,.2f})")

    # 4. 转换持仓数据
    # 直接使用 ref_close_price 或计划中的价格计算 volume
    new_positions = []
    exec_date = datetime.date.today() # 默认执行日期为今天，也可从 csv 读取 buy_date

    for _, row in df_plan.iterrows():
        price = float(row['ref_close_price'])
        # 计算股数：预算 / 价格，向下取整到 100 股（手）
        volume = int((per_stock_budget / price) // 100 * 100)
        
        if volume > 0:
            new_positions.append({
                'symbol': row['symbol'],
                'name': row.get('name', 'unknown'),
                'volume': volume,
                'cost_price': price,
                'entry_date': row.get('buy_date', exec_date),
                'status': 'active'
            })

    # 5. 数据库原子操作：清空旧仓，插入新仓
    if new_positions:
        with engine.begin() as conn: # 使用 begin 确保事务
            # 将旧持仓设为 closed
            conn.execute(text(f"UPDATE {TBL_POSITIONS} SET status = 'closed' WHERE status = 'active'"))
            
            # 插入新持仓
            insert_sql = text(f"""
                INSERT INTO {TBL_POSITIONS} (symbol, name, volume, cost_price, entry_date, status)
                VALUES (:symbol, :name, :volume, :cost_price, :entry_date, :status)
            """)
            conn.execute(insert_sql, new_positions)
            
        print(f"✅ 成功插入 {len(new_positions)} 条新持仓记录到 {TBL_POSITIONS}")
    else:
        print("⚠️ 资金不足以买入计划中的任何股票")

if __name__ == '__main__':
    run_strategy()