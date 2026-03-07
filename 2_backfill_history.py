import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os

# --- 配置 ---
load_dotenv('.env')
DSN = os.getenv('DB_DSN1')

# 表名配置
SRC_TABLE = "my_holding"             # 你的1月持仓表
DST_TABLE = "strategy_performance"   # 目标收益表

# --- 在配置部分增加初始总投入 ---
TOTAL_BUDGET = 1000000.0  # 假设你最初投入了 100 万

# 截止日期 (下次调仓日)
END_DATE_STR = '2026-02-05'

def get_db_engine():
    return create_engine(DSN)

def backfill_history():
    engine = get_db_engine()
    
    print("1. 📂 读取历史持仓数据...")
    # 读取 my_holding 表，获取 symbol, volume, buy_date
    # 假设 my_holding 表结构与之前的持仓表类似
    pos_sql = f"SELECT symbol, volume, buy_date FROM {SRC_TABLE}"
    try:
        df_pos = pd.read_sql(pos_sql, engine)
    except Exception as e:
        print(f"❌ 读取 {SRC_TABLE} 失败: {e}")
        print("   请确认该表存在，且包含 symbol, volume, buy_date 列")
        return

    if df_pos.empty:
        print("❌ {SRC_TABLE} 表中没有数据")
        return

    # 获取开始日期 (取表里记录的 buy_date)
    # 假设这批数据都是同一天买入的，取第一条即可
    start_date = pd.to_datetime(df_pos['buy_date'].iloc[0]).date()
    end_date = pd.to_datetime(END_DATE_STR).date()
    
    print(f"   📅 回测区间: {start_date} 至 {end_date}")
    print(f"   📊 持仓股票数量: {len(df_pos)}")

    # 准备股票代码列表 (处理字符串格式)
    symbols = df_pos['symbol'].astype(str).str.zfill(6).tolist()
    symbols_str = "'" + "','".join(symbols) + "'"
    
    # 建立股票 -> 持仓数量的映射，方便后续计算
    volume_map = dict(zip(df_pos['symbol'].astype(str).str.zfill(6), df_pos['volume']))

    print("\n2. 📈 获取区间内的行情数据...")
    # 查询该时间段内所有股票的收盘价
    price_sql = f"""
    SELECT trade_date, symbol, close
    FROM stock_history
    WHERE symbol IN ({symbols_str})
      AND trade_date >= '{start_date}'
      AND trade_date <= '{end_date}'
    ORDER BY trade_date, symbol
    """
    
    try:
        df_prices = pd.read_sql(price_sql, engine)
    except Exception as e:
        print(f"❌ 获取行情数据失败: {e}")
        return

    if df_prices.empty:
        print("❌ 该时间段内没有行情数据，无法计算收益。")
        return

    print(f"   ✅ 获取到 {len(df_prices)} 条行情记录")

    print("\n3. 🧮 计算每日持仓总市值...")
    # 确保类型一致
    df_prices['symbol'] = df_prices['symbol'].astype(str).str.zfill(6)
    
    # 透视表：行是日期，列是股票，值是收盘价
    df_pivot = df_prices.pivot(index='trade_date', columns='symbol', values='close')

    # 🔥🔥🔥 核心修复：在这一步，先把所有股票缺失的价格填补上
    # 解释：如果某只股票某天没读到数据（可能是停牌，也可能是代码格式不对没查到），
    # 我们就假设它价格不变，沿用前一天的价格。
    # 这样可以防止 "NaN + 数字 = NaN" 导致整个组合市值失效。
    df_pivot = df_pivot.ffill()
       
    # 1. 计算初始买入时的总成本 (假设 buy_date 当天的数据就是初始状态)
    # 如果你的 my_holding 表里有买入价格，可以直接算。
    # 如果没有，我们用行情表里 start_date 那天的收盘价作为基准
    first_day_prices = df_pivot.iloc[0]
    initial_market_value = 0

    for symbol, vol in volume_map.items():
        if symbol in first_day_prices:
            initial_market_value += first_day_prices[symbol] * vol
    
    # 计算剩余现金 (假设回测期间现金不再变动)
    remaining_cash = TOTAL_BUDGET - initial_market_value
    if remaining_cash < 0:
        print(f"⚠️ 警告：初始持仓市值 ({initial_market_value}) 超过了总预算 ({TOTAL_BUDGET})")

    # 2. 计算每日持仓市值
    total_market_values = pd.Series(0.0, index=df_pivot.index)
    for symbol in df_pivot.columns:
        if symbol in volume_map:
            total_market_values += df_pivot[symbol] * volume_map[symbol]
    
    total_market_values = total_market_values.ffill().fillna(0)

    # 3. 构建结果表
    df_result = pd.DataFrame({
        'date': total_market_values.index,
        'market_value': total_market_values.values,        # 原来的 total_value 改名为持仓市值
        'cash': remaining_cash,                            # 剩余现金
        'total_account_value': total_market_values.values + remaining_cash # 总金额
    })
    
    # 4. 基于“总金额”计算收益率，这样更真实
    df_result['daily_return'] = df_result['total_account_value'].pct_change().fillna(0)

    # 计算日收益率
    df_result['daily_return'] = df_result['market_value'].pct_change()
    # 第一天的收益率设为 0
    df_result['daily_return'] = df_result['daily_return'].fillna(0)
    
    # 添加备注
    df_result['note'] = f"Backfill from {SRC_TABLE}"

    print("\n4. 💾 写入数据库...")
    
    # 策略：先删除该时间段的旧数据，再插入新数据 (避免重复)
    delete_sql = f"""
    DELETE FROM {DST_TABLE} 
    WHERE date >= '{start_date}' AND date <= '{end_date}'
    """
    
    with engine.connect() as conn:
        # 删除旧数据
        conn.execute(text(delete_sql))
        conn.commit()
        print(f"   🧹 已清理 {start_date} 至 {end_date} 的旧收益记录")
        
        # 插入新数据
        # to_sql 比 execute many 更快
        df_result.to_sql(
            DST_TABLE, 
            con=conn, 
            if_exists='append', 
            index=False
        )
        conn.commit()
        
    print("\n" + "="*50)
    print(f"✅ 历史收益回填完成！")
    print(f"   写入记录数: {len(df_result)} 条")
    print(f"   起始市值: {df_result.iloc[0]['market_value']:,.2f}")
    print(f"   结束市值: {df_result.iloc[-1]['market_value']:,.2f}")
    print(f"   期间收益: {(df_result.iloc[-1]['market_value'] / df_result.iloc[0]['market_value'] - 1):.2%}")
    print("="*50)

if __name__ == '__main__':
    backfill_history()