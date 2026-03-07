import pandas as pd
import numpy as np
import os
from sqlalchemy import create_engine, text
import datetime

# --- 配置 ---
from dotenv import load_dotenv
load_dotenv('.env')
DSN = os.getenv('DB_DSN1')
HOLDINGS_PLAN_FILE = "my_holdings.csv"  # 理论列表
FACTOR_DIR = "./factor_cache_global"    # 辅助判断

# 表名配置
TBL_POSITIONS = "strategy_positions"    # 实际持仓表
TBL_PERFORMANCE = "strategy_performance" # 收益记录表

# 模拟参数
INITIAL_CAPITAL = 1000000.0
FRICTION_RATE = 0.003  # 调仓时的摩擦成本

def get_db_engine():
    return create_engine(DSN)

def init_tables(engine):
    """初始化数据库表结构"""
    pos_sql = f"""
    CREATE TABLE IF NOT EXISTS {TBL_POSITIONS} (
        id SERIAL PRIMARY KEY,
        symbol VARCHAR(10),
        name VARCHAR(50),
        volume INT,
        cost_price DECIMAL(10, 2),
        entry_date DATE,
        status VARCHAR(20) DEFAULT 'active',
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
    perf_sql = f"""
    CREATE TABLE IF NOT EXISTS {TBL_PERFORMANCE} (
        date DATE PRIMARY KEY,
        total_value DECIMAL(20, 2),
        daily_return DECIMAL(10, 4),
        note VARCHAR(255),
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
    with engine.connect() as conn:
        conn.execute(text(pos_sql))
        conn.execute(text(perf_sql))
        conn.commit()
    print("✅ 数据库表检查完成")

def get_current_market_value(engine, symbol_list):
    """获取一批股票的最新市值"""
    if not symbol_list: return 0.0, datetime.date.today()
    
    symbols_str = "'" + "','".join(symbol_list) + "'"
    sql = f"""
    SELECT DISTINCT ON (symbol) symbol, close, trade_date
    FROM stock_history 
    WHERE symbol IN ({symbols_str})
    ORDER BY symbol, trade_date DESC
    """
    try:
        df = pd.read_sql(sql, engine)
    except Exception as e:
        print(f"获取行情失败: {e}")
        return 0.0, datetime.date.today()

    if df.empty: return 0.0, datetime.date.today()
    
    pos_sql = f"SELECT symbol, volume FROM {TBL_POSITIONS} WHERE status = 'active'"
    df_pos = pd.read_sql(pos_sql, engine)
    
    if df_pos.empty: return 0.0, datetime.date.today()
    
    df_merge = pd.merge(df_pos, df, on='symbol')
    total_value = (df_merge['volume'] * df_merge['close']).sum()
    latest_date = df['trade_date'].max()
    return total_value, latest_date

def log_performance(engine, trade_date, total_value, note=""):
    """
    重构后的收益记录：基于个股行情计算 daily_return，不受资金总量和摩擦成本干扰
    """
    # 1. 获取当前活跃持仓
    pos_sql = f"SELECT symbol, volume, cost_price FROM {TBL_POSITIONS} WHERE status = 'active'"
    df_pos = pd.read_sql(pos_sql, engine)
    
    daily_return = 0.0
    
    if not df_pos.empty:
        # 2. 从行情表获取这些票在 trade_date 这一天的表现
        symbols = df_pos['symbol'].tolist()
        symbols_str = "'" + "','".join(symbols) + "'"
        
        # 获取当天的开盘价和收盘价
        mkt_sql = f"""
            SELECT symbol, open, close 
            FROM stock_history 
            WHERE symbol IN ({symbols_str}) AND trade_date = :d
        """
        df_mkt = pd.read_sql(text(mkt_sql), engine, params={"d": trade_date})
        
        if not df_mkt.empty:
            # 合并数据算涨跌幅
            df_calc = pd.merge(df_pos, df_mkt, on='symbol')
            # 简化逻辑：当日收益 = (收盘 - 开盘) / 开盘 
            # (注：更严格的应该是：如果不是买入当天，则用 (今日收盘 - 昨日收盘)/昨日收盘)
            df_calc['pct'] = (df_calc['close'] - df_calc['open']) / df_calc['open']
            daily_return = df_calc['pct'].mean()  # 假设等权重
    
    # 3. 写入数据库 (保持原有的 Upsert 逻辑)
    upsert_sql = f"""
    INSERT INTO {TBL_PERFORMANCE} (date, total_value, daily_return, note)
    VALUES (:d, :v, :r, :n)
    ON CONFLICT (date) DO UPDATE SET
        total_value = EXCLUDED.total_value,
        daily_return = EXCLUDED.daily_return,
        note = EXCLUDED.note
    """
    with engine.connect() as conn:
        conn.execute(text(upsert_sql), {
            "d": trade_date, 
            "v": float(total_value),   # 这里的 total_value 仅作为账户余额展示
            "r": float(daily_return),  # 这里的 daily_return 是纯粹的选股收益率
            "n": note
        })
        conn.commit()

    print(f"📊 策略收益 | 日期: {trade_date} | 选股表现: {daily_return:.4%} | 账户余额: {total_value:,.2f}")

def simulate_execution(engine, df_plan, capital_available):
    """
    核心逻辑：根据 my_holdings.csv (理论) 生成 validlist (实际)
    """
    plan_date_str = df_plan['buy_date'].iloc[0] # T日
    symbols = df_plan['symbol'].tolist()
    
    # 格式化 symbols 列表
    symbols_str = "'" + "','".join([str(s) for s in symbols]) + "'"
    
    print(f"   🔍 开始模拟执行 (基准日: {plan_date_str})...")
    
    # 1. 查询 T+1 日行情
    print("   - 正在获取 T+1 行情 (stock_history)...")
    history_sql = f"""
    SELECT DISTINCT ON (symbol) 
        symbol, open as exec_price, trade_date as exec_date, volume
    FROM stock_history 
    WHERE symbol IN ({symbols_str})
      AND trade_date > '{plan_date_str}'
    ORDER BY symbol, trade_date ASC
    """
    try:
        df_market = pd.read_sql(history_sql, engine)
    except Exception as e:
        print(f"   ❌ 获取T+1行情失败: {e}")
        return pd.DataFrame(), None, None

    # 统一类型
    df_plan['symbol'] = df_plan['symbol'].astype(str)
    if not df_market.empty:
        df_market['symbol'] = df_market['symbol'].astype(str)
    else:
        print("   ⚠️ 警告：T+1 日没有任何行情数据！")
        return pd.DataFrame(), None, None

    # 🔥 修改点 1：使用 how='left' 保留所有计划股票，即使没有行情数据
    df_merge = pd.merge(df_plan, df_market, on='symbol', how='left')
    
    # 如果 df_merge 为空，直接返回
    if df_merge.empty:
        return pd.DataFrame(), None, None

    # 获取实际交易日期（取非空的最大日期）
    real_trade_date = df_merge['exec_date'].dropna().mode()
    if real_trade_date.empty:
        print("   ⚠️ 无法确定 T+1 交易日期，可能所有股票都停牌。")
        return pd.DataFrame(), None, None
    real_trade_date = real_trade_date[0]
    print(f"   📅 锁定执行日期: {real_trade_date}")
    
    valid_list = []
    
    # 🔥 修改点 2：新增分类统计
    count_no_data = 0      # 无行情数据
    count_suspended = 0    # 停牌
    count_limit_up = 0     # 涨停
    count_no_money = 0     # 资金不足 (后续计算)
    
    for _, row in df_merge.iterrows():
        sym = row['symbol']
        name = row['name'] if pd.notna(row['name']) else ""
        
        # 检查是否有 T+1 行情数据
        if pd.isna(row['exec_price']) or pd.isna(row['exec_date']):
            count_no_data += 1
            # 可以取消下面这行的注释，打印具体是哪些股票没数据
            # print(f"      [无数据] {sym} {name}")
            continue
            
        close_t = row['ref_close_price']
        open_t1 = row['exec_price']
        vol_mkt = row['volume']
        
        # 1. 剔除停牌 (成交量为0 或 日期不对)
        if vol_mkt == 0 or row['exec_date'] != real_trade_date:
            count_suspended += 1
            continue
            
        # 2. 剔除涨停
        limit_ratio = 0.10 
        if 'ST' in name.upper() or '*' in name: limit_ratio = 0.05 
        elif sym.startswith(('688', '300')): limit_ratio = 0.20 
        elif sym.startswith(('8', '4')): limit_ratio = 0.30
            
        limit_up_price = close_t * (1 + limit_ratio)
        
        if open_t1 >= (limit_up_price * 0.9995):
            count_limit_up += 1
            continue
            
        valid_list.append({
            'symbol': sym,
            'name': name,
            'cost_price': open_t1,
            'entry_date': real_trade_date,
            'volume': 0 
        })
        
    # 打印详细的剔除统计
    print(f"   🛑 过滤统计:")
    print(f"      - 计划总数: {len(df_plan)}")
    print(f"      - ❌ 无行情数据 (剔除): {count_no_data} 只")
    print(f"      - 🚫 停牌 (剔除): {count_suspended} 只")
    print(f"      - 🔥 涨停 (剔除): {count_limit_up} 只")
    print(f"      - ✅ 初步通过: {len(valid_list)} 只")
    
    df_valid = pd.DataFrame(valid_list)
    if df_valid.empty: return df_valid, real_trade_date, 0
    
    # 资金分配
    weight = 1.0 / len(df_valid)
    df_valid['target_amt'] = capital_available * weight
    df_valid['volume'] = (df_valid['target_amt'] / df_valid['cost_price']) // 100 * 100
    
    # 🔥 修改点 3：统计并剔除资金不足的股票
    original_count = len(df_valid)
    df_valid = df_valid[df_valid['volume'] > 0]
    final_count = len(df_valid)
    count_no_money = original_count - final_count
    
    if count_no_money > 0:
        print(f"      - 💸 资金不足买不起1手 (剔除): {count_no_money} 只")
        
    print(f"      - 🏆 最终买入: {final_count} 只")
    
    return df_valid, real_trade_date, capital_available

def get_market_value_on_date(engine, symbol_list, target_date):
    """获取指定日期的持仓市值"""
    if not symbol_list: return 0.0, target_date
    
    symbols_str = "'" + "','".join(symbol_list) + "'"
    # 修改 SQL：指定 trade_date
    sql = f"""
    SELECT symbol, close 
    FROM stock_history 
    WHERE symbol IN ({symbols_str}) AND trade_date = '{target_date}'
    """
    df = pd.read_sql(sql, engine)
    
    if df.empty: 
        return 0.0, target_date # 或者根据业务逻辑处理缺失行情
    
    pos_sql = f"SELECT symbol, volume FROM {TBL_POSITIONS} WHERE status = 'active'"
    df_pos = pd.read_sql(pos_sql, engine)
    
    df_merge = pd.merge(df_pos, df, on='symbol')
    total_value = (df_merge['volume'] * df_merge['close']).sum()
    return total_value, target_date

def run_strategy():
    engine = get_db_engine()
    init_tables(engine)
    
    # --- 1. 确定回溯的起始日期 ---
    # 查找 performance 表中最后的记录日期
    last_perf_sql = f"SELECT MAX(date) FROM {TBL_PERFORMANCE}"
    last_perf_date = pd.read_sql(last_perf_sql, engine).iloc[0, 0]
    
    # 确定起点：如果有记录就从 T+1 开始，没有就从 2023-01-01（或你自定义的起点）开始
    if last_perf_date:
        start_search_date = last_perf_date + datetime.timedelta(days=1)
    else:
        start_search_date = datetime.date(2023, 1, 1) # 默认回溯起点

    # --- 2. 获取【实际交易日】列表 ---
    # 从行情表中提取这段时间内所有存在的交易日，按升序排列
    cal_sql = f"""
        SELECT DISTINCT trade_date 
        FROM stock_history 
        WHERE trade_date >= '{start_search_date}' 
          AND trade_date <= CURRENT_DATE
        ORDER BY trade_date ASC
    """
    trade_calendar = pd.read_sql(cal_sql, engine)['trade_date'].tolist()

    if not trade_calendar:
        print(f"✨ 暂无新交易日需要处理 (起始检查点: {start_search_date})")
        return

    print(f"🔄 检测到 {len(trade_calendar)} 个待处理交易日: {trade_calendar[0]} 至 {trade_calendar[-1]}")

    # --- 3. 预载调仓计划 ---
    all_plans = pd.DataFrame()
    if os.path.exists(HOLDINGS_PLAN_FILE):
        all_plans = pd.read_csv(HOLDINGS_PLAN_FILE)
        all_plans['symbol'] = all_plans['symbol'].astype(str).str.zfill(6)
        # 将 buy_date 转为 date 类型方便对比
        all_plans['buy_date'] = pd.to_datetime(all_plans['buy_date']).dt.date

    # --- 4. 遍历交易日历进行补跑 ---
    for current_trade_date in trade_calendar:
        print(f"\n📅 正在处理交易日: {current_trade_date} ...")
        
        # A. 更新当前活跃持仓的市值
        current_pos_sql = f"SELECT symbol FROM {TBL_POSITIONS} WHERE status = 'active'"
        df_curr_pos = pd.read_sql(current_pos_sql, engine)
        
        if not df_curr_pos.empty:
            # 修改 get_current_market_value 使其支持查询特定日期的市值
            # 注意：这里需要传入 current_trade_date 确保是当天的市值
            total_val, _ = get_market_value_on_date(engine, df_curr_pos['symbol'].tolist(), current_trade_date)
            log_performance(engine, current_trade_date, total_val, note="自动补跑-每日盯市")
            latest_total_value = total_val
        else:
            # 如果没持仓，查询上一次的账户余额
            last_val_sql = f"SELECT total_value FROM {TBL_PERFORMANCE} ORDER BY date DESC LIMIT 1"
            res = pd.read_sql(last_val_sql, engine)
            latest_total_value = float(res.iloc[0,0]) if not res.empty else INITIAL_CAPITAL
            log_performance(engine, current_trade_date, latest_total_value, note="空仓观望")

        # B. 检查调仓逻辑 (如果这一天有计划)
        day_plan = all_plans[all_plans['buy_date'] == current_trade_date]
        if not day_plan.empty:
            print(f"   🧾 发现调仓计划，准备执行...")
            # 这里的 simulate_execution 内部会自动寻找 T+1 的行情
            available_capital = latest_total_value * (1 - FRICTION_RATE)
            df_valid, exec_date, _ = simulate_execution(engine, day_plan, available_capital)
            
            if not df_valid.empty:
                with engine.connect() as conn:
                    # 清空旧仓，写入新仓
                    conn.execute(text(f"UPDATE {TBL_POSITIONS} SET status = 'closed' WHERE status = 'active'"))
                    records = df_valid[['symbol', 'name', 'volume', 'cost_price', 'entry_date']].to_dict('records')
                    insert_sql = f"INSERT INTO {TBL_POSITIONS} (symbol, name, volume, cost_price, entry_date, status) VALUES (:symbol, :name, :volume, :cost_price, :entry_date, 'active')"
                    conn.execute(text(insert_sql), records)
                    conn.commit()
                print(f"   ✅ 调仓成功，实际执行日期: {exec_date}")

    print("\n✅ 历史数据补跑完成！")

if __name__ == '__main__':
    run_strategy()