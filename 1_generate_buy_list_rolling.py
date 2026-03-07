import pandas as pd
import numpy as np
import os
import glob
import shutil
from sqlalchemy import create_engine, text
import datetime

# --- 配置部分 ---
from dotenv import load_dotenv
load_dotenv('.env')
DSN = os.getenv('DB_DSN1')
FACTOR_DIR = "./factor_cache_global"
HOLDINGS_FILE = "my_holdings.csv"
HISTORY_DIR = "./history_holdings"
TOP_N_PCT = 0.03   

# 🔥 新增配置：每日收益记录表名
PERFORMANCE_TABLE = "my_holdings_performance" 

DEFAULT_START_CAPITAL = 1000000.0 
FRICTION_RATE = 0.003 

os.makedirs(HISTORY_DIR, exist_ok=True)

def get_db_engine():
    return create_engine(DSN)

def ensure_performance_table(engine):
    """
    确保收益记录表存在，不存在则创建
    """
    create_sql = f"""
    CREATE TABLE IF NOT EXISTS {PERFORMANCE_TABLE} (
        date DATE PRIMARY KEY,
        total_value DECIMAL(20, 2),
        daily_return DECIMAL(10, 4),
        note VARCHAR(255),
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
    with engine.connect() as conn:
        conn.execute(text(create_sql))
        conn.commit()
    print(f"✅ 已检查/创建收益记录表: {PERFORMANCE_TABLE}")

def log_daily_performance(engine, trade_date, total_value, note=""):
    """
    记录当日收益到数据库 (Upsert: 存在则更新，不存在则插入)
    """
    # 1. 获取上一交易日的市值，计算日收益率
    prev_value_sql = f"SELECT total_value FROM {PERFORMANCE_TABLE} WHERE date < :date ORDER BY date DESC LIMIT 1"
    
    daily_return = 0.0
    with engine.connect() as conn:
        # 使用 text() 包装 SQL 语句
        result = conn.execute(text(prev_value_sql), {"date": trade_date}).fetchone()
        if result and result[0]:
            prev_val = result[0]
            if prev_val > 0:
                daily_return = (total_value - prev_val) / prev_val
        
        # 2. 插入或更新数据 (PostgreSQL 语法)
        # 注意：如果你的数据库是 MySQL，语法略有不同 (ON DUPLICATE KEY UPDATE)
        upsert_sql = f"""
        INSERT INTO {PERFORMANCE_TABLE} (date, total_value, daily_return, note)
        VALUES (:date, :total_value, :daily_return, :note)
        ON CONFLICT (date) 
        DO UPDATE SET 
            total_value = EXCLUDED.total_value,
            daily_return = EXCLUDED.daily_return,
            note = EXCLUDED.note,
            updated_at = CURRENT_TIMESTAMP
        """
        
        conn.execute(text(upsert_sql), {
            "date": trade_date,
            "total_value": total_value,
            "daily_return": daily_return,
            "note": note
        })
        conn.commit()
        
    print(f"📊 已记录收益数据 | 日期: {trade_date} | 总市值: {total_value:,.2f} | 日收益: {daily_return:.4%}")

def get_current_asset_value():
    """
    计算当前持仓的清算价值 (滚动资金)
    返回: (总市值, 最新的交易日期, 是否存在旧持仓)
    """
    if not os.path.exists(HOLDINGS_FILE):
        print("ℹ️ 未找到旧持仓文件，将使用默认初始资金启动。")
        return DEFAULT_START_CAPITAL, None, False

    print("🔄 正在计算旧持仓的清算价值...")
    df_old = pd.read_csv(HOLDINGS_FILE)
    df_old['symbol'] = df_old['symbol'].astype(str).str.zfill(6)
    
    symbol_list = df_old['symbol'].tolist()
    if not symbol_list: return DEFAULT_START_CAPITAL, None, False
        
    engine = get_db_engine()
    symbols_str = "'" + "','".join(symbol_list) + "'"
    
    # 获取最新收盘价及其对应的日期
    # 注意：这里取的是每只股票的最新日期，可能不同。
    # 简单起见，我们取这些日期中最近的一个作为整个组合的“估值日”
    sql = f"""
    SELECT DISTINCT ON (symbol) symbol, close, trade_date
    FROM stock_history WHERE symbol IN ({symbols_str})
    ORDER BY symbol, trade_date DESC
    """
    try:
        df_price = pd.read_sql(sql, engine)
        price_map = df_price.set_index('symbol')['close'].to_dict()
        # 获取所有股票的最新交易日期，取最大值作为组合估值日
        latest_trade_date = df_price['trade_date'].max()
    except Exception as e:
        print(f"❌ 无法获取旧持仓行情: {e}")
        return DEFAULT_START_CAPITAL, None, False

    total_value = 0
    for _, row in df_old.iterrows():
        sym = row['symbol']
        vol = row['volume']
        # 兜底逻辑：如果停牌取不到价，用成本价
        curr_price = price_map.get(sym, row['cost_price']) 
        total_value += curr_price * vol
        
    print(f"   旧持仓总市值: {total_value:,.2f} (估值日: {latest_trade_date})")
    return total_value, latest_trade_date, True

def load_latest_factor(factor_dir):
    """自动寻找目录下最新的因子文件"""
    files = sorted(glob.glob(os.path.join(factor_dir, "factor_*.parquet")))
    if not files: 
        raise FileNotFoundError(f"❌ 目录 {factor_dir} 下没有找到任何 factor_*.parquet 文件")
    latest_file = files[-1]
    print(f"📂 自动锁定最新因子文件: {os.path.basename(latest_file)}")
    return pd.read_parquet(latest_file)

def get_next_open_batch(current_date_str, symbol_list):
    """获取 T+1 开盘价与成交量"""
    if not symbol_list: return None
    engine = get_db_engine()
    symbols_str = "'" + "','".join(symbol_list) + "'"
    sql = f"""
    SELECT DISTINCT ON (symbol) 
        symbol, 
        open as next_open, 
        trade_date as next_date,
        volume
    FROM stock_history
    WHERE trade_date > '{current_date_str}' 
      AND symbol IN ({symbols_str})
    ORDER BY symbol, trade_date ASC
    """
    try:
        return pd.read_sql(sql, engine)
    except Exception as e:
        print(f"SQL Error: {e}")
        return pd.DataFrame()

def generate_buy_list():
    engine = get_db_engine()
    
    # 0. 初始化收益记录表
    ensure_performance_table(engine)

    # 1. 计算当前资产价值 (这是在调仓前的市值)
    current_asset, last_val_date, is_rollover = get_current_asset_value()
    
    # 🔥🔥 核心修改：记录当前持仓的今日收益 (在调仓前记录)
    if last_val_date:
        log_daily_performance(engine, last_val_date, current_asset, note="调仓前市值")

    if is_rollover:
        available_capital = current_asset * (1 - FRICTION_RATE)
        # 归档旧文件
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M")
        shutil.copy(HOLDINGS_FILE, os.path.join(HISTORY_DIR, f"holdings_{timestamp}.csv"))
        print(f"   扣除预估磨损 ({FRICTION_RATE:.1%}) -> 本期可用: {available_capital:,.2f}")
    else:
        available_capital = current_asset
        print(f"   💰 初始启动资金: {available_capital:,.2f}")

    # 2. 自动加载最新因子
    print(f"\n1. 读取因子数据...")
    df_factor = load_latest_factor(FACTOR_DIR)
    
    # 获取因子日期
    factor_date = df_factor['trade_date'].max()
    factor_date_str = pd.to_datetime(factor_date).strftime('%Y-%m-%d')
    print(f"   ⏱️ 因子数据截止日期 (T日): {factor_date_str}")
    
    # 检查数据过期
    days_lag = (datetime.datetime.now() - pd.to_datetime(factor_date)).days
    if days_lag > 10:
        print(f"   ⚠️⚠️⚠️ 警告: 因子数据滞后 {days_lag} 天")

    df_current = df_factor[df_factor['trade_date'] == factor_date].copy()
    
    # 加载基础信息
    df_basic = pd.read_sql("SELECT symbol, list_date, name FROM stock_basic", engine)
    df_basic['list_date'] = pd.to_datetime(df_basic['list_date'])
    df_merge = pd.merge(df_current, df_basic, on='symbol', how='left')
    
    # 初步筛选
    candidates = []
    curr_time = pd.Timestamp.now()
    for _, row in df_merge.iterrows():
        if pd.isna(row['list_date']) or (curr_time - row['list_date']).days < 60: continue
        if pd.isna(row['factor']) or pd.isna(row['close']): continue
        candidates.append({
            'symbol': row['symbol'], 'name': row['name'], 
            'factor': row['factor'], 'pre_close': row['close']
        })
    df_candidates = pd.DataFrame(candidates)
    
    # 3. 获取 T+1 (真实交易日) 行情
    print(f"2. 寻找 {factor_date_str} 之后的首个交易日...")
    df_next = get_next_open_batch(factor_date_str, df_candidates['symbol'].tolist())
    
    if df_next.empty:
        print("❌ 错误：数据库里没有找到 T+1 日的数据！")
        return

    df_final = pd.merge(df_candidates, df_next, on='symbol', how='inner')
    market_trade_date = df_final['next_date'].mode()[0]
    trade_date_str = pd.to_datetime(market_trade_date).strftime('%Y-%m-%d')
    print(f"   📅 锁定目标交易日 (T+1): {trade_date_str}")
    
    # 4. 过滤 (停牌/涨停)
    valid_list = []
    suspend_count = 0
    limit_count = 0
    
    for _, row in df_final.iterrows():
        sym = row['symbol']
        next_date = row['next_date']
        vol = row['volume']
        
        if next_date != market_trade_date or vol == 0:
            suspend_count += 1
            continue
            
        limit_ratio = 0.10
        if 'ST' in str(row['name']): limit_ratio = 0.05
        elif sym.startswith(('688', '300')): limit_ratio = 0.20
        elif sym.startswith(('8', '4')): limit_ratio = 0.30
        
        pct_chg = (row['next_open'] - row['pre_close']) / row['pre_close']
        if pct_chg > (limit_ratio - 0.005):
            limit_count += 1
            continue
            
        valid_list.append({
            'symbol': sym, 'name': row['name'],
            'factor': row['factor'], 'cost_price': row['next_open'],
            'buy_date': next_date
        })

    print(f"   🚫 停牌剔除: {suspend_count} | 一字涨停剔除: {limit_count}")
    
    # 5. 生成清单
    df_valid = pd.DataFrame(valid_list)
    df_valid = df_valid.sort_values(by='factor', ascending=False)
    
    top_n = int(len(df_valid) * TOP_N_PCT)
    top_n = max(10, min(top_n, len(df_valid)))
    
    df_buy = df_valid.head(top_n).copy()
    
    # 资金分配
    df_buy['target_weight'] = 1.0 / len(df_buy)
    df_buy['volume'] = (available_capital * df_buy['target_weight'] / df_buy['cost_price']) // 100 * 100
    df_buy = df_buy[df_buy['volume'] > 0].copy()

    # 输出 CSV
    output_cols = ['symbol', 'name', 'cost_price', 'volume', 'buy_date', 'factor', 'target_weight']
    df_buy[output_cols].to_csv(HOLDINGS_FILE, index=False, encoding='utf-8-sig')
    
    # 🔥🔥 可选：同时记录新组合在 T+1 日的预计开盘市值
    # 这一步是为了让资金曲线连续，但注意此时用的是 Open 价格，不是 Close
    # 如果你希望用收盘价记录，建议每天专门运行一个只记录不含调仓的脚本
    # new_portfolio_value = (df_buy['cost_price'] * df_buy['volume']).sum()
    # log_daily_performance(engine, market_trade_date, new_portfolio_value, note="新组合开盘市值")
    
    print("\n" + "="*50)
    print(f"✅ 购买清单已更新: {HOLDINGS_FILE}")
    print(f"   实际执行日期: {trade_date_str}")
    print(f"   买入股票数: {len(df_buy)}")
    print("="*50)

if __name__ == '__main__':
    generate_buy_list()