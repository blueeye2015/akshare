import pandas as pd
import numpy as np
import os
import glob
import shutil
from sqlalchemy import create_engine
import datetime

# --- 配置部分 ---
from dotenv import load_dotenv
load_dotenv('.env')
DSN = os.getenv('DB_DSN1')
FACTOR_DIR = "./factor_cache_global"
HOLDINGS_FILE = "my_holdings.csv"
HISTORY_DIR = "./history_holdings"
TOP_N_PCT = 0.03   

# 🔥 初始资金 & 摩擦成本
DEFAULT_START_CAPITAL = 1000000.0 
FRICTION_RATE = 0.003 

os.makedirs(HISTORY_DIR, exist_ok=True)

def get_db_engine():
    return create_engine(DSN)

def get_current_asset_value():
    """计算当前持仓的清算价值 (滚动资金)"""
    if not os.path.exists(HOLDINGS_FILE):
        print("ℹ️ 未找到旧持仓文件，将使用默认初始资金启动。")
        return DEFAULT_START_CAPITAL, False

    print("🔄 正在计算旧持仓的清算价值...")
    df_old = pd.read_csv(HOLDINGS_FILE)
    df_old['symbol'] = df_old['symbol'].astype(str).str.zfill(6)
    
    symbol_list = df_old['symbol'].tolist()
    if not symbol_list: return DEFAULT_START_CAPITAL, False
        
    engine = get_db_engine()
    symbols_str = "'" + "','".join(symbol_list) + "'"
    
    # 获取最新收盘价
    sql = f"""
    SELECT DISTINCT ON (symbol) symbol, close, trade_date
    FROM stock_history WHERE symbol IN ({symbols_str})
    ORDER BY symbol, trade_date DESC
    """
    try:
        df_price = pd.read_sql(sql, engine)
        price_map = df_price.set_index('symbol')['close'].to_dict()
    except Exception as e:
        print(f"❌ 无法获取旧持仓行情: {e}")
        return DEFAULT_START_CAPITAL, False

    total_value = 0
    for _, row in df_old.iterrows():
        sym = row['symbol']
        vol = row['volume']
        # 兜底逻辑：如果停牌取不到价，用成本价
        curr_price = price_map.get(sym, row['cost_price']) 
        total_value += curr_price * vol
        
    print(f"   旧持仓总市值: {total_value:,.2f}")
    return total_value, True

def load_latest_factor(factor_dir):
    """
    🔥 自动寻找目录下最新的因子文件 (按文件名排序)
    """
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
    
    # 查找比 current_date_str 晚的第一条数据
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
    # 1. 资金计算
    current_asset, is_rollover = get_current_asset_value()
    
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
    
    # 🔥🔥🔥 核心：获取因子文件里的真实最大日期 🔥🔥🔥
    factor_date = df_factor['trade_date'].max()
    factor_date_str = pd.to_datetime(factor_date).strftime('%Y-%m-%d')
    print(f"   ⏱️ 因子数据截止日期 (T日): {factor_date_str}")
    
    # ⚠️ 过期检查 (可选)
    days_lag = (datetime.datetime.now() - pd.to_datetime(factor_date)).days
    if days_lag > 10:
        print(f"   ⚠️⚠️⚠️ 警告: 你的因子数据已经是 {days_lag} 天前的了！")
        print(f"   请确认是否忘记运行 prepare_data_daily 更新数据？")
        # x = input("   按回车键继续使用旧数据，或 Ctrl+C 中止: ")

    # 提取当日数据
    df_current = df_factor[df_factor['trade_date'] == factor_date].copy()
    
    # 加载基础信息
    engine = get_db_engine()
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
        print("   原因可能是：stock_history 没有更新到最新日期。")
        return

    # 合并
    df_final = pd.merge(df_candidates, df_next, on='symbol', how='inner')
    
    # 确定目标交易日 (众数)
    market_trade_date = df_final['next_date'].mode()[0]
    trade_date_str = pd.to_datetime(market_trade_date).strftime('%Y-%m-%d')
    print(f"   📅 锁定目标交易日 (T+1): {trade_date_str}")
    
    # 再次检查日期距离
    days_gap = (pd.to_datetime(market_trade_date) - pd.to_datetime(factor_date)).days
    if days_gap > 10:
        print(f"   ⚠️ 注意：因子日期({factor_date_str}) 与 交易日期({trade_date_str}) 相差 {days_gap} 天。")
        print("   这意味着你在用很久以前的信号做交易。")

    # 4. 过滤 (停牌/涨停)
    valid_list = []
    suspend_count = 0
    limit_count = 0
    
    for _, row in df_final.iterrows():
        sym = row['symbol']
        next_date = row['next_date']
        vol = row['volume']
        
        # 停牌过滤
        if next_date != market_trade_date or vol == 0:
            suspend_count += 1
            continue
            
        # 涨停过滤
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

    # 输出
    output_cols = ['symbol', 'name', 'cost_price', 'volume', 'buy_date', 'factor', 'target_weight']
    df_buy[output_cols].to_csv(HOLDINGS_FILE, index=False, encoding='utf-8-sig')
    
    print("\n" + "="*50)
    print(f"✅ 购买清单已更新: {HOLDINGS_FILE}")
    print(f"   实际执行日期: {trade_date_str}")
    print(f"   买入股票数: {len(df_buy)}")
    print("="*50)

if __name__ == '__main__':
    generate_buy_list()