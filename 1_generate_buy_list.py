import pandas as pd
import numpy as np
import os
import glob
from sqlalchemy import create_engine
from dotenv import load_dotenv

# 加载环境变量
load_dotenv('.env')
DSN = os.getenv('DB_DSN1')

# --- 配置部分 ---
FACTOR_DIR = "./factor_cache_global_short"  # 你的因子文件夹路径
OUTPUT_FILE = "my_holdings.csv"       # 输出的清单文件名

# 🔥 核心配置：选股比例
TOP_N_PCT = 0.03  

def load_latest_factor(factor_dir):
    """
    自动查找目录下最新的 factor_*.parquet 文件并加载
    """
    files = sorted(glob.glob(os.path.join(factor_dir, "factor_*.parquet")))
    if not files:
        raise FileNotFoundError(f"❌ 目录下没有找到任何 factor_*.parquet 文件: {factor_dir}")
    
    latest_file = files[-1]
    print(f"✅ 自动加载最新的因子文件: {os.path.basename(latest_file)}")
    return pd.read_parquet(latest_file)

def get_basic_info():
    """
    获取股票基础信息（名称、上市日期）
    """
    engine = create_engine(DSN)
    df_basic = pd.read_sql("SELECT symbol, list_date, name FROM stock_basic", engine)
    df_basic['list_date'] = pd.to_datetime(df_basic['list_date'])
    return df_basic

def generate_buy_list():
    print("1. 正在加载最新的因子数据...")
    df_factor = load_latest_factor(FACTOR_DIR)
    
    # 如果是多重索引，重置索引
    if isinstance(df_factor.index, pd.MultiIndex): 
        df_factor = df_factor.reset_index()
    
    # 获取因子文件中对应的最新交易日期 (即 T 日)
    factor_date = df_factor['trade_date'].max()
    factor_date_str = pd.to_datetime(factor_date).strftime('%Y-%m-%d')
    print(f"   📅 因子基准日 (T日): {factor_date_str}")
    
    # 只取该日期的数据
    df_current = df_factor[df_factor['trade_date'] == factor_date].copy()
    
    print("2. 加载基础信息并筛选...")
    df_basic = get_basic_info()
    df_merge = pd.merge(df_current, df_basic, on='symbol', how='left')
    
    candidates = []
    current_time = pd.Timestamp.now()
    
    for _, row in df_merge.iterrows():
        symbol = row['symbol']
        name = row['name'] if 'name' in row and row['name'] else "Unknown"
        list_date = row['list_date']
        factor_val = row['factor']
        
        # 基础过滤：剔除上市不足 60 天的新股，剔除因子为空的，剔除 ST 股
        if pd.isna(list_date): continue
        if (current_time - list_date).days < 60: continue
        if pd.isna(factor_val): continue
        if name and 'ST' in name: continue
        
        # 记录候选股票
        candidates.append({
            'symbol': symbol,
            'name': name,
            'factor': factor_val,
            'close_price': row.get('close', np.nan), # T日收盘价，仅供参考
            'signal_date': factor_date_str            # 信号产生日期
        })
    
    df_candidates = pd.DataFrame(candidates)
    
    if df_candidates.empty:
        print("❌ 筛选后无候选股票")
        return

    # --- 3. 选股逻辑 ---
    print(f"3. 按 Top {int(TOP_N_PCT*100)}% 选股...")
    
    # 按因子降序排列
    df_candidates = df_candidates.sort_values(by='factor', ascending=False)
    
    # 计算目标数量
    top_n = int(len(df_candidates) * TOP_N_PCT)
    if top_n < 10: top_n = min(10, len(df_candidates))
    
    df_buy = df_candidates.head(top_n).copy()
    
    # --- 4. 分配权重 ---
    # 这里只分配“理论权重”，不计算具体股数，因为资金由执行脚本动态获取
    df_buy['target_weight'] = 1.0 / len(df_buy)
    
    # 重命名列以保持兼容性，虽然 cost_price 这里只是参考价
    df_buy.rename(columns={'close_price': 'ref_close_price', 'signal_date': 'buy_date'}, inplace=True)

    # --- 5. 输出结果 ---
    output_cols = ['symbol', 'name', 'ref_close_price', 'buy_date', 'factor', 'target_weight']
    
    df_buy[output_cols].to_csv(OUTPUT_FILE, index=False, encoding='utf-8-sig')
    
    print("\n" + "="*50)
    print(f"✅ 理论买入清单已生成: {OUTPUT_FILE}")
    print(f"   信号日期: {factor_date_str}")
    print(f"   候选股票: {len(df_buy)} 只")
    print(f"   ⚠️  注意: 此文件仅包含股票列表及权重，实际买入将在下一个交易日由执行脚本处理。")
    print("="*50)
    print(df_buy[['symbol', 'name', 'ref_close_price', 'target_weight']].head(10))

if __name__ == '__main__':
    generate_buy_list()