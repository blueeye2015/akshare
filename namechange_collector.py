# -*- coding: utf-8 -*-
"""
历史名称变更记录采集器（Tushare namechange 接口）
用法：
python namechange_collector.py --tushare_token YOUR_TOKEN
"""

import tushare as ts
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
import logging
import time
import random
import argparse
from datetime import datetime

# ---------------- 日志 ----------------
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('namechange_collector.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ---------------- 数据库配置 ----------------
DB_PARAMS = {
    'host': '192.168.50.149',
    'port': 5432,
    'user': 'postgres',
    'password': '12',
    'database': 'Financialdata'
}
TABLE_NAME = 'stock_namechange'

# ---------------- 初始化 ----------------
def init_table():
    sql = f"""
    CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
        ts_code        VARCHAR(12),
        name           TEXT,
        start_date     DATE,
        end_date       DATE,
        ann_date       DATE,
        change_reason  TEXT,
        PRIMARY KEY (ts_code, start_date)
    );
    """
    with psycopg2.connect(**DB_PARAMS) as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            conn.commit()
    logger.info("数据表初始化完成")

def get_existing_ts_codes() -> set:
    """获取已采集的 ts_code 集合，避免重复"""
    try:
        with psycopg2.connect(**DB_PARAMS) as conn:
            with conn.cursor() as cur:
                cur.execute(f"SELECT DISTINCT ts_code FROM {TABLE_NAME}")
                return {row[0] for row in cur.fetchall()}
    except Exception as e:
        logger.warning(f"读取已存在代码失败: {e}")
        return set()

def save_to_db(df: pd.DataFrame):
    if df.empty:
        return

    cols = ['ts_code', 'name', 'start_date', 'end_date', 'ann_date', 'change_reason']
    df = df[cols].copy()

    # 去重
    df = df.drop_duplicates(subset=['ts_code', 'start_date'], keep='last')

    # === 彻底清洗日期列：确保只含 datetime.date 或 None ===
    for col in ['start_date', 'end_date', 'ann_date']:
        if col in df.columns:
            cleaned_dates = []
            for val in df[col]:
                if pd.isna(val) or val is None or str(val).strip().upper() in ('NAT', ''):
                    cleaned_dates.append(None)
                else:
                    try:
                        # 尝试转为 pandas datetime，再取 date
                        dt = pd.to_datetime(val, errors='coerce')
                        if pd.isna(dt):
                            cleaned_dates.append(None)
                        else:
                            cleaned_dates.append(dt.date())
                    except Exception:
                        cleaned_dates.append(None)
            df[col] = cleaned_dates
    # =====================================================

    # 清洗其他列：替换 NaN / NaT / 空字符串为 None
    def clean_value(x):
        if pd.isna(x) or x is None:
            return None
        s = str(x).strip()
        if s.upper() == 'NAT' or s == '':
            return None
        return x

    for col in ['ts_code', 'name', 'change_reason']:
        if col in df.columns:
            df[col] = df[col].apply(clean_value)

    # 转为 tuple list
    vals = []
    for row in df.itertuples(index=False, name=None):
        vals.append(tuple(row))

    sql = f"""
        INSERT INTO {TABLE_NAME} ({','.join(cols)})
        VALUES %s
        ON CONFLICT (ts_code, start_date)
        DO UPDATE SET
            name = EXCLUDED.name,
            end_date = EXCLUDED.end_date,
            ann_date = EXCLUDED.ann_date,
            change_reason = EXCLUDED.change_reason
    """

    with psycopg2.connect(**DB_PARAMS) as conn:
        with conn.cursor() as cur:
            execute_values(cur, sql, vals, page_size=50)
            conn.commit()
    logger.info(f"保存 {len(df)} 条记录（ts_code: {df['ts_code'].iloc[0]}）")

# ---------------- 主逻辑 ----------------
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--tushare_token', required=True, help='Tushare Pro Token')
    args = parser.parse_args()

    # 设置 Tushare
    ts.set_token(args.tushare_token)
    pro = ts.pro_api()

    init_table()

    # 获取全部股票列表（作为采集范围）
    logger.info("正在获取股票列表...")
    stock_list = pro.stock_basic(
        fields='ts_code',
        list_status='L'  # 只取上市状态
    )
    all_codes = stock_list['ts_code'].tolist()
    logger.info(f"共获取 {len(all_codes)} 只股票")

    # 增量：跳过已处理的
    existing = get_existing_ts_codes()
    target_codes = [code for code in all_codes if code not in existing]
    logger.info(f"待处理: {len(target_codes)} 只股票（已跳过 {len(existing)} 只）")

    if not target_codes:
        logger.info("无新股票需要处理，退出。")
        return

    # 单进程逐个采集（必须限速！）
    ok, ng = 0, 0
    for idx, ts_code in enumerate(target_codes, 1):
        try:
            logger.info(f"[{idx}/{len(target_codes)}] 开始处理 {ts_code}")
            df = pro.namechange(ts_code=ts_code)

            if df is not None and not df.empty:
                save_to_db(df)
                ok += 1
            else:
                logger.debug(f"{ts_code} 无更名记录")

            # ⚠️ 关键：严格限速！至少 30 秒间隔（确保 ≤ 2 次/分钟）
            if idx < len(target_codes):
                sleep_time = random.uniform(1, 3)
                logger.info(f"等待 {sleep_time:.1f} 秒...")
                time.sleep(sleep_time)

        except Exception as e:
            ng += 1
            logger.error(f"{ts_code} 采集失败: {e}", exc_info=True)
            # 出错也 sleep，避免连续请求
            time.sleep(10)

    logger.info(f"采集完成！成功: {ok}, 失败: {ng}")

if __name__ == '__main__':
    main()