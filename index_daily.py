# -*- coding: utf-8 -*-
"""
指数每日行情采集（index_daily）
用法：
python index_daily.py --tushare_token YOUR_TOKEN  \
                      --mode incremental          \
                      --processes 4               \
                      --start_date 20040101
"""
import tushare as ts
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
import logging
import random
import time
import multiprocessing
import argparse
import sys
from datetime import datetime, timedelta
from functools import wraps
from typing import List, Dict, Optional

# ---------------- 日志 ----------------
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('index_daily.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ---------------- 工具 ----------------
def chunks(lst: List, n: int) -> List[List]:
    if not lst:
        return []
    chunk_size = (len(lst) + n - 1) // n
    return [lst[i:i + chunk_size] for i in range(0, len(lst), chunk_size)]

def retry_on_exception(retries=3, delay=5, backoff=2, exceptions=(Exception,)):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            retry_delay = delay
            for retry in range(retries + 1):
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    if retry == retries:
                        raise
                    wait_time = retry_delay + random.uniform(0, 1)
                    logger.warning(f"尝试 {retry + 1}/{retries} 失败: {e}，{wait_time:.2f}s 后重试...")
                    time.sleep(wait_time)
                    retry_delay *= backoff
        return wrapper
    return decorator

# ---------------- 采集器 ----------------
class IndexDailyCollector:
    def __init__(self, db_params: dict, tushare_token: str):
        self.db_params = db_params
        self.table_name = 'index_daily'
        self.pro = ts.pro_api(tushare_token)

    # ---------- PG 连接 ----------
    def get_db_connection(self):
        return psycopg2.connect(**self.db_params)

    def init_table(self):
        sql = f"""
        CREATE TABLE IF NOT EXISTS {self.table_name} (
            ts_code      VARCHAR(10),
            trade_date   DATE,
            open         FLOAT,
            close        FLOAT,
            high         FLOAT,
            low          FLOAT,
            pre_close    FLOAT,
            change       FLOAT,
            pct_chg      FLOAT,
            vol          FLOAT,
            amount       FLOAT,
            PRIMARY KEY (ts_code, trade_date)
        );
        """
        with self.get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql)
                conn.commit()
        logger.info("数据表初始化完成")

    # ---------- 指数列表 ----------
    def get_index_list(self) -> List[str]:
        """覆盖主流指数，399107.SZ 为深证 A 股总成交"""
        return [
            '000001.SH',  # 上证综指
            '000300.SH',  # 沪深300
            '000905.SH',  # 中证500
            '000016.SH',  # 上证50
            '399001.SZ',  # 深证成指（500 只成分）
            '399005.SZ',  # 中小板指
            '399006.SZ',  # 创业板指
            '399107.SZ',  # 深证 A 指（全市场成交）
            '399106.SZ'   # 深证综指
        ]

    # ---------- 增量用 ----------
    def get_latest_trade_date(self, ts_code: str) -> Optional[datetime]:
        sql = f"""
            SELECT MAX(trade_date)
            FROM {self.table_name}
            WHERE ts_code = %s
        """
        with self.get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (ts_code,))
                row = cur.fetchone()
                return row[0] if row and row[0] else None

    # ---------- 拉数据 ----------
    @retry_on_exception(retries=3, delay=5, backoff=2)
    def fetch_data(self, ts_code: str, start_date: str, end_date: str) -> pd.DataFrame:
        df = self.pro.index_daily(
            ts_code=ts_code,
            start_date=start_date,
            end_date=end_date,
            fields='ts_code,trade_date,open,close,high,low,pre_close,'
                   'change,pct_chg,vol,amount'
        )
        return df

    # ---------- 清洗 ----------
    def process_data(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return pd.DataFrame()
        df['trade_date'] = pd.to_datetime(df['trade_date']).dt.date
        return df

    # ---------- 存库 ----------
    def save_to_db(self, df: pd.DataFrame):
        if df.empty:
            return
        cols = df.columns.tolist()
        vals = [tuple(x) for x in df.values]
        sql = f"""
            INSERT INTO {self.table_name} ({','.join(cols)})
            VALUES %s
            ON CONFLICT (ts_code, trade_date)
            DO UPDATE SET
            {','.join(f"{c}=EXCLUDED.{c}" for c in cols if c not in ['ts_code','trade_date'])}
        """
        with self.get_db_connection() as conn:
            with conn.cursor() as cur:
                execute_values(cur, sql, vals)
                conn.commit()
        logger.info(f"Upsert {len(df)} 条记录")

# ---------------- 多进程任务 ----------------
def process_index_batch(db_params: Dict, token: str, index_batch: List[str],
                       batch_id: int, start_date: str, end_date: str, mode: str):
    if not index_batch:
        logger.warning(f"Batch {batch_id} 空列表，跳过")
        return
    collector = IndexDailyCollector(db_params, token)
    ok, ng = 0, 0
    for idx, ts_code in enumerate(index_batch, 1):
        try:
            time.sleep(random.uniform(0.8, 2.4))          # 限速
            if mode == 'incremental':
                latest = collector.get_latest_trade_date(ts_code)
                if latest:
                    start_date = (latest + timedelta(days=1)).strftime('%Y%m%d')
            df = collector.fetch_data(ts_code, start_date, end_date)
            if df is not None and not df.empty:
                collector.save_to_db(collector.process_data(df))
                ok += 1
                logger.info(f"Batch {batch_id}  [{idx}/{len(index_batch)}]  {ts_code} 完成")
            else:
                logger.warning(f"Batch {batch_id}  {ts_code} 无数据")
        except Exception as e:
            ng += 1
            logger.error(f"Batch {batch_id}  {ts_code} 出错: {e}", exc_info=True)
    logger.info(f"Batch {batch_id} 结束，成功 {ok}，失败 {ng}")

# ---------------- main ----------------
def main():
    parser = argparse.ArgumentParser(description='指数每日行情采集（index_daily）')
    parser.add_argument('--tushare_token', required=True, help='Tushare token')
    parser.add_argument('--mode', choices=['incremental', 'full'], default='incremental')
    parser.add_argument('--processes', type=int, default=4, help='并行进程数')
    parser.add_argument('--start_date', type=str, default='20040101')
    parser.add_argument('--end_date', type=str, default=datetime.now().strftime('%Y%m%d'))
    args = parser.parse_args()

    db_params = dict(host='192.168.50.149', port=5432, user='postgres',
                    password='12', database='Financialdata')

    collector = IndexDailyCollector(db_params, args.tushare_token)
    collector.init_table()

    indexes = collector.get_index_list()
    num = min(args.processes, len(indexes))
    batches = chunks(indexes, num)
    tasks = [(db_params, args.tushare_token, b, i, args.start_date, args.end_date, args.mode)
            for i, b in enumerate(batches) if b]

    logger.info(f"共 {len(indexes)} 只指数，{num} 进程，{len(tasks)} 个批次，模式={args.mode}")
    with multiprocessing.Pool(processes=num) as pool:
        pool.starmap(process_index_batch, tasks)
    logger.info("全部完成")

if __name__ == '__main__':
    multiprocessing.freeze_support()
    main()