# -*- coding: utf-8 -*-
"""
每日停复牌信息采集（suspend_d）
用法：
python suspend_daily.py --tushare_token YOUR_TOKEN \
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
        logging.FileHandler('suspend_daily.log'),
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
class SuspendDailyCollector:
    def __init__(self, db_params: dict, tushare_token: str):
        self.db_params = db_params
        self.table_name = 'suspend_daily'
        self.pro = ts.pro_api(tushare_token)

    # ---------- PG 连接 ----------
    def get_db_connection(self):
        return psycopg2.connect(**self.db_params)

    def init_table(self):
        sql = f"""
        CREATE TABLE IF NOT EXISTS {self.table_name} (
            ts_code         VARCHAR(10),
            trade_date      DATE,
            suspend_timing  VARCHAR(20),
            suspend_type    VARCHAR(5),
            PRIMARY KEY (ts_code, trade_date)
        );
        """
        with self.get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql)
                conn.commit()
        logger.info("数据表初始化完成")

    # ---------- 股票列表 ----------
    def get_stock_list(self) -> List[str]:
        """获取当日可交易股票列表（可自己扩展）"""
        df = self.pro.stock_basic(exchange='', list_status='L',
                                 fields='ts_code')
        return df['ts_code'].tolist()

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
        df = self.pro.suspend_d(ts_code=ts_code,
                               start_date=start_date,
                               end_date=end_date)
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

        # 1. 只保留主键需要的列
        cols = ['ts_code', 'trade_date', 'suspend_timing', 'suspend_type']
        df = df[cols].copy()

        # 2. 按主键去重：同一天多条 -> 保留最后一条（可按业务调整）
        df = df.sort_values(['ts_code', 'trade_date'])
        df = df.drop_duplicates(subset=['ts_code', 'trade_date'], keep='last')

        # 3. 生成 SQL
        vals = [tuple(x) for x in df.values]
        sql = f"""
            INSERT INTO {self.table_name} ({','.join(cols)})
            VALUES %s
            ON CONFLICT (ts_code, trade_date)
            DO UPDATE SET
            {','.join(f"{c}=EXCLUDED.{c}" for c in cols if c not in ['ts_code','trade_date'])}
        """

        # 4. 写入
        with self.get_db_connection() as conn:
            with conn.cursor() as cur:
                execute_values(cur, sql, vals)
                conn.commit()
        logger.info(f"Upsert {len(df)} 条记录")

# ---------------- 多进程任务 ----------------
def process_stock_batch(db_params: Dict, token: str, stock_batch: List[str],
                       batch_id: int, start_date: str, end_date: str, mode: str):
    if not stock_batch:
        logger.warning(f"Batch {batch_id} 空列表，跳过")
        return
    collector = SuspendDailyCollector(db_params, token)
    ok, ng = 0, 0
    for idx, ts_code in enumerate(stock_batch, 1):
        try:
            time.sleep(random.uniform(0.6, 1.8))          # 限速
            if mode == 'incremental':
                latest = collector.get_latest_trade_date(ts_code)
                if latest:
                    start_date = (latest + timedelta(days=1)).strftime('%Y%m%d')
            df = collector.fetch_data(ts_code, start_date, end_date)
            if df is not None and not df.empty:
                collector.save_to_db(collector.process_data(df))
                ok += 1
                logger.info(f"Batch {batch_id}  [{idx}/{len(stock_batch)}]  {ts_code} 完成")
            else:
                logger.warning(f"Batch {batch_id}  {ts_code} 无数据")
        except Exception as e:
            ng += 1
            logger.error(f"Batch {batch_id}  {ts_code} 出错: {e}", exc_info=True)
    logger.info(f"Batch {batch_id} 结束，成功 {ok}，失败 {ng}")

# ---------------- main ----------------
def main():
    parser = argparse.ArgumentParser(description='每日停复牌信息采集（suspend_d）')
    parser.add_argument('--tushare_token', required=True, help='Tushare token')
    parser.add_argument('--mode', choices=['incremental', 'full'], default='incremental')
    parser.add_argument('--processes', type=int, default=4, help='并行进程数')
    parser.add_argument('--start_date', type=str, default='20040101')
    parser.add_argument('--end_date', type=str, default=datetime.now().strftime('%Y%m%d'))
    args = parser.parse_args()

    db_params = dict(host='192.168.50.149', port=5432, user='postgres',
                    password='12', database='Financialdata')

    collector = SuspendDailyCollector(db_params, args.tushare_token)
    collector.init_table()

    stocks = collector.get_stock_list()
    num = min(args.processes, len(stocks))
    batches = chunks(stocks, num)
    tasks = [(db_params, args.tushare_token, b, i, args.start_date, args.end_date, args.mode)
            for i, b in enumerate(batches) if b]

    logger.info(f"共 {len(stocks)} 只股票，{num} 进程，{len(tasks)} 个批次，模式={args.mode}")
    with multiprocessing.Pool(processes=num) as pool:
        pool.starmap(process_stock_batch, tasks)
    logger.info("全部完成")

if __name__ == '__main__':
    multiprocessing.freeze_support()
    main()