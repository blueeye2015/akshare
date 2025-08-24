# zh_daily_basic_ak.py
import akshare as ak
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
import logging
from typing import List, Dict, Optional
import time
import random
import multiprocessing
import argparse
import sys
from datetime import datetime, timedelta
from functools import wraps

# -------------------- 日志 --------------------
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('daily_basic.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# -------------------- 重试装饰器 --------------------
def retry_on_exception(retries=3, delay=5, backoff=2, exceptions=(Exception,)):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            retry_delay = delay
            last_exception = None
            for retry in range(retries + 1):
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    last_exception = e
                    if retry == retries:
                        raise last_exception
                    wait_time = retry_delay + random.uniform(0, 1)
                    logger.warning(f"尝试 {retry + 1}/{retries} 失败: {str(e)}. "
                                   f"{wait_time:.2f} 秒后重试...")
                    time.sleep(wait_time)
                    retry_delay *= backoff
            raise last_exception
        return wrapper
    return decorator

# -------------------- 采集器 --------------------
class DailyBasicCollector:
    def __init__(self, db_params: dict):
        self.db_params = db_params
        self.table_name = 'daily_basic'

    def get_db_connection(self):
        return psycopg2.connect(**self.db_params)

    def init_table(self):
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {self.table_name} (
            ts_code VARCHAR(10),
            trade_date DATE,
            close DECIMAL(10,4),
            turnover_rate DECIMAL(10,4),
            turnover_rate_f DECIMAL(10,4),
            volume_ratio DECIMAL(10,4),
            pe DECIMAL(20,4),
            pe_ttm DECIMAL(20,4),
            pb DECIMAL(10,4),
            ps DECIMAL(10,4),
            ps_ttm DECIMAL(10,4),
            dv_ratio DECIMAL(10,4),
            dv_ttm DECIMAL(10,4),
            total_share DECIMAL(20,4),
            float_share DECIMAL(20,4),
            free_share DECIMAL(20,4),
            total_mv DECIMAL(20,4),
            circ_mv DECIMAL(20,4),
            PRIMARY KEY (ts_code, trade_date)
        )
        """
        with self.get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(create_table_sql)
                conn.commit()
                logger.info("数据表初始化完成")

    # -------------------- 股票列表 --------------------
    @retry_on_exception(retries=3, delay=5, backoff=2)
    def get_stock_list(self) -> List[str]:
        """获取股票列表，带重试机制"""
        try:
            logger.info("正在获取股票列表...")
            df = ak.stock_a_indicator_lg(symbol="all")
            
            if df is None or df.empty:
                logger.warning("获取股票列表返回空数据，重试...")
                raise Exception("Empty stock list")
            
            stock_codes = df['code'].tolist()
            logger.info(f"成功获取 {len(stock_codes)} 只股票代码")
            return stock_codes
            
        except Exception as e:
            logger.error(f"获取股票列表失败: {str(e)}")
            raise

    def get_latest_trade_date(self, ts_code: str) -> Optional[datetime]:
        with self.get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(f"""
                    SELECT trade_date 
                    FROM {self.table_name}
                    WHERE ts_code = %s
                    ORDER BY trade_date DESC
                    LIMIT 1
                """, (ts_code,))
                result = cur.fetchone()
                return result[0] if result else None

    # -------------------- 核心：用 ak.stock_zh_a_hist --------------------
    @retry_on_exception(retries=3, delay=5, backoff=2)
    def fetch_data(self, ts_code: str, start_date: str, end_date: str) -> pd.DataFrame:
        df = ak.stock_zh_a_hist(
            symbol=ts_code,
            period="daily",
            start_date=start_date,
            end_date=end_date,
            adjust=""
        )
        if df.empty:
            return pd.DataFrame()

        # 重命名 AkShare → 表字段
        rename_map = {
            '日期': 'trade_date',
            '收盘': 'close',
            '换手率': 'turnover_rate',
            '量比': 'volume_ratio',
            '市净率': 'pb',
            '总市值': 'total_mv',
            '流通市值': 'circ_mv',
        }
        df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})

        # 缺失字段补空
        need_cols = [
            'close', 'turnover_rate', 'volume_ratio', 'pe', 'pe_ttm', 'pb',
            'ps', 'ps_ttm', 'dv_ratio', 'dv_ttm',
            'total_share', 'float_share', 'free_share', 'total_mv', 'circ_mv'
        ]
        for col in need_cols:
            if col not in df.columns:
                df[col] = None

        df['ts_code'] = ts_code
        df['trade_date'] = pd.to_datetime(df['trade_date']).dt.date
        return df[['ts_code', 'trade_date'] + need_cols]

    def process_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """如无额外转换，直接返回"""
        return df

    def save_to_db(self, df: pd.DataFrame):
        if df.empty:
            return
        with self.get_db_connection() as conn:
            with conn.cursor() as cur:
                columns = df.columns.tolist()
                values = [tuple(x) for x in df.values]
                sql = f"""
                    INSERT INTO {self.table_name} ({','.join(columns)})
                    VALUES %s
                    ON CONFLICT (ts_code, trade_date)
                    DO UPDATE SET
                    {','.join(f"{c}=EXCLUDED.{c}" for c in columns
                             if c not in ['ts_code', 'trade_date'])}
                """
                try:
                    execute_values(cur, sql, values)
                    conn.commit()
                    logger.info(f"成功保存 {len(df)} 条记录")
                except Exception as e:
                    conn.rollback()
                    logger.error(f"保存数据失败: {str(e)}")
                    raise

# -------------------- 多进程辅助 --------------------
def process_stock_batch(db_params: Dict, stock_batch: List[str],
                        batch_id: int, start_date: str, end_date: str, mode: str):
    if not stock_batch:
        logger.warning(f"Batch {batch_id}: Empty stock batch, skipping")
        return
    try:
        collector = DailyBasicCollector(db_params)
        total = len(stock_batch)
        logger.info(f"Batch {batch_id}: Starting {total} stocks")
        succ = err = 0
        for idx, code in enumerate(stock_batch, 1):
            try:
                time.sleep(random.uniform(1, 3))
                if mode == 'incremental':
                    latest = collector.get_latest_trade_date(code)
                    if latest:
                        start_date = (latest + timedelta(days=1)).strftime('%Y%m%d')
                df = collector.fetch_data(code, start_date, end_date)
                if not df.empty:
                    df = collector.process_data(df)
                    collector.save_to_db(df)
                    succ += 1
                    logger.info(f"Batch {batch_id}: {idx}/{total} - {code} OK")
                else:
                    logger.warning(f"Batch {batch_id}: No data {code}")
            except Exception as e:
                err += 1
                logger.error(f"Batch {batch_id}: {code} failed - {e}")
        logger.info(f"Batch {batch_id} done. Success: {succ}, Errors: {err}")
    except Exception as e:
        logger.error(f"Batch {batch_id} fatal: {e}")

def chunks(lst: List, n: int) -> List[List]:
    if not lst:
        return []
    size = len(lst)
    chunk_size = (size + n - 1) // n
    return [lst[i:i + chunk_size] for i in range(0, size, chunk_size)]

# -------------------- 入口 --------------------
def main():
    parser = argparse.ArgumentParser(description='基于 AkShare 的 daily_basic 采集')
    parser.add_argument('--mode', choices=['incremental', 'full'], default='incremental')
    parser.add_argument('--processes', type=int, default=10)
    parser.add_argument('--start_date', type=str, default='20100101')
    parser.add_argument('--end_date', type=str, default=datetime.now().strftime('%Y%m%d'))
    args = parser.parse_args()

    db_params = {
        'host': '192.168.50.149',
        'port': 5432,
        'user': 'postgres',
        'password': '12',
        'database': 'Financialdata'
    }

    try:
        collector = DailyBasicCollector(db_params)
        collector.init_table()
        stocks = collector.get_stock_list()
        total = len(stocks)
        if total == 0:
            logger.error("股票列表为空，退出")
            return
        num_proc = min(args.processes, total)
        logger.info(f"开始 {args.mode} 模式，{num_proc} 进程")
        batches = chunks(stocks, num_proc)
        tasks = [
            (db_params, batch, i, args.start_date, args.end_date, args.mode)
            for i, batch in enumerate(batches) if batch
        ]
        with multiprocessing.Pool(processes=num_proc) as pool:
            pool.starmap(process_stock_batch, tasks)
        logger.info("全部完成")
    except KeyboardInterrupt:
        logger.info("用户中断")
    except Exception as e:
        logger.error(f"程序异常: {e}")
        raise

if __name__ == "__main__":
    multiprocessing.freeze_support()
    main()