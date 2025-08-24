import tushare as ts
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

# 设置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('daily_basic.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def retry_on_exception(retries=3, delay=5, backoff=2, exceptions=(Exception,)):
    """重试装饰器"""
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

class DailyBasicCollector:
    def __init__(self, db_params: dict, tushare_token: str):
        self.db_params = db_params
        self.table_name = 'daily_basic'
        self.pro = ts.pro_api(tushare_token)
        
    def get_db_connection(self):
        return psycopg2.connect(**self.db_params)

    def init_table(self):
        """初始化数据表"""
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS {} (
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
        """.format(self.table_name)
        
        with self.get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(create_table_sql)
                conn.commit()
                logger.info("数据表初始化完成")

    @retry_on_exception(retries=3, delay=5, backoff=2)
    def get_stock_list(self) -> List[str]:
        """获取股票列表"""
        try:
            df = self.pro.stock_basic(
                exchange='',
                list_status='L',
                fields='ts_code'
            )
            return df['ts_code'].tolist()
        except Exception as e:
            logger.error(f"获取股票列表失败: {str(e)}")
            return []

    def get_latest_trade_date(self, ts_code: str) -> Optional[datetime]:
        """获取最新交易日期"""
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

    @retry_on_exception(retries=3, delay=5, backoff=2)
    def fetch_data(self, ts_code: str, start_date: str, end_date: str) -> pd.DataFrame:
        """获取每日基本面指标数据"""
        df = self.pro.daily_basic(
            ts_code=ts_code,
            start_date=start_date,
            end_date=end_date
        )
        return df

    def process_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """处理数据"""
        if df.empty:
            return pd.DataFrame()
        
        # 转换日期格式
        df['trade_date'] = pd.to_datetime(df['trade_date']).dt.date
        
        return df

    def save_to_db(self, df: pd.DataFrame):
        """保存数据到数据库"""
        if df.empty:
            return
            
        with self.get_db_connection() as conn:
            with conn.cursor() as cur:
                columns = df.columns.tolist()
                values = [tuple(x) for x in df.values]
                
                insert_stmt = f"""
                    INSERT INTO {self.table_name} ({','.join(columns)})
                    VALUES %s
                    ON CONFLICT (ts_code, trade_date)
                    DO UPDATE SET
                    {','.join(f"{col}=EXCLUDED.{col}" for col in columns 
                             if col not in ['ts_code', 'trade_date'])}
                """
                
                try:
                    execute_values(cur, insert_stmt, values)
                    conn.commit()
                    logger.info(f"成功保存 {len(df)} 条记录")
                except Exception as e:
                    conn.rollback()
                    logger.error(f"保存数据失败: {str(e)}")
                    raise

def process_stock_batch(db_params: Dict, tushare_token: str, stock_batch: List[str], 
                       batch_id: int, start_date: str, end_date: str, mode: str):
    """处理一批股票的数据采集"""
    if not stock_batch:
        logger.warning(f"Batch {batch_id}: Empty stock batch, skipping")
        return
        
    try:
        collector = DailyBasicCollector(db_params, tushare_token)
        total_stocks = len(stock_batch)
        
        logger.info(f"Batch {batch_id}: Starting processing {total_stocks} stocks")
        
        success_count = 0
        error_count = 0
        
        for idx, ts_code in enumerate(stock_batch, 1):
            try:
                # 随机延时1-3秒，避免请求过于频繁
                time.sleep(random.uniform(1, 3))
                
                # 如果是增量模式，获取最新数据日期
                if mode == 'incremental':
                    latest_date = collector.get_latest_trade_date(ts_code)
                    if latest_date:
                        start_date = (latest_date + timedelta(days=1)).strftime('%Y%m%d')
                
                # 获取数据
                df = collector.fetch_data(ts_code, start_date, end_date)
                
                if df is not None and not df.empty:
                    df = collector.process_data(df)
                    collector.save_to_db(df)
                    
                    success_count += 1
                    logger.info(f"Batch {batch_id} Progress: {idx}/{total_stocks} "
                              f"- Successfully processed {ts_code}")
                else:
                    logger.warning(f"Batch {batch_id}: No data available for {ts_code}")
                
            except Exception as e:
                error_count += 1
                logger.error(f"Batch {batch_id}: Error processing {ts_code}: {str(e)}")
                continue
                
        logger.info(f"Batch {batch_id} completed. Success: {success_count}, Errors: {error_count}")
        
    except Exception as e:
        logger.error(f"Batch {batch_id}: Fatal error in batch processing: {str(e)}")

def chunks(lst: List, n: int) -> List[List]:
    """将列表分割成n个大致相等的块"""
    if not lst:
        return []
    
    size = len(lst)
    chunk_size = (size + n - 1) // n
    
    return [lst[i:i + chunk_size] for i in range(0, size, chunk_size)]

def main():
    parser = argparse.ArgumentParser(description='股票每日基本面指标数据采集工具')
    parser.add_argument(
        '--mode',
        choices=['incremental', 'full'],
        default='incremental',
        help='运行模式: incremental-增量更新, full-全量更新'
    )
    parser.add_argument(
        '--processes',
        type=int,
        default=10,
        help='并行进程数'
    )
    parser.add_argument(
        '--start_date',
        type=str,
        default='20100101',
        help='开始日期 (YYYYMMDD)'
    )
    parser.add_argument(
        '--end_date',
        type=str,
        default=datetime.now().strftime('%Y%m%d'),
        help='结束日期 (YYYYMMDD)'
    )
    parser.add_argument(
        '--tushare_token',
        type=str,
        required=True,
        help='Tushare API token'
    )
    
    try:
        args = parser.parse_args()
    except SystemExit:
        parser.print_help()
        sys.exit(1)

    # 数据库连接参数
    db_params = {
        'host': '192.168.50.149',
        'port': 5432,
        'user': 'postgres',
        'password': '12',
        'database': 'Financialdata'
    }
    
    try:
        collector = DailyBasicCollector(db_params, args.tushare_token)
        collector.init_table()
        
        # 获取股票列表
        stocks = collector.get_stock_list()
        total_stocks = len(stocks)
        
        if total_stocks == 0:
            logger.error("没有获取到股票列表，无法进行数据采集")
            return
        
        # 调整进程数
        num_processes = min(args.processes, total_stocks)
        
        logger.info(f"开始{args.mode}模式的并行数据采集（使用 {num_processes} 个进程）...")
        
        # 将股票列表分成多个批次
        stock_batches = chunks(stocks, num_processes)
        
        # 创建任务列表
        tasks = []
        for i, batch in enumerate(stock_batches):
            if batch:
                tasks.append((
                    db_params, 
                    args.tushare_token,
                    batch, 
                    i, 
                    args.start_date, 
                    args.end_date, 
                    args.mode
                ))
        
        # 创建进程池执行任务
        with multiprocessing.Pool(processes=num_processes) as pool:
            pool.starmap(process_stock_batch, tasks)
            
        logger.info("所有批次处理完成")
        
    except KeyboardInterrupt:
        logger.info("程序被用户中断")
    except Exception as e:
        logger.error(f"程序运行出错: {str(e)}")
        raise
    finally:
        logger.info("程序运行完成")

if __name__ == "__main__":
    multiprocessing.freeze_support()
    main()
