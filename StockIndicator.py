import akshare as ak
import pandas as pd
from datetime import datetime
import psycopg2
from psycopg2.extras import execute_values
import logging
from typing import List, Optional, Dict
import time
from retrying import retry
import multiprocessing
from concurrent.futures import ThreadPoolExecutor, as_completed
import argparse
import sys
import random

# 设置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('stock_indicator.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def chunks(lst: List, n: int) -> List[List]:
    """将列表分割成n个大致相等的块"""
    if not lst:
        return []
    
    size = len(lst)
    chunk_size = (size + n - 1) // n  # 向上取整确保覆盖所有元素
    
    return [lst[i:i + chunk_size] for i in range(0, size, chunk_size)]

def process_stock_batch(db_params: Dict, stock_batch: List[str], batch_id: int, mode: str):
    """处理一批股票的数据采集"""
    if not stock_batch:
        logger.warning(f"Batch {batch_id}: Empty stock batch, skipping")
        return
        
    try:
        collector = StockIndicatorCollector(db_params)
        total_stocks = len(stock_batch)
        
        logger.info(f"Batch {batch_id}: Starting processing {total_stocks} stocks")
        
        success_count = 0
        error_count = 0
        
        for idx, symbol in enumerate(stock_batch, 1):
            try:
                time.sleep(random.uniform(1, 3))
                
                # 获取数据
                df = ak.stock_a_indicator_lg(symbol=symbol)
                
                if df is not None and not df.empty:
                    # 如果是增量模式，只保留最新数据
                    if mode == 'incremental':
                        latest_date = collector.get_latest_trade_date(symbol)
                        if latest_date:
                            df = df[df['trade_date'] > latest_date]
                    
                    df = collector.process_data(df, symbol)
                    collector.save_to_db(df)
                    
                    success_count += 1
                    logger.info(f"Batch {batch_id} Progress: {idx}/{total_stocks} - Successfully processed {symbol}")
                else:
                    logger.warning(f"Batch {batch_id}: No indicator data available for {symbol}")
                
            except Exception as e:
                error_count += 1
                logger.error(f"Batch {batch_id}: Error processing {symbol}: {str(e)}")
                continue
                
        logger.info(f"Batch {batch_id} completed. Success: {success_count}, Errors: {error_count}")
        
    except Exception as e:
        logger.error(f"Batch {batch_id}: Fatal error in batch processing: {str(e)}")

class StockIndicatorCollector:
    def __init__(self, db_params: dict):
        self.db_params = db_params
        self.table_name = 'stock_indicator'
    
    def get_db_connection(self):
        return psycopg2.connect(**self.db_params)

    def get_stock_list(self) -> List[str]:
        try:
            df = ak.stock_a_indicator_lg(symbol="all")
            return df['code'].tolist()
        except Exception as e:
            logger.error(f"获取股票列表失败: {str(e)}")
            return []

    def get_latest_trade_date(self, symbol: str) -> Optional[str]:
        with self.get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(f"""
                    SELECT trade_date 
                    FROM {self.table_name}
                    WHERE symbol = %s
                    ORDER BY trade_date DESC
                    LIMIT 1
                """, (symbol,))
                result = cur.fetchone()
                return result[0] if result else None

    def process_data(self, df: pd.DataFrame, symbol: str) -> pd.DataFrame:
        if df.empty:
            return pd.DataFrame()
            
        df['symbol'] = symbol
        df['trade_date'] = pd.to_datetime(df['trade_date']).dt.date
        
        numeric_columns = [
            'pe', 'pe_ttm', 'pb', 'ps', 'ps_ttm',
            'dv_ratio', 'dv_ttm', 'total_mv'
        ]
        
        for col in numeric_columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
        
        columns = ['symbol', 'trade_date'] + numeric_columns
        return df[columns]

    def save_to_db(self, df: pd.DataFrame):
        if df.empty:
            return
            
        with self.get_db_connection() as conn:
            with conn.cursor() as cur:
                columns = df.columns.tolist()
                values = [tuple(x) for x in df.values]
                
                insert_stmt = f"""
                    INSERT INTO {self.table_name} ({','.join(columns)})
                    VALUES %s
                    ON CONFLICT (symbol, trade_date)
                    DO UPDATE SET
                    {','.join(f"{col}=EXCLUDED.{col}" for col in columns if col not in ['symbol', 'trade_date'])}
                """
                
                try:
                    execute_values(cur, insert_stmt, values)
                    conn.commit()
                    logger.info(f"成功保存 {len(df)} 条记录")
                except Exception as e:
                    conn.rollback()
                    logger.error(f"保存数据失败: {str(e)}")
                    raise

    def parallel_data_collection(self, mode: str = 'incremental', num_processes: int = 10):
        """并行数据采集"""
        stocks = self.get_stock_list()
        total_stocks = len(stocks)
        
        if total_stocks == 0:
            logger.error("没有获取到股票列表，无法进行数据采集")
            return
        
        logger.info(f"开始并行数据采集，共 {total_stocks} 只股票，使用 {num_processes} 个进程")
        
        # 调整进程数，确保不会超过股票数量
        num_processes = min(num_processes, total_stocks)
        
        try:
            # 将股票列表分成多个批次
            stock_batches = chunks(stocks, num_processes)
            
            # 创建任务列表
            tasks = []
            for i, batch in enumerate(stock_batches):
                if batch:  # 确保批次不为空
                    tasks.append((self.db_params, batch, i, mode))
            
            if not tasks:
                logger.error("没有创建有效的任务")
                return
                
            logger.info(f"创建了 {len(tasks)} 个任务批次")
            
            # 创建进程池
            with multiprocessing.Pool(processes=num_processes) as pool:
                # 使用进程池并行处理每个批次
                pool.starmap(process_stock_batch, tasks)
                
            logger.info("所有批次处理完成")
            
        except Exception as e:
            logger.error(f"并行数据采集出错: {str(e)}")
            raise

def main():
    parser = argparse.ArgumentParser(description='股票指标数据采集工具')
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
        collector = StockIndicatorCollector(db_params)
        logger.info(f"开始{args.mode}模式的并行数据采集（使用 {args.processes} 个进程）...")
        collector.parallel_data_collection(mode=args.mode, num_processes=args.processes)
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
