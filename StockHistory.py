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

# 设置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('stock_history.log'),
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

def retry_on_exception(retries=3, delay=5, backoff=2, exceptions=(Exception,)):
    """
    重试装饰器
    
    参数:
        retries: 最大重试次数
        delay: 初始延迟时间（秒）
        backoff: 延迟时间的增长因子
        exceptions: 需要重试的异常类型
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            retry_delay = delay
            last_exception = None
            
            for retry in range(retries + 1):  # +1 是为了包含第一次尝试
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    last_exception = e
                    if retry == retries:  # 如果是最后一次尝试
                        raise last_exception
                    
                    # 计算下一次重试的延迟时间
                    wait_time = retry_delay + random.uniform(0, 1)
                    logger.warning(f"尝试 {retry + 1}/{retries} 失败: {str(e)}. "
                                 f"{wait_time:.2f} 秒后重试...")
                    
                    time.sleep(wait_time)
                    retry_delay *= backoff  # 增加延迟时间
                    
            raise last_exception
        return wrapper
    return decorator

@retry_on_exception(retries=3, delay=5, backoff=2, 
                   exceptions=(Exception, TimeoutError, ConnectionError))
def fetch_stock_data(symbol: str, period: str, start_date: str, 
                    end_date: str, adjust: str) -> pd.DataFrame:
    """
    获取股票数据的函数，带有重试机制
    """
    return ak.stock_zh_a_hist(
        symbol=symbol,
        period=period,
        start_date=start_date,
        end_date=end_date,
        adjust=adjust
    )
    
def process_stock_batch(db_params: Dict, stock_batch: List[str], batch_id: int, 
                       start_date: str, end_date: str, mode: str, adjust: str):
    """处理一批股票的数据采集"""
    if not stock_batch:
        logger.warning(f"Batch {batch_id}: Empty stock batch, skipping")
        return
        
    try:
        collector = StockHistoryCollector(db_params)
        total_stocks = len(stock_batch)
        
        logger.info(f"Batch {batch_id}: Starting processing {total_stocks} stocks")
        
        success_count = 0
        error_count = 0
        
        for idx, symbol in enumerate(stock_batch, 1):
            try:
                # 随机延时1-3秒，避免请求过于频繁
                time.sleep(random.uniform(1, 3))
                
                # 如果是增量模式，获取最新数据日期
                if mode == 'incremental':
                    latest_date = collector.get_latest_trade_date(symbol, adjust)
                    if latest_date:
                        start_date = (latest_date + timedelta(days=1)).strftime('%Y%m%d')
                
                # 获取数据
                df = fetch_stock_data(
                    symbol=symbol,
                    period="daily",
                    start_date=start_date,
                    end_date=end_date,
                    adjust=adjust
                )
                
                if df is not None and not df.empty:
                    df = collector.process_data(df, symbol, adjust)
                    collector.save_to_db(df)
                    
                    success_count += 1
                    logger.info(f"Batch {batch_id} Progress: {idx}/{total_stocks} - Successfully processed {symbol}")
                else:
                    logger.warning(f"Batch {batch_id}: No history data available for {symbol}")
                
            except Exception as e:
                error_count += 1
                logger.error(f"Batch {batch_id}: Error processing {symbol}: {str(e)}")
                continue
                
        logger.info(f"Batch {batch_id} completed. Success: {success_count}, Errors: {error_count}")
        
    except Exception as e:
        logger.error(f"Batch {batch_id}: Fatal error in batch processing: {str(e)}")

class StockHistoryCollector:
    def __init__(self, db_params: dict):
        self.db_params = db_params
        self.table_name = 'stock_history'
    
    def get_db_connection(self):
        return psycopg2.connect(**self.db_params)

    def init_table(self):
        """初始化数据表"""
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS {} (
            trade_date DATE,
            symbol VARCHAR(10),
            open FLOAT,
            close FLOAT,
            high FLOAT,
            low FLOAT,
            volume BIGINT,
            amount FLOAT,
            amplitude FLOAT,
            pct_change FLOAT,
            change FLOAT,
            turnover FLOAT,
            adjust_type VARCHAR(5),
            PRIMARY KEY (symbol, trade_date, adjust_type)
        )
        """.format(self.table_name)
        
        with self.get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(create_table_sql)
                conn.commit()
                logger.info("数据表初始化完成")

    # 可以直接复用之前的代码
    def get_all_stocks(self) -> List[str]:
        """获取所有股票代码"""
        try:
            df = ak.stock_info_a_code_name()
            stock_list = [
                code
                for code in df['code'].tolist()
                if str(code).startswith(('0', '3', '6'))
            ]
            logger.info(f"Successfully retrieved {len(stock_list)} stock codes")
            return stock_list
        except Exception as e:
            logger.error(f"Error getting stock list: {str(e)}")
            return []

    def get_latest_trade_date(self, symbol: str, adjust: str) -> Optional[datetime]:
        """获取最新交易日期"""
        with self.get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(f"""
                    SELECT trade_date 
                    FROM {self.table_name}
                    WHERE symbol = %s AND adjust_type = %s
                    ORDER BY trade_date DESC
                    LIMIT 1
                """, (symbol, adjust))
                result = cur.fetchone()
                return result[0] if result else None

    def process_data(self, df: pd.DataFrame, symbol: str, adjust: str) -> pd.DataFrame:
        """处理数据"""
        if df.empty:
            return pd.DataFrame()
        
        # 重命名列
        column_mapping = {
            '日期': 'trade_date',
            '开盘': 'open',
            '收盘': 'close',
            '最高': 'high',
            '最低': 'low',
            '成交量': 'volume',
            '成交额': 'amount',
            '振幅': 'amplitude',
            '涨跌幅': 'pct_change',
            '涨跌额': 'change',
            '换手率': 'turnover'
        }
        
        df = df.rename(columns=column_mapping)
        df['symbol'] = symbol
        df['adjust_type'] = adjust
        df['trade_date'] = pd.to_datetime(df['trade_date']).dt.date
        
        # 选择需要的列
        columns = ['trade_date', 'symbol', 'open', 'close', 'high', 'low', 
                  'volume', 'amount', 'amplitude', 'pct_change', 'change', 
                  'turnover', 'adjust_type']
        
        return df[columns]

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
                    ON CONFLICT (symbol, trade_date, adjust_type)
                    DO UPDATE SET
                    {','.join(f"{col}=EXCLUDED.{col}" for col in columns 
                             if col not in ['symbol', 'trade_date', 'adjust_type'])}
                """
                
                try:
                    execute_values(cur, insert_stmt, values)
                    conn.commit()
                    logger.info(f"成功保存 {len(df)} 条记录")
                except Exception as e:
                    conn.rollback()
                    logger.error(f"保存数据失败: {str(e)}")
                    raise

    def parallel_data_collection(self, start_date: str, end_date: str, 
                               mode: str = 'incremental', adjust: str = '',
                               num_processes: int = 10):
        """并行数据采集"""
        # 初始化数据表
        self.init_table()
        
        # 获取股票列表
        stocks = self.get_all_stocks()
        total_stocks = len(stocks)
        
        if total_stocks == 0:
            logger.error("没有获取到股票列表，无法进行数据采集")
            return
        
        logger.info(f"开始并行数据采集，共 {total_stocks} 只股票，使用 {num_processes} 个进程")
        
        # 调整进程数
        num_processes = min(num_processes, total_stocks)
        
        try:
            # 将股票列表分成多个批次
            stock_batches = chunks(stocks, num_processes)
            
            # 创建任务列表
            tasks = []
            for i, batch in enumerate(stock_batches):
                if batch:
                    tasks.append((self.db_params, batch, i, start_date, end_date, mode, adjust))
            
            if not tasks:
                logger.error("没有创建有效的任务")
                return
                
            logger.info(f"创建了 {len(tasks)} 个任务批次")
            
            # 创建进程池
            with multiprocessing.Pool(processes=num_processes) as pool:
                pool.starmap(process_stock_batch, tasks)
                
            logger.info("所有批次处理完成")
            
        except Exception as e:
            logger.error(f"并行数据采集出错: {str(e)}")
            raise

def main():
    parser = argparse.ArgumentParser(description='股票历史数据采集工具')
    parser.add_argument(
        '--mode',
        choices=['incremental', 'full'],
        default='incremental',
        help='运行模式: incremental-增量更新, full-全量更新'
    )
    parser.add_argument(
        '--adjust',
        choices=['', 'qfq', 'hfq'],
        default='',
        help='复权方式: 空-不复权, qfq-前复权, hfq-后复权'
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
        default='20170101',
        help='开始日期 (YYYYMMDD)'
    )
    parser.add_argument(
        '--end_date',
        type=str,
        default=datetime.now().strftime('%Y%m%d'),
        help='结束日期 (YYYYMMDD)'
    )
    
    try:
        args = parser.parse_args()
    except SystemExit:
        parser.print_help()
        sys.exit(1)

    # 数据库连接参数
    db_params = {
        'host': 'localhost',
        'port': 5432,
        'user': 'postgres',
        'password': '12',
        'database': 'Financialdata'
    }
    
    try:
        collector = StockHistoryCollector(db_params)
        logger.info(f"开始{args.mode}模式的并行数据采集（使用 {args.processes} 个进程）...")
        collector.parallel_data_collection(
            start_date=args.start_date,
            end_date=args.end_date,
            mode=args.mode,
            adjust=args.adjust,
            num_processes=args.processes
        )
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
