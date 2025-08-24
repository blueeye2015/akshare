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
import threading

# 设置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('industry_board.log'),
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
                    logger.warning(f"尝试 {retry + 1}/{retries + 1} 失败: {str(e)}. "
                                 f"{wait_time:.2f} 秒后重试...")
                    
                    time.sleep(wait_time)
                    retry_delay *= backoff
                    
            raise last_exception
        return wrapper
    return decorator

class IndustryBoardCollector:
    def __init__(self, db_params: dict):
        self.db_params = db_params
        self.board_table = 'industry_board_info'
        self.member_table = 'industry_board_members'
        
    def get_db_connection(self):
        return psycopg2.connect(**self.db_params)

    def init_tables(self):
        """初始化数据表"""
        # 行业板块信息表
        board_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {self.board_table} (
            update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            排名 INTEGER,
            板块名称 VARCHAR(100),
            板块代码 VARCHAR(20),
            最新价 DECIMAL(15,4),
            涨跌额 DECIMAL(15,4),
            涨跌幅 DECIMAL(10,4),
            总市值 BIGINT,
            换手率 DECIMAL(10,4),
            上涨家数 INTEGER,
            下跌家数 INTEGER,
            领涨股票 VARCHAR(100),
            领涨股票_涨跌幅 DECIMAL(10,4),
            PRIMARY KEY (update_time, 板块代码)
        )
        """
        
        # 行业板块成分股表
        member_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {self.member_table} (
            update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            板块代码 VARCHAR(20),
            板块名称 VARCHAR(100),
            序号 INTEGER,
            代码 VARCHAR(20),
            名称 VARCHAR(100),
            最新价 DECIMAL(15,4),
            涨跌幅 DECIMAL(10,4),
            涨跌额 DECIMAL(15,4),
            成交量 DECIMAL(20,2),
            成交额 DECIMAL(20,2),
            振幅 DECIMAL(10,4),
            最高 DECIMAL(15,4),
            最低 DECIMAL(15,4),
            今开 DECIMAL(15,4),
            昨收 DECIMAL(15,4),
            换手率 DECIMAL(10,4),
            市盈率_动态 DECIMAL(15,4),
            市净率 DECIMAL(10,4),
            PRIMARY KEY (update_time, 板块代码, 代码)
        )
        """
        
        with self.get_db_connection() as conn:
            with conn.cursor() as cur:
                # 检查行业板块表
                cur.execute(f"""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_name = '{self.board_table}'
                    )
                """)
                board_exists = cur.fetchone()[0]
                
                if not board_exists:
                    cur.execute(board_table_sql)
                    logger.info(f"创建行业板块表 {self.board_table}")
                else:
                    logger.info(f"行业板块表 {self.board_table} 已存在")
                
                # 检查成分股表
                cur.execute(f"""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_name = '{self.member_table}'
                    )
                """)
                member_exists = cur.fetchone()[0]
                
                if not member_exists:
                    cur.execute(member_table_sql)
                    logger.info(f"创建成分股表 {self.member_table}")
                else:
                    logger.info(f"成分股表 {self.member_table} 已存在")
                
                conn.commit()
                logger.info("数据表初始化完成")

    @retry_on_exception(retries=3, delay=5, backoff=2)
    def fetch_industry_board_data(self) -> pd.DataFrame:
        """获取行业板块数据"""
        try:
            logger.info("获取行业板块数据")
            df = ak.stock_board_industry_name_em()
            time.sleep(1)  # 添加延时避免请求过快
            return df
        except Exception as e:
            logger.error(f"获取行业板块数据失败: {str(e)}")
            raise

    @retry_on_exception(retries=3, delay=5, backoff=2)
    def fetch_industry_members_data(self, symbol: str) -> pd.DataFrame:
        """获取指定行业板块的成分股数据"""
        try:
            logger.info(f"获取行业板块 {symbol} 的成分股数据")
            df = ak.stock_board_industry_cons_em(symbol=symbol)
            time.sleep(1)  # 添加延时避免请求过快
            return df
        except Exception as e:
            logger.error(f"获取行业板块 {symbol} 成分股数据失败: {str(e)}")
            raise

    def process_board_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """处理行业板块数据"""
        if df.empty:
            return pd.DataFrame()
        
        # 添加更新时间
        df['update_time'] = datetime.now()
        
        # 重命名列名以符合数据库字段（处理特殊字符）
        df = df.rename(columns={
            '领涨股票-涨跌幅': '领涨股票_涨跌幅'
        })
        
        return df

    def process_member_data(self, df: pd.DataFrame, board_code: str, board_name: str) -> pd.DataFrame:
        """处理成分股数据"""
        if df.empty:
            return pd.DataFrame()
        
        # 添加更新时间和板块信息
        df['update_time'] = datetime.now()
        df['板块代码'] = board_code
        df['板块名称'] = board_name
        
        # 重命名列名以符合数据库字段
        df = df.rename(columns={
            '市盈率-动态': '市盈率_动态'
        })
        
        return df

    def save_board_data(self, df: pd.DataFrame):
        """保存行业板块数据到数据库"""
        if df.empty:
            return
            
        with self.get_db_connection() as conn:
            with conn.cursor() as cur:
                try:
                    # 获取列名
                    columns = df.columns.tolist()
                    values = [tuple(x) for x in df.values]
                    
                    # 直接插入数据（每次运行都是新的时间戳）
                    insert_sql = f"""
                        INSERT INTO {self.board_table} ({','.join(columns)}) 
                        VALUES %s
                    """
                    execute_values(cur, insert_sql, values)
                    
                    conn.commit()
                    logger.info(f"成功保存行业板块数据 {len(df)} 条记录")
                    
                except Exception as e:
                    conn.rollback()
                    logger.error(f"保存行业板块数据失败: {str(e)}")
                    raise

    def save_member_data(self, df: pd.DataFrame):
        """保存成分股数据到数据库"""
        if df.empty:
            return
            
        with self.get_db_connection() as conn:
            with conn.cursor() as cur:
                try:
                    # 获取列名
                    columns = df.columns.tolist()
                    values = [tuple(x) for x in df.values]
                    
                    # 直接插入数据（每次运行都是新的时间戳）
                    insert_sql = f"""
                        INSERT INTO {self.member_table} ({','.join(columns)}) 
                        VALUES %s
                    """
                    execute_values(cur, insert_sql, values)
                    
                    conn.commit()
                    logger.info(f"成功保存成分股数据 {len(df)} 条记录")
                    
                except Exception as e:
                    conn.rollback()
                    logger.error(f"保存成分股数据失败: {str(e)}")
                    raise

    def collect_all_data(self, include_members: bool = True):
        """采集所有行业板块数据"""
        try:
            # 1. 获取行业板块列表
            board_df = self.fetch_industry_board_data()
            
            if board_df is not None and not board_df.empty:
                # 处理并保存板块数据
                board_df = self.process_board_data(board_df)
                self.save_board_data(board_df)
                
                logger.info(f"成功获取 {len(board_df)} 个行业板块")
                
                if include_members:
                    # 2. 获取每个板块的成分股数据
                    all_member_data = []
                    
                    for index, row in board_df.iterrows():
                        board_code = row['板块代码']
                        board_name = row['板块名称']
                        
                        try:
                            # 使用板块代码获取成分股
                            member_df = self.fetch_industry_members_data(board_code)
                            
                            if member_df is not None and not member_df.empty:
                                member_df = self.process_member_data(member_df, board_code, board_name)
                                all_member_data.append(member_df)
                                logger.info(f"获取板块 {board_name}({board_code}) 成分股 {len(member_df)} 只")
                            else:
                                logger.warning(f"板块 {board_name}({board_code}) 没有成分股数据")
                                
                        except Exception as e:
                            logger.error(f"处理板块 {board_name}({board_code}) 成分股时出错: {str(e)}")
                            continue
                    
                    # 3. 批量保存所有成分股数据
                    if all_member_data:
                        combined_member_df = pd.concat(all_member_data, ignore_index=True)
                        self.save_member_data(combined_member_df)
                        logger.info(f"成功保存所有成分股数据，共 {len(combined_member_df)} 条记录")
                    else:
                        logger.warning("没有获取到任何成分股数据")
                        
            else:
                logger.error("没有获取到行业板块数据")
                
        except Exception as e:
            logger.error(f"采集数据时出错: {str(e)}")
            raise

def process_board_batch(db_params: Dict, board_batch: List[tuple], worker_id: int):
    """处理一批板块的成分股数据"""
    try:
        collector = IndustryBoardCollector(db_params)
        logger.info(f"Worker {worker_id}: 开始处理 {len(board_batch)} 个板块的成分股")
        
        all_member_data = []
        
        for i, (board_code, board_name) in enumerate(board_batch):
            try:
                member_df = collector.fetch_industry_members_data(board_code)
                
                if member_df is not None and not member_df.empty:
                    member_df = collector.process_member_data(member_df, board_code, board_name)
                    all_member_data.append(member_df)
                    logger.info(f"Worker {worker_id}: 获取板块 {board_name}({board_code}) 成分股 {len(member_df)} 只 ({i+1}/{len(board_batch)})")
                else:
                    logger.warning(f"Worker {worker_id}: 板块 {board_name}({board_code}) 没有成分股数据")
                    
            except Exception as e:
                logger.error(f"Worker {worker_id}: 处理板块 {board_name}({board_code}) 时出错: {str(e)}")
                continue
        
        # 保存该批次的成分股数据
        if all_member_data:
            combined_member_df = pd.concat(all_member_data, ignore_index=True)
            collector.save_member_data(combined_member_df)
            logger.info(f"Worker {worker_id}: 成功保存 {len(combined_member_df)} 条成分股记录")
        
    except Exception as e:
        logger.error(f"Worker {worker_id} 处理批次时出错: {str(e)}")

def chunk_list(lst, n):
    """将列表分成n个大小相近的块"""
    k, m = divmod(len(lst), n)
    return [lst[i * k + min(i, m):(i + 1) * k + min(i + 1, m)] for i in range(n)]

def main():
    parser = argparse.ArgumentParser(description='东方财富行业板块数据采集工具')
    parser.add_argument(
        '--board_only',
        action='store_true',
        help='只采集板块信息，不采集成分股'
    )
    parser.add_argument(
        '--processes',
        type=int,
        default=1,
        help='并行进程数（用于采集成分股）'
    )
    parser.add_argument(
        '--interval',
        type=int,
        default=0,
        help='定时采集间隔（分钟），0表示只运行一次'
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
    
    def run_collection():
        try:
            collector = IndustryBoardCollector(db_params)
            collector.init_tables()
            
            if args.board_only:
                # 只采集板块信息
                logger.info("开始采集行业板块信息（不包含成分股）")
                collector.collect_all_data(include_members=False)
            else:
                if args.processes == 1:
                    # 单进程采集全部数据
                    logger.info("开始采集行业板块和成分股数据（单进程）")
                    collector.collect_all_data(include_members=True)
                else:
                    # 多进程采集成分股数据
                    logger.info(f"开始采集行业板块和成分股数据（{args.processes}进程）")
                    
                    # 先采集板块信息
                    board_df = collector.fetch_industry_board_data()
                    if board_df is not None and not board_df.empty:
                        board_df = collector.process_board_data(board_df)
                        collector.save_board_data(board_df)
                        
                        # 准备板块列表用于多进程处理成分股
                        board_list = [(row['板块代码'], row['板块名称']) for _, row in board_df.iterrows()]
                        board_batches = chunk_list(board_list, args.processes)
                        
                        # 创建进程池处理成分股
                        with multiprocessing.Pool(processes=args.processes) as pool:
                            tasks = [(db_params, batch, i) for i, batch in enumerate(board_batches)]
                            pool.starmap(process_board_batch, tasks)
                    else:
                        logger.error("没有获取到行业板块数据")
            
            logger.info("数据采集完成")
            
        except Exception as e:
            logger.error(f"程序运行出错: {str(e)}")
            raise
    
    try:
        if args.interval > 0:
            # 定时运行模式
            logger.info(f"启动定时采集模式，间隔 {args.interval} 分钟")
            while True:
                try:
                    start_time = datetime.now()
                    logger.info(f"开始第 {start_time.strftime('%Y-%m-%d %H:%M:%S')} 轮采集")
                    
                    run_collection()
                    
                    end_time = datetime.now()
                    duration = (end_time - start_time).total_seconds()
                    logger.info(f"本轮采集完成，耗时 {duration:.2f} 秒")
                    
                    # 等待下次采集
                    sleep_seconds = args.interval * 60
                    logger.info(f"等待 {args.interval} 分钟后进行下次采集...")
                    time.sleep(sleep_seconds)
                    
                except KeyboardInterrupt:
                    logger.info("定时采集被用户中断")
                    break
                except Exception as e:
                    logger.error(f"定时采集出错: {str(e)}")
                    logger.info(f"等待 {args.interval} 分钟后重试...")
                    time.sleep(args.interval * 60)
        else:
            # 单次运行模式
            run_collection()
            
    except KeyboardInterrupt:
        logger.info("程序被用户中断")
    finally:
        logger.info("程序运行完成")

if __name__ == "__main__":
    multiprocessing.freeze_support()
    main()
