import multiprocessing
from concurrent.futures import ThreadPoolExecutor, as_completed
from sqlalchemy import create_engine, Column, String, Float, DateTime, Integer,text
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy.dialects.postgresql import insert
from datetime import datetime
import time
import random
from typing import List, Dict
import pandas as pd
from retrying import retry
import logging
import akshare as ak
import sys
import argparse

# 设置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('profit_sheet_collector.log'),
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

def process_stock_batch(db_config: Dict, stock_batch: List[str], batch_id: int):
    """处理一批股票的数据采集"""
    if not stock_batch:
        logger.warning(f"Batch {batch_id}: Empty stock batch, skipping")
        return
        
    try:
        collector = ProfitSheetCollector(db_config)
        total_stocks = len(stock_batch)
        
        logging.info(f"Batch {batch_id}: Starting processing {total_stocks} stocks")
        
        success_count = 0
        error_count = 0
        
        for idx, symbol in enumerate(stock_batch, 1):
            try:
                time.sleep(random.uniform(1, 3))
                
                df = collector.fetch_profit_sheet_data(symbol)
                if df is not None and not df.empty:
                    records = collector.process_data(df, symbol)
                    collector.upsert_records(records)
                    
                    success_count += 1
                    logging.info(f"Batch {batch_id} Progress: {idx}/{total_stocks} - Successfully processed {symbol}")
                else:
                    logging.warning(f"Batch {batch_id}: No profit sheet data available for {symbol}")
                
            except Exception as e:
                error_count += 1
                logger.error(f"Batch {batch_id}: Error processing {symbol}: {str(e)}")
                continue
                
        logger.info(f"Batch {batch_id} completed. Success: {success_count}, Errors: {error_count}")
        
    except Exception as e:
        logger.error(f"Batch {batch_id}: Fatal error in batch processing: {str(e)}")

Base = declarative_base()

class ProfitSheet(Base):
    __tablename__ = 'profit_sheet'
    
    # 主键
    symbol = Column(String, primary_key=True)  # 股票代码
    report_date = Column(String, primary_key=True)  # 报告期
    
    # 基础信息
    security_name = Column(String)  # 股票名称
    
    # 利润表主要科目（单位：亿元）
    total_operate_income = Column(Float)      # 营业总收入
    operate_income = Column(Float)            # 营业收入
    total_operate_cost = Column(Float)        # 营业总成本
    operate_cost = Column(Float)              # 营业成本
    sale_expense = Column(Float)              # 销售费用
    manage_expense = Column(Float)            # 管理费用
    finance_expense = Column(Float)           # 财务费用
    operate_profit = Column(Float)            # 营业利润
    total_profit = Column(Float)              # 利润总额
    income_tax = Column(Float)                # 所得税费用
    netprofit = Column(Float)                 # 净利润
    parent_netprofit = Column(Float)          # 归属于母公司股东的净利润
    deduct_parent_netprofit = Column(Float)   # 扣除非经常性损益后的净利润
    basic_eps = Column(Float)                 # 基本每股收益
    diluted_eps = Column(Float)               # 稀释每股收益
    
    # 时间戳
    create_time = Column(DateTime, default=datetime.now)
    update_time = Column(DateTime, default=datetime.now, onupdate=datetime.now)

class ProfitSheetCollector:
    def __init__(self, db_config: Dict):
        self.db_config = db_config  # 保存db_config作为实例变量
        self.db_url = f"postgresql://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}"
        self.engine = create_engine(self.db_url)
        self.Session = sessionmaker(bind=self.engine)

    def initial_data_collection(self, num_processes=10):
        """并行初始化数据收集"""
        stocks = self.get_all_stocks()
        total_stocks = len(stocks)
        
        if total_stocks == 0:
            logger.error("No stocks retrieved, cannot proceed with data collection")
            return
        
        logger.info(f"Starting parallel initial profit sheet data collection for {total_stocks} stocks using {num_processes} processes")
        
        # 调整进程数，确保不会超过股票数量
        num_processes = min(num_processes, total_stocks)
        
        try:
            # 将股票列表分成num_processes份
            stock_batches = chunks(stocks, num_processes)
            
            # 创建任务列表
            tasks = []
            for i, batch in enumerate(stock_batches):
                if batch:  # 确保批次不为空
                    tasks.append((self.db_config, batch, i))
            
            if not tasks:
                logger.error("No valid tasks created")
                return
                
            logger.info(f"Created {len(tasks)} tasks for processing")
            
            # 创建进程池
            with multiprocessing.Pool(processes=num_processes) as pool:
                # 使用进程池并行处理每个批次
                pool.starmap(process_stock_batch, tasks)
                
            logger.info("All batches completed")
            
        except Exception as e:
            logger.error(f"Error in initial_data_collection: {str(e)}")
            raise
    
    @retry(stop_max_attempt_number=3, wait_random_min=2000, wait_random_max=5000)
    def fetch_profit_sheet_data(self, symbol: str) -> pd.DataFrame:
        """从东方财富获取利润表数据"""
        try:
            # 转换股票代码格式（如：600519.SH -> SH600519）
            market = 'SH' if symbol.startswith('6') else 'SZ'
            code = symbol.replace('.SH', '').replace('.SZ', '')
            em_symbol = f"{market}{code}"
            
            df = ak.stock_profit_sheet_by_report_em(symbol=em_symbol)
            logger.info(f"Successfully fetched profit sheet data for {symbol}")
            return df
        except Exception as e:
            logger.error(f"Error fetching data for {symbol}: {str(e)}")
            raise

    def process_data(self, df: pd.DataFrame, symbol: str) -> List[Dict]:
        """处理数据，转换为数据库记录格式"""
        records = []
        
        # 定义需要的字段映射
        field_mapping = {
            'TOTAL_OPERATE_INCOME': 'total_operate_income',    # 营业总收入
            'OPERATE_INCOME': 'operate_income',                # 营业收入
            'TOTAL_OPERATE_COST': 'total_operate_cost',       # 营业总成本
            'OPERATE_COST': 'operate_cost',                   # 营业成本
            'SALE_EXPENSE': 'sale_expense',                   # 销售费用
            'MANAGE_EXPENSE': 'manage_expense',               # 管理费用
            'FINANCE_EXPENSE': 'finance_expense',             # 财务费用
            'OPERATE_PROFIT': 'operate_profit',               # 营业利润
            'TOTAL_PROFIT': 'total_profit',                   # 利润总额
            'INCOME_TAX': 'income_tax',                      # 所得税费用
            'NETPROFIT': 'netprofit',                        # 净利润
            'PARENT_NETPROFIT': 'parent_netprofit',          # 归属母公司股东的净利润
            'DEDUCT_PARENT_NETPROFIT': 'deduct_parent_netprofit',  # 扣非净利润
            'BASIC_EPS': 'basic_eps',                        # 基本每股收益
            'DILUTED_EPS': 'diluted_eps',                    # 稀释每股收益
        }
        
        for _, row in df.iterrows():
            record = {
                'symbol': symbol,
                'report_date': row['REPORT_DATE'],
                'security_name': row['SECURITY_NAME_ABBR'],
            }
            
            # 处理所有映射字段
            for source_field, target_field in field_mapping.items():
                value = row.get(source_field)
                # 转换为亿元（如果原始数据是万元）
                if value is not None:
                    value = float(value) / 10000  # 假设原始数据单位为万元
                record[target_field] = value
            
            record['update_time'] = datetime.now()
            records.append(record)
            
        return records

    def upsert_records(self, records: List[Dict]):
        """使用upsert操作更新或插入记录"""
        if not records:
            return
        
        stmt = insert(ProfitSheet.__table__).values(records)
        stmt = stmt.on_conflict_do_update(
            index_elements=['symbol', 'report_date'],
            set_={
                col.name: col
                for col in stmt.excluded
                if col.name not in ['symbol', 'report_date']
            }
        )
        
        with self.Session() as session:
            try:
                session.execute(stmt)
                session.commit()
                logger.info(f"Successfully upserted {len(records)} records")
            except Exception as e:
                session.rollback()
                logger.error(f"Error upserting records: {str(e)}")
                raise e
                
    def incremental_update(self, num_processes=10):
        """并行增量更新数据"""
        stock_list = self.get_7day_stocks()
        total_stocks = len(stock_list)
        
        if total_stocks == 0:
            logger.info("No stocks need to be updated")
            return
            
        logger.info(f"Starting parallel incremental update for {total_stocks} stocks using {num_processes} processes")
        
        # 调整进程数，确保不会超过股票数量
        num_processes = min(num_processes, total_stocks)
        
        try:
            # 将股票列表分成num_processes份
            stock_batches = chunks(stock_list, num_processes)
            
            # 创建任务列表
            tasks = []
            for i, batch in enumerate(stock_batches):
                if batch:
                    tasks.append((self.db_config, batch, i))
            
            if not tasks:
                logger.error("No valid tasks created")
                return
                
            logger.info(f"Created {len(tasks)} tasks for processing")
            
            # 创建进程池
            with multiprocessing.Pool(processes=num_processes) as pool:
                # 使用进程池并行处理每个批次
                pool.starmap(process_stock_batch, tasks)
                
            logger.info("All update batches completed")
            
        except Exception as e:
            logger.error(f"Error in incremental_update: {str(e)}")
            raise
    
    # 其他方法（get_all_stocks, get_stocks_to_update等）与资产负债表采集器相同
    # 可以直接复用之前的代码
    def get_all_stocks(self) -> List[str]:
        """获取所有股票代码"""
        try:
            df = ak.stock_info_a_code_name()
            stock_list = [
                f"{code}.{'SH' if str(code).startswith('6') else 'SZ'}"
                for code in df['code'].tolist()
                if str(code).startswith(('000', '001', '002', '003', '300', '600', '601', '603', '605'))
            ]
            logger.info(f"Successfully retrieved {len(stock_list)} stock codes")
            return stock_list
        except Exception as e:
            logger.error(f"Error getting stock list: {str(e)}")
            return []

def main():
    parser = argparse.ArgumentParser(description='利润表数据采集工具')
    parser.add_argument(
        '--mode',
        choices=['initial', 'update'],
        required=True,
        help='运行模式: initial-首次运行完整采集, update-增量更新'
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

    db_config = {
        'host': '192.168.50.149',
        'port': 5432,
        'user': 'postgres',
        'password': '12',
        'database': 'Financialdata'
    }
    
    try:
        collector = ProfitSheetCollector(db_config)
        
        if args.mode == 'initial':
            logger.info(f"开始并行初始数据采集（使用 {args.processes} 个进程）...")
            collector.initial_data_collection(num_processes=args.processes)
        else:
            logger.info(f"开始并行增量更新（使用 {args.processes} 个进程）...")
            collector.incremental_update(num_processes=args.processes)
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
