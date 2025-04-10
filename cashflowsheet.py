import multiprocessing
from concurrent.futures import ThreadPoolExecutor, as_completed
from sqlalchemy import create_engine, Column, String, Float, DateTime, Integer, text
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy.dialects.postgresql import insert
from datetime import datetime
import pandas as pd
import akshare as ak
import logging
import argparse
import sys
import time
import random
from typing import Dict, List
from retrying import retry

# 设置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('cash_flow_sheet.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# 创建 Base
Base = declarative_base()

class CashFlowSheet(Base):
    __tablename__ = 'cash_flow_sheet'
    
    # 复合主键
    symbol = Column(String(10), primary_key=True)  # 股票代码
    report_date = Column(String(10), primary_key=True)  # 报告期
    
    # 基本信息
    security_code = Column(String(10))  # 证券代码
    security_name_abbr = Column(String(50))  # 证券简称
    
    # 经营活动产生的现金流量
    sales_services = Column(Float)  # 销售商品、提供劳务收到的现金
    tax_refund = Column(Float)  # 收到的税费返还
    other_operate_received = Column(Float)  # 收到其他与经营活动有关的现金
    total_operate_received = Column(Float)  # 经营活动现金流入小计
    goods_services_received = Column(Float)  # 购买商品、接受劳务支付的现金
    employee_received = Column(Float)  # 支付给职工以及为职工支付的现金
    tax_payments = Column(Float)  # 支付的各项税费
    other_operate_payments = Column(Float)  # 支付其他与经营活动有关的现金
    total_operate_payments = Column(Float)  # 经营活动现金流出小计
    operate_net_cash_flow = Column(Float)  # 经营活动产生的现金流量净额

    # 投资活动产生的现金流量
    invest_withdrawal = Column(Float)  # 收回投资收到的现金
    invest_income = Column(Float)  # 取得投资收益收到的现金
    fix_asset_disposal = Column(Float)  # 处置固定资产、无形资产和其他长期资产收回的现金净额
    subsidiary_received = Column(Float)  # 处置子公司及其他营业单位收到的现金净额
    other_invest_received = Column(Float)  # 收到其他与投资活动有关的现金
    total_invest_received = Column(Float)  # 投资活动现金流入小计
    fix_asset_acquisition = Column(Float)  # 购建固定资产、无形资产和其他长期资产支付的现金
    invest_payments = Column(Float)  # 投资支付的现金
    subsidiary_payments = Column(Float)  # 取得子公司及其他营业单位支付的现金净额
    other_invest_payments = Column(Float)  # 支付其他与投资活动有关的现金
    total_invest_payments = Column(Float)  # 投资活动现金流出小计
    invest_net_cash_flow = Column(Float)  # 投资活动产生的现金流量净额

    # 筹资活动产生的现金流量
    accept_invest_received = Column(Float)  # 吸收投资收到的现金
    subsidiary_accept_invest = Column(Float)  # 子公司吸收少数股东投资收到的现金
    loan_received = Column(Float)  # 取得借款收到的现金
    bond_issue = Column(Float)  # 发行债券收到的现金
    other_finance_received = Column(Float)  # 收到其他与筹资活动有关的现金
    total_finance_received = Column(Float)  # 筹资活动现金流入小计
    loan_repayment = Column(Float)  # 偿还债务支付的现金
    dividend_interest_payments = Column(Float)  # 分配股利、利润或偿付利息支付的现金
    subsidiary_dividend_payments = Column(Float)  # 子公司支付给少数股东的股利、利润
    other_finance_payments = Column(Float)  # 支付其他与筹资活动有关的现金
    total_finance_payments = Column(Float)  # 筹资活动现金流出小计
    finance_net_cash_flow = Column(Float)  # 筹资活动产生的现金流量净额

    # 汇率变动对现金的影响
    exchange_rate_effects = Column(Float)  # 汇率变动对现金及现金等价物的影响

    # 现金及现金等价物净增加
    cash_equivalent_increase = Column(Float)  # 现金及现金等价物净增加额
    begin_cash_equivalent = Column(Float)  # 期初现金及现金等价物余额
    end_cash_equivalent = Column(Float)  # 期末现金及现金等价物余额

    # 时间戳
    create_time = Column(DateTime, default=datetime.now)
    update_time = Column(DateTime, default=datetime.now, onupdate=datetime.now)
    
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
        collector = CashFlowSheetCollector(db_config)
        total_stocks = len(stock_batch)
        
        logger.info(f"Batch {batch_id}: Starting processing {total_stocks} stocks")
        
        success_count = 0
        error_count = 0
        
        for idx, symbol in enumerate(stock_batch, 1):
            try:
                time.sleep(random.uniform(1, 3))
                
                # 获取数据
                df = ak.stock_cash_flow_sheet_by_report_em(symbol=symbol)
                if df is not None and not df.empty:
                    df = collector.process_data(df, symbol)
                    collector.save_to_database(df)
                    
                    success_count += 1
                    logger.info(f"Batch {batch_id} Progress: {idx}/{total_stocks} - Successfully processed {symbol}")
                else:
                    logger.warning(f"Batch {batch_id}: No cash flow data available for {symbol}")
                
            except Exception as e:
                error_count += 1
                logger.error(f"Batch {batch_id}: Error processing {symbol}: {str(e)}")
                continue
                
        logger.info(f"Batch {batch_id} completed. Success: {success_count}, Errors: {error_count}")
        
    except Exception as e:
        logger.error(f"Batch {batch_id}: Fatal error in batch processing: {str(e)}")

class CashFlowSheetCollector:
    def __init__(self, db_config: Dict):
        self.db_config = db_config  # 保存db_config作为实例变量
        self.db_url = f"postgresql://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}"
        self.engine = create_engine(self.db_url)
        self.Session = sessionmaker(bind=self.engine)

    # 重试配置
        self.max_retries = 3
        self.min_wait = 1  # 最小等待时间（秒）
        self.max_wait = 5  # 最大等待时间（秒）

    @retry(stop_max_attempt_number=3, wait_random_min=2000, wait_random_max=5000)
    def process_data(self, df: pd.DataFrame, symbol: str) -> pd.DataFrame:
        """处理原始数据"""
        if df.empty:
            return pd.DataFrame()
            
        # 打印原始列名，用于调试
        logger.debug(f"Original columns: {df.columns.tolist()}")
        logger.debug(f"Data sample:\n{df.head()}")
        
        # 重命名列（根据实际的 akshare 返回数据调整）
        column_mapping = {
            'SECURITY_CODE': 'security_code',
            'SECURITY_NAME_ABBR': 'security_name_abbr',
            'REPORT_DATE': 'report_date',
            'SALES_SERVICES': 'sales_services',
            'RECEIVE_TAX_REFUND': 'tax_refund',
            'RECEIVE_OTHER_OPERATE': 'other_operate_received',
            'TOTAL_OPERATE_INFLOW': 'total_operate_received',
            'BUY_SERVICES': 'goods_services_received',
            'PAY_STAFF_CASH': 'employee_received',
            'PAY_ALL_TAX': 'tax_payments',
            'PAY_OTHER_OPERATE': 'other_operate_payments',
            'TOTAL_OPERATE_OUTFLOW': 'total_operate_payments',
            'NETCASH_OPERATE': 'operate_net_cash_flow',
            'WITHDRAW_INVEST': 'invest_withdrawal',
            'RECEIVE_INVEST_INCOME': 'invest_income',
            'DISPOSAL_LONG_ASSET': 'fix_asset_disposal',
            'DISPOSAL_SUBSIDIARY_OTHER': 'subsidiary_received',
            'RECEIVE_OTHER_INVEST': 'other_invest_received',
            'TOTAL_INVEST_INFLOW': 'total_invest_received',
            'CONSTRUCT_LONG_ASSET': 'fix_asset_acquisition',
            'INVEST_PAY_CASH': 'invest_payments',
            'OBTAIN_SUBSIDIARY_OTHER': 'subsidiary_payments',
            'PAY_OTHER_INVEST': 'other_invest_payments',
            'TOTAL_INVEST_OUTFLOW': 'total_invest_payments',
            'NETCASH_INVEST': 'invest_net_cash_flow',
            'ACCEPT_INVEST_CASH': 'accept_invest_received',
            'SUBSIDIARY_ACCEPT_INVEST': 'subsidiary_accept_invest',
            'RECEIVE_LOAN_CASH': 'loan_received',
            'ISSUE_BOND': 'bond_issue',
            'RECEIVE_OTHER_FINANCE': 'other_finance_received',
            'TOTAL_FINANCE_INFLOW': 'total_finance_received',
            'PAY_DEBT_CASH': 'loan_repayment',
            'ASSIGN_DIVIDEND_PORFIT': 'dividend_interest_payments',
            'SUBSIDIARY_PAY_DIVIDEND': 'subsidiary_dividend_payments',
            'PAY_OTHER_FINANCE': 'other_finance_payments',
            'TOTAL_FINANCE_OUTFLOW': 'total_finance_payments',
            'NETCASH_FINANCE': 'finance_net_cash_flow',
            'RATE_CHANGE_EFFECT': 'exchange_rate_effects',
            'CCE_ADD': 'cash_equivalent_increase',
            'BEGIN_CCE': 'begin_cash_equivalent',
            'END_CCE': 'end_cash_equivalent'
        }

        
         # 创建结果 DataFrame
        result_df = pd.DataFrame()
        
        # 添加 symbol 列
        result_df['symbol'] = [symbol] * len(df)
        
        # 复制必要的列
        for col in ['REPORT_DATE', 'SECURITY_CODE', 'SECURITY_NAME_ABBR']:
            if col in df.columns:
                result_df[column_mapping[col]] = df[col]
            else:
                result_df[column_mapping[col]] = None
        
        # 处理其他数值列
        for original_col, new_col in column_mapping.items():
            if original_col not in ['REPORT_DATE', 'SECURITY_CODE', 'SECURITY_NAME_ABBR']:
                if original_col in df.columns:
                    result_df[new_col] = df[original_col]
                else:
                    result_df[new_col] = 0
                    logger.debug(f"Column {original_col} not found in data, filling with 0")
        
        # 确保所有必要的列都存在
        for col in column_mapping.values():
            if col not in result_df.columns:
                result_df[col] = 0
                
        logger.info(f"处理完成，共 {len(result_df)} 条记录")
        return result_df

    def save_to_database(self, df: pd.DataFrame):
        """保存数据到数据库"""
        if df.empty:
            return
            
        session = self.Session()
        try:
            update_columns = [c.name for c in CashFlowSheet.__table__.columns 
                            if c.name not in ['symbol', 'report_date', 'create_time']]
            
            stmt = insert(CashFlowSheet).values(df.to_dict('records'))
            update_dict = {col: stmt.excluded[col] for col in update_columns}
            update_dict['update_time'] = datetime.now()
            
            stmt = stmt.on_conflict_do_update(
                constraint=CashFlowSheet.__table__.primary_key,
                set_=update_dict
            )
            
            session.execute(stmt)
            session.commit()
            logger.info(f"成功保存 {len(df)} 条记录")
        except Exception as e:
            session.rollback()
            logger.error(f"数据保存失败: {str(e)}")
            raise
        finally:
            session.close()
    def get_stock_list(self) -> List[str]:
        """获取A股股票列表"""
        try:
            # 使用 akshare 获取股票列表
            stock_df = ak.stock_info_a_code_name()
            
            # 添加市场标识
            def add_market_prefix(code):
                if code.startswith('6'):
                    return f"SH{code}"
                return f"SZ{code}"
            
            stock_list = [add_market_prefix(code) for code in stock_df['code']]
            logger.info(f"成功获取股票列表，共 {len(stock_list)} 只股票")
            return stock_list
        except Exception as e:
            logger.error(f"获取股票列表失败: {str(e)}")
            raise

    def initial_data_collection(self, num_processes=10):
        """并行初始化数据收集"""
        stocks = self.get_stock_list()
        total_stocks = len(stocks)
        
        if total_stocks == 0:
            logger.error("No stocks retrieved, cannot proceed with data collection")
            return
        
        logger.info(f"Starting parallel initial cash flow data collection for {total_stocks} stocks using {num_processes} processes")
        
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

    def incremental_update(self, num_processes=10):
        """并行增量更新数据"""
        stock_list = self.get_7days_stocks()
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

def main():
    parser = argparse.ArgumentParser(description='现金流量表数据采集工具')
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

    # 数据库配置
    db_config = {
        'host': '192.168.50.149',
        'port': 5432,
        'user': 'postgres',
        'password': '12',
        'database': 'Financialdata'
    }
    
    try:
        collector = CashFlowSheetCollector(db_config)
        
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
