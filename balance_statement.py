from sqlalchemy import create_engine, Column, String, Float, DateTime, Integer, text, Date, Numeric
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
import multiprocessing
from itertools import islice
from functools import partial

# 设置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('balance_sheet_collector.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

Base = declarative_base()

def chunks(lst: List, n: int) -> List[List]:
    """将列表分割成n个大致相等的块"""
    if not lst:
        return []
    
    # 计算每个块的大小
    size = len(lst)
    chunk_size = (size + n - 1) // n  # 向上取整确保覆盖所有元素
    
    # 生成分块
    return [lst[i:i + chunk_size] for i in range(0, size, chunk_size)]

def process_stock_batch(db_config: Dict, stock_batch: List[str], batch_id: int):
    """处理一批股票的数据采集"""
    collector = BalanceSheetCollector(db_config)
    total_stocks = len(stock_batch)
    
    logger.info(f"Batch {batch_id}: Starting processing {total_stocks} stocks")
    
    for idx, symbol in enumerate(stock_batch, 1):
        try:
            time.sleep(random.uniform(1, 3))
            
            df = collector.fetch_balance_sheet_data(symbol)
            if df is not None and not df.empty:
                records = collector.process_data(df, symbol)
                collector.upsert_records(records)
                
                logger.info(f"Batch {batch_id} Progress: {idx}/{total_stocks} - Successfully processed {symbol}")
            else:
                logger.warning(f"Batch {batch_id}: No balance sheet data available for {symbol}")
            
        except Exception as e:
            logger.error(f"Batch {batch_id}: Error processing {symbol}: {str(e)}")
            continue

class BalanceSheet(Base):
    __tablename__ = 'financial_statement'
    
    # 主键
    id = Column(Integer, primary_key=True)  # PostgreSQL的BIGSERIAL
    symbol = Column(String(10), nullable=False)
    security_code = Column(String(10), nullable=False)
    
    # 基础信息
    security_name_abbr = Column(String(50))
    org_code = Column(String(50))
    org_type = Column(String(20))
    report_date = Column(Date, nullable=False)  # 改用Date类型
    report_type = Column(String(20))
    report_date_name = Column(String(50))
    security_type_code = Column(String(20))
    notice_date = Column(Date)  # 改用Date类型
    update_date = Column(DateTime)
    currency = Column(String(10))

    # 资产类项目 - 使用Numeric替代Float以确保精确计算
    monetaryfunds = Column(Numeric(20,4))
    trading_assets = Column(Numeric(20,4))
    notes_receivable = Column(Numeric(20,4))
    accounts_rece = Column(Numeric(20,4))
    financing_rece = Column(Numeric(20,4))
    prepayment = Column(Numeric(20,4))
    other_rece = Column(Numeric(20,4))
    inventory = Column(Numeric(20,4))
    contract_asset = Column(Numeric(20,4))
    current_asset_other = Column(Numeric(20,4))
    current_asset_balance = Column(Numeric(20,4))
    
    # 非流动资产
    fixed_asset = Column(Numeric(20,4))
    cip = Column(Numeric(20,4))
    intangible_asset = Column(Numeric(20,4))
    goodwill = Column(Numeric(20,4))
    long_prepaid_expense = Column(Numeric(20,4))
    defer_tax_asset = Column(Numeric(20,4))
    noncurrent_asset_other = Column(Numeric(20,4))
    noncurrent_asset_balance = Column(Numeric(20,4))
    
    # 负债类项目
    short_loan = Column(Numeric(20,4))
    loan_pbc = Column(Numeric(20,4))
    note_payable = Column(Numeric(20,4))
    accounts_payable = Column(Numeric(20,4))
    advance_receivables = Column(Numeric(20,4))
    contract_liab = Column(Numeric(20,4))
    staff_salary_payable = Column(Numeric(20,4))
    tax_payable = Column(Numeric(20,4))
    other_payable = Column(Numeric(20,4))
    current_liab_other = Column(Numeric(20,4))
    current_liab_balance = Column(Numeric(20,4))

    # 非流动负债
    long_loan = Column(Numeric(20,4))
    bond_payable = Column(Numeric(20,4))
    lease_liab = Column(Numeric(20,4))
    long_payable = Column(Numeric(20,4))
    predict_liab = Column(Numeric(20,4))
    defer_income = Column(Numeric(20,4))
    defer_tax_liab = Column(Numeric(20,4))
    noncurrent_liab_other = Column(Numeric(20,4))
    noncurrent_liab_balance = Column(Numeric(20,4))
    liab_balance = Column(Numeric(20,4))

    # 所有者权益
    share_capital = Column(Numeric(20,4))
    capital_reserve = Column(Numeric(20,4))
    treasury_shares = Column(Numeric(20,4))
    special_reserve = Column(Numeric(20,4))
    surplus_reserve = Column(Numeric(20,4))
    unassign_rpofit = Column(Numeric(20,4))
    minority_equity = Column(Numeric(20,4))
    other_compre_income = Column(Numeric(20,4))
    equity_balance = Column(Numeric(20,4))

    # 特殊项目
    total_assets = Column(Numeric(20,4))
    total_liabilities = Column(Numeric(20,4))
    total_equity = Column(Numeric(20,4))

    # 同比增长率字段
    # monetaryfunds_yoy = Column(Numeric(10,6))
    # total_assets_yoy = Column(Numeric(10,6))
    # total_liabilities_yoy = Column(Numeric(10,6))
    # equity_balance_yoy = Column(Numeric(10,6))

    # 状态字段
    opinion_type = Column(String(50))
    osopinion_type = Column(String(50))
    listing_state = Column(String(20))

    # 时间戳
    created_time = Column(DateTime, server_default='CURRENT_TIMESTAMP')
    update_time = Column(DateTime, server_default='CURRENT_TIMESTAMP')

    
    

class BalanceSheetCollector:
    def __init__(self, db_config: Dict):
        self.db_config = db_config  # 保存db_config作为实例变量
        self.db_url = f"postgresql://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}"
        self.engine = create_engine(self.db_url)
        
        self.Session = sessionmaker(bind=self.engine)
        
    @retry(stop_max_attempt_number=3, wait_random_min=2000, wait_random_max=5000)
    def fetch_balance_sheet_data(self, symbol: str) -> pd.DataFrame:
        """从东方财富获取资产负债表数据"""
        try:
            # 转换股票代码格式（如：600519.SH -> SH600519）
            market = 'SH' if symbol.startswith('6') else 'SZ'
            code = symbol.replace('.SH', '').replace('.SZ', '')
            em_symbol = f"{market}{code}"
            
            df = ak.stock_balance_sheet_by_report_em(symbol=em_symbol)
            logger.info(f"Successfully fetched balance sheet data for {symbol}")
            return df
        except Exception as e:
            logger.error(f"Error fetching data for {symbol}: {str(e)}")
            raise

    def process_data(self, df: pd.DataFrame, symbol: str) -> List[Dict]:
        """处理数据，转换为数据库记录格式"""
        records = []
        
        # 定义需要的字段映射
        field_mapping = {
            # 基础信息字段
            'SECURITY_CODE': 'security_code',
            'SECURITY_NAME_ABBR': 'security_name_abbr',
            'ORG_CODE': 'org_code',
            'ORG_TYPE': 'org_type',
            'REPORT_TYPE': 'report_type',
            'REPORT_DATE_NAME': 'report_date_name',
            'SECURITY_TYPE_CODE': 'security_type_code',
            'CURRENCY': 'currency',
            
            # 资产类项目
            'MONETARYFUNDS': 'monetaryfunds',
            'TRADING_ASSETS': 'trading_assets',
            'NOTES_RECEIVABLE': 'notes_receivable',
            'ACCOUNTS_RECE': 'accounts_rece',
            'FINANCING_RECE': 'financing_rece',
            'PREPAYMENT': 'prepayment',
            'OTHER_RECE': 'other_rece',
            'INVENTORY': 'inventory',
            'CONTRACT_ASSET': 'contract_asset',
            'CURRENT_ASSET_OTHER': 'current_asset_other',
            'CURRENT_ASSET_BALANCE': 'current_asset_balance',
            
            # 非流动资产
            'FIXED_ASSET': 'fixed_asset',
            'CIP': 'cip',
            'INTANGIBLE_ASSET': 'intangible_asset',
            'GOODWILL': 'goodwill',
            'LONG_PREPAID_EXPENSE': 'long_prepaid_expense',
            'DEFER_TAX_ASSET': 'defer_tax_asset',
            'NONCURRENT_ASSET_OTHER': 'noncurrent_asset_other',
            'NONCURRENT_ASSET_BALANCE': 'noncurrent_asset_balance',
            
            # 负债类项目
            'SHORT_LOAN': 'short_loan',
            'LOAN_PBC': 'loan_pbc',
            'NOTE_PAYABLE': 'note_payable',
            'ACCOUNTS_PAYABLE': 'accounts_payable',
            'ADVANCE_RECEIVABLES': 'advance_receivables',
            'CONTRACT_LIAB': 'contract_liab',
            'STAFF_SALARY_PAYABLE': 'staff_salary_payable',
            'TAX_PAYABLE': 'tax_payable',
            'OTHER_PAYABLE': 'other_payable',
            'CURRENT_LIAB_OTHER': 'current_liab_other',
            'CURRENT_LIAB_BALANCE': 'current_liab_balance',
            
            # 非流动负债
            'LONG_LOAN': 'long_loan',
            'BOND_PAYABLE': 'bond_payable',
            'LEASE_LIAB': 'lease_liab',
            'LONG_PAYABLE': 'long_payable',
            'PREDICT_LIAB': 'predict_liab',
            'DEFER_INCOME': 'defer_income',
            'DEFER_TAX_LIAB': 'defer_tax_liab',
            'NONCURRENT_LIAB_OTHER': 'noncurrent_liab_other',
            'NONCURRENT_LIAB_BALANCE': 'noncurrent_liab_balance',
            'LIAB_BALANCE': 'liab_balance',
            
            # 所有者权益
            'SHARE_CAPITAL': 'share_capital',
            'CAPITAL_RESERVE': 'capital_reserve',
            'TREASURY_SHARES': 'treasury_shares',
            'SPECIAL_RESERVE': 'special_reserve',
            'SURPLUS_RESERVE': 'surplus_reserve',
            'UNASSIGN_RPOFIT': 'unassign_rpofit',
            'MINORITY_EQUITY': 'minority_equity',
            'OTHER_COMPRE_INCOME': 'other_compre_income',
            'EQUITY_BALANCE': 'equity_balance',
            
            # 特殊项目
            'TOTAL_ASSETS': 'total_assets',
            'TOTAL_LIABILITIES': 'total_liabilities',
            'TOTAL_EQUITY': 'total_equity',
            
            # 同比增长率字段 (YOY)
            # 'MONETARYFUNDS_YOY': 'monetaryfunds_yoy',
            # 'TOTAL_ASSETS_YOY': 'total_assets_yoy',
            # 'TOTAL_LIABILITIES_YOY': 'total_liabilities_yoy',
            # 'EQUITY_BALANCE_YOY': 'equity_balance_yoy',
            
            # 状态字段
            'OPINION_TYPE': 'opinion_type',
            'OSOPINION_TYPE': 'osopinion_type',
            'LISTING_STATE': 'listing_state'
        }
        
        for _, row in df.iterrows():
            record = {
                'symbol': symbol,
                'report_date': row['REPORT_DATE'],
                'security_name_abbr': row['SECURITY_NAME_ABBR'],
            }
            
            # 处理所有映射字段
            for source_field, target_field in field_mapping.items():
                value = row.get(source_field)
                record[target_field] = value
            
            record['update_time'] = datetime.now()
            records.append(record)
            
        return records

    def upsert_records(self, records: List[Dict]):
        """使用upsert操作更新或插入记录"""
        if not records:
            return
        
        stmt = insert(BalanceSheet.__table__).values(records)
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
        
    def get_7day_stocks(self) -> List[str]:
        """从report_schedule表获取最近一周的股票代码"""
        try:
            with self.Session() as session:
                # 构建查询语句
                query = text("""
                    SELECT DISTINCT stock_code || '.' || exchange as symbol
                    FROM report_schedule 
                    WHERE exchange IN ('SZ', 'SH')
                    AND created_at >= CURRENT_DATE - INTERVAL '7 days'
                    AND stock_code ~ '^[0-9]+$'  -- 确保股票代码只包含数字
                    AND stock_code ~ '^(000|001|002|003|300|600|601|603|605)'  -- 确保是主板、创业板、科创板的股票
                """)
                
                result = session.execute(query)
                stock_list = [row[0] for row in result]
                
                logger.info(f"从report_schedule获取到 {len(stock_list)} 个最近一周的股票代码")
                return stock_list
                
        except Exception as e:
            logger.error(f"从report_schedule获取股票列表时出错: {str(e)}")
            return [] 


    def get_stocks_to_update(self) -> List[str]:
        """获取需要更新的股票代码"""
        try:
            session = self.Session()
            
            # 获取当前日期
            now = datetime.now()
            current_year = now.year
            current_month = now.month
            
            # 确定最新的财报期
            # 一季报（4月底前）：去年年报 + 今年一季报
            # 中报（8月底前）：今年一季报 + 今年中报
            # 三季报（10月底前）：今年中报 + 今年三季报
            # 年报（来年4月底前）：今年三季报 + 今年年报
            if current_month <= 4:  # 1-4月
                report_dates = [
                    f"{current_year-1}-12-31",  # 去年年报
                    f"{current_year}-03-31"     # 今年一季报
                ]
            elif current_month <= 8:  # 5-8月
                report_dates = [
                    f"{current_year}-03-31",    # 今年一季报
                    f"{current_year}-06-30"     # 今年中报
                ]
            elif current_month <= 10:  # 9-10月
                report_dates = [
                    f"{current_year}-06-30",    # 今年中报
                    f"{current_year}-09-30"     # 今年三季报
                ]
            else:  # 11-12月
                report_dates = [
                    f"{current_year}-09-30",    # 今年三季报
                    f"{current_year}-12-31"     # 今年年报
                ]
                
            # 获取所有应该存在的股票代码
            all_stocks = set(self.get_all_stocks())
            
            # 获取数据库中最新两期财报都已更新的股票
            query = text("""
                WITH latest_updates AS (
                    SELECT symbol, report_date, update_time,
                        COUNT(*) OVER (PARTITION BY symbol) as report_count
                    FROM balance_sheet
                        WHERE report_date IN :report_dates
                    )
                    SELECT DISTINCT symbol
                    FROM latest_updates
                    WHERE report_count = 2
                    AND update_time >= CURRENT_DATE - INTERVAL '7 days'
                """)
            
            result = session.execute(query, {'report_dates': tuple(report_dates)})
            updated_stocks = set(row[0] for row in result)
            
            # 需要更新的股票 = 所有股票 - 已更新的股票
            stocks_to_update = list(all_stocks - updated_stocks)
            
            session.close()
            
            logger.info(f"当前检查的报告期: {report_dates}")
            logger.info(f"需要更新的股票数量: {len(stocks_to_update)}")
            logger.info(f"已更新的股票数量: {len(updated_stocks)}")
            
            return stocks_to_update
                
        except Exception as e:
            logger.error(f"获取待更新股票时出错: {str(e)}")
            return []

    # def get_stocks_to_update(self) -> List[str]:
    #     """获取需要更新的股票代码"""
    #     try:
    #         session = self.Session()
            
    #         # 获取当前季度的第一天
    #         now = datetime.now()
    #         current_quarter = (now.month - 1) // 3 + 1
    #         quarter_start = datetime(now.year, (current_quarter - 1) * 3 + 1, 1)
            
    #         query = """
    #             SELECT DISTINCT symbol 
    #             FROM balance_sheet
    #             WHERE update_time < :quarter_start 
    #             OR symbol NOT IN (
    #                 SELECT DISTINCT symbol 
    #                 FROM balance_sheet
    #             )
    #         """
            
    #         result = session.execute(query, {'quarter_start': quarter_start})
    #         stocks_to_update = [row[0] for row in result]
            
    #         session.close()
    #         logger.info(f"Found {len(stocks_to_update)} stocks to update")
    #         return stocks_to_update
            
    #     except Exception as e:
    #         logger.error(f"Error getting stocks to update: {str(e)}")
    #         return []
    
    def initial_data_collection(self, num_processes=10):
        """并行初始化数据收集"""
        stocks = self.get_all_stocks()
        total_stocks = len(stocks)
        
        logger.info(f"Starting parallel initial balance sheet data collection for {total_stocks} stocks using {num_processes} processes")
        
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

    def incremental_update(self):
        """增量更新数据"""
        stocks = self.get_7day_stocks()
        total_stocks = len(stocks)
        
        logger.info(f"Starting incremental update for {total_stocks} stocks")
        
        for idx, symbol in enumerate(stocks, 1):
            try:
                time.sleep(random.uniform(1, 3))
                
                df = self.fetch_balance_sheet_data(symbol)
                if df is not None and not df.empty:
                    records = self.process_data(df,symbol)
                    self.upsert_records(records)
                    
                    logger.info(f"Progress: {idx}/{total_stocks} - Successfully updated {symbol}")
                else:
                    logger.warning(f"No balance sheet data available for {symbol}")
                
            except Exception as e:
                logger.error(f"Error updating {symbol}: {str(e)}")
                continue

def main():
    parser = argparse.ArgumentParser(description='财务报表数据采集工具')
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
    
    collector = BalanceSheetCollector(db_config)
    
    try:
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
    # 确保在 Windows 环境下正确启动多进程
    multiprocessing.freeze_support()
    main()
