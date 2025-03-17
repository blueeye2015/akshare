from sqlalchemy import create_engine, Column, String, Float, DateTime, Integer, text
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
        logging.FileHandler('balance_sheet_collector.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

Base = declarative_base()

class BalanceSheet(Base):
    __tablename__ = 'balance_sheet'
    
    # 主键
    symbol = Column(String, primary_key=True)  # 股票代码
    report_date = Column(String, primary_key=True)  # 报告期
    
    # 基础信息
    security_name = Column(String)  # 股票名称
    
    # 重要资产负债表科目（单位：亿元）
    total_current_assets = Column(Float)  # 流动资产合计
    total_current_liab = Column(Float)   # 流动负债合计
    goodwill = Column(Float)             # 商誉
    intangible_assets = Column(Float)    # 无形资产
    long_loan = Column(Float)            # 长期借款
    bonds_payable = Column(Float)        # 应付债券
    long_payable = Column(Float)         # 长期应付款
    special_payable = Column(Float)      # 专项应付款
    predict_liab = Column(Float)         # 预计负债
    defer_tax_liab = Column(Float)       # 递延所得税负债
    develop_expense = Column(Float)       # 开发支出
    long_rece = Column(Float)            # 长期应收款
    total_parent_equity = Column(Float)  # 归属于母公司股东权益合计
    preferred_stock = Column(Float)       # 优先股
    perpetual_bond = Column(Float)       # 永续债（其他权益工具）
    accounts_rece = Column(Float)        # 应收账款
    note_rece = Column(Float)            # 应收票据
    other_rece = Column(Float)           # 其他票据
    
    # 时间戳
    create_time = Column(DateTime, default=datetime.now)
    update_time = Column(DateTime, default=datetime.now, onupdate=datetime.now)

class BalanceSheetCollector:
    def __init__(self, db_config: Dict):
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
            'TOTAL_CURRENT_ASSETS': 'total_current_assets',    # 流动资产合计
            'TOTAL_CURRENT_LIAB': 'total_current_liab',       # 流动负债合计
            'GOODWILL': 'goodwill',                          # 商誉
            'INTANGIBLE_ASSET': 'intangible_assets',         # 无形资产
            'LONG_LOAN': 'long_loan',                        # 长期借款
            'BOND_PAYABLE': 'bonds_payable',                 # 应付债券
            'LONG_PAYABLE': 'long_payable',                  # 长期应付款
            'SPECIAL_PAYABLE': 'special_payable',            # 专项应付款
            'PREDICT_LIAB': 'predict_liab',                  # 预计负债
            'DEFER_TAX_LIAB': 'defer_tax_liab',             # 递延所得税负债
            'DEVELOP_EXPENSE': 'develop_expense',            # 开发支出
            'LONG_RECE': 'long_rece',                       # 长期应收款
            'TOTAL_PARENT_EQUITY': 'total_parent_equity',    # 归属于母公司股东权益
            'PREFERRED_STOCK': 'preferred_stock',            # 优先股
            'PERPETUAL_BOND': 'perpetual_bond',             # 永续债
            'ACCOUNTS_RECE': 'accounts_rece',               # 应收账款
            'NOTE_RECE': 'note_rece',                       # 应收票据
            'OTHER_RECE': 'other_rece',                     # 其他票据
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
    
    def initial_data_collection(self):
        """初始化数据收集"""
        stocks = self.get_all_stocks()
        total_stocks = len(stocks)
        
        logger.info(f"Starting initial balance sheet data collection for {total_stocks} stocks")
        
        for idx, symbol in enumerate(stocks, 1):
            try:
                time.sleep(random.uniform(1, 3))
                
                df = self.fetch_balance_sheet_data(symbol)
                if df is not None and not df.empty:
                    records = self.process_data(df,symbol)
                    self.upsert_records(records)
                    
                    logger.info(f"Progress: {idx}/{total_stocks} - Successfully processed {symbol}")
                else:
                    logger.warning(f"No balance sheet data available for {symbol}")
                
            except Exception as e:
                logger.error(f"Error processing {symbol}: {str(e)}")
                continue

    def incremental_update(self):
        """增量更新数据"""
        stocks = self.get_stocks_to_update()
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
    parser = argparse.ArgumentParser(description='业绩预告数据采集工具')
    parser.add_argument(
        '--mode',
        choices=['initial', 'update'],
        required=True,
        help='运行模式: initial-首次运行完整采集, update-增量更新'
    )
    
    try:
        args = parser.parse_args()
    except SystemExit:
        parser.print_help()
        sys.exit(1)

    # 数据库配置
    db_config = {
        'host': 'localhost',
        'port': 5432,
        'user': 'postgres',
        'password': '12',
        'database': 'Financialdata'
    }
    
    collector = BalanceSheetCollector(db_config)
    
    try:
        if args.mode == 'initial':
            logger.info("开始初始数据采集...")
            collector.initial_data_collection()
        else:
            logger.info("开始增量更新...")
            collector.incremental_update()
    except KeyboardInterrupt:
        logger.info("程序被用户中断")
    except Exception as e:
        logger.error(f"程序运行出错: {str(e)}")
        raise
    finally:
        logger.info("程序运行完成")

if __name__ == "__main__":
    main()
