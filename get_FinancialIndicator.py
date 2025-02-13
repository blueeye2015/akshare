from sqlalchemy import create_engine, Column, String, Float, DateTime, Integer, Text, Date, Numeric
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
        logging.FileHandler('financial_data_collector.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

Base = declarative_base()

class FinancialForecast(Base):
    __tablename__ = 'financial_forecast'
    
    id = Column(Integer, primary_key=True)
    symbol = Column(String(10))
    stock_name = Column(String(50))
    report_period = Column(String(8))
    forecast_indicator = Column(String(50))
    forecast_content = Column(Text)
    forecast_value = Column(Numeric(20,2))
    change_ratio = Column(Numeric(10,2))
    change_reason = Column(Text)
    forecast_type = Column(String(20))
    last_year_value = Column(Numeric(20,2))
    announcement_date = Column(Date)
    create_time = Column(DateTime, default=datetime.now)
    update_time = Column(DateTime, default=datetime.now, onupdate=datetime.now)

class FinancialExpress(Base):
    __tablename__ = 'financial_express'
    
    id = Column(Integer, primary_key=True)
    symbol = Column(String(10))
    stock_name = Column(String(50))
    eps = Column(Numeric(10,4))
    revenue = Column(Numeric(20,2))
    revenue_last_year = Column(Numeric(20,2))
    revenue_yoy_ratio = Column(Numeric(10,2))
    revenue_qoq_ratio = Column(Numeric(10,2))
    net_profit = Column(Numeric(20,2))
    net_profit_last_year = Column(Numeric(20,2))
    net_profit_yoy_ratio = Column(Numeric(10,2))
    net_profit_qoq_ratio = Column(Numeric(10,2))
    bps = Column(Numeric(10,2))
    roe = Column(Numeric(10,2))
    industry = Column(String(50))
    announcement_date = Column(Date)
    
    
    report_period = Column(String(8))
    create_time = Column(DateTime, default=datetime.now)
    update_time = Column(DateTime, default=datetime.now, onupdate=datetime.now)

class FinancialDataCollector:
    def __init__(self, db_config: Dict):
        self.db_url = f"postgresql://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}"
        self.engine = create_engine(self.db_url)
        Base.metadata.create_all(self.engine)
        self.Session = sessionmaker(bind=self.engine)
        
    @retry(stop_max_attempt_number=3, wait_random_min=2000, wait_random_max=5000)
    def fetch_forecast_data(self, date: str) -> pd.DataFrame:
        """从东方财富获取业绩预告数据"""
        try:
            df = ak.stock_yjyg_em(date=date)
            logger.info(f"Successfully fetched forecast data for period {date}")
            return df
        except Exception as e:
            logger.error(f"Error fetching forecast data for period {date}: {str(e)}")
            raise

    @retry(stop_max_attempt_number=3, wait_random_min=2000, wait_random_max=5000)
    def fetch_express_data(self, date: str) -> pd.DataFrame:
        """从东方财富获取业绩快报数据"""
        try:
            df = ak.stock_yjkb_em(date=date)
            logger.info(f"Successfully fetched express data for period {date}")
            return df
        except Exception as e:
            logger.error(f"Error fetching express data for period {date}: {str(e)}")
            raise

    def process_forecast_data(self, df: pd.DataFrame, report_period: str) -> List[Dict]:
        """处理业绩预告数据"""
        records = []
        for _, row in df.iterrows():
            try:
                record = {
                    'symbol': row['股票代码'],
                    'stock_name': row['股票简称'],
                    'report_period': report_period,
                    'forecast_indicator': row['预测指标'],
                    'forecast_content': row['业绩变动'],
                    'forecast_value': row['预测数值'],
                    'change_ratio': row['业绩变动幅度'],
                    'change_reason': row['业绩变动原因'],
                    'forecast_type': row['预告类型'],
                    'last_year_value': row['上年同期值'],
                    'announcement_date': pd.to_datetime(row['公告日期']).date(),
                    'update_time': datetime.now()
                }
                records.append(record)
            except Exception as e:
                logger.error(f"Error processing forecast row: {row}, error: {str(e)}")
                continue
        return records

    def process_express_data(self, df: pd.DataFrame, report_period: str) -> List[Dict]:
        """处理业绩快报数据"""
        records = []
        for _, row in df.iterrows():
            try:
                record = {
                    'symbol': row['股票代码'],
                    'stock_name': row['股票简称'],
                    'eps': row['每股收益'],
                    'revenue': row['营业收入-营业收入'],
                    'revenue_last_year': row['营业收入-去年同期'],
                    'revenue_yoy_ratio': row['营业收入-同比增长'],
                    'revenue_qoq_ratio': row['营业收入-季度环比增长'],
                    'net_profit': row['净利润-净利润'],
                    'net_profit_last_year': row['净利润-去年同期'],
                    'net_profit_yoy_ratio': row['净利润-同比增长'],
                    'net_profit_qoq_ratio': row['净利润-季度环比增长'],
                    'bps': row['每股净资产'],
                    'roe': row['净资产收益率'],
                    'industry': row['所处行业'],
                    'announcement_date': pd.to_datetime(row['公告日期']).date(),
            
                   
                    'report_period': report_period,
                    'update_time': datetime.now()
                }
                records.append(record)
            except Exception as e:
                logger.error(f"Error processing express row: {row}, error: {str(e)}")
                continue
        return records

    def upsert_records(self, records: List[Dict], table):
        """使用upsert操作更新或插入记录"""
        if not records:
            return
        
        stmt = insert(table.__table__).values(records)
        
        if table == FinancialForecast:
            constraint = 'financial_forecast_symbol_report_period_forecast_indicator_key'
            exclude_cols = ['symbol', 'report_period', 'forecast_indicator', 'create_time']
        else:
            constraint = 'financial_express_symbol_report_period_key'
            exclude_cols = ['symbol', 'report_period', 'create_time']
            
        stmt = stmt.on_conflict_do_update(
            constraint=constraint,
            set_={
                col.name: col
                for col in stmt.excluded
                if col.name not in exclude_cols
            }
        )
        
        with self.Session() as session:
            try:
                session.execute(stmt)
                session.commit()
                logger.info(f"Successfully upserted {len(records)} records to {table.__tablename__}")
            except Exception as e:
                session.rollback()
                logger.error(f"Error upserting records to {table.__tablename__}: {str(e)}")
                raise e

    def collect_data(self, date: str):
        """收集指定日期的数据"""
        try:
            # 采集业绩预告数据
            forecast_df = self.fetch_forecast_data(date)
            if forecast_df is not None and not forecast_df.empty:
                forecast_records = self.process_forecast_data(forecast_df, date)
                self.upsert_records(forecast_records, FinancialForecast)
                logger.info(f"Successfully collected forecast data for period {date}")
            else:
                logger.warning(f"No forecast data available for period {date}")

            # 采集业绩快报数据
            express_df = self.fetch_express_data(date)
            if express_df is not None and not express_df.empty:
                express_records = self.process_express_data(express_df, date)
                self.upsert_records(express_records, FinancialExpress)
                logger.info(f"Successfully collected express data for period {date}")
            else:
                logger.warning(f"No express data available for period {date}")
                
        except Exception as e:
            logger.error(f"Error collecting data for period {date}: {str(e)}")
            raise

def main():
    parser = argparse.ArgumentParser(description='财务数据采集工具')
    parser.add_argument(
        '--date',
        type=str,
        required=True,
        help='报告期,格式如:20200331'
    )
    
    args = parser.parse_args()
    
    # 数据库配置
    db_config = {
        'host': 'localhost',
        'port': 5432,
        'user': 'postgres',
        'password': '12',
        'database': 'Financialdata'
    }
    
    collector = FinancialDataCollector(db_config)
    
    try:
        collector.collect_data(args.date)
        logger.info("数据采集完成")
    except Exception as e:
        logger.error(f"程序运行出错: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()
