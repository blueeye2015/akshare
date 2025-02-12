from sqlalchemy import create_engine, Column, String, Float, DateTime, Integer
from sqlalchemy.orm import declarative_base, sessionmaker  # 更新的导入方式
from sqlalchemy.orm import sessionmaker
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

class FinancialIndicator(Base):
    __tablename__ = 'financial_indicators_ths'
    
    # 复合主键
    symbol = Column(String, primary_key=True)
    report_period = Column(String, primary_key=True)
    
    # 财务指标
    net_profit = Column(Float)
    net_profit_yoy = Column(Float)
    deducted_net_profit = Column(Float)
    deducted_net_profit_yoy = Column(Float)
    total_revenue = Column(Float)
    total_revenue_yoy = Column(Float)
    eps = Column(Float)
    nav_per_share = Column(Float)
    capital_reserve_per_share = Column(Float)
    undistributed_profit_per_share = Column(Float)
    ocf_per_share = Column(Float)
    net_profit_margin = Column(Float)
    gross_profit_margin = Column(Float)
    roe = Column(Float)
    roe_diluted = Column(Float)
    operating_cycle = Column(Float)
    inventory_turnover = Column(Float)
    inventory_days = Column(Float)
    receivables_days = Column(Float)
    current_ratio = Column(Float)
    quick_ratio = Column(Float)
    conservative_quick_ratio = Column(Float)
    equity_ratio = Column(Float)
    debt_asset_ratio = Column(Float)
    
    # 时间戳
    create_time = Column(DateTime, default=datetime.now)
    update_time = Column(DateTime, default=datetime.now, onupdate=datetime.now)

class FinancialDataCollector:
    def __init__(self, db_config: Dict):
        self.db_url = f"postgresql://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}"
        self.engine = create_engine(self.db_url)
        self.Session = sessionmaker(bind=self.engine)
        self.last_update_time = {}  # 用于记录每个股票的最后更新时间
        
    
    @retry(stop_max_attempt_number=3, wait_random_min=2000, wait_random_max=5000)
    def fetch_financial_data(self, symbol: str, indicator: str = "按报告期") -> pd.DataFrame:
        """从同花顺获取财务数据"""
        try:
            # 这里使用 akshare 的接口获取数据
            # 注意：需要替换为实际的同花顺接口
            df = ak.stock_financial_abstract_ths(symbol=symbol, indicator=indicator)
            logger.info(f"Successfully fetched data for {symbol}")
            return df
        except Exception as e:
            logger.error(f"Error fetching data for {symbol}: {str(e)}")
            raise

    def process_data(self, df: pd.DataFrame, symbol: str) -> List[Dict]:
        """处理数据，转换为数据库记录格式"""
        records = []
        for _, row in df.iterrows():
            record = {
                'symbol': symbol,
                'report_period': row['报告期'],
                'net_profit': row.get('净利润'),
                'net_profit_yoy': row.get('净利润同比增长率'),
                'deducted_net_profit': row.get('扣非净利润'),
                'deducted_net_profit_yoy': row.get('扣非净利润同比增长率'),
                'total_revenue': row.get('营业总收入'),
                'total_revenue_yoy': row.get('营业总收入同比增长率'),
                'eps': row.get('基本每股收益'),
                'nav_per_share': row.get('每股净资产'),
                'capital_reserve_per_share': row.get('每股资本公积金'),
                'undistributed_profit_per_share': row.get('每股未分配利润'),
                'ocf_per_share': row.get('每股经营现金流'),
                'net_profit_margin': row.get('销售净利率'),
                'gross_profit_margin': row.get('销售毛利率'),
                'roe': row.get('净资产收益率'),
                'roe_diluted': row.get('净资产收益率-摊薄'),
                'operating_cycle': row.get('营业周期'),
                'inventory_turnover': row.get('存货周转率'),
                'inventory_days': row.get('存货周转天数'),
                'receivables_days': row.get('应收账款周转天数'),
                'current_ratio': row.get('流动比率'),
                'quick_ratio': row.get('速动比率'),
                'conservative_quick_ratio': row.get('保守速动比率'),
                'equity_ratio': row.get('产权比率'),
                'debt_asset_ratio': row.get('资产负债率'),
                'update_time': datetime.now()
            }
            records.append(record)
        return records

    def upsert_records(self, records: List[Dict]):
        """使用upsert操作更新或插入记录"""
        if not records:
            return
        
        stmt = insert(FinancialIndicator.__table__).values(records)
        stmt = stmt.on_conflict_do_update(
            index_elements=['symbol', 'report_period'],
            set_={
                col.name: col
                for col in stmt.excluded
                if col.name not in ['symbol', 'report_period']
            }
        )
        
        with self.Session() as session:
            try:
                session.execute(stmt)
                session.commit()
            except Exception as e:
                session.rollback()
                raise e

    def get_all_stocks(self) -> List[str]:
        """获取所有股票代码"""
        try:
            # 获取股票代码和名称
            df = ak.stock_info_a_code_name()
            # 只保留主板、中小板、创业板的股票（排除北交所等）
            stock_list = [
                code for code in df['code'].tolist()
                if code.startswith(('000', '001', '002', '003', '300', '600', '601', '603', '605'))
            ]
            logger.info(f"Successfully retrieved {len(stock_list)} stock codes")
            return stock_list
        except Exception as e:
            logger.error(f"Error getting stock list: {str(e)}")
            return []

    def get_stocks_to_update(self) -> List[str]:
        """获取需要更新的股票代码"""
        try:
            # 获取最近有财报更新的股票
            session = self.Session()
            
            # 获取当前季度的第一天
            now = datetime.now()
            current_quarter = (now.month - 1) // 3 + 1
            quarter_start = datetime(now.year, (current_quarter - 1) * 3 + 1, 1)
            
            # 查询最近一个季度内没有更新的股票
            query = """
                SELECT DISTINCT symbol 
                FROM financial_indicators_ths 
                WHERE update_time < :quarter_start 
                OR symbol NOT IN (
                    SELECT DISTINCT symbol 
                    FROM financial_indicators_ths
                )
            """
            
            result = session.execute(query, {'quarter_start': quarter_start})
            stocks_to_update = [row[0] for row in result]
            
            session.close()
            
            logger.info(f"Found {len(stocks_to_update)} stocks to update")
            return stocks_to_update
            
        except Exception as e:
            logger.error(f"Error getting stocks to update: {str(e)}")
            return []

    def initial_data_collection(self):
        """初始化数据收集"""
        stocks = self.get_all_stocks()
        total_stocks = len(stocks)
        
        logger.info(f"Starting initial data collection for {total_stocks} stocks")
        
        for idx, symbol in enumerate(stocks, 1):
            try:
                # 添加随机延时，避免请求过于频繁
                time.sleep(random.uniform(1, 3))
                
                df = self.fetch_financial_data(symbol)
                if df is not None and not df.empty:
                    records = self.process_data(df, symbol)
                    self.upsert_records(records)
                    
                    logger.info(f"Progress: {idx}/{total_stocks} - Successfully processed {symbol}")
                else:
                    logger.warning(f"No data available for {symbol}")
                
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
                
                df = self.fetch_financial_data(symbol)
                if df is not None and not df.empty:
                    records = self.process_data(df, symbol)
                    self.upsert_records(records)
                    
                    # 更新最后更新时间
                    self.last_update_time[symbol] = datetime.now()
                    
                    logger.info(f"Progress: {idx}/{total_stocks} - Successfully updated {symbol}")
                else:
                    logger.warning(f"No data available for {symbol}")
                
            except Exception as e:
                logger.error(f"Error updating {symbol}: {str(e)}")
                continue

def main():
    # 解析命令行参数
    parser = argparse.ArgumentParser(description='金融数据采集工具')
    parser.add_argument(
        '--mode',
        choices=['initial', 'update'],
        required=True,
        help='运行模式: initial-首次运行完整采集, update-增量更新'
    )
    
    try:
        args = parser.parse_args()
    except SystemExit:
        # 如果没有提供参数，显示帮助信息并退出
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
    
    collector = FinancialDataCollector(db_config)
    
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
