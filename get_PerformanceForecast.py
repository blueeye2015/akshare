from sqlalchemy import create_engine, Column, String, Float, DateTime, Text
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy.dialects.postgresql import insert
from datetime import datetime, timedelta
import pandas as pd
import akshare as ak
import logging
import argparse
import sys
from typing import Dict, List

# 设置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('performance_forecast.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# 创建 Base
Base = declarative_base()

class PerformanceForecast(Base):
    __tablename__ = 'performance_forecast'
    
    # 复合主键
    symbol = Column(String(10), primary_key=True)
    report_period = Column(String(8), primary_key=True)  # 报告期 YYYYMMDD
    announce_date = Column(String(10), primary_key=True)  # 公告日期 YYYY-MM-DD
    
    # 业绩预告数据
    stock_name = Column(String(50))  # 股票简称
    forecast_indicator = Column(Float)  # 预测指标
    performance_change = Column(Float)  # 业绩变动
    forecast_value = Column(Float)  # 预测数值
    change_rate = Column(Float)  # 业绩变动幅度
    change_reason = Column(Text)  # 业绩变动原因
    forecast_type = Column(String(20))  # 预告类型
    last_year_value = Column(Float)  # 上年同期值
    
    # 时间戳
    create_time = Column(DateTime, default=datetime.now)
    update_time = Column(DateTime, default=datetime.now, onupdate=datetime.now)

class PerformanceForecastCollector:
    def __init__(self, db_config: Dict):
        self.db_url = f"postgresql://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}"
        self.engine = create_engine(self.db_url)
        self.Session = sessionmaker(bind=self.engine)
        

    def get_report_periods(self, start_year: int = 2008) -> List[str]:
        """生成所有需要获取的报告期"""
        current_year = datetime.now().year
        periods = []
        for year in range(start_year, current_year + 1):
            periods.extend([
                f"{year}0331",
                f"{year}0630",
                f"{year}0930",
                f"{year}1231"
            ])
        return periods

    def process_data(self, df: pd.DataFrame, report_period: str) -> pd.DataFrame:
        """处理原始数据"""
        if df.empty:
            return pd.DataFrame()
            
        # 重命名列
        df = df.rename(columns={
            '股票代码': 'symbol',
            '股票简称': 'stock_name',
            '预测指标': 'forecast_indicator',
            '业绩变动': 'performance_change',
            '预测数值': 'forecast_value',
            '业绩变动幅度': 'change_rate',
            '业绩变动原因': 'change_reason',
            '预告类型': 'forecast_type',
            '上年同期值': 'last_year_value',
            '公告日期': 'announce_date'
        })
        
        # 添加报告期
        df['report_period'] = report_period
        
        # 确保日期格式正确
        df['announce_date'] = pd.to_datetime(df['announce_date']).dt.strftime('%Y-%m-%d')
        
        # 数值类型转换
        numeric_columns = ['forecast_indicator', 'performance_change', 'forecast_value', 
                        'change_rate', 'last_year_value']
        for col in numeric_columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # 文本类型转换
        text_columns = ['change_reason', 'forecast_type']
        for col in text_columns:
            df[col] = df[col].astype(str)
        
        # 选择需要的列
        columns = ['symbol', 'report_period', 'announce_date', 'stock_name', 
                'forecast_indicator', 'performance_change', 'forecast_value',
                'change_rate', 'change_reason', 'forecast_type', 'last_year_value']
        
        df = df[columns]
        
        # 去除重复记录，保留最新的记录
        df = df.sort_values('announce_date', ascending=False)
        df = df.drop_duplicates(subset=['symbol', 'report_period', 'announce_date'], keep='first')
        
        return df

    def save_to_database(self, df: pd.DataFrame):
        """保存数据到数据库"""
        if df.empty:
            return
            
        session = self.Session()
        try:
            # 使用 upsert
            stmt = insert(PerformanceForecast).values(df.to_dict('records'))
            stmt = stmt.on_conflict_do_update(
                constraint=PerformanceForecast.__table__.primary_key,
                set_={
                    'stock_name': stmt.excluded.stock_name,
                    'forecast_indicator': stmt.excluded.forecast_indicator,
                    'performance_change': stmt.excluded.performance_change,
                    'forecast_value': stmt.excluded.forecast_value,
                    'change_rate': stmt.excluded.change_rate,
                    'change_reason': stmt.excluded.change_reason,
                    'forecast_type': stmt.excluded.forecast_type,
                    'last_year_value': stmt.excluded.last_year_value,
                    'update_time': datetime.now()
                }
            )
            session.execute(stmt)
            session.commit()
        except Exception as e:
            session.rollback()
            logger.error(f"数据保存失败: {str(e)}")
            raise
        finally:
            session.close()

    def initial_data_collection(self):
        """初始化数据采集"""
        report_periods = self.get_report_periods()
        total = len(report_periods)
        
        for i, period in enumerate(report_periods, 1):
            logger.info(f"正在处理 {period} ({i}/{total})")
            try:
                df = ak.stock_yjyg_em(date=period)
                df = self.process_data(df, period)
                self.save_to_database(df)
                logger.info(f"成功保存 {period} 的数据，共 {len(df)} 条记录")
            except Exception as e:
                logger.error(f"处理 {period} 失败: {str(e)}")
            
    def incremental_update(self):
        """增量更新数据"""
        # 获取最近4个季度的数据
        current_year = datetime.now().year
        periods = [
            f"{current_year}0331",
            f"{current_year}0630",
            f"{current_year}0930",
            f"{current_year}1231",
            f"{current_year-1}1231"  # 去年年报
        ]
        
        for period in periods:
            logger.info(f"更新 {period} 的数据")
            try:
                df = ak.stock_yjyg_em(date=period)
                df = self.process_data(df, period)
                self.save_to_database(df)
                logger.info(f"成功更新 {period} 的数据，共 {len(df)} 条记录")
            except Exception as e:
                logger.error(f"更新 {period} 失败: {str(e)}")

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
        'host': '192.168.50.149',
        'port': 5432,
        'user': 'postgres',
        'password': '12',
        'database': 'Financialdata'
    }
    
    collector = PerformanceForecastCollector(db_config)
    
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
