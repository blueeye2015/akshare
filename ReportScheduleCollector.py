import akshare as ak
import pandas as pd
from datetime import datetime, timedelta
import psycopg2
from psycopg2.extras import execute_batch
import logging
from typing import Optional, List, Dict
import time

class ReportScheduleCollector:
    def __init__(self, db_params: Dict):
        """初始化收集器
        
        Args:
            db_params: 数据库连接参数
                {
                    "host": "192.168.50.149",
                    "database": "Financialdata",
                    "user": "postgres",
                    "password": "12",
                    "port": 5432
                }
        """
        self.db_params = db_params
        self.setup_logging()
        
    def setup_logging(self):
        """设置日志"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('report_schedule.log'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger('ReportSchedule')

    def get_connection(self):
        """获取数据库连接"""
        return psycopg2.connect(**self.db_params)

    def fetch_report_schedule(self, date_str: str) -> Optional[pd.DataFrame]:
        """获取指定日期的财报发行数据"""
        try:
            df = ak.news_report_time_baidu(date=date_str)
            if df is not None and not df.empty:
                # 将日期字符串转换为datetime对象
                report_date = datetime.strptime(date_str, '%Y%m%d').date()
                df['report_date'] = report_date  # 直接使用date对象而不是pd.to_datetime
                
                df = df.rename(columns={
                    '股票代码': 'stock_code',
                    '交易所': 'exchange',
                    '股票简称': 'stock_name',
                    '财报期': 'report_period'
                })
                return df
            return None
        except Exception as e:
            self.logger.error(f"获取数据失败 {date_str}: {str(e)}")
            return None

    def save_to_database(self, df: pd.DataFrame) -> int:
        """保存数据到数据库"""
        if df is None or df.empty:
            return 0

        insert_query = """
        INSERT INTO report_schedule 
            (stock_code, exchange, stock_name, report_period, report_date)
        VALUES 
            (%s, %s, %s, %s, %s)
        ON CONFLICT (stock_code, report_date) 
        DO UPDATE SET 
            exchange = EXCLUDED.exchange,
            stock_name = EXCLUDED.stock_name,
            report_period = EXCLUDED.report_period;
        """
        
        # 转换数据为列表而不是使用to_records
        data = [
            (
                row.stock_code,
                row.exchange,
                row.stock_name,
                row.report_period,
                row.report_date  # 这里已经是date对象
            )
            for row in df.itertuples()
        ]
        
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    execute_batch(cur, insert_query, data)
                conn.commit()
                return len(data)
        except Exception as e:
            self.logger.error(f"保存数据失败: {str(e)}")
            if hasattr(e, '__cause__'):
                self.logger.error(f"原因: {str(e.__cause__)}")
            return 0

    def collect_date_range(self, start_date: str, end_date: str):
        """收集日期范围内的数据"""
        start = datetime.strptime(start_date, '%Y%m%d')
        end = datetime.strptime(end_date, '%Y%m%d')
        
        current = start
        while current <= end:
            date_str = current.strftime('%Y%m%d')
            self.logger.info(f"开始获取 {date_str} 的数据")
            
            try:
                df = self.fetch_report_schedule(date_str)
                if df is not None and not df.empty:
                    saved_count = self.save_to_database(df)
                    self.logger.info(f"保存了 {saved_count} 条记录")
                else:
                    self.logger.info(f"{date_str} 没有数据")
            except Exception as e:
                self.logger.error(f"处理 {date_str} 时发生错误: {str(e)}")
            
            time.sleep(1)
            current += timedelta(days=1)

def main():
    # 数据库连接参数
    db_params = {
        "host": "192.168.50.149",
        "database": "Financialdata",
        "user": "postgres",
        "password": "12",
        "port": 5432
    }
    
    collector = ReportScheduleCollector(db_params)
    
    # 获取最近一周的数据
    today = datetime.now()
    start_date = (today - timedelta(days=7)).strftime('%Y%m%d')
    end_date = today.strftime('%Y%m%d')
    
    collector.collect_date_range(start_date, end_date)

if __name__ == "__main__":
    main()
