import akshare as ak
import pandas as pd
from datetime import datetime, timedelta
import psycopg2
from psycopg2.extras import execute_values
import logging
from typing import List, Optional
import time

# 设置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('performance_express.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class PerformanceExpressCollector:
    def __init__(self, db_params: dict):
        """初始化数据采集器
        
        Args:
            db_params: 数据库连接参数
        """
        self.db_params = db_params
        self.table_name = 'performance_express'
    
    def get_db_connection(self):
        """获取数据库连接"""
        return psycopg2.connect(**self.db_params)

    def get_latest_report_period(self) -> Optional[str]:
        """获取数据库中最新的报告期"""
        with self.get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(f"""
                    SELECT report_period 
                    FROM {self.table_name}
                    ORDER BY report_period DESC
                    LIMIT 1
                """)
                result = cur.fetchone()
                return result[0] if result else None

    def generate_report_periods(self, start_date: str, end_date: str) -> List[str]:
        """生成报告期列表
        
        Args:
            start_date: 开始日期 YYYYMMDD
            end_date: 结束日期 YYYYMMDD
            
        Returns:
            报告期列表
        """
        periods = []
        start_year = int(start_date[:4])
        end_year = int(end_date[:4])
        
        for year in range(start_year, end_year + 1):
            for month in ['03', '06', '09', '12']:
                period = f"{year}{month}31"
                if start_date <= period <= end_date:
                    periods.append(period)
        
        return periods

    def process_data(self, df: pd.DataFrame, report_period: str) -> pd.DataFrame:
        """处理原始数据
        
        Args:
            df: 原始数据
            report_period: 报告期
            
        Returns:
            处理后的数据
        """
        if df.empty:
            return pd.DataFrame()
            
        # 重命名列
        df = df.rename(columns={
            '股票代码': 'symbol',
            '股票简称': 'stock_name',
            '每股收益': 'eps',
            '营业收入-营业收入': 'revenue',
            '营业收入-去年同期': 'revenue_last_year',
            '营业收入-同比增长': 'revenue_yoy_change',
            '营业收入-季度环比增长': 'revenue_qoq_change',
            '净利润-净利润': 'net_profit',
            '净利润-去年同期': 'net_profit_last_year',
            '净利润-同比增长': 'net_profit_yoy_change',
            '净利润-季度环比增长': 'net_profit_qoq_change',
            '每股净资产': 'bps',
            '净资产收益率': 'roe',
            '所处行业': 'industry',
            '公告日期': 'announce_date',
            '市场板块': 'market_type',
            '证券类型': 'security_type'
        })
        
        # 添加报告期
        df['report_period'] = report_period
        
        # 数值类型转换
        numeric_columns = [
            'eps', 'revenue', 'revenue_last_year', 'revenue_yoy_change',
            'revenue_qoq_change', 'net_profit', 'net_profit_last_year',
            'net_profit_yoy_change', 'net_profit_qoq_change', 'bps', 'roe'
        ]
        
        for col in numeric_columns:
            try:
                df[col] = pd.to_numeric(df[col].str.replace(',', ''), errors='coerce')
            except Exception as e:
                logger.warning(f"列 {col} 数值转换警告: {str(e)}")
        
        # 转换日期格式
        df['announce_date'] = pd.to_datetime(df['announce_date']).dt.date
        
        # 选择需要的列
        columns = [
            'symbol', 'report_period', 'announce_date', 'stock_name',
            'eps', 'revenue', 'revenue_last_year', 'revenue_yoy_change',
            'revenue_qoq_change', 'net_profit', 'net_profit_last_year',
            'net_profit_yoy_change', 'net_profit_qoq_change', 'bps',
            'roe', 'industry', 'market_type', 'security_type'
        ]
        
        return df[columns]

    def save_to_db(self, df: pd.DataFrame):
        """保存数据到数据库
        
        Args:
            df: 处理后的数据
        """
        if df.empty:
            return
            
        with self.get_db_connection() as conn:
            with conn.cursor() as cur:
                # 准备数据
                columns = df.columns.tolist()
                values = [tuple(x) for x in df.values]
                
                # 构建 UPSERT 语句
                insert_stmt = f"""
                    INSERT INTO {self.table_name} ({','.join(columns)})
                    VALUES %s
                    ON CONFLICT (symbol, report_period)
                    DO UPDATE SET
                    {','.join(f"{col}=EXCLUDED.{col}" for col in columns if col not in ['symbol', 'report_period'])}
                """
                
                try:
                    execute_values(cur, insert_stmt, values)
                    conn.commit()
                    logger.info(f"成功保存 {len(df)} 条记录")
                except Exception as e:
                    conn.rollback()
                    logger.error(f"保存数据失败: {str(e)}")
                    raise

    def collect_data(self, mode: str = 'incremental', start_date: str = '20100331'):
        """采集数据
        
        Args:
            mode: 采集模式, 'incremental' 或 'full'
            start_date: 开始日期, 格式 YYYYMMDD
        """
        end_date = datetime.now().strftime('%Y%m%d')
        
        if mode == 'incremental':
            latest_period = self.get_latest_report_period()
            if latest_period:
                start_date = latest_period
        
        report_periods = self.generate_report_periods(start_date, end_date)
        
        for period in report_periods:
            try:
                logger.info(f"开始获取 {period} 的数据")
                df = ak.stock_yjkb_em(date=period)
                
                if not df.empty:
                    df = self.process_data(df, period)
                    self.save_to_db(df)
                    logger.info(f"完成 {period} 的数据获取和保存")
                else:
                    logger.warning(f"{period} 没有数据")
                    
                # 添加延时避免请求过于频繁
                time.sleep(1)
                
            except Exception as e:
                logger.error(f"处理 {period} 失败: {str(e)}")
                continue

def main():
    # 数据库连接参数
    db_config = {
        'host': '192.168.50.149',
        'port': 5432,
        'user': 'postgres',
        'password': '12',
        'database': 'Financialdata'
    }
    
    # 创建采集器实例
    collector = PerformanceExpressCollector(db_config)
    
    # 执行采集
    try:
        # 增量模式
        #.collect_data(mode='incremental')
        
        # 全量模式
        collector.collect_data(mode='full', start_date='20100331')
        
    except Exception as e:
        logger.error(f"数据采集失败: {str(e)}")

if __name__ == '__main__':
    main()
