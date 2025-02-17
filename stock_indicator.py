import akshare as ak
import pandas as pd
from datetime import datetime
import psycopg2
from psycopg2.extras import execute_values
import logging
from typing import List, Optional
import time
from retrying import retry
from sqlalchemy import text  # 添加这行导入

# 设置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('stock_indicator.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class StockIndicatorCollector:
    def __init__(self, db_params: dict):
        """初始化数据采集器
        
        Args:
            db_params: 数据库连接参数
        """
        self.db_params = db_params
        self.table_name = 'stock_indicator'
    
    def get_db_connection(self):
        """获取数据库连接"""
        return psycopg2.connect(**self.db_params)

    def get_stock_list(self) -> List[str]:
        """获取股票列表"""
        try:
            df = ak.stock_a_indicator_lg(symbol="all")
            return df['code'].tolist()
        except Exception as e:
            logger.error(f"获取股票列表失败: {str(e)}")
            return []

    def get_latest_trade_date(self, symbol: str) -> Optional[str]:
        """获取数据库中最新的交易日期
        
        Args:
            symbol: 股票代码
        """
        with self.get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(f"""
                    SELECT trade_date 
                    FROM {self.table_name}
                    WHERE symbol = %s
                    ORDER BY trade_date DESC
                    LIMIT 1
                """, (symbol,))
                result = cur.fetchone()
                return result[0] if result else None

    def process_data(self, df: pd.DataFrame, symbol: str) -> pd.DataFrame:
        """处理原始数据
        
        Args:
            df: 原始数据
            symbol: 股票代码
            
        Returns:
            处理后的数据
        """
        if df.empty:
            return pd.DataFrame()
            
        # 添加股票代码
        df['symbol'] = symbol
        
        # 确保日期格式正确
        df['trade_date'] = pd.to_datetime(df['trade_date']).dt.date
        
        # 数值类型转换
        numeric_columns = [
            'pe', 'pe_ttm', 'pb', 'ps', 'ps_ttm',
            'dv_ratio', 'dv_ttm', 'total_mv'
        ]
        
        for col in numeric_columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # 选择需要的列
        columns = ['symbol', 'trade_date'] + numeric_columns
        
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
                insert_stmt = text("""
                    INSERT INTO {self.table_name} ({','.join(columns)})
                    VALUES %s
                    ON CONFLICT (symbol, trade_date)
                    DO UPDATE SET
                    {','.join(f"{col}=EXCLUDED.{col}" for col in columns if col not in ['symbol', 'trade_date'])}
                """)
                
                try:
                    execute_values(cur, insert_stmt, values)
                    conn.commit()
                    logger.info(f"成功保存 {len(df)} 条记录")
                except Exception as e:
                    conn.rollback()
                    logger.error(f"保存数据失败: {str(e)}")
                    raise

    def collect_data(self, mode: str = 'incremental'):
        """采集数据
        
        Args:
            mode: 采集模式, 'incremental' 或 'full'
        """
        stock_list = self.get_stock_list()
        total_stocks = len(stock_list)
        
        for i, symbol in enumerate(stock_list, 1):
            try:
                logger.info(f"开始获取股票 {symbol} 的数据 ({i}/{total_stocks})")
                
                # 获取数据
                df = ak.stock_a_indicator_lg(symbol=symbol)
                
                if not df.empty:
                    # 如果是增量模式，只保留最新数据
                    if mode == 'incremental':
                        latest_date = self.get_latest_trade_date(symbol)
                        if latest_date:
                            df = df[df['trade_date'] > latest_date]
                    
                    df = self.process_data(df, symbol)
                    self.save_to_db(df)
                    logger.info(f"完成股票 {symbol} 的数据获取和保存")
                else:
                    logger.warning(f"股票 {symbol} 没有数据")
                
                # 添加延时避免请求过于频繁
                time.sleep(1)
                
            except Exception as e:
                logger.error(f"处理股票 {symbol} 失败: {str(e)}")
                continue

def main():
    # 数据库连接参数
    db_params = {
        'host': 'localhost',
        'port': 5432,
        'user': 'postgres',
        'password': '12',
        'database': 'Financialdata'
    }
    
    # 创建采集器实例
    collector = StockIndicatorCollector(db_params)
    
    # 执行采集
    try:
        # 增量模式
        collector.collect_data(mode='incremental')
        
        # 全量模式
        #collector.collect_data(mode='full')
        
    except Exception as e:
        logger.error(f"数据采集失败: {str(e)}")

if __name__ == "__main__":
    main()
