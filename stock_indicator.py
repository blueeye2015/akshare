import akshare as ak
import pandas as pd
from datetime import datetime
import psycopg2
from psycopg2.extras import execute_values
import logging
from typing import List, Optional
import time
from retrying import retry
import json
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
#from sqlalchemy import text  # 添加这行导入

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
        self.session = self._create_session()
    
    def _create_session(self):
        """创建带有重试机制的HTTP会话"""
        session = requests.Session()
        
        # 配置重试策略
        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        
        return session

    def get_db_connection(self):
        """获取数据库连接"""
        return psycopg2.connect(**self.db_params)

    @retry(stop_max_attempt_number=3, wait_fixed=2000)
    def get_stock_list(self) -> List[str]:
        """获取股票列表，带重试机制"""
        try:
            df = ak.stock_a_indicator_lg(symbol="all")
            if df is None or df.empty:
                logger.warning("获取股票列表返回空数据，重试...")
                raise Exception("Empty stock list")
            return df['code'].tolist()
        except Exception as e:
            logger.error(f"获取股票列表失败: {str(e)}")
            raise

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

    @retry(stop_max_attempt_number=3, wait_fixed=3000)
    def get_stock_data(self, symbol: str) -> Optional[pd.DataFrame]:
        """获取单个股票数据，带重试机制和验证
        
        Args:
            symbol: 股票代码
            
        Returns:
            股票数据DataFrame或None
        """
        try:
            logger.debug(f"正在获取股票 {symbol} 的数据...")
            
            # 获取数据
            df = ak.stock_a_indicator_lg(symbol=symbol)
            
            # 验证数据有效性
            if df is None:
                logger.warning(f"股票 {symbol} 返回None数据")
                return None
                
            if df.empty:
                logger.warning(f"股票 {symbol} 返回空数据")
                return None
                
            # 检查关键列是否存在
            required_columns = ['trade_date']
            if not all(col in df.columns for col in required_columns):
                logger.warning(f"股票 {symbol} 数据缺少必要列: {required_columns}")
                return None
                
            # 检查数据类型
            if not isinstance(df, pd.DataFrame):
                logger.warning(f"股票 {symbol} 返回的数据类型不正确: {type(df)}")
                return None
                
            logger.debug(f"股票 {symbol} 数据获取成功，共 {len(df)} 条记录")
            return df
            
        except json.JSONDecodeError as e:
            logger.warning(f"股票 {symbol} JSON解析失败: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"获取股票 {symbol} 数据时发生错误: {str(e)}")
            raise

    def process_data(self, df: pd.DataFrame, symbol: str) -> pd.DataFrame:
        """处理原始数据
        
        Args:
            df: 原始数据
            symbol: 股票代码
            
        Returns:
            处理后的数据
        """
        if df is None or df.empty:
            return pd.DataFrame()
            
        try:
            # 添加股票代码
            df['symbol'] = symbol
            
            # 确保日期格式正确
            df['trade_date'] = pd.to_datetime(df['trade_date'], errors='coerce')
            
            # 过滤掉无效日期
            df = df.dropna(subset=['trade_date'])
            
            if df.empty:
                logger.warning(f"股票 {symbol} 处理后没有有效数据")
                return pd.DataFrame()
            
            # 转换日期格式
            df['trade_date'] = df['trade_date'].dt.date
            
            # 数值类型转换
            numeric_columns = [
                'pe', 'pe_ttm', 'pb', 'ps', 'ps_ttm',
                'dv_ratio', 'dv_ttm', 'total_mv'
            ]
            
            for col in numeric_columns:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
            
            # 选择需要的列
            available_columns = ['symbol', 'trade_date'] + [col for col in numeric_columns if col in df.columns]
            
            return df[available_columns]
            
        except Exception as e:
            logger.error(f"处理股票 {symbol} 数据时发生错误: {str(e)}")
            return pd.DataFrame()

    def save_to_db(self, df: pd.DataFrame):
        """保存数据到数据库
        
        Args:
            df: 处理后的数据
        """
        if df is None or df.empty:
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
                    ON CONFLICT (symbol, trade_date)
                    DO UPDATE SET
                    {','.join(f"{col}=EXCLUDED.{col}" for col in columns if col not in ['symbol', 'trade_date'])}
                """
                
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
        try:
            stock_list = self.get_stock_list()
            total_stocks = len(stock_list)
            logger.info(f"开始{mode}模式的数据采集，共 {total_stocks} 只股票")
            
            success_count = 0
            error_count = 0
            skipped_count = 0
            
            for i, symbol in enumerate(stock_list, 1):
                try:
                    logger.info(f"开始获取股票 {symbol} 的数据 ({i}/{total_stocks})")
                    
                    # 获取数据
                    df = self.get_stock_data(symbol)
                    
                    if df is not None and not df.empty:
                        # 如果是增量模式，只保留最新数据
                        if mode == 'incremental':
                            latest_date = self.get_latest_trade_date(symbol)
                            if latest_date:
                                df = df[df['trade_date'] > latest_date]
                        
                        if not df.empty:
                            df = self.process_data(df, symbol)
                            if not df.empty:
                                self.save_to_db(df)
                                success_count += 1
                                logger.info(f"完成股票 {symbol} 的数据获取和保存")
                            else:
                                skipped_count += 1
                                logger.info(f"股票 {symbol} 处理后没有有效数据，跳过")
                        else:
                            skipped_count += 1
                            logger.info(f"股票 {symbol} 增量模式下没有新数据，跳过")
                    else:
                        skipped_count += 1
                        logger.warning(f"股票 {symbol} 没有数据")
                    
                    # 添加延时避免请求过于频繁
                    time.sleep(1)
                    
                except Exception as e:
                    error_count += 1
                    logger.error(f"处理股票 {symbol} 失败: {str(e)}")
                    
                    # 如果连续失败次数过多，增加延时
                    if error_count % 10 == 0:
                        logger.warning(f"连续失败 {error_count} 次，增加延时到 5 秒")
                        time.sleep(5)
                    
                    continue
                
                # 每处理100只股票输出一次进度
                if i % 100 == 0:
                    logger.info(f"进度: {i}/{total_stocks}, 成功: {success_count}, 错误: {error_count}, 跳过: {skipped_count}")
            
            logger.info(f"数据采集完成！总计: {total_stocks}, 成功: {success_count}, 错误: {error_count}, 跳过: {skipped_count}")
            
        except Exception as e:
            logger.error(f"数据采集过程中发生严重错误: {str(e)}")
            raise

def main():
    # 数据库连接参数
    db_params = {
        'host': '192.168.50.149',
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
