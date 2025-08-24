import akshare as ak
import pandas as pd
from datetime import datetime, timedelta
import psycopg2
from psycopg2.extras import execute_values
import logging
from typing import List, Optional, Dict, Any
import time
from retrying import retry
import json
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import random
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

# 设置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('stock_indicator_enhanced.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class EnhancedStockIndicatorCollector:
    def __init__(self, db_params: dict, max_workers: int = 5):
        """初始化增强版数据采集器
        
        Args:
            db_params: 数据库连接参数
            max_workers: 最大并发工作线程数
        """
        self.db_params = db_params
        self.table_name = 'stock_indicator'
        self.max_workers = max_workers
        self.session = self._create_session()
        self.lock = threading.Lock()
        
        # 统计信息
        self.stats = {
            'total_processed': 0,
            'success_count': 0,
            'error_count': 0,
            'skipped_count': 0,
            'retry_count': 0
        }
    
    def _create_session(self):
        """创建带有重试机制的HTTP会话"""
        session = requests.Session()
        
        # 配置重试策略
        retry_strategy = Retry(
            total=3,
            backoff_factor=2,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["HEAD", "GET", "OPTIONS", "POST"]
        )
        
        adapter = HTTPAdapter(max_retries=retry_strategy, pool_connections=10, pool_maxsize=20)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        
        # 设置超时
        session.timeout = (10, 30)  # (连接超时, 读取超时)
        
        return session

    def get_db_connection(self):
        """获取数据库连接"""
        return psycopg2.connect(**self.db_params)

    @retry(stop_max_attempt_number=3, wait_exponential_multiplier=1000, wait_exponential_max=10000)
    def get_stock_list(self) -> List[str]:
        """获取股票列表，带指数退避重试机制"""
        try:
            logger.info("正在获取股票列表...")
            df = ak.stock_a_indicator_lg(symbol="all")
            
            if df is None or df.empty:
                logger.warning("获取股票列表返回空数据，重试...")
                raise Exception("Empty stock list")
            
            stock_codes = df['code'].tolist()
            logger.info(f"成功获取 {len(stock_codes)} 只股票代码")
            return stock_codes
            
        except Exception as e:
            logger.error(f"获取股票列表失败: {str(e)}")
            raise

    def get_latest_trade_date(self, symbol: str) -> Optional[str]:
        """获取数据库中最新的交易日期"""
        try:
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
        except Exception as e:
            logger.warning(f"获取股票 {symbol} 最新交易日期失败: {str(e)}")
            return None

    def _smart_delay(self, base_delay: float = 1.0, jitter: float = 0.5):
        """智能延时，添加随机抖动避免被识别为机器人"""
        delay = base_delay + random.uniform(0, jitter)
        time.sleep(delay)

    @retry(stop_max_attempt_number=3, wait_exponential_multiplier=2000, wait_exponential_max=15000)
    def get_stock_data(self, symbol: str) -> Optional[pd.DataFrame]:
        """获取单个股票数据，带智能重试机制和验证
        
        Args:
            symbol: 股票代码
            
        Returns:
            股票数据DataFrame或None
        """
        try:
            # 智能延时
            self._smart_delay(1.0, 0.5)
            
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
                
            # 检查数据质量
            if len(df) == 0:
                logger.warning(f"股票 {symbol} 数据行数为0")
                return None
                
            return df
            
        except json.JSONDecodeError as e:
            logger.warning(f"股票 {symbol} JSON解析失败: {str(e)}")
            raise
        except requests.exceptions.RequestException as e:
            logger.warning(f"股票 {symbol} 网络请求失败: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"获取股票 {symbol} 数据时发生未知错误: {str(e)}")
            raise

    def process_data(self, df: pd.DataFrame, symbol: str) -> pd.DataFrame:
        """处理原始数据，增强版"""
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
            
            # 数值类型转换，更安全的处理
            numeric_columns = [
                'pe', 'pe_ttm', 'pb', 'ps', 'ps_ttm',
                'dv_ratio', 'dv_ttm', 'total_mv'
            ]
            
            for col in numeric_columns:
                if col in df.columns:
                    # 处理特殊字符和无效值
                    df[col] = df[col].astype(str).str.replace('--', '').str.replace('N/A', '')
                    df[col] = pd.to_numeric(df[col], errors='coerce')
            
            # 选择需要的列
            available_columns = ['symbol', 'trade_date'] + [col for col in numeric_columns if col in df.columns]
            
            return df[available_columns]
            
        except Exception as e:
            logger.error(f"处理股票 {symbol} 数据时发生错误: {str(e)}")
            return pd.DataFrame()

    def save_to_db(self, df: pd.DataFrame):
        """保存数据到数据库，带锁保护"""
        if df is None or df.empty:
            return
            
        with self.lock:  # 使用锁保护数据库操作
            with self.get_db_connection() as conn:
                with conn.cursor() as cur:
                    try:
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
                        
                        execute_values(cur, insert_stmt, values)
                        conn.commit()
                        logger.info(f"成功保存 {len(df)} 条记录")
                        
                    except Exception as e:
                        conn.rollback()
                        logger.error(f"保存数据失败: {str(e)}")
                        raise

    def process_single_stock(self, symbol: str, mode: str = 'incremental') -> Dict[str, Any]:
        """处理单个股票，返回处理结果"""
        result = {
            'symbol': symbol,
            'status': 'unknown',
            'message': '',
            'records_saved': 0
        }
        
        try:
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
                        result['status'] = 'success'
                        result['records_saved'] = len(df)
                        result['message'] = f"成功保存 {len(df)} 条记录"
                    else:
                        result['status'] = 'skipped'
                        result['message'] = "处理后没有有效数据"
                else:
                    result['status'] = 'skipped'
                    result['message'] = "增量模式下没有新数据"
            else:
                result['status'] = 'skipped'
                result['message'] = "没有数据"
                
        except Exception as e:
            result['status'] = 'error'
            result['message'] = str(e)
            logger.error(f"处理股票 {symbol} 失败: {str(e)}")
        
        return result

    def collect_data_parallel(self, mode: str = 'incremental'):
        """并行采集数据"""
        try:
            stock_list = self.get_stock_list()
            total_stocks = len(stock_list)
            logger.info(f"开始{mode}模式的并行数据采集，共 {total_stocks} 只股票，使用 {self.max_workers} 个线程")
            
            # 重置统计信息
            self.stats = {
                'total_processed': 0,
                'success_count': 0,
                'error_count': 0,
                'skipped_count': 0,
                'retry_count': 0
            }
            
            # 使用线程池并行处理
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                # 提交所有任务
                future_to_symbol = {
                    executor.submit(self.process_single_stock, symbol, mode): symbol 
                    for symbol in stock_list
                }
                
                # 处理完成的任务
                for future in as_completed(future_to_symbol):
                    symbol = future_to_symbol[future]
                    try:
                        result = future.result()
                        self.stats['total_processed'] += 1
                        
                        # 更新统计信息
                        if result['status'] == 'success':
                            self.stats['success_count'] += 1
                        elif result['status'] == 'error':
                            self.stats['error_count'] += 1
                        elif result['status'] == 'skipped':
                            self.stats['skipped_count'] += 1
                        
                        # 输出进度
                        if self.stats['total_processed'] % 100 == 0:
                            self._log_progress()
                            
                    except Exception as e:
                        self.stats['error_count'] += 1
                        logger.error(f"处理股票 {symbol} 时发生异常: {str(e)}")
            
            # 最终统计
            self._log_final_stats()
            
        except Exception as e:
            logger.error(f"并行数据采集过程中发生严重错误: {str(e)}")
            raise

    def collect_data(self, mode: str = 'incremental'):
        """串行采集数据（兼容原版本）"""
        try:
            stock_list = self.get_stock_list()
            total_stocks = len(stock_list)
            logger.info(f"开始{mode}模式的数据采集，共 {total_stocks} 只股票")
            
            for i, symbol in enumerate(stock_list, 1):
                try:
                    logger.info(f"开始获取股票 {symbol} 的数据 ({i}/{total_stocks})")
                    
                    result = self.process_single_stock(symbol, mode)
                    
                    # 更新统计信息
                    if result['status'] == 'success':
                        self.stats['success_count'] += 1
                    elif result['status'] == 'error':
                        self.stats['error_count'] += 1
                    elif result['status'] == 'skipped':
                        self.stats['skipped_count'] += 1
                    
                    self.stats['total_processed'] += 1
                    
                    # 每处理100只股票输出一次进度
                    if i % 100 == 0:
                        self._log_progress()
                    
                except Exception as e:
                    self.stats['error_count'] += 1
                    logger.error(f"处理股票 {symbol} 失败: {str(e)}")
                    continue
            
            # 最终统计
            self._log_final_stats()
            
        except Exception as e:
            logger.error(f"数据采集过程中发生严重错误: {str(e)}")
            raise

    def _log_progress(self):
        """输出进度信息"""
        logger.info(f"进度: {self.stats['total_processed']}, "
                   f"成功: {self.stats['success_count']}, "
                   f"错误: {self.stats['error_count']}, "
                   f"跳过: {self.stats['skipped_count']}")

    def _log_final_stats(self):
        """输出最终统计信息"""
        logger.info(f"数据采集完成！总计: {self.stats['total_processed']}, "
                   f"成功: {self.stats['success_count']}, "
                   f"错误: {self.stats['error_count']}, "
                   f"跳过: {self.stats['skipped_count']}")

def main():
    # 数据库连接参数
    db_params = {
        'host': '192.168.50.149',
        'port': 5432,
        'user': 'postgres',
        'password': '12',
        'database': 'Financialdata'
    }
    
    # 创建增强版采集器实例
    collector = EnhancedStockIndicatorCollector(db_params, max_workers=5)
    
    # 执行采集
    try:
        # 并行模式（推荐）
        collector.collect_data_parallel(mode='incremental')
        
        # 串行模式（兼容原版本）
        #collector.collect_data(mode='incremental')
        
    except Exception as e:
        logger.error(f"数据采集失败: {str(e)}")

if __name__ == "__main__":
    main() 