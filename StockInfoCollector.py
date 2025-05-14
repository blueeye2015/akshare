import akshare as ak
import pandas as pd
import psycopg2
import time
import logging
from tqdm import tqdm
from psycopg2.extras import execute_values

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger()

class StockInfoCollector:
    def __init__(self, db_host, db_port, db_user, db_password, db_name):
        """初始化数据库连接参数"""
        self.db_host = db_host
        self.db_port = db_port
        self.db_user = db_user
        self.db_password = db_password
        self.db_name = db_name
        self.table_name = "stock_individual_info"
        
    def get_db_connection(self):
        """获取数据库连接"""
        return psycopg2.connect(
            host=self.db_host,
            port=self.db_port,
            user=self.db_user,
            password=self.db_password,
            dbname=self.db_name
        )
    
    def get_all_stock_codes(self):
        """从数据库获取所有股票代码"""
        with self.get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT left(code,6) FROM stock_info_a_code_name")
                codes = [row[0] for row in cur.fetchall()]
                return codes
    
    def create_table_if_not_exists(self):
        """创建股票信息表（如果不存在）"""
        with self.get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                CREATE TABLE IF NOT EXISTS {} (
                    symbol VARCHAR(10) PRIMARY KEY,
                    total_market_value NUMERIC,
                    circulating_market_value NUMERIC,
                    industry VARCHAR(50),
                    listing_date VARCHAR(20),
                    stock_name VARCHAR(50),
                    total_shares NUMERIC,
                    circulating_shares NUMERIC,
                    update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """.format(self.table_name))
                conn.commit()
    
    def save_to_db(self, df):
        """保存数据到数据库"""
        if df.empty:
            return
        
        # 重命名列以匹配数据库表结构
        column_mapping = {
            'symbol': 'symbol',
            '总市值': 'total_market_value',
            '流通市值': 'circulating_market_value',
            '行业': 'industry',
            '上市时间': 'listing_date',
            '股票简称': 'stock_name',
            '总股本': 'total_shares',
            '流通股': 'circulating_shares'
        }
        
        try:
            # 创建一个新的DataFrame，确保有正确的索引
            result_df = pd.DataFrame(index=[0])
            
            # 添加symbol列
            result_df['symbol'] = df.get('symbol', df.index[0] if isinstance(df.index, pd.Index) else None)
            
            # 添加其他列
            for orig_col, new_col in column_mapping.items():
                if orig_col != 'symbol':  # symbol已经处理过了
                    if orig_col in df:
                        result_df[new_col] = df[orig_col].values[0] if isinstance(df, pd.DataFrame) else df.get(orig_col)
            
            with self.get_db_connection() as conn:
                with conn.cursor() as cur:
                    # 准备数据
                    columns = result_df.columns.tolist()
                    values = [tuple(x) for x in result_df.values]
                    
                    # 构建列名部分
                    columns_str = ','.join(columns)
                    
                    # 构建更新部分
                    update_parts = []
                    for col in columns:
                        if col != 'symbol':
                            update_parts.append(f"{col}=EXCLUDED.{col}")
                    update_str = ','.join(update_parts)
                    
                    # 构建SQL语句
                    insert_stmt = f"INSERT INTO {self.table_name} ({columns_str}) VALUES %s ON CONFLICT (symbol) DO UPDATE SET {update_str}, update_time=CURRENT_TIMESTAMP"
                    
                    execute_values(cur, insert_stmt, values)
                    conn.commit()
                    logger.info(f"成功保存 {len(result_df)} 条记录")
                    
        except Exception as e:
            logger.error(f"保存数据失败: {str(e)}")
            raise
    
    def collect_stock_info(self):
        """收集所有股票的详细信息"""
        # 创建表（如果不存在）
        self.create_table_if_not_exists()
        
        # 获取所有股票代码
        stock_codes = self.get_all_stock_codes()
        logger.info(f"共获取到 {len(stock_codes)} 只股票")
        
        # 处理每只股票
        for i, code in enumerate(tqdm(stock_codes)):
            try:
                # 获取股票详细信息
                logger.info(f"正在获取股票 {code} 的信息 ({i+1}/{len(stock_codes)})")
                stock_info = ak.stock_individual_info_em(symbol=code)
                
                # 将行列转置，方便处理
                stock_info_dict = dict(zip(stock_info['item'], stock_info['value']))
                stock_info_df = pd.DataFrame([stock_info_dict])
                
                # 添加股票代码列
                stock_info_df['symbol'] = code
                
                # 保存到数据库
                self.save_to_db(stock_info_df)
                
                # 适当延时，避免请求过快
                time.sleep(0.5)
                
            except Exception as e:
                logger.error(f"处理股票 {code} 失败: {str(e)}")
                time.sleep(1)  # 出错后稍微多等待一下
                continue

if __name__ == "__main__":
    # 数据库连接参数
    db_config = {
        "db_host": "192.168.50.149",
        "db_port": 5432,
        "db_user": "postgres",
        "db_password": "12",
        "db_name": "Financialdata"
    }
    
    # 创建收集器并开始收集
    collector = StockInfoCollector(**db_config)
    collector.collect_stock_info()
