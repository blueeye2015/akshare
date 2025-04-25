import akshare as ak
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
import logging
from typing import List, Dict, Optional
import time

# 设置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('index_constituent.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class IndexConstituentCollector:
    def __init__(self, db_params: dict):
        """初始化数据采集器"""
        self.db_params = db_params
        self.table_name = 'index_constituent'
        
    def get_db_connection(self):
        """获取数据库连接"""
        return psycopg2.connect(**self.db_params)

    def init_table(self):
        """初始化数据表"""
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS {} (
            index_code VARCHAR(10),
            stock_code VARCHAR(10),
            stock_name VARCHAR(50),
            in_date DATE,
            update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (index_code, stock_code)
        )
        """.format(self.table_name)
        
        with self.get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(create_table_sql)
                conn.commit()
                logger.info("数据表初始化完成")

    def process_data(self, df: pd.DataFrame, index_code: str) -> pd.DataFrame:
        """处理原始数据"""
        if df.empty:
            return pd.DataFrame()
            
        logger.info(f"原始数据列名: {df.columns.tolist()}")
        
        # 根据实际返回的列名进行映射
        column_mapping = {
            '品种代码': 'stock_code',
            '品种名称': 'stock_name',
            '纳入日期': 'in_date'
        }
        
        # 重命名列
        df = df.rename(columns=column_mapping)
        
        # 添加指数代码
        df['index_code'] = index_code
        
        # 处理日期格式
        if 'in_date' in df.columns:
            df['in_date'] = pd.to_datetime(df['in_date']).dt.date
        
        # 选择需要的列
        columns = ['index_code', 'stock_code', 'stock_name', 'in_date']
        result_df = pd.DataFrame(columns=columns)
        
        for col in columns:
            if col in df.columns:
                result_df[col] = df[col]
            else:
                result_df[col] = None
        
        # 去除重复记录，保留最新的记录
        result_df = result_df.drop_duplicates(subset=['index_code', 'stock_code'], keep='last')
                
        return result_df

    def save_to_db(self, df: pd.DataFrame):
        """保存数据到数据库"""
        if df.empty:
            return
            
        with self.get_db_connection() as conn:
            with conn.cursor() as cur:
                # 删除当前指数的所有记录
                index_code = df['index_code'].iloc[0]
                delete_stmt = f"""
                    DELETE FROM {self.table_name}
                    WHERE index_code = %s
                """
                cur.execute(delete_stmt, (index_code,))
                
                # 准备数据
                columns = df.columns.tolist()
                values = [tuple(x) for x in df.values]
                
                # 构建 INSERT 语句
                insert_stmt = f"""
                    INSERT INTO {self.table_name} ({','.join(columns)})
                    VALUES %s
                """
                
                try:
                    execute_values(cur, insert_stmt, values)
                    conn.commit()
                    logger.info(f"成功保存 {len(df)} 条记录")
                except Exception as e:
                    conn.rollback()
                    logger.error(f"保存数据失败: {str(e)}")
                    raise

    def collect_data(self, index_list: List[str]):
        """采集指数成份股数据"""
        total_indices = len(index_list)
        
        for i, index_code in enumerate(index_list, 1):
            try:
                logger.info(f"开始获取指数 {index_code} 的成份股数据 ({i}/{total_indices})")
                
                # 获取数据
                df = ak.index_stock_cons(symbol=index_code)
                
                if df is not None and not df.empty:
                    logger.info(f"成功获取指数 {index_code} 数据，开始处理...")
                    df = self.process_data(df, index_code)
                    if not df.empty:
                        self.save_to_db(df)
                        logger.info(f"完成指数 {index_code} 的数据获取和保存")
                    else:
                        logger.warning(f"指数 {index_code} 数据处理后为空")
                else:
                    logger.warning(f"指数 {index_code} 没有成份股数据")
                
                # 添加延时避免请求过于频繁
                time.sleep(2)
                
            except Exception as e:
                logger.error(f"处理指数 {index_code} 失败: {str(e)}")
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
    
    # 要获取的指数列表
    index_list = [
        '000300',  # 沪深300
        '000905',  # 中证500
        '000852',  # 中证1000
        '399006',  # 创业板指
        '000016'   # 上证50
    ]
    
    try:
        # 创建采集器实例
        collector = IndexConstituentCollector(db_params)
        
        # 初始化数据表
        collector.init_table()
        
        # 执行数据采集
        collector.collect_data(index_list)
        
    except Exception as e:
        logger.error(f"数据采集失败: {str(e)}")
    finally:
        logger.info("程序运行完成")

if __name__ == "__main__":
    main()
