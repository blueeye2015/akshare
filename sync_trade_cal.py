import tushare as ts
import pandas as pd
from sqlalchemy import create_engine
import logging

# --- 配置部分 ---

# 设置日志记录
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Tushare API Token
TUSHARE_TOKEN = '0c7918f3c7233d240c1e0271c00e6bf8d0cf947863b01d63b2c8ecf3'

# 数据库连接参数
db_params = {
    'user': 'postgres',
    'password': '12',
    'host': '192.168.50.149',
    'port': 5432,
    'database': 'Financialdata'
}

# 数据库中的表名
TABLE_NAME = 'trade_cal'

# --- 主程序 ---

def fetch_trade_cal_data():
    """
    从Tushare获取交易日历数据
    """
    try:
        logging.info("正在初始化Tushare Pro接口...")
        pro = ts.pro_api(TUSHARE_TOKEN)
        
        logging.info("正在拉取所有交易日历数据...")
        # 拉取数据，不设置任何日期限制，获取全部数据
        df = pro.trade_cal(exchange='', fields=["exchange", "cal_date", "is_open", "pretrade_date"])
        
        if df is not None and not df.empty:
            logging.info(f"成功获取 {len(df)} 条数据。")
            # Tushare返回的日期是'YYYYMMDD'格式的字符串，最好转换为pandas的datetime类型再入库
            # 这样数据库中的字段类型会是DATE或TIMESTAMP，便于后续按日期查询
            df['cal_date'] = pd.to_datetime(df['cal_date'])
            df['pretrade_date'] = pd.to_datetime(df['pretrade_date'], errors='coerce') # pretrade_date可能为空，使用coerce处理错误
            return df
        else:
            logging.warning("未能从Tushare获取到任何数据。")
            return None
    except Exception as e:
        logging.error(f"从Tushare获取数据时发生错误: {e}")
        return None

def write_df_to_postgres(df, table_name):
    """
    将DataFrame写入PostgreSQL数据库
    """
    if df is None:
        logging.warning("DataFrame为空，跳过数据库写入操作。")
        return

    try:
        logging.info("正在连接到PostgreSQL数据库...")
        # 创建数据库连接引擎
        # DSN格式: postgresql+psycopg2://user:password@host:port/database
        engine_dsn = (
            f"postgresql+psycopg2://{db_params['user']}:{db_params['password']}@"
            f"{db_params['host']}:{db_params['port']}/{db_params['database']}"
        )
        engine = create_engine(engine_dsn)
        
        logging.info(f"正在将数据写入到表 '{table_name}'...")
        # 使用pandas的to_sql方法写入数据
        # if_exists='replace': 如果表存在，则删除表，重新创建并插入数据
        # index=False: 不将DataFrame的索引作为一列写入数据库
        df.to_sql(table_name, engine, if_exists='replace', index=False, chunksize=1000) # 使用chunksize提高大批量数据写入效率
        
        logging.info(f"数据成功写入数据库表 '{table_name}'。")
        
    except Exception as e:
        logging.error(f"写入数据库时发生错误: {e}")
    finally:
        if 'engine' in locals() and engine is not None:
            engine.dispose()
            logging.info("数据库连接已关闭。")

if __name__ == '__main__':
    # 1. 获取数据
    trade_cal_df = fetch_trade_cal_data()
    
    # 打印前5行数据以供预览
    if trade_cal_df is not None:
        print("\n--- 获取到的数据预览 (前5行) ---")
        print(trade_cal_df.head())
        print("---------------------------------\n")
    
    # 2. 写入数据库
    write_df_to_postgres(trade_cal_df, TABLE_NAME)