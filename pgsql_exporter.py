import time
from sqlalchemy import create_engine, text
from dotenv import load_dotenv 
from prometheus_client import start_http_server, Gauge
import os
import pandas as pd

# --- 配置 ---
load_dotenv('.env')
DSN = os.getenv('DB_DSN1')
# 1. 定义 Prometheus 指标
# 使用 Gauge（仪表盘），因为成交额是上下波动的
market_volume = Gauge('china_market_vol_billion', 'Total trading volume of A-share market in billions')


def get_db_engine():
    return create_engine(DSN)

def get_market_volume_from_db():
    # 数据库连接配置
    engine = get_db_engine()
    
    try:
        with engine.connect() as conn:
            # 使用你之前提供的精准 SQL 逻辑
            # 建议取最近一个交易日的数据
            sql = """
            SELECT sum(amount)/1000000000 as vol
            FROM index_daily 
            WHERE ts_code in ('399001.SZ','000001.SH')
            AND trade_date = (SELECT max(trade_date) FROM index_daily)
            """
            df = pd.read_sql(sql, engine)
            
            print(f"执行 SQL 结果: {df}")
            return pd.to_numeric(df["vol"][0]) 
    except Exception as e:
        print(f"数据库读取错误: {e}")
        return 0
    finally:
        if 'conn' in locals():
            conn.close()

if __name__ == '__main__':
    # 2. 启动 Exporter 服务，端口设为 8000
    start_http_server(8000)
    print("🚀 Quant Exporter 已启动，监听端口: 8000")
    
    # 3. 循环采集数据并更新指标
    while True:
        vol = get_market_volume_from_db()
        market_volume.set(vol)
        print(f"📊 当前成交额采集值: {vol}B")
        # 每 15 秒更新一次，与 Prometheus 的 scrape_interval 保持一致
        time.sleep(3600)