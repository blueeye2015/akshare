# -*- coding: utf-8 -*-
"""
指数成分权重采集器 (index_weight)
针对 000018.SH (180金融) 等指数进行历史权重爬取
"""
import tushare as ts
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
import logging
import time
import argparse
from datetime import datetime

# ---------------- 日志配置 ----------------
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class IndexWeightCollector:
    def __init__(self, db_params: dict, tushare_token: str):
        self.db_params = db_params
        self.table_name = 'index_weight'
        self.pro = ts.pro_api(tushare_token)

    def get_db_connection(self):
        return psycopg2.connect(**self.db_params)

    def init_table(self):
        """初始化权重表"""
        sql = f"""
        CREATE TABLE IF NOT EXISTS {self.table_name} (
            index_code      VARCHAR(20),
            con_code        VARCHAR(20),
            trade_date      DATE,
            weight          NUMERIC(10, 4),
            PRIMARY KEY (index_code, con_code, trade_date)
        );
        """
        with self.get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql)
                conn.commit()
        logger.info(f"表 {self.table_name} 初始化完成")

    def fetch_weight_data(self, index_code: str, start_date: str, end_date: str):
        """调用 Tushare 接口获取数据"""
        try:
            # 根据截图接口说明：建议输入月度开始和结束日期
            df = self.pro.index_weight(index_code=index_code, 
                                      start_date=start_date, 
                                      end_date=end_date)
            return df
        except Exception as e:
            logger.error(f"接口调用失败: {e}")
            return None

    def save_to_db(self, df: pd.DataFrame):
        if df.empty: return
        
        # 转换日期格式
        df['trade_date'] = pd.to_datetime(df['trade_date']).dt.date
        cols = ['index_code', 'con_code', 'trade_date', 'weight']
        vals = [tuple(x) for x in df[cols].values]

        sql = f"""
            INSERT INTO {self.table_name} ({','.join(cols)})
            VALUES %s
            ON CONFLICT (index_code, con_code, trade_date) DO UPDATE SET
            weight = EXCLUDED.weight
        """
        with self.get_db_connection() as conn:
            with conn.cursor() as cur:
                execute_values(cur, sql, vals)
                conn.commit()
        logger.info(f"成功 Upsert {len(df)} 条权重记录")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--token', required=True, help='Tushare Token')
    parser.add_argument('--index', default='000018.SH', help='指数代码')
    args = parser.parse_args()

    # 使用你提供的数据库配置 [cite: 36]
    db_params = dict(host='192.168.50.149', port=5432, user='postgres',
                    password='12', database='Financialdata')

    collector = IndexWeightCollector(db_params, args.token)
    collector.init_table()

    # 生成 2014 年至今的每一个月的时间区间
    # Tushare index_weight 主要是月度数据
    date_ranges = pd.date_range(start='2014-01-01', end=datetime.now(), freq='MS')
    
    for start_month in date_ranges:
        s_date = start_month.strftime('%Y%m%d')
        e_date = (start_month + pd.offsets.MonthEnd(0)).strftime('%Y%m%d')
        
        logger.info(f"正在拉取 {args.index} : {s_date} 至 {e_date}")
        df = collector.fetch_weight_data(args.index, s_date, e_date)
        
        if df is not None and not df.empty:
            collector.save_to_db(df)
        
        time.sleep(0.6) # 尊重接口频次限制

    logger.info("所有历史权重数据采集完成。")

if __name__ == '__main__':
    main()