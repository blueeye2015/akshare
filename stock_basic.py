# -*- coding: utf-8 -*-
"""
股票基础信息全量采集（stock_basic）
一次性拉取，本地 CSV + PostgreSQL 双备份
用法：
python stock_basic.py --tushare_token YOUR_TOKEN
"""
import tushare as ts
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
import logging
import argparse
import sys
from datetime import datetime

# ---------- 日志 ----------
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('stock_basic.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ---------- 采集器 ----------
class StockBasicCollector:
    def __init__(self, db_params: dict, tushare_token: str):
        self.db_params = db_params
        self.table_name = 'stock_basic'
        self.pro = ts.pro_api(tushare_token)

    # ---------- PG 连接 ----------
    def get_db_connection(self):
        return psycopg2.connect(**self.db_params)

    def init_table(self):
        sql = f"""
        CREATE TABLE IF NOT EXISTS {self.table_name} (
            ts_code      VARCHAR(10) PRIMARY KEY,
            symbol       VARCHAR(10),
            name         VARCHAR(50),
            area         VARCHAR(20),
            industry     VARCHAR(30),
            fullname     VARCHAR(100),
            enname       VARCHAR(200),
            cnspell      VARCHAR(50),
            market       VARCHAR(20),
            exchange     VARCHAR(10),
            curr_type    VARCHAR(10),
            list_status  VARCHAR(5),
            list_date    DATE,
            delist_date  DATE,
            is_hs        VARCHAR(5),
            act_name     VARCHAR(50),
            act_ent_type VARCHAR(30)
        );
        """
        with self.get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql)
                conn.commit()
        logger.info("数据库表初始化完成")

    # ---------- 拉数据 ----------
    def fetch_basic(self, **kwargs) -> pd.DataFrame:
        """
        一次性拉全量，支持命令行过滤参数
        """
        df = self.pro.stock_basic(**kwargs)
        return df

    # ---------- 清洗 ----------
    def process(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return pd.DataFrame()
        # 日期字段转日期类型
        date_cols = ['list_date', 'delist_date']
        for c in date_cols:
            if c in df.columns:
                df[c] = pd.to_datetime(df[c], errors='coerce').dt.date
        return df

    # ---------- 存库 ----------
    def save_to_db(self, df: pd.DataFrame):
        if df.empty:
            return
        cols = df.columns.tolist()
        vals = [tuple(x) for x in df.values]
        sql = f"""
            INSERT INTO {self.table_name} ({','.join(cols)})
            VALUES %s
            ON CONFLICT (ts_code)
            DO UPDATE SET
            {','.join(f"{c}=EXCLUDED.{c}" for c in cols if c != 'ts_code')}
        """
        with self.get_db_connection() as conn:
            with conn.cursor() as cur:
                execute_values(cur, sql, vals)
                conn.commit()
        logger.info(f"数据库 Upsert 完成，共 {len(df)} 条")

    # ---------- 本地 CSV ----------
    def save_to_csv(self, df: pd.DataFrame, path: str = None):
        if path is None:
            path = f"stock_basic_{datetime.now().strftime('%Y%m%d')}.csv"
        df.to_csv(path, index=False, encoding='utf-8-sig')
        logger.info(f"本地 CSV 已保存：{path}")

# ---------- main ----------
def main():
    parser = argparse.ArgumentParser(description='股票基础信息全量采集')
    parser.add_argument('--tushare_token', required=True, help='Tushare token')
    parser.add_argument('--market', type=str, help='市场类别 主板/创业板/科创板/CDR/北交所')
    parser.add_argument('--list_status', type=str, default='L', help='上市状态 L/D/P，默认L')
    parser.add_argument('--exchange', type=str, help='交易所 SSE/SZSE/BSE')
    parser.add_argument('--is_hs', type=str, help='沪深港通 N/H/S')
    parser.add_argument('--csv', type=str, help='指定 CSV 保存路径')
    parser.add_argument('--skip_csv', action='store_true', help='不保存 CSV')
    args = parser.parse_args()

    # 数据库连接
    db_params = dict(host='192.168.50.149', port=5432, user='postgres',
                    password='12', database='Financialdata')

    collector = StockBasicCollector(db_params, args.tushare_token)
    collector.init_table()

    # 组装过滤参数
    filters = {}
    for k in ['market', 'list_status', 'exchange', 'is_hs']:
        v = getattr(args, k)
        if v:
            filters[k] = v

    # 拉取 & 处理
    df_raw = collector.fetch_basic(**filters)
    df = collector.process(df_raw)

    # 保存
    if not args.skip_csv:
        collector.save_to_csv(df, args.csv)
    collector.save_to_db(df)

    logger.info("===  stock_basic 全量采集完成  ===")

if __name__ == '__main__':
    main()