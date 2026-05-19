# -*- coding: utf-8 -*-
"""
月度宏观数据采集并入库：
- sf_month（社融月度）
- cn_m（货币供应量月度）
- us_tycr（美债收益率曲线，日频）

用法：
python macro_monthly.py --mode incremental
python macro_monthly.py --mode full --start_m 201901 --start_date 20190101
python macro_monthly.py --mode incremental --apis sf_month,cn_m
"""

import os
import argparse
import logging
from datetime import datetime, timedelta
from typing import List, Dict

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

from python_fetch import fetch_sf_month, fetch_cn_m, fetch_us_tycr

load_dotenv('.env')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler('macro_monthly.log'), logging.StreamHandler()]
)
logger = logging.getLogger(__name__)


def month_add_one(yyyymm: str) -> str:
    dt = datetime.strptime(yyyymm, '%Y%m')
    year = dt.year + (1 if dt.month == 12 else 0)
    month = 1 if dt.month == 12 else dt.month + 1
    return f"{year:04d}{month:02d}"


class MacroMonthlyCollector:
    def __init__(self, db_params: Dict[str, str]):
        self.db_params = db_params
        self.sf_table = 'sf_monthly'
        self.cnm_table = 'cn_m_monthly'
        self.tycr_table = 'us_tycr_daily'

    def get_db_connection(self):
        return psycopg2.connect(**self.db_params)

    def init_tables(self):
        sql_sf = f"""
        CREATE TABLE IF NOT EXISTS {self.sf_table} (
            month VARCHAR(6) PRIMARY KEY,
            inc_month DOUBLE PRECISION,
            inc_cumval DOUBLE PRECISION,
            stk_endval DOUBLE PRECISION
        );
        """
        sql_cnm = f"""
        CREATE TABLE IF NOT EXISTS {self.cnm_table} (
            month VARCHAR(6) PRIMARY KEY
        );
        """
        sql_tycr = f"""
        CREATE TABLE IF NOT EXISTS {self.tycr_table} (
            date DATE PRIMARY KEY
        );
        """
        with self.get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql_sf)
                cur.execute(sql_cnm)
                cur.execute(sql_tycr)
                conn.commit()
        logger.info('数据表初始化完成')

    def get_latest_month(self, table_name: str):
        with self.get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(f"SELECT MAX(month) FROM {table_name}")
                row = cur.fetchone()
                return row[0] if row and row[0] else None

    def get_latest_date(self, table_name: str):
        with self.get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(f"SELECT MAX(date) FROM {table_name}")
                row = cur.fetchone()
                return row[0] if row and row[0] else None

    def _existing_columns(self, table_name: str) -> set:
        sql = """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = %s
        """
        with self.get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (table_name,))
                return {r[0] for r in cur.fetchall()}

    def _ensure_columns(self, table_name: str, df: pd.DataFrame, key_col: str):
        existing = self._existing_columns(table_name)
        add_cols = [c for c in df.columns if c not in existing]
        if not add_cols:
            return

        with self.get_db_connection() as conn:
            with conn.cursor() as cur:
                for col in add_cols:
                    if col == key_col:
                        continue
                    if col in ('month',):
                        col_type = 'VARCHAR(6)'
                    elif col in ('date',):
                        col_type = 'DATE'
                    else:
                        col_type = 'DOUBLE PRECISION'
                    cur.execute(f'ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS "{col}" {col_type}')
                conn.commit()
        logger.info(f'{table_name} 自动补充字段: {add_cols}')

    def _upsert_df(self, df: pd.DataFrame, table_name: str, key_col: str):
        if df is None or df.empty:
            logger.warning(f'{table_name} 本次无数据')
            return

        self._ensure_columns(table_name, df, key_col)

        cols = list(df.columns)
        vals = [tuple(x) for x in df[cols].values]
        set_cols = [c for c in cols if c != key_col]

        sql = f'''
        INSERT INTO {table_name} ({','.join(f'"{c}"' for c in cols)})
        VALUES %s
        ON CONFLICT ({key_col})
        DO UPDATE SET {','.join(f'"{c}"=EXCLUDED."{c}"' for c in set_cols)}
        '''

        with self.get_db_connection() as conn:
            with conn.cursor() as cur:
                execute_values(cur, sql, vals)
                conn.commit()
        logger.info(f'{table_name} Upsert {len(df)} 条')

    def run_sf_month(self, mode: str, start_m: str, end_m: str):
        if mode == 'incremental':
            latest = self.get_latest_month(self.sf_table)
            if latest:
                start_m = month_add_one(latest)
        if start_m > end_m:
            logger.info('sf_month 无需更新')
            return

        logger.info(f'sf_month 拉取区间: {start_m} -> {end_m}')
        df = fetch_sf_month(start_m=start_m, end_m=end_m)
        if df is None or df.empty:
            logger.warning('sf_month 返回空')
            return

        if 'month' in df.columns:
            df['month'] = df['month'].astype(str)
        self._upsert_df(df, self.sf_table, 'month')

    def run_cn_m(self, mode: str, start_m: str, end_m: str):
        if mode == 'incremental':
            latest = self.get_latest_month(self.cnm_table)
            if latest:
                start_m = month_add_one(latest)
        if start_m > end_m:
            logger.info('cn_m 无需更新')
            return

        logger.info(f'cn_m 拉取区间: {start_m} -> {end_m}')
        df = fetch_cn_m(start_m=start_m, end_m=end_m)
        if df is None or df.empty:
            logger.warning('cn_m 返回空')
            return

        if 'month' in df.columns:
            df['month'] = df['month'].astype(str)
        self._upsert_df(df, self.cnm_table, 'month')

    def run_us_tycr(self, mode: str, start_date: str, end_date: str):
        if mode == 'incremental':
            latest = self.get_latest_date(self.tycr_table)
            if latest:
                start_date = (latest + timedelta(days=1)).strftime('%Y%m%d')
        if start_date > end_date:
            logger.info('us_tycr 无需更新')
            return

        logger.info(f'us_tycr 拉取区间: {start_date} -> {end_date}')
        df = fetch_us_tycr(start_date=start_date, end_date=end_date)
        if df is None or df.empty:
            logger.warning('us_tycr 返回空')
            return

        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date']).dt.date
        self._upsert_df(df, self.tycr_table, 'date')


def main():
    parser = argparse.ArgumentParser(description='月度宏观数据采集并入库')
    parser.add_argument('--mode', choices=['incremental', 'full'], default='incremental')
    parser.add_argument('--apis', type=str, default='sf_month,cn_m,us_tycr',
                        help='逗号分隔：sf_month,cn_m,us_tycr')
    parser.add_argument('--start_m', type=str, default='201901')
    parser.add_argument('--end_m', type=str, default=datetime.now().strftime('%Y%m'))
    parser.add_argument('--start_date', type=str, default='20190101')
    parser.add_argument('--end_date', type=str, default=datetime.now().strftime('%Y%m%d'))
    args = parser.parse_args()

    db_params = {
        'host': os.getenv('PGHOST', '192.168.50.149'),
        'port': int(os.getenv('PGPORT', '5432')),
        'user': os.getenv('PGUSER', 'postgres'),
        'password': os.getenv('PGPASSWORD', '12'),
        'database': os.getenv('PGDATABASE', 'Financialdata'),
    }

    collector = MacroMonthlyCollector(db_params)
    collector.init_tables()

    targets: List[str] = [x.strip() for x in args.apis.split(',') if x.strip()]
    logger.info(f'开始执行 mode={args.mode}, apis={targets}')

    if 'sf_month' in targets:
        collector.run_sf_month(args.mode, args.start_m, args.end_m)
    if 'cn_m' in targets:
        collector.run_cn_m(args.mode, args.start_m, args.end_m)
    if 'us_tycr' in targets:
        collector.run_us_tycr(args.mode, args.start_date, args.end_date)

    logger.info('任务完成')


if __name__ == '__main__':
    main()
