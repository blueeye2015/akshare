#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
全球主要指数日K数据获取与入库

支持数据源：
  - 新浪全球指数 (akshare.index_global_hist_sina): 欧洲/亚洲/其他指数，约4年历史
  - 新浪美股指数 (akshare.index_us_stock_sina): 美股三大指数，约20年历史
  - 东方财富全球指数 (akshare.index_global_hist_em): 更长历史，但易被限流

入库表: global_index_daily
  symbol, name, trade_date, open, close, high, low, amplitude, volume, amount
"""

import pandas as pd
import numpy as np
import psycopg2
from psycopg2.extras import RealDictCursor, execute_values
from urllib.parse import urlparse
from dotenv import load_dotenv
import os
import time
import logging
import argparse
from datetime import datetime
import akshare as ak

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def load_db_config() -> dict:
    env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '.env')
    load_dotenv(env_path)
    dsn = os.getenv('DB_DSN1')
    if not dsn:
        raise ValueError("DB_DSN1 未设置")
    parsed = urlparse(dsn)
    return {
        'host': parsed.hostname or '127.0.0.1',
        'port': parsed.port or 5432,
        'database': parsed.path.lstrip('/'),
        'user': parsed.username or 'postgres',
        'password': parsed.password or ''
    }


# ---------------------------------------------------------------------------
# 指数配置
# ---------------------------------------------------------------------------
SINA_GLOBAL_INDICES = {
    # 名称: (新浪中文名, 英文代码)
    'NKY':  ('日经225指数', 'NKY'),
    'UKX':  ('英国富时100指数', 'UKX'),
    'DAX':  ('德国DAX 30种股价指数', 'DAX'),
    'CAC':  ('法CAC40指数', 'CAC'),
    'SX5E': ('欧洲Stoxx50指数', 'SX5E'),
    'TWII': ('中国台湾加权指数', 'TWII'),
    'KOSPI':('首尔综合指数', 'KOSPI'),
    'AS51': ('澳大利亚标准普尔200指数', 'AS51'),
    'SENSEX': ('印度孟买SENSEX指数', 'SENSEX'),
    'GSPTSE': ('加拿大S&P/TSX综合指数', 'GSPTSE'),
    'IBOV': ('巴西BOVESPA股票指数', 'IBOV'),
    'MXX':  ('墨西哥BOLSA指数', 'MXX'),
    'STI':  ('富时新加坡海峡时报', 'STI'),
    'JKSE': ('印尼雅加达综合', 'JKSE'),
    'NZ50': ('新西兰NZSE 50指数', 'NZ50'),
    'SWI20':('瑞士股票指数', 'SWI20'),
    'FTSEMIB':('富时意大利MIB指数', 'FTSEMIB'),
    'AEX':  ('荷兰AEX综合指数', 'AEX'),
    'IBEX': ('西班牙IBEX指数', 'IBEX'),
    'ASE':  ('希腊雅典ASE', 'ASE'),
    'BFX':  ('比利时BFX', 'BFX'),
    'PX':   ('布拉格指数', 'PX'),
    'ICEXI':('冰岛ICEX', 'ICEXI'),
    'OSEBX':('挪威OSEBX', 'OSEBX'),
    'WIG':  ('波兰WIG', 'WIG'),
    'OMXSPI':('瑞典OMXSPI', 'OMXSPI'),
    'HEX':  ('芬兰赫尔辛基', 'HEX'),
    'ATX':  ('奥地利ATX', 'ATX'),
    'ISEQ': ('爱尔兰综合', 'ISEQ'),
    'OMXC20':('OMX哥本哈根20', 'OMXC20'),
}

US_STOCK_INDICES = {
    # 名称: 新浪代码
    'SPX':  '.INX',   # 标普500
    'NDX':  '.IXIC',  # 纳斯达克
    'DJI':  '.DJI',   # 道琼斯
}

EM_GLOBAL_INDICES = {
    # 东方财富接口（名称: 英文名）
    'HSI':  '恒生指数',
    'HSCEI':'国企指数',
    'UDI':  '美元指数',
    'SH':   '上证指数',
    'SZ':   '深证成指',
    'CSI300':'沪深300',
    'CY':   '创业板指',
    'DJIA': '道琼斯',
    'SPX_EM':'标普500',
    'NDX_EM':'纳斯达克',
    # 亚太
    'NKY_EM': '日经225',
    'KOSPI_EM': '韩国KOSPI',
    'TWII_EM': '台湾加权',
    'STI_EM': '富时新加坡海峡时报',
    'SENSEX_EM': '印度孟买SENSEX',
    'JKSE_EM': '印尼雅加达综合',
    'AS51_EM': '澳大利亚标普200',
    'NZ50_EM': '新西兰50',
    # 欧洲
    'UKX_EM': '英国富时100',
    'DAX_EM': '德国DAX30',
    'CAC_EM': '法国CAC40',
    'SX5E_EM': '欧洲斯托克50',
    'SWI20_EM': '瑞士SMI',
    'FTSEMIB_EM': '富时意大利MIB',
    'IBEX_EM': '西班牙IBEX35',
    'AEX_EM': '荷兰AEX',
    # 其他
    'GSPTSE_EM': '加拿大S&P/TSX',
    'IBOV_EM': '巴西BOVESPA',
    'MXX_EM': '墨西哥BOLSA',
    'ASE_EM': '希腊雅典ASE',
    'BFX_EM': '比利时BFX',
    'ATX_EM': '奥地利ATX',
}


def fetch_sina_global(name: str, sleep_sec: float = 0.5):
    """通过新浪接口获取全球指数日K"""
    sina_name, _ = SINA_GLOBAL_INDICES.get(name, (name, name))
    try:
        time.sleep(sleep_sec)
        df = ak.index_global_hist_sina(symbol=sina_name)
        if df.empty:
            return None
        df = df.rename(columns={
            'date': 'trade_date',
            'open': 'open',
            'close': 'close',
            'high': 'high',
            'low': 'low',
        })
        df['symbol'] = name
        df['name'] = sina_name.replace('指数', '')
        df['amplitude'] = np.nan
        df['trade_date'] = pd.to_datetime(df['trade_date']).dt.date
        # 选择统一列
        cols = ['symbol', 'name', 'trade_date', 'open', 'close', 'high', 'low', 'amplitude']
        if 'volume' in df.columns:
            cols.append('volume')
        if 'amount' in df.columns:
            cols.append('amount')
        return df[cols]
    except Exception as e:
        logger.warning(f"新浪接口获取 {name} 失败: {e}")
        return None


def fetch_us_stock(name: str, sleep_sec: float = 0.5):
    """通过新浪美股接口获取美股指数日K"""
    sina_code = US_STOCK_INDICES.get(name)
    if not sina_code:
        return None
    try:
        time.sleep(sleep_sec)
        df = ak.index_us_stock_sina(symbol=sina_code)
        if df.empty:
            return None
        df = df.rename(columns={
            'date': 'trade_date',
            'open': 'open',
            'close': 'close',
            'high': 'high',
            'low': 'low',
        })
        df['symbol'] = name
        name_map = {'SPX': '标普500', 'NDX': '纳斯达克', 'DJI': '道琼斯'}
        df['name'] = name_map.get(name, name)
        df['amplitude'] = np.nan
        df['trade_date'] = pd.to_datetime(df['trade_date']).dt.date
        cols = ['symbol', 'name', 'trade_date', 'open', 'close', 'high', 'low', 'amplitude']
        if 'volume' in df.columns:
            df['volume'] = pd.to_numeric(df['volume'], errors='coerce')
            cols.append('volume')
        if 'amount' in df.columns:
            df['amount'] = pd.to_numeric(df['amount'], errors='coerce')
            cols.append('amount')
        return df[cols]
    except Exception as e:
        logger.warning(f"美股接口获取 {name} 失败: {e}")
        return None


def fetch_em_global(name: str, sleep_sec: float = 1.0, max_retry: int = 3):
    """通过东方财富接口获取全球指数日K（带重试）"""
    em_name = EM_GLOBAL_INDICES.get(name)
    if not em_name:
        return None
    for attempt in range(max_retry):
        try:
            time.sleep(sleep_sec)
            df = ak.index_global_hist_em(symbol=em_name)
            if df.empty:
                return None
            df = df.rename(columns={
                '日期': 'trade_date',
                '今开': 'open',
                '最新价': 'close',
                '最高': 'high',
                '最低': 'low',
                '振幅': 'amplitude',
            })
            df['symbol'] = name
            df['name'] = em_name
            df['trade_date'] = pd.to_datetime(df['trade_date']).dt.date
            cols = ['symbol', 'name', 'trade_date', 'open', 'close', 'high', 'low', 'amplitude']
            return df[cols]
        except Exception as e:
            err = str(e)
            if 'RemoteDisconnected' in err or 'Connection aborted' in err:
                logger.warning(f"东方财富接口限流 {name}，等待30秒后重试 ({attempt+1}/{max_retry})...")
                time.sleep(30)
            else:
                logger.warning(f"东方财富接口获取 {name} 失败: {e}")
                return None
    logger.warning(f"东方财富接口获取 {name} 重试耗尽")
    return None


def save_to_db(conn, df: pd.DataFrame):
    """批量入库，ON CONFLICT 更新"""
    if df.empty:
        return 0

    records = []
    for _, row in df.iterrows():
        records.append((
            row.get('symbol'), row.get('name'), row.get('trade_date'),
            row.get('open'), row.get('close'), row.get('high'), row.get('low'),
            row.get('amplitude'),
            row.get('volume') if 'volume' in row else None,
            row.get('amount') if 'amount' in row else None,
        ))

    with conn.cursor() as cur:
        execute_values(cur, """
            INSERT INTO global_index_daily (symbol, name, trade_date, open, close, high, low, amplitude, volume, amount)
            VALUES %s
            ON CONFLICT (symbol, trade_date) DO UPDATE SET
                name = EXCLUDED.name,
                open = EXCLUDED.open,
                close = EXCLUDED.close,
                high = EXCLUDED.high,
                low = EXCLUDED.low,
                amplitude = EXCLUDED.amplitude,
                volume = EXCLUDED.volume,
                amount = EXCLUDED.amount,
                create_time = CURRENT_TIMESTAMP
        """, records)
    conn.commit()
    return len(records)


def get_existing_latest_date(conn, symbol: str):
    """获取数据库中某指数的最新日期"""
    with conn.cursor() as cur:
        cur.execute("SELECT MAX(trade_date) FROM global_index_daily WHERE symbol = %s", (symbol,))
        result = cur.fetchone()[0]
    return result


def main():
    parser = argparse.ArgumentParser(description='获取全球主要指数日K数据')
    parser.add_argument('--source', type=str, default='all', choices=['all', 'sina', 'us', 'em'],
                        help='数据源: all(全部), sina(新浪全球), us(美股), em(东方财富)')
    parser.add_argument('--symbol', type=str, default=None,
                        help='指定单个指数代码，如 SPX, NKY, HSI')
    parser.add_argument('--sleep', type=float, default=0.8, help='每次API调用间隔(秒)')
    args = parser.parse_args()

    db_config = load_db_config()
    conn = psycopg2.connect(**db_config)
    logger.info(f"数据库已连接: {db_config['host']}:{db_config['port']}/{db_config['database']}")

    # 构建任务列表
    tasks = []

    if args.source in ('all', 'sina'):
        for symbol in SINA_GLOBAL_INDICES.keys():
            if args.symbol is None or args.symbol == symbol:
                tasks.append(('sina', symbol))

    if args.source in ('all', 'us'):
        for symbol in US_STOCK_INDICES.keys():
            if args.symbol is None or args.symbol == symbol:
                tasks.append(('us', symbol))

    if args.source in ('all', 'em'):
        for symbol in EM_GLOBAL_INDICES.keys():
            if args.symbol is None or args.symbol == symbol:
                tasks.append(('em', symbol))

    logger.info(f"共 {len(tasks)} 个指数待获取")

    success_count = 0
    fail_count = 0
    total_records = 0

    for source, symbol in tasks:
        logger.info(f"[{source.upper()}] 获取 {symbol} ...")
        latest_db = get_existing_latest_date(conn, symbol)
        if latest_db:
            logger.info(f"  数据库最新: {latest_db}")

        if source == 'sina':
            df = fetch_sina_global(symbol, sleep_sec=args.sleep)
        elif source == 'us':
            df = fetch_us_stock(symbol, sleep_sec=args.sleep)
        else:
            df = fetch_em_global(symbol, sleep_sec=args.sleep)

        if df is not None and not df.empty:
            # 增量过滤
            if latest_db:
                df = df[df['trade_date'] > latest_db]
            if df.empty:
                logger.info(f"  无新数据")
                success_count += 1
                continue

            n = save_to_db(conn, df)
            total_records += n
            logger.info(f"  ✅ 入库 {n} 条, 范围 {df['trade_date'].min()} ~ {df['trade_date'].max()}")
            success_count += 1
        else:
            logger.warning(f"  ❌ 获取失败")
            fail_count += 1

    conn.close()
    logger.info(f"\n完成: 成功 {success_count}, 失败 {fail_count}, 新增 {total_records} 条记录")


if __name__ == '__main__':
    main()
