"""
统一 Tushare 接口调用模块，支持数据自动存入 PostgreSQL。

命令格式（直接运行本文件）：
1. python python_fetch.py --api <接口名> [--arg key=value ...] [--token <token>] [--head <n>]
2. 参数说明：
   - --api: Tushare Pro 接口名，如 daily / daily_basic / index_daily / suspend_d / sf_month / cn_m / us_tycr
   - --arg: 接口参数，支持重复传入，如 --arg ts_code=000001.SZ --arg start_date=20260101
   - --token: 可选；不传则读取环境变量 TUSHARE 或 TUSHARE_TOKEN
   - --head: 打印前 n 行（默认 5）
   - --save: 可选；加上此参数会将数据保存到数据库

示例：
1. python python_fetch.py --api suspend_d --arg trade_date=20260325
2. python python_fetch.py --api sf_month --arg start_m=202401 --arg end_m=202412 --save
3. python python_fetch.py --api cn_m --arg start_m=202401 --arg end_m=202412 --arg fields=month,m0,m1,m2 --save
4. python python_fetch.py --api us_tycr --arg start_date=20260101 --arg end_date=20260325 --save
"""

import argparse
import os
from typing import Any, Optional

import tushare as ts
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.types import Date, Float, String, BigInteger

load_dotenv('/data/akshare/.env')

_PRO_CLIENT_CACHE = {}

# 数据库连接配置（从 .env 读取）
DSN = os.getenv('DB_DSN1')

# 支持的宏观数据接口配置
MACRO_TABLES = {
    'sf_month': {
        'table_name': 'macro_sf_month',
        'pk_columns': ['month'],
        'dtype_mapping': {
            'month': String(6),
            'inc_month': Float,
            'inc_cumval': Float,
            'stk_endval': Float
        }
    },
    'cn_m': {
        'table_name': 'macro_cn_m',
        'pk_columns': ['month'],
        'dtype_mapping': {
            'month': String(6),
            'm0': Float,
            'm0_yoy': Float,
            'm0_mom': Float,
            'm1': Float,
            'm1_yoy': Float,
            'm1_mom': Float,
            'm2': Float,
            'm2_yoy': Float,
            'm2_mom': Float
        }
    },
    'us_tycr': {
        'table_name': 'macro_us_tycr',
        'pk_columns': ['date'],
        'dtype_mapping': {
            'date': Date,
            'm1': Float,
            'm2': Float,
            'm3': Float,
            'm6': Float,
            'y1': Float,
            'y2': Float,
            'y3': Float,
            'y5': Float,
            'y7': Float,
            'y10': Float,
            'y20': Float,
            'y30': Float
        }
    }
}


def get_db_engine():
    """创建数据库连接引擎"""
    if not DSN:
        raise ValueError("环境变量 DB_DSN1 未设置")
    return create_engine(DSN)


def get_pro_client(token: Optional[str] = None):
    resolved_token = token or os.getenv('TUSHARE') or os.getenv('TUSHARE_TOKEN')
    if not resolved_token:
        raise ValueError('Missing TUSHARE or TUSHARE_TOKEN in environment')

    client = _PRO_CLIENT_CACHE.get(resolved_token)
    if client is None:
        client = ts.pro_api(resolved_token)
        _PRO_CLIENT_CACHE[resolved_token] = client
    return client


def python_fetch(api_name: str, pro=None, token: Optional[str] = None, **kwargs: Any):
    """
    统一 Tushare 接口调用入口：
    - api_name: 接口名，如 daily / daily_basic / index_daily / sf_month / cn_m / us_tycr
    - pro: 可选，已初始化的 ts.pro_api 客户端（推荐在批处理里复用）
    - token: 可选，不传时自动读取环境变量
    - kwargs: 透传给具体接口的参数
    """
    client = pro or get_pro_client(token=token)
    if not hasattr(client, api_name):
        raise AttributeError(f"Tushare Pro has no API named '{api_name}'")
    return getattr(client, api_name)(**kwargs)


def ensure_columns_exist(conn, table_name: str, columns: dict):
    """
    动态检查并添加缺失的列
    """
    # 获取现有列
    result = conn.execute(text(f"""
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_name = '{table_name}'
    """))
    existing_columns = {row[0] for row in result}
    
    # 需要添加的列
    for col_name, col_type in columns.items():
        if col_name not in existing_columns and col_name != 'update_time':
            # 映射 SQLAlchemy 类型到 PostgreSQL 类型
            if col_type == Float:
                pg_type = 'FLOAT'
            elif col_type == String:
                pg_type = 'VARCHAR(255)'
            elif col_type == Date:
                pg_type = 'DATE'
            elif col_type == BigInteger:
                pg_type = 'BIGINT'
            else:
                pg_type = 'TEXT'
            
            conn.execute(text(f'ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS {col_name} {pg_type}'))
            print(f"  添加列: {table_name}.{col_name}")


def init_macro_tables():
    """初始化宏观数据表（如果不存在）"""
    engine = get_db_engine()
    
    with engine.connect() as conn:
        # sf_month 表 - 社会融资规模
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS macro_sf_month (
                month VARCHAR(6) PRIMARY KEY,
                inc_month FLOAT,
                inc_cumval FLOAT,
                stk_endval FLOAT,
                update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """))
        
        # cn_m 表 - 货币供应量（创建基础结构，后续动态添加列）
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS macro_cn_m (
                month VARCHAR(6) PRIMARY KEY,
                m0 FLOAT,
                m1 FLOAT,
                m2 FLOAT,
                update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """))
        # 动态确保所有列存在
        ensure_columns_exist(conn, 'macro_cn_m', MACRO_TABLES['cn_m']['dtype_mapping'])
        
        # us_tycr 表 - 美债收益率
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS macro_us_tycr (
                date DATE PRIMARY KEY,
                m1 FLOAT,
                m2 FLOAT,
                m3 FLOAT,
                m6 FLOAT,
                y1 FLOAT,
                y2 FLOAT,
                y3 FLOAT,
                y5 FLOAT,
                y7 FLOAT,
                y10 FLOAT,
                y20 FLOAT,
                y30 FLOAT,
                update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """))
        
        conn.commit()
        print("✅ 宏观数据表初始化完成")


def save_to_db(df: pd.DataFrame, api_name: str) -> bool:
    """
    将数据保存到数据库
    - 支持增量更新（UPSERT）
    - 动态适应 DataFrame 中的字段
    - 仅支持配置的宏观数据接口
    """
    if df is None or df.empty:
        print(f"⚠️ {api_name}: 无数据可保存")
        return False
    
    if api_name not in MACRO_TABLES:
        print(f"⚠️ {api_name}: 暂不支持自动存储，仅支持: {list(MACRO_TABLES.keys())}")
        return False
    
    config = MACRO_TABLES[api_name]
    table_name = config['table_name']
    pk_columns = config['pk_columns']
    
    try:
        engine = get_db_engine()
        
        # 数据预处理
        df = df.copy()
        if api_name == 'us_tycr':
            # 日期格式转换
            df['date'] = pd.to_datetime(df['date']).dt.date
        
        # 清理数据：将 NaN/None/inf 替换为 None（SQL NULL）
        df = df.replace({pd.NA: None, float('inf'): None, float('-inf'): None})
        df = df.where(pd.notnull(df), None)
        
        with engine.connect() as conn:
            # 获取 DataFrame 实际有的列（与数据库表取交集）
            result = conn.execute(text(f"""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = '{table_name}'
            """))
            db_columns = {row[0] for row in result}
            
            # 过滤 DataFrame，只保留数据库中存在的列
            df_columns = [col for col in df.columns if col in db_columns]
            df_filtered = df[df_columns]
            
            # 将 DataFrame 转换为字典列表
            records = df_filtered.to_dict('records')
            
            if not records:
                print(f"⚠️ {api_name}: 无有效记录")
                return False
            
            # 构建 INSERT ... ON CONFLICT 语句（只包含实际有的列）
            columns = df_columns
            column_str = ', '.join(columns)
            placeholders = ', '.join([f':{col}' for col in columns])
            
            # 排除主键的更新列
            update_columns = [col for col in columns if col not in pk_columns]
            if update_columns:
                update_str = ', '.join([f"{col} = EXCLUDED.{col}" for col in update_columns])
                upsert_sql = f"""
                    INSERT INTO {table_name} ({column_str})
                    VALUES ({placeholders})
                    ON CONFLICT ({', '.join(pk_columns)})
                    DO UPDATE SET {update_str}, update_time = CURRENT_TIMESTAMP
                """
            else:
                # 只有主键的情况
                upsert_sql = f"""
                    INSERT INTO {table_name} ({column_str})
                    VALUES ({placeholders})
                    ON CONFLICT ({', '.join(pk_columns)})
                    DO NOTHING
                """
            
            # 执行批量插入/更新
            conn.execute(text(upsert_sql), records)
            conn.commit()
            
            print(f"✅ {api_name}: 成功保存 {len(records)} 条记录到 {table_name}（字段: {', '.join(columns)}）")
            return True
            
    except Exception as e:
        print(f"❌ {api_name}: 保存失败 - {e}")
        return False


def fetch_sf_month(*, m: Optional[str] = None, start_m: Optional[str] = None,
                   end_m: Optional[str] = None, fields: Optional[str] = None,
                   pro=None, token: Optional[str] = None, save: bool = False):
    """获取社会融资规模数据"""
    kwargs = {}
    if m:
        kwargs['m'] = m
    if start_m:
        kwargs['start_m'] = start_m
    if end_m:
        kwargs['end_m'] = end_m
    if fields:
        kwargs['fields'] = fields
    
    df = python_fetch('sf_month', pro=pro, token=token, **kwargs)
    
    if save and df is not None and not df.empty:
        init_macro_tables()  # 确保表存在
        save_to_db(df, 'sf_month')
    
    return df


def fetch_cn_m(*, m: Optional[str] = None, start_m: Optional[str] = None,
               end_m: Optional[str] = None, fields: Optional[str] = None,
               pro=None, token: Optional[str] = None, save: bool = False):
    """获取货币供应量数据"""
    kwargs = {}
    if m:
        kwargs['m'] = m
    if start_m:
        kwargs['start_m'] = start_m
    if end_m:
        kwargs['end_m'] = end_m
    if fields:
        kwargs['fields'] = fields
    
    df = python_fetch('cn_m', pro=pro, token=token, **kwargs)
    
    if save and df is not None and not df.empty:
        init_macro_tables()
        save_to_db(df, 'cn_m')
    
    return df


def fetch_us_tycr(*, date: Optional[str] = None, start_date: Optional[str] = None,
                  end_date: Optional[str] = None, fields: Optional[str] = None,
                  pro=None, token: Optional[str] = None, save: bool = False):
    """获取美债收益率数据"""
    kwargs = {}
    if date:
        kwargs['date'] = date
    if start_date:
        kwargs['start_date'] = start_date
    if end_date:
        kwargs['end_date'] = end_date
    if fields:
        kwargs['fields'] = fields
    
    df = python_fetch('us_tycr', pro=pro, token=token, **kwargs)
    
    if save and df is not None and not df.empty:
        init_macro_tables()
        save_to_db(df, 'us_tycr')
    
    return df


def _parse_kv(items):
    kwargs = {}
    for item in items or []:
        if '=' not in item:
            raise ValueError(f"Invalid --arg format: {item}. Expected key=value")
        key, value = item.split('=', 1)
        kwargs[key] = value
    return kwargs


def main():
    parser = argparse.ArgumentParser(description='Unified Tushare fetch CLI with DB storage')
    parser.add_argument('--api', required=True, help='Tushare API name, e.g. sf_month, cn_m, us_tycr')
    parser.add_argument('--arg', action='append', default=[], help='API argument in key=value format')
    parser.add_argument('--token', default=None, help='Optional Tushare token')
    parser.add_argument('--head', type=int, default=5, help='Print first N rows')
    parser.add_argument('--save', action='store_true', help='Save data to database')
    args = parser.parse_args()

    kwargs = _parse_kv(args.arg)
    
    # 如果是宏观数据接口，使用专门的函数
    if args.api in ('sf_month', 'cn_m', 'us_tycr'):
        if args.api == 'sf_month':
            df = fetch_sf_month(**kwargs, token=args.token, save=args.save)
        elif args.api == 'cn_m':
            df = fetch_cn_m(**kwargs, token=args.token, save=args.save)
        elif args.api == 'us_tycr':
            df = fetch_us_tycr(**kwargs, token=args.token, save=args.save)
    else:
        # 其他接口使用通用方法
        df = python_fetch(args.api, token=args.token, **kwargs)

    rows = 0 if df is None else len(df)
    print(f"api={args.api}, rows={rows}")
    if rows > 0:
        print(df.head(args.head).to_string(index=False))


if __name__ == '__main__':
    main()
