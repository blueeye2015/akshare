#!/usr/bin/env python3
"""
通过 akshare 接口 stock_repurchase_em 获取回购数据，并存入本地 PostgreSQL 数据库
"""
import os
import akshare as ak
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# 加载 .env 配置
load_dotenv()

DB_DSN = os.getenv("DB_DSN1", "postgresql://postgres:12@127.0.0.1:5432/Financialdata")
TABLE_NAME = "stock_repurchase"

# 列名映射（中文 -> 英文）
COLUMN_MAPPING = {
    "序号": "seq_no",
    "股票代码": "stock_code",
    "股票简称": "stock_name",
    "最新价": "latest_price",
    "计划回购价格区间": "planned_repurchase_price_range",
    "计划回购数量区间-下限": "planned_repurchase_qty_lower",
    "计划回购数量区间-上限": "planned_repurchase_qty_upper",
    "占公告前一日总股本比例-下限": "total_equity_ratio_lower",
    "占公告前一日总股本比例-上限": "total_equity_ratio_upper",
    "计划回购金额区间-下限": "planned_repurchase_amount_lower",
    "计划回购金额区间-上限": "planned_repurchase_amount_upper",
    "回购起始时间": "repurchase_start_date",
    "实施进度": "implementation_progress",
    "已回购股份价格区间-下限": "repurchased_price_lower",
    "已回购股份价格区间-上限": "repurchased_price_upper",
    "已回购股份数量": "repurchased_shares",
    "已回购金额": "repurchased_amount",
    "最新公告日期": "latest_announcement_date",
}


def fetch_data():
    print("正在从 akshare 获取股票回购数据...")
    df = ak.stock_repurchase_em()
    print(f"获取到 {len(df)} 条记录")
    return df


def transform_data(df: pd.DataFrame) -> pd.DataFrame:
    # 重命名列
    df = df.rename(columns=COLUMN_MAPPING)
    # 添加入库时间
    df["created_at"] = pd.Timestamp.now()
    return df


def save_to_db(df: pd.DataFrame):
    engine = create_engine(DB_DSN)
    
    # 测试连接
    with engine.connect() as conn:
        result = conn.execute(text("SELECT version()"))
        print("数据库连接成功:", result.scalar())
    
    # 写入数据（如果表存在则替换，或追加）
    # 这里使用 replace 方式，确保每次获取都是最新全量数据
    df.to_sql(
        TABLE_NAME,
        engine,
        if_exists="replace",
        index=False,
        chunksize=1000,
    )
    print(f"数据已成功写入表 {TABLE_NAME}")
    
    # 确认写入行数
    with engine.connect() as conn:
        result = conn.execute(text(f"SELECT COUNT(*) FROM {TABLE_NAME}"))
        count = result.scalar()
        print(f"表中现有 {count} 条记录")


def main():
    df = fetch_data()
    if df.empty:
        print("未获取到数据，退出")
        return
    df = transform_data(df)
    save_to_db(df)
    print("Done.")


if __name__ == "__main__":
    main()
