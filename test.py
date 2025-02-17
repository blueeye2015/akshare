import akshare as ak
import pandas as pd
from sqlalchemy import create_engine
import numpy as np
database_url = 'postgresql://postgres:12@localhost:5432/Financialdata'
engine = create_engine(database_url)
# 获取数据
#stock_balance_sheet_by_report_em_df = ak.stock_balance_sheet_by_report_em(symbol="SH600519")
#stock_profit_sheet_by_quarterly_em_df = ak.stock_profit_sheet_by_quarterly_em(symbol="SH600519")
#stock_yjkb_em_df = ak.stock_yjkb_em(date="20241231")
df = ak.stock_info_a_code_name()
df.to_sql('stock_info_a_code_name', engine, if_exists='replace', index=False)
#stock_yjkb_em_df = ak.stock_yjkb_em(date="20200331")
# 保存为CSV文件
#stock_yjkb_em_df.to_csv('yjkb_sheet.csv', encoding='utf-8-sig', index=False)