import akshare as ak
import pandas as pd
from sqlalchemy import create_engine
import numpy as np
# database_url = 'postgresql://postgres:12@192.168.50.149:5432/Financialdata'
# engine = create_engine(database_url)
# 获取数据
#stock_financial_analysis_indicator = ak.stock_financial_analysis_indicator(symbol='002315', start_year='2024')
#print(stock_financial_analysis_indicator)
#stock_balance_sheet_by_report_em_df = ak.stock_balance_sheet_by_report_em(symbol="SZ000099")
#stock_cash_flow_sheet_by_report_em = ak.stock_cash_flow_sheet_by_report_em(symbol="SZ000534")
#Wnews_report_time_baidu=ak.news_report_time_baidu(date="20250331")
#stock_profit_sheet_by_report_em = ak.stock_profit_sheet_by_report_em(symbol="SH603409")
#stock_yjkb_em_df = ak.stock_yjkb_em(date="20241231")
# df = ak.stock_info_a_code_name()
# df.to_sql('stock_info_a_code_name', engine, if_exists='replace', index=False)
#stock_yjyg_em = ak.stock_yjyg_em(date="20241231")
# 保存为CSV文件
#stock_yjyg_em.to_csv('20241231.csv', encoding='utf-8-sig', index=False)
#print(stock_yjyg_em)
stock_a_indicator_lg = ak.stock_a_indicator_lg(symbol='SZ301560')
print(stock_a_indicator_lg)
#stock_individual_info_em_df = ak.stock_individual_info_em(symbol="SZ000001")
#print(stock_balance_sheet_by_report_em_df)
