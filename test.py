import akshare as ak
import pandas as pd
from sqlalchemy import create_engine
import numpy as np
import tushare as ts
#database_url = 'postgresql://postgres:12@localhost:5432/Financialdata'
#engine = create_engine(database_url)
# 获取数据
#stock_financial_analysis_indicator = ak.stock_financial_analysis_indicator(symbol='002315', start_year='2024')
#print(stock_financial_analysis_indicator)
#stock_balance_sheet_by_report_em_df = ak.stock_balance_sheet_by_report_em(symbol="SZ000099")
#stock_cash_flow_sheet_by_report_em = ak.stock_cash_flow_sheet_by_report_em(symbol="SZ000534")
#Wnews_report_time_baidu=ak.news_report_time_baidu(date="20250331")
#stock_profit_sheet_by_report_em = ak.stock_profit_sheet_by_report_em(symbol="SZ300502")
#stock_yjkb_em_df = ak.stock_yjkb_em(date="20241231")
# df = ak.stock_info_a_code_name()
# df.to_sql('stock_info_a_code_name', engine, if_exists='replace', index=False)
#stock_yjyg_em = ak.stock_yjyg_em(date="20250331")
# 保存为CSV文件
#stock_profit_sheet_by_report_em.to_csv('SZ300502.csv', encoding='utf-8-sig', index=False)
#print(stock_yjyg_em)
#stock_a_indicator_lg = ak.stock_a_indicator_lg(symbol='SZ301560')
#print(stock_a_indicator_lg)
#stock_board_industry_name_em = ak.stock_board_industry_name_em() 
#print (stock_board_industry_name_em)
#stock_individual_info_em_df = ak.stock_individual_info_em(symbol="SZ000001")
#print(stock_balance_sheet_by_report_em_df)
# df = ak.stock_info_sz_name_code(symbol="A股列表")
# df.to_sql('stock_info_a_code_name', engine, if_exists='replace', index=False)
#stock_zh_a_hist_df = ak.stock_zh_a_hist(symbol="000001", period="daily", start_date="20170301", end_date='20240528', adjust="")
#print(stock_zh_a_hist_df)
#stock_individual_info_em_df = ak.stock_individual_info_em(symbol="000001")
#print(stock_individual_info_em_df)
#stock_mda_ym_df = ak.stock_mda_ym(symbol="002259")
#stock_mda_ym_df.to_csv('SZ002259.csv', encoding='utf-8-sig', index=False)

pro = ts.pro_api('540a303240aac02dc8cfeaa32f1110aacf880c9fb8d3cd5dd395af4c')

df = pro.balancesheet(ts_code='688186.SH', start_date='20180101', end_date='20250430')
df.to_csv('688186.csv', encoding='utf-8-sig', index=False)