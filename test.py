import akshare as ak

# 获取数据
#stock_balance_sheet_by_report_em_df = ak.stock_balance_sheet_by_report_em(symbol="SH600519")
#stock_profit_sheet_by_quarterly_em_df = ak.stock_profit_sheet_by_quarterly_em(symbol="SH600519")
stock_yjyg_em_df = ak.stock_yjyg_em(date="20240331")

#stock_yjkb_em_df = ak.stock_yjkb_em(date="20200331")
# 保存为CSV文件
stock_yjyg_em_df.to_csv('yjyg_sheet.csv', encoding='utf-8-sig', index=False)