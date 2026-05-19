#!/usr/bin/env python3
import psycopg2

conn = psycopg2.connect(host='localhost', port=5432, database='Financialdata', user='postgres', password='12')
cur = conn.cursor()

print('=== 金财互联数据验证 ===')

# 今年预告
cur.execute("SELECT forecast_value FROM performance_forecast WHERE symbol = '002530' AND report_period = '20251231' AND forecast_indicator = '扣除非经常性损益后的净利润'")
fy_2025 = cur.fetchone()[0]
print(f'今年预告全年: {fy_2025}')

# 今年Q3
cur.execute("SELECT deduct_parent_netprofit FROM profit_sheet WHERE symbol = '002530.SZ' AND report_date::text LIKE '2025-09-30%' LIMIT 1")
q3_2025 = cur.fetchone()[0]
print(f'今年Q3累计: {q3_2025}')

# 去年预告
cur.execute("SELECT forecast_value FROM performance_forecast WHERE symbol = '002530' AND report_period = '20241231' AND forecast_indicator = '扣除非经常性损益后的净利润'")
fy_2024 = cur.fetchone()[0]
print(f'去年预告全年: {fy_2024}')

# 去年Q3
cur.execute("SELECT deduct_parent_netprofit FROM profit_sheet WHERE symbol = '002530.SZ' AND report_date::text LIKE '2024-09-30%' LIMIT 1")
q3_2024 = cur.fetchone()[0]
print(f'去年Q3累计: {q3_2024}')

q4_2025 = float(fy_2025) - float(q3_2025)
q4_2024 = float(fy_2024) - float(q3_2024)
yoy = (q4_2025 - q4_2024) / abs(q4_2024)

print(f'\n今年Q4单季度: {q4_2025:.0f}')
print(f'去年Q4单季度: {q4_2024:.0f}')
print(f'Q4单季度同比: {yoy:.2%}')

conn.close()
