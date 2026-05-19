#!/usr/bin/env python3
"""验证金财互联Q4单季度计算"""

import psycopg2
import pandas as pd

conn = psycopg2.connect(host='localhost', port=5432, database='Financialdata', user='postgres', password='12')

# 获取金财互联数据
symbol_full = '002530.SZ'
symbol_short = '002530'

print('=== 金财互联 (002530) 数据验证 ===\n')

# 1. 今年预告全年 (2025)
cur = conn.cursor()
cur.execute("""
    SELECT forecast_value, report_period 
    FROM performance_forecast 
    WHERE symbol = %s AND report_period = '20251231' 
    AND forecast_indicator = '扣除非经常性损益后的净利润'
""", (symbol_short,))
row = cur.fetchone()
if row:
    fy_2025_wan = row[0] / 10000  # 转万元
    print(f"1. 今年预告全年 (2025): {fy_2025_wan:.2f} 万元")

# 2. 今年Q3累计 (2025-09-30)
cur.execute("""
    SELECT deduct_parent_netprofit, report_date 
    FROM profit_sheet 
    WHERE symbol = %s AND report_date LIKE '2025-09-30%%'
""", (symbol_full,))
row = cur.fetchone()
if row:
    q3_2025 = float(row[0])
    print(f"2. 今年Q3累计 (2025-09-30): {q3_2025:.2f} 万元")

# 3. 去年Q4单季度 = 2024年报 - 2024Q3
cur.execute("""
    SELECT 
        MAX(CASE WHEN report_date LIKE '2024-12-31%%' THEN deduct_parent_netprofit END) as fy_2024,
        MAX(CASE WHEN report_date LIKE '2024-09-30%%' THEN deduct_parent_netprofit END) as q3_2024
    FROM profit_sheet 
    WHERE symbol = %s
""", (symbol_full,))
row = cur.fetchone()
if row and row[0] and row[1]:
    fy_2024 = float(row[0])
    q3_2024 = float(row[1])
    q4_2024 = fy_2024 - q3_2024
    print(f"3. 去年年报 (2024-12-31): {fy_2024:.2f} 万元")
    print(f"4. 去年Q3累计 (2024-09-30): {q3_2024:.2f} 万元")
    print(f"5. 去年Q4单季度: {q4_2024:.2f} 万元\n")

# 6. 计算今年Q4单季度
q4_2025 = fy_2025_wan - q3_2025
print(f"=== 计算结果 ===")
print(f"今年Q4单季度 = {fy_2025_wan:.2f} - {q3_2025:.2f} = {q4_2025:.2f} 万元")
print(f"去年Q4单季度 = {fy_2024:.2f} - {q3_2024:.2f} = {q4_2024:.2f} 万元")

# 7. 同比增速
yoy = (q4_2025 - q4_2024) / abs(q4_2024)
print(f"Q4单季度同比 = ({q4_2025:.2f} - ({q4_2024:.2f})) / {abs(q4_2024):.2f} = {yoy:.2%}")

conn.close()
