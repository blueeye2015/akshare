#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
概念股筛选脚本 - 使用 pywencai 获取概念板块数据
"""

import pywencai
import pandas as pd
from datetime import datetime

# 要查询的概念列表
CONCEPTS = ["户用储能", "钾肥", "煤化工"]

def get_concept_data(concept_name):
    """
    获取单个概念板块的数据
    """
    print(f"\n{'='*60}")
    print(f"概念板块: {concept_name}")
    print('='*60)
    
    try:
        # 获取该概念的所有股票
        query = f"{concept_name}概念股"
        df = pywencai.get(query=query, loop=True)
        
        if df is None or df.empty:
            print(f"  未获取到数据")
            return
        
        # 显示股票数量
        stock_count = len(df)
        print(f"\n📊 股票数量: {stock_count} 只")
        
        # 打印列名以便调试
        # print(f"\n可用字段: {list(df.columns)}")
        
        # 涨幅领涨前3只
        print("\n📈 涨幅领涨 TOP 3:")
        if '涨跌幅' in df.columns:
            top_gainers = df.nlargest(3, '涨跌幅')[['股票代码', '股票简称', '涨跌幅']]
        elif '涨幅' in df.columns:
            top_gainers = df.nlargest(3, '涨幅')[['股票代码', '股票简称', '涨幅']]
        else:
            # 尝试查找涨幅相关列
            gain_col = [c for c in df.columns if '涨' in c or '幅' in c]
            if gain_col:
                top_gainers = df.nlargest(3, gain_col[0])[['股票代码', '股票简称', gain_col[0]]]
            else:
                top_gainers = df.head(3)[['股票代码', '股票简称']]
        
        for idx, row in top_gainers.iterrows():
            print(f"   {row['股票代码']} {row['股票简称']}", end="")
            for col in row.index:
                if col not in ['股票代码', '股票简称']:
                    print(f" {row[col]}", end="")
            print()
        
        # 市值龙头前3只
        print("\n💰 市值龙头 TOP 3:")
        if '总市值' in df.columns:
            top_cap = df.nlargest(3, '总市值')[['股票代码', '股票简称', '总市值']]
        elif '流通市值' in df.columns:
            top_cap = df.nlargest(3, '流通市值')[['股票代码', '股票简称', '流通市值']]
        else:
            cap_col = [c for c in df.columns if '市值' in c]
            if cap_col:
                top_cap = df.nlargest(3, cap_col[0])[['股票代码', '股票简称', cap_col[0]]]
            else:
                top_cap = df.head(3)[['股票代码', '股票简称']]
        
        for idx, row in top_cap.iterrows():
            print(f"   {row['股票代码']} {row['股票简称']}", end="")
            for col in row.index:
                if col not in ['股票代码', '股票简称']:
                    val = row[col]
                    if isinstance(val, (int, float)) and val > 100000000:
                        print(f" {val/100000000:.2f}亿", end="")
                    else:
                        print(f" {val}", end="")
            print()
        
        # 失意者（跌幅最大）前3只
        print("\n📉 失意者(跌幅最大) TOP 3:")
        if '涨跌幅' in df.columns:
            bottom_gainers = df.nsmallest(3, '涨跌幅')[['股票代码', '股票简称', '涨跌幅']]
        elif '涨幅' in df.columns:
            bottom_gainers = df.nsmallest(3, '涨幅')[['股票代码', '股票简称', '涨幅']]
        else:
            gain_col = [c for c in df.columns if '涨' in c or '幅' in c]
            if gain_col:
                bottom_gainers = df.nsmallest(3, gain_col[0])[['股票代码', '股票简称', gain_col[0]]]
            else:
                bottom_gainers = df.tail(3)[['股票代码', '股票简称']]
        
        for idx, row in bottom_gainers.iterrows():
            print(f"   {row['股票代码']} {row['股票简称']}", end="")
            for col in row.index:
                if col not in ['股票代码', '股票简称']:
                    print(f" {row[col]}", end="")
            print()
        
        # 业绩增速最佳前3只
        print("\n🚀 业绩增速最佳 TOP 3:")
        growth_cols = [c for c in df.columns if any(k in c for k in ['净利润增长', '业绩增长', '营收增长', '增长率', '同比'])]
        if growth_cols:
            # 选择第一个包含增长数据的列
            growth_col = growth_cols[0]
            # 过滤掉空值和异常值
            df_growth = df[df[growth_col].notna()].copy()
            if not df_growth.empty:
                top_growth = df_growth.nlargest(3, growth_col)[['股票代码', '股票简称', growth_col]]
                for idx, row in top_growth.iterrows():
                    print(f"   {row['股票代码']} {row['股票简称']} {growth_col}:{row[growth_col]}")
            else:
                print("   无有效业绩数据")
        else:
            print("   未找到业绩增速字段")
        
    except Exception as e:
        print(f"  错误: {e}")

def main():
    print("="*60)
    print("概念股筛选报告")
    print(f"生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*60)
    
    for concept in CONCEPTS:
        get_concept_data(concept)
    
    print("\n" + "="*60)
    print("报告生成完毕")
    print("="*60)

if __name__ == "__main__":
    main()
