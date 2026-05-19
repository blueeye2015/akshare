#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
选股结果分类分析器

把量化选股结果（双增/单增 CSV）组织成"赛道地图 + 个股速览"，
辅助人工通过行业判断和财报分析做二次精选。

不新增任何买入/卖出量化指标，只做数据组织与呈现。
"""

import pandas as pd
import numpy as np
import argparse
import re
from pathlib import Path
from collections import Counter


def load_selection(csv_path: str) -> pd.DataFrame:
    """加载选股结果 CSV"""
    df = pd.read_csv(csv_path, encoding='utf-8-sig')
    # 数值列确保是 float
    for col in ['market_cap', 'pe_ttm', 'deduct_pe', 'peg',
                'quarter_profit_growth', 'quarter_revenue_growth', 'ltm_profit_growth']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    return df


def build_sector_map(df: pd.DataFrame) -> pd.DataFrame:
    """
    按行业分组，输出"赛道画像"：
    - 该赛道有多少只股票
    - 市值/增速/PE 的中位数
    - 代表股（PEG最低的前3只）
    """
    if 'industry' not in df.columns:
        return pd.DataFrame()

    sectors = []
    for industry, group in df.groupby('industry'):
        group = group.sort_values('peg', ascending=True)
        representatives = group.head(3)[['symbol', 'name', 'peg', 'quarter_profit_growth']].to_dict('records')
        sectors.append({
            '赛道（行业）': industry,
            '股票数': len(group),
            '总市值中位数(亿)': group['market_cap'].median(),
            'PE_TTM中位数': group['pe_ttm'].median(),
            '扣非PE中位数': group['deduct_pe'].median(),
            'PEG中位数': group['peg'].median(),
            '季度扣非增速中位数': group['quarter_profit_growth'].median(),
            '季度营收增速中位数': group['quarter_revenue_growth'].median(),
            '代表股': ' | '.join([f"{r['name']}({r['symbol']}, PEG={r['peg']:.3f})" for r in representatives])
        })

    sector_df = pd.DataFrame(sectors)
    sector_df = sector_df.sort_values('股票数', ascending=False)
    return sector_df


def build_concept_map(df: pd.DataFrame, top_k: int = 30) -> pd.DataFrame:
    """
    按概念分组统计（取出现频次最高的 top_k 个概念）
    """
    if 'concepts' not in df.columns:
        return pd.DataFrame()

    all_concepts = []
    concept_symbols = {}  # concept -> list of symbols
    for _, row in df.iterrows():
        concepts_str = str(row['concepts']) if pd.notna(row['concepts']) else ''
        for c in concepts_str.split(';'):
            c = c.strip()
            if c and c != 'nan':
                all_concepts.append(c)
                concept_symbols.setdefault(c, []).append(row['symbol'])

    counter = Counter(all_concepts)
    results = []
    for concept, count in counter.most_common(top_k):
        symbols = concept_symbols[concept]
        sub = df[df['symbol'].isin(symbols)]
        results.append({
            '概念': concept,
            '出现次数': count,
            '占比': f"{count/len(df)*100:.1f}%",
            'PEG中位数': sub['peg'].median(),
            '市值中位数(亿)': sub['market_cap'].median(),
            '代表股': ' '.join(sub.sort_values('peg').head(3)['name'].tolist())
        })
    return pd.DataFrame(results)


def extract_mainbiz_keywords(df: pd.DataFrame, top_k: int = 20) -> pd.DataFrame:
    """
    从主营业务中提取高频关键词（2~4字），帮助发现细分赛道
    """
    if 'main_business' not in df.columns:
        return pd.DataFrame()

    # 简单分词：按 '+' 和常见分隔符拆分
    keywords = []
    for _, row in df.iterrows():
        biz = str(row['main_business']) if pd.notna(row['main_business']) else ''
        parts = re.split(r'[\+\s\|;/]', biz)
        for p in parts:
            p = p.strip()
            if 2 <= len(p) <= 8 and p not in ['N/A', 'nan', '其他业务', '其他']:
                keywords.append(p)

    counter = Counter(keywords)
    results = []
    for kw, count in counter.most_common(top_k):
        mask = df['main_business'].astype(str).str.contains(kw, na=False, regex=False)
        sub = df[mask]
        results.append({
            '主营关键词': kw,
            '出现次数': count,
            '相关股票': ' '.join(sub.sort_values('peg').head(5)['name'].tolist())
        })
    return pd.DataFrame(results)


def flag_attention_signals(df: pd.DataFrame) -> pd.DataFrame:
    """
    标记一些需要人工特别注意的信号，不做买卖判断，只提示风险/特征：
    - 基数效应：单季增速极高（>1000%）但 LTM 增速一般（<100%）
    - 大市值：市值 > 2000亿（散户弹性可能不足）
    - 小市值：市值 < 50亿（流动性风险）
    - 高PE：PE_TTM > 40（估值偏贵）
    - 扭亏为盈：单季利润为正但 LTM 增速极高（可能刚扭亏）
    """
    df = df.copy()
    df['attention'] = ''

    signals = []

    # 1. 基数效应嫌疑
    mask = (df['quarter_profit_growth'] > 10.0) & (df['ltm_profit_growth'] < 1.0)
    signals.append((mask, '基数效应(单季暴增但LTM一般)'))

    # 2. 大市值
    mask = df['market_cap'] > 2000
    signals.append((mask, '大市值(弹性或不足)'))

    # 3. 小市值
    mask = df['market_cap'] < 50
    signals.append((mask, '小市值(关注流动性)'))

    # 4. 高PE
    mask = df['pe_ttm'] > 40
    signals.append((mask, 'PE偏高'))

    # 5. 扭亏为盈嫌疑（LTM增速极高 >500%，但PE不高）
    mask = (df['ltm_profit_growth'] > 5.0) & (df['pe_ttm'] < 30)
    signals.append((mask, '可能刚扭亏(LTM增速极高)'))

    for mask, signal in signals:
        df.loc[mask, 'attention'] += signal + '; '

    df['attention'] = df['attention'].str.rstrip('; ')
    return df


def build_stock_cards(df: pd.DataFrame) -> str:
    """
    为每只股票生成"速览卡片"，按 PEG 升序排列
    """
    lines = []
    df = df.sort_values('peg', ascending=True)

    for _, row in df.iterrows():
        name = row.get('name', '')
        symbol = row.get('symbol', '')
        industry = row.get('industry', '')
        main_biz = str(row.get('main_business', '')).replace('nan', 'N/A')
        concepts = str(row.get('concepts', '')).replace('nan', '')
        attention = str(row.get('attention', '')).replace('nan', '')

        mc = row.get('market_cap', np.nan)
        pe = row.get('pe_ttm', np.nan)
        dpe = row.get('deduct_pe', np.nan)
        peg = row.get('peg', np.nan)
        qpg = row.get('quarter_profit_growth', np.nan)
        qrg = row.get('quarter_revenue_growth', np.nan)
        ltmg = row.get('ltm_profit_growth', np.nan)

        card = []
        card.append(f"【{name} {symbol} | {industry}】")
        card.append(f"  主营: {main_biz}")
        if concepts:
            card.append(f"  概念: {concepts}")
        card.append(f"  市值:{mc:.1f}亿 PE:{pe:.1f} 扣非PE:{dpe:.1f} PEG:{peg:.3f}")
        card.append(f"  增速: 单季扣非+{qpg*100:.1f}% 单季营收+{qrg*100:.1f}% LTM扣非+{ltmg*100:.1f}%")
        if attention:
            card.append(f"  ⚠️ 注意: {attention}")
        card.append("")
        lines.append('\n'.join(card))

    return '\n'.join(lines)


def generate_analysis_report(df: pd.DataFrame, strategy_name: str) -> str:
    """生成完整分析报告"""
    report = []
    report.append("=" * 80)
    report.append(f"📋 {strategy_name} 选股决策手册")
    report.append("=" * 80)
    report.append(f"总股票数: {len(df)}")
    report.append("")

    # --- 赛道地图 ---
    report.append("-" * 80)
    report.append("📊 一、赛道地图（按行业分组）")
    report.append("-" * 80)
    sector_df = build_sector_map(df)
    if not sector_df.empty:
        report.append(sector_df.to_string(index=False))
    report.append("")

    # --- 概念热度 ---
    report.append("-" * 80)
    report.append("🔥 二、概念热度 Top 30")
    report.append("-" * 80)
    concept_df = build_concept_map(df, top_k=30)
    if not concept_df.empty:
        report.append(concept_df.to_string(index=False))
    report.append("")

    # --- 主营关键词 ---
    report.append("-" * 80)
    report.append("🏭 三、主营关键词（发现细分赛道）")
    report.append("-" * 80)
    kw_df = extract_mainbiz_keywords(df, top_k=20)
    if not kw_df.empty:
        report.append(kw_df.to_string(index=False))
    report.append("")

    # --- 个股速览 ---
    report.append("-" * 80)
    report.append("📑 四、个股速览卡片（按 PEG 排序）")
    report.append("-" * 80)
    report.append("说明：以下不做买卖建议，只帮你快速了解每只股票在'做什么、增速如何、有无异常'\n")

    df_flagged = flag_attention_signals(df)
    cards = build_stock_cards(df_flagged)
    report.append(cards)

    report.append("=" * 80)
    return '\n'.join(report)


def main():
    parser = argparse.ArgumentParser(description='选股结果分类分析器')
    parser.add_argument('csv', type=str, help='选股结果 CSV 文件路径')
    parser.add_argument('--output', '-o', type=str, default=None, help='输出报告文件路径')
    args = parser.parse_args()

    csv_path = Path(args.csv)
    if not csv_path.exists():
        print(f"❌ 文件不存在: {csv_path}")
        return

    df = load_selection(str(csv_path))
    if df.empty:
        print("❌ CSV 为空")
        return

    strategy_name = csv_path.stem
    report = generate_analysis_report(df, strategy_name)

    print(report)

    output_path = args.output or csv_path.with_suffix('.analysis.txt')
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(report)
    print(f"\n✅ 报告已保存: {output_path}")


if __name__ == '__main__':
    main()
