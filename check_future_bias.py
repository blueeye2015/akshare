import pandas as pd
import numpy as np
import glob
import os

# 1. 加载所有因子数据
print("正在加载因子数据...")
files = glob.glob('factor_cache_global/*.parquet')
df = pd.concat([pd.read_parquet(f) for f in files])

# 2. 准备验证数据
# 我们需要计算：当天的因子 vs 明天的收益率
df['trade_date'] = pd.to_datetime(df['trade_date'])
df = df.sort_values(['symbol', 'trade_date'])

# 计算"未来收益率" (这就是我们要预测的答案)
# shift(-1) 是把明天的涨幅拉到今天这一行
df['next_ret'] = df.groupby('symbol')['close'].pct_change().shift(-1)

# 3. 核心测试：计算 Rank IC (秩相关系数)
# 每天算一次因子和下期收益的相关性，然后取平均
ic_series = df.groupby('trade_date').apply(
    lambda x: x['factor'].corr(x['next_ret'], method='spearman')
)

print("\n" + "="*30)
print("🔍 未来函数测谎结果")
print("="*30)
print(f"IC 均值 (Mean IC): {ic_series.mean():.4f}")
print(f"IC 标准差 (IC Std): {ic_series.std():.4f}")
print(f"IR (IC/Std): {ic_series.mean() / ic_series.std():.4f}")

# 4. 判决
mean_ic = ic_series.mean()
if mean_ic > 0.3:
    print("\n❌【高危警报】IC值过高 (>0.3)！")
    print("正常策略不可能有这么高的预测能力。")
    print("极大概率使用了未来函数（比如用当天的收盘价预测当天的涨跌）。")
elif mean_ic > 0.02:
    print("\n✅【通过】IC值在合理区间 (0.02 - 0.15)。")
    print("这看起来像是一个真实的有效策略。")
else:
    print("\n⚠️【存疑】IC值很低或为负，策略可能不赚钱，但应该没有未来函数。")