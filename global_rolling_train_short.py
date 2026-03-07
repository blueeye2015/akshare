#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
STEP 2: 全市场滚动训练 (适配新版特征名 & Batch文件读取)
功能：
1. 滚动读取过去 60 个月的数据（包含所有Batch碎片）
2. 训练 LightGBM 大模型，学习 D-MOM (动量+波动+反转) 逻辑
3. 预测当月所有股票的 Alpha 分数
"""
import os, pandas as pd, numpy as np, lightgbm as lgb
import glob
from datetime import datetime

# --- 配置 ---
DATA_DIR = 'cache/monthly_chunks'
FACTOR_OUTPUT_DIR = 'factor_cache_global_short'
os.makedirs(FACTOR_OUTPUT_DIR, exist_ok=True)

# ✅ 关键修改：特征名必须与 prepare_data_daily.py 生成的完全一致
FEATURES = [
    'log_mv_t1',            # 市值
    'turnover_1m_t1',       # 换手率
    'IV_20d_t1',            # 特质波动率 (D-MOM核心)
    'up_streak_t1',         # 连涨天数 (D-MOM核心)
    'down_streak_t1',       # 连跌天数 (D-MOM核心)
    'return_1m_t1',         # 月度反转
    'return_6m_t1'          # 中期动量
]
TARGET = 'target_label'

LGB_PARAMS = {
    'objective': 'binary',
    'metric': 'auc',
    'n_estimators': 300,
    'learning_rate': 0.05,
    'num_leaves': 31,
    'verbose': -1,
    'n_jobs': 4  # 训练也可以并行
}

def get_all_months():
    # 扫描目录下所有的 parquet 文件，提取月份
    files = glob.glob(os.path.join(DATA_DIR, "*.parquet"))
    months = set()
    for f in files:
        # 文件名可能是 "2015-01.parquet" 或 "2015-01_batch_1.parquet"
        filename = os.path.basename(f)
        # 提取 "2015-01"
        m = filename[:7] 
        months.add(m)
    return sorted(list(months))

def train_and_predict():
    months = get_all_months()
    if not months:
        print("❌ 未找到数据文件，请先运行 prepare_data_daily.py")
        return

    # --- 🔴 原代码 (太保守了) ---
    # 从 2019年开始预测 (给前面留 5-10 年训练期)
    # start_pred_index = months.index('2019-01') if '2019-01' in months else 60
    
    # --- ✅ 修改后 (激进模式：Walk-Forward) ---
    # 只要有 36 个月 (3年) 的数据，就开始预测第 37 个月
    MIN_TRAIN_MONTHS = 36
    
    # 找到 2013-01 或者 第36个月作为起点
    if '2013-01' in months:
        start_pred_index = months.index('2013-01')
    else:
        start_pred_index = MIN_TRAIN_MONTHS

    print(f"检测到数据覆盖: {months[0]} -> {months[-1]}")
    print(f"🔥 策略升级：将从 {months[start_pred_index]} 开始滚动回测 (大幅延长回测周期)！")

    # --- 滚动循环 ---
    for i in range(start_pred_index, len(months)):
        pred_month = months[i]
        
        # 1. 动态确定训练窗口
        # 方案A：固定窗口 (例如过去 60 个月)，旧数据会被丢弃，适应性强
        # 方案B：累计窗口 (从头开始)，数据越来越多，模型越稳
        
        # 这里建议用方案A (Rolling Window)，保持模型对市场风格切换的敏感度
        ROLLING_WINDOW = 60 # 5年
        
        train_end_idx = i - 2 # 隔离一个月 (不变)

        # 关键：如果训练数据不足 60 个月，有多少用多少，而不是跳过
        train_start_idx = max(0, train_end_idx - ROLLING_WINDOW)
        
        # 只要训练数据超过 24 个月就可以跑 (不必非要等满60个月)
        if (train_end_idx - train_start_idx + 1) < 24:
            print(f"[{pred_month}] ⚠️ 历史数据不足24个月，跳过预测")
            continue

        train_months = months[train_start_idx : train_end_idx + 1]

        print(f"\n[{pred_month}] 正在训练...")
        print(f"  - 训练集范围: {train_months[0]} -> {train_months[-1]} (共{len(train_months)}个月)")
        
        # 2. 加载训练数据 (支持 Batch 读取)
        train_dfs = []
        for m in train_months:
            # 匹配该月所有 batch 文件
            batch_files = glob.glob(os.path.join(DATA_DIR, f"{m}*.parquet"))
            for f in batch_files:
                train_dfs.append(pd.read_parquet(f))
        
        if not train_dfs:
            print(f"  - ⚠️ 训练数据为空，跳过")
            continue

        df_train = pd.concat(train_dfs)
        
        # 再次确保只保留需要的列，节省内存
        df_train = df_train[FEATURES + [TARGET]].dropna()
        
        if df_train.empty:
            print("  - ⚠️ 有效样本不足，跳过")
            continue

        # 3. 训练全市场模型
        model = lgb.LGBMClassifier(**LGB_PARAMS)
        model.fit(df_train[FEATURES], df_train[TARGET])
        
        # 打印 Top 特征 (看看模型是不是真的学会了 D-MOM)
        imp = pd.Series(model.feature_importances_, index=FEATURES).sort_values(ascending=False)
        print(f"  - Top3特征: {imp.index[0]}({imp.iloc[0]}), {imp.index[1]}({imp.iloc[1]}), {imp.index[2]}({imp.iloc[2]})")
        
        # 4. 预测当月
        # 同样要读取当月所有的 batch 文件
        pred_batch_files = glob.glob(os.path.join(DATA_DIR, f"{pred_month}*.parquet"))
        pred_dfs = []
        for f in pred_batch_files:
            pred_dfs.append(pd.read_parquet(f))
            
        if not pred_dfs:
            print(f"  - ⚠️ 预测月无数据")
            continue
            
        df_pred = pd.concat(pred_dfs)
        X_pred = df_pred[FEATURES].fillna(0)
        
        # 生成因子分数
        # predict_proba 返回 [概率0, 概率1]，我们取 概率1
        df_pred['factor'] = model.predict_proba(X_pred)[:, 1]
        
        # 5. 保存结果
        output_path = os.path.join(FACTOR_OUTPUT_DIR, f"factor_{pred_month}.parquet")
        # 只保留必要的列用于回测
        cols_to_save = ['trade_date', 'symbol', 'factor', 'close']
        df_pred[cols_to_save].to_parquet(output_path)
        print(f"  - 预测完成，已保存至 {output_path}")

if __name__ == '__main__':
    train_and_predict()