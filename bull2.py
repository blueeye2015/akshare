import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

# ---------------------------------------------------------
# 1. 配置区域：你可以修改这里的文件名和日期参数
# ---------------------------------------------------------
file_path = 'vol.csv'  # 请替换为你的csv文件名
output_filename = 'bull2_volume_analysis.png'
# 定义各个"牛二"周期的峰值日期（T=0时刻）
bull2_peaks = {
    '2009 Bull 2': '2009-11-24',
    '2014 Bull 2': '2014-12-09',
    '2020 Bull 2': '2020-07-07',
    '2025 Bull 2 (Current)': '2025-08-25'
}

# 定义颜色方案 (历史用冷色调，当前用醒目的红色)
colors = {
    '2009 Bull 2': '#1f77b4',  # 蓝色
    '2014 Bull 2': '#2ca02c',  # 绿色
    '2020 Bull 2': '#9467bd',  # 紫色
    '2025 Bull 2 (Current)': '#d62728' # 红色
}

# 定义线型 (历史用虚线，当前用实线)
styles = {
    '2009 Bull 2': '--',
    '2014 Bull 2': '--',
    '2020 Bull 2': '--',
    '2025 Bull 2 (Current)': '-'
}

# 定义线宽
widths = {
    '2009 Bull 2': 2.0,
    '2014 Bull 2': 2.0,
    '2020 Bull 2': 2.0,
    '2025 Bull 2 (Current)': 3.0
}

# ---------------------------------------------------------
# 2. 数据处理逻辑
# ---------------------------------------------------------

# 读取数据
df = pd.read_csv(file_path)

# 重命名列以匹配逻辑 (根据你的csv结构调整)
# 假设csv第一列是金额，第二列是日期
df.rename(columns={df.columns[0]: 'amount', df.columns[1]: 'date'}, inplace=True)
df['date'] = pd.to_datetime(df['date'])

# 数据平滑处理：计算10日移动平均，过滤掉单日剧烈波动
df['amount_smooth'] = df['amount'].rolling(window=10).mean()

# ---------------------------------------------------------
# 3. 绘图逻辑
# ---------------------------------------------------------
plt.figure(figsize=(14, 8))

# 遍历每个周期进行绘图
for label, peak_date_str in bull2_peaks.items():
    peak_date = pd.to_datetime(peak_date_str)
    
    # 如果日期不在数据中，跳过
    if peak_date not in df['date'].values:
        print(f"警告: 数据中找不到日期 {peak_date_str}，跳过该周期。")
        continue
        
    # 获取峰值当天的索引
    start_idx = df[df['date'] == peak_date].index[0]
    
    # 获取归一化的基准值：通常使用【峰值当天的原始成交额】
    # 这样可以展示从"最高点"掉下来的真实比例
    peak_val_raw = df.loc[start_idx, 'amount']
    
    # 截取峰值之后的 150 个交易日 (约 7 个月)
    subset = df.iloc[start_idx : start_idx + 150].copy()
    
    # 生成相对时间轴 (Day 0, Day 1, ...)
    subset['days_since'] = range(len(subset))
    
    # 计算归一化曲线：(平滑后的成交额 / 峰值当天成交额)
    subset['norm_smooth'] = subset['amount_smooth'] / peak_val_raw
    
    # 绘制曲线
    plt.plot(subset['days_since'], subset['norm_smooth'], 
             label=label,
             color=colors[label],
             linestyle=styles[label],
             linewidth=widths[label],
             alpha=0.9)

# ---------------------------------------------------------
# 4. 添加辅助元素
# ---------------------------------------------------------

# 添加参考线
plt.axhline(y=0.50, color='orange', linestyle=':', linewidth=2, label='当前水位 (~50%)')
plt.axhline(y=0.30, color='black', linestyle=':', linewidth=2, label='历史底部水位 (~30%)')

# 添加标题和坐标轴标签
plt.title('A股"牛二"阶段成交额缩量路径对比 (归一化)', fontsize=16, fontproperties='SimHei') # 如果没有中文字体，请删除 fontproperties 或改为英文
plt.xlabel('距离峰值的交易天数', fontsize=12, fontproperties='SimHei')
plt.ylabel('成交额留存比例 (1.0 = 峰值)', fontsize=12, fontproperties='SimHei')

# 图例和网格
plt.legend(fontsize=10, loc='upper right')
plt.grid(True, alpha=0.3)

# 添加说明文本框
note_text = (
    "历史规律:\n"
    "2009/2014/2020年的调整均显示，\n"
    "成交额需缩量至峰值的 30% 左右方见底。\n\n"
    "当前状态 (红线):\n"
    "缩量仅 50%，距离历史底部仍有空间。"
)
plt.text(0.02, 0.05, note_text, transform=plt.gca().transAxes, 
         fontsize=11, fontproperties='SimHei',
         bbox=dict(facecolor='white', alpha=0.9, edgecolor='gray'))

plt.tight_layout()
print(f"正在保存图片到: {output_filename} ...")
plt.savefig(output_filename, dpi=300, bbox_inches='tight')
print("保存成功！")