import json

def json_to_markdown(json_file, output_file):
    with open(json_file, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write("# 骑行客投资逻辑知识库\n\n")
        
        for item in data:
            # 标题与元数据
            f.write(f"## {item['decoded_title']}\n")
            f.write(f"- **文件名**: {item.get('filename', 'N/A')}\n")
            f.write(f"- **市场阶段**: {item['market_phase']}\n")
            f.write(f"- **标签**: {', '.join(item['tags'])}\n")
            f.write(f"- **置信度**: {item['confidence_level']}/10\n\n")
            
            # 定量逻辑提取
            f.write("### 核心定量逻辑与准则\n")
            quant = item['quant_logic']
            if isinstance(quant, dict):
                for k, v in quant.items():
                    f.write(f"- **{k}**: {json.dumps(v, ensure_ascii=False)}\n")
            else:
                f.write(f"> {quant}\n")
            
            # 图表与深度洞察
            f.write("\n### 深度洞察与图表分析\n")
            f.write(f"{item.get('chart_insight', '暂无深度洞察')}\n\n")
            f.write("---\n\n")

# 执行转换
json_to_markdown('knowledge_base_with_charts.json', 'dify_kb_ready.md')
print("转换完成！请将 'dify_kb_ready.md' 上传至 Dify 知识库。")