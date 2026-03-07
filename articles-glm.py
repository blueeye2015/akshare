import os
import base64
import fitz  # PyMuPDF
from zai import ZhipuAiClient
from dotenv import load_dotenv

load_dotenv('.env')

api_key = os.getenv("ZHIPU_API_KEY")
if not api_key:
    raise ValueError("未找到 ZHIPU_API_KEY")

client = ZhipuAiClient(api_key=api_key)

def image_to_base64(image_bytes):
    return base64.b64encode(image_bytes).decode('utf-8')

def get_audit_prompt(filename):
    # 保持你的完整 Prompt
    return f"""
    你是一个顶级量化投资审计专家。请结合文字与图表深度审计文档：{filename}。
    
    ### 输出格式要求 (必须严格遵守)：
    1. 全文仅使用一个 ## 二级标题作为总标题。
    2. 内部知识条目必须使用 ### 三级标题作为分段标识符。
    3. 每个 ### 小节的第一行，必须紧跟一行 "- **来源**: {filename}"。
    4. 必须包含一个 Markdown 格式的量化对标表格。

    ## 【核心专题】{filename.replace('.pdf', '')}：量化逻辑与估值审计报告
    - **文件名**: {filename}
    - **核心定义**: [一句话概括]

    ### 1. 核心定量指标与决策红线
    - **来源**: {filename}
    - **成交额总闸门**: [提取关键数字]
    - **仓位/买卖红线**: [提取百分比]
    - **估值参考点**: [提取日期或点位]

    ### 2. 重点标的：量化对标数据表
    - **来源**: {filename}
    | 标的名称 | 当前数据/估值 | 历史极值对比 | 审计判定 | 风险/机会评估 |
    | :--- | :--- | :--- | :--- | :--- |
    | [提取 3-5 个核心标的] | | | | |

    ### 3. 风险预警与图表洞察
    - **来源**: {filename}
    - **背离信号**: [描述背离]
    - **图表数据点**: [描述走势]
    - **审计结论**: [最终建议]
    """

def process_pdf_to_markdown(file_path):
    filename = os.path.basename(file_path)
    print(f"🔄 正在处理: {filename} ...")
    
    try:
        doc = fitz.open(file_path)
        all_text = []
        images_data = []
        
        # 【关键修改 1】只处理第 1 页，避免多图导致 Token 超限或参数错误
        # 如果第一页没有图，可以尝试改为 range(min(len(doc), 2)) 看前两页
        max_pages = 1 
        print(f"   -> 仅提取前 {max_pages} 页进行视觉分析以节省 Token 并避免报错")
        
        for page_num in range(max_pages):
            page = doc[page_num]
            text_content = page.get_text()
            if text_content.strip():
                all_text.append(f"--- 第 {page_num+1} 页 ---\n{text_content}")
            
            # 渲染图片
            # 【关键修改 2】稍微降低分辨率 (1.2 倍)，防止图片 Base64 过长导致请求体过大
            pix = page.get_pixmap(matrix=fitz.Matrix(1.2, 1.2))
            img_bytes = pix.tobytes('png')
            
            # 检查图片大小，如果超过 4MB (API 限制)，则缩小
            if len(img_bytes) > 4 * 1024 * 1024:
                print(f"   -> 警告: 第 {page_num+1} 页图片过大，正在压缩...")
                pix = page.get_pixmap(matrix=fitz.Matrix(0.8, 0.8))
                img_bytes = pix.tobytes('png')
                
            img_base64 = image_to_base64(img_bytes)
            
            images_data.append({
                "type": "image_url",
                "image_url": {
                    "url": f"data:image/png;base64,{img_base64}"
                }
            })
        doc.close()

        if not all_text and not images_data:
            print(f"⚠️ {filename} 内容为空，跳过。")
            return

        prompt_text = get_audit_prompt(filename)
        
        # 构造消息体
        messages_content = [
            {
                "type": "text",
                "text": f"{prompt_text}\n\n参考文字内容：\n{' '.join(all_text)[:3000]}" # 限制文字长度
            }
        ]
        messages_content.extend(images_data)

        # 【关键修改 3】调整参数，确保兼容性
        # max_tokens 设为 4096 (大多数模型的安全上限)
        # 移除 temperature 或使用默认值
        response = client.chat.completions.create(
            model="glm-4.6v-flash",  # 强制使用最稳定的视觉模型
            messages=[{"role": "user", "content": messages_content}],
            max_tokens=4096,      # 降低 Token 上限，避免 1210 错误
            # temperature=0.7,    # 暂时移除，使用默认值
        )

        result_content = response.choices[0].message.content
        
        md_path = file_path.replace(".pdf", ".md")
        with open(md_path, "w", encoding="utf-8") as f:
            f.write(result_content)
        
        print(f"✅ 成功生成: {md_path}")

    except Exception as e:
        print(f"❌ 处理 {filename} 失败: {e}")
        # 尝试打印更底层的错误信息
        if hasattr(e, 'response') and e.response is not None:
            try:
                err_detail = e.response.json()
                print(f"   -> 服务器详细错误: {err_detail}")
            except:
                print(f"   -> 服务器原始响应: {e.response.text}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    articles_dir = "./articles"
    if os.path.exists(articles_dir):
        pdf_files = [f for f in os.listdir(articles_dir) if f.endswith(".pdf")]
        if not pdf_files:
            print("未找到 PDF 文件")
        else:
            for f in pdf_files:
                process_pdf_to_markdown(os.path.join(articles_dir, f))
    else:
        print(f"目录 {articles_dir} 不存在")