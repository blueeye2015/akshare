import os
import base64
import fitz  # PyMuPDF
from openai import OpenAI
import httpx # 用于配置代理
from dotenv import load_dotenv

load_dotenv('.env')

# --- 1. 代理配置 ---
# 请修改为你的实际代理地址和端口 (如 Clash 的 7890)
PROXY_URL = "http://127.0.0.1:7890"

# 创建支持代理的 http 客户端
# 将 proxies 改为 proxy
proxy_client = httpx.Client(proxy=PROXY_URL)

# --- 2. 初始化 OpenAI 客户端 (Gemini 兼容模式) ---
client = OpenAI(
    api_key=os.getenv("GOOGLE_API_KEY"),
    base_url="https://generativelanguage.googleapis.com/v1beta/openai/",
    http_client=proxy_client  # 👈 核心：让所有请求走代理
)

def image_to_base64(image_bytes):
    return base64.b64encode(image_bytes).decode('utf-8')

# --- 统一的 Prompt 模板 ---
def get_audit_prompt(filename):
    return f"""
    你是一个顶级量化投资审计专家。请结合文字与图表深度审计文档：{filename}。

    ### 输出格式要求 (必须严格遵守)：
    1. 全文仅使用一个 ## 二级标题作为总标题。
    2. 内部知识条目必须使用 ### 三级标题作为分段标识符（用于 Dify 自动切分）。
    3. 每个 ### 小节的第一行，必须紧跟一行 "- **来源**: {filename}"，确保上下文不丢失。
    4. 必须包含一个 Markdown 格式的量化对标表格。

    ### 必须输出的结构如下：

    ## 【核心专题】{filename.replace('.pdf', '')}：量化逻辑与估值审计报告
    - **文件名**: {filename}
    - **核心定义**: [此处填入博主对本文逻辑的定性判断，一句话概括]

    ### 1. 核心定量指标与决策红线
    - **来源**: {filename}
    - **成交额总闸门**: [必须提取如 1.5 万亿等关键数字]
    - **仓位/买卖红线**: [提取文中提到的百分比或环比增长/下滑标准]
    - **估值参考点**: [提取博主对标的历史日期或点位]

    ### 2. 重点标的：量化对标数据表
    - **来源**: {filename}
    | 标的名称 | 当前数据/估值 | 历史极值对比 | 审计判定 | 风险/机会评估 |
    | :--- | :--- | :--- | :--- | :--- |
    | [从文中提取 3-5 个核心标的填入此表] | | | | |

    ### 3. 风险预警与图表洞察
    - **来源**: {filename}
    - **背离信号**: [描述文中提到的价格与指标背离情况]
    - **图表数据点**: [从 K 线或成交量图中提取具体走势描述]
    - **审计结论**: [基于博主逻辑给出的最终操作建议]
    """

# --- 在生成内容时调用 ---
# prompt = get_audit_prompt(filename)
# completion = client.chat.completions.create(model="gemini-1.5-flash", messages=[{"role": "user", "content": [{"type": "text", "text": prompt}, ...]})

def process_pdf_to_markdown(file_path):
    filename = os.path.basename(file_path)
    try:
        doc = fitz.open(file_path)
        all_text = []
        images_data = []
        
        # 处理前 5 页以确保关键图表和数据不遗漏 
        max_pages = min(len(doc), 5) 
        for page_num in range(max_pages):
            page = doc[page_num]
            all_text.append(f"--- 第 {page_num+1} 页 ---\n{page.get_text()}")
            
            # 渲染页面为图片以进行多模态量化审计
            pix = page.get_pixmap(matrix=fitz.Matrix(1.5, 1.5))
            images_data.append({
                "type": "image_url",
                "image_url": {"url": f"data:image/png;base64,{image_to_base64(pix.tobytes('png'))}"}
            })
        doc.close()

        # --- 3. 构造审计 Prompt ---
        # 强制要求输出 ## 和 ### 格式，锁定 1.5 万亿等灵魂数字 [cite: 135-136, 143]
        # 1. 获取格式化的 Prompt 字符串
        prompt_text = get_audit_prompt(filename)

        # 2. 构造符合 OpenAI/Gemini 规范的消息列表
        # 这里的 messages_content 必须是 list 类型，才能使用 .extend()
        messages_content = [
            {
                "type": "text",
                "text": f"{prompt_text}\n\n以下是文档提取的参考文字：\n{' '.join(all_text)[:4000]}"
            }
        ]

        # 3. 将图片列表（images_data 也是 list）合并进来
        # 这样 Gemini 就能同时看到“指令+文字”和“图片”
        messages_content.extend(images_data)

        # 调用 Gemini 1.5 Flash (免费且支持多模态)
        completion = client.chat.completions.create(
            model="gemini-3-flash-preview", 
            messages=[{"role": "user", "content": messages_content}]
        )

        # 保存为 .md
        md_path = file_path.replace(".pdf", ".md")
        with open(md_path, "w", encoding="utf-8") as f:
            f.write(completion.choices[0].message.content)
        
        print(f"✅ 成功生成 Markdown: {filename}")

    except Exception as e:
        print(f"❌ 处理 {filename} 失败: {e}")

if __name__ == "__main__":
    articles_dir = "./articles"
    for f in os.listdir(articles_dir):
        if f.endswith(".pdf"):
            process_pdf_to_markdown(os.path.join(articles_dir, f))