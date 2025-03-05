from pdfminer.high_level import extract_text
from langchain.text_splitter import RecursiveCharacterTextSplitter
from openai import OpenAI

# Step 1: 提取 PDF 文本
def extract_pdf_text(pdf_path):
    text = extract_text(pdf_path)
    return text

# Step 2: 切分文本为多个块
# Step 2: 切分文本为多个块
def split_text_into_chunks(text, chunk_size=5000, chunk_overlap=500):
    text_splitter = RecursiveCharacterTextSplitter(
        chunk_size=chunk_size,
        chunk_overlap=chunk_overlap
    )
    chunks = text_splitter.create_documents([text])
    return chunks

# Step 3: 调用 DeepSeek API 生成摘要
def generate_summary_with_deepseek(text, api_key):
    # 配置 DeepSeek API
    client = OpenAI(
        base_url="https://api.deepseek.com/v1",  # 或 "https://api.deepseek.com"
        api_key=api_key
    )
    # 调用 DeepSeek-V3 模型
    response = client.chat.completions.create(
        model="deepseek-chat",  # 指定模型为 DeepSeek-V3
        messages=[
            {"role": "user", "content": f"请为以下文本生成摘要：{text}"}
        ]
    )
    return response.choices[0].message.content

# Step 4: 主函数
def main(pdf_path, api_key):
    # 提取文本
    text = extract_pdf_text(pdf_path)
    print("提取文本成功！")

    # 切分文本
    chunks = split_text_into_chunks(text)
    print(f"文本已切分为 {len(chunks)} 个块。")

    # 为每个块生成摘要
    summaries = []
    for chunk in chunks:
        summary = generate_summary_with_deepseek(chunk.page_content, api_key)
        summaries.append(summary)
        print(f"已处理块 {len(summaries)}：{summary[:100]}...")  # 打印部分摘要

    # 合并所有摘要
    final_summary = "\n".join(summaries)
    print("摘要生成完成：")
    print(final_summary)

# 运行
if __name__ == "__main__":
    pdf_path = "南都电源：2023年年度报告.pdf"  # 替换为你的 PDF 文件路径
    api_key = "sk-ccb36a53dcd843979a7fb85715b66d8b"  # 替换为你的 DeepSeek API 密钥
    main(pdf_path, api_key)
