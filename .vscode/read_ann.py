from pdfminer.high_level import extract_text
from langchain.text_splitter import RecursiveCharacterTextSplitter
from openai import OpenAI, APIConnectionError
import time

# Step 1: 提取 PDF 文本
def extract_pdf_text(pdf_path):
    text = extract_text(pdf_path)
    return text


# Step 2: 切分文本为多个块
def split_text_into_chunks(text, chunk_size=5000, chunk_overlap=500):
    text_splitter = RecursiveCharacterTextSplitter(
        chunk_size=chunk_size,
        chunk_overlap=chunk_overlap
    )
    chunks = text_splitter.create_documents([text])
    return chunks

# Step 3: 调用 DeepSeek API 生成摘要
def generate_summary_with_deepseek(text, api_key, retries=3, delay=5):
    # 配置 DeepSeek API
    client = OpenAI(
        base_url="https://api.deepseek.com/v1",  # 或 "https://api.deepseek.com"
        api_key=api_key
    )
    # 调用 DeepSeek-V3 模型
    for attempt in range(retries):
        try:
            response = client.chat.completions.create(
                model="deepseek-chat",
                messages=[
                    {"role": "user", "content": f"请为以下文本生成摘要：{text}"}
                ]
            )
            return response.choices[0].message.content
        except APIConnectionError as e:
            print(f"连接失败，第 {attempt + 1} 次重试...")
            time.sleep(delay)
    raise Exception("重试次数用尽，请检查网络或联系技术支持。")

# Step 4: 主函数
def main(pdf_path, api_key,  output_file):
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
    # print("摘要生成完成：")
    # print(final_summary)
    
    # 将摘要保存到文件
    with open(output_file, "w", encoding="utf-8") as f:
        f.write(final_summary)
    print(f"摘要已保存到文件：{output_file}")
    

# 运行
if __name__ == "__main__":
    pdf_path = "南都电源：2023年年度报告.pdf"  # 替换为你的 PDF 文件路径
    api_key = ""  # 替换为你的 DeepSeek API 密钥
    output_file = "summary.txt"  # 替换为你想保存的文件名
    main(pdf_path, api_key, output_file)
