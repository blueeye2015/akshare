from openai import OpenAI

client = OpenAI(
    base_url='https://ms-fc-92745792-c7ad.api-inference.modelscope.cn/v1',
    api_key='cdf50ebf-918e-4bf0-9e0c-cba542021a29', # ModelScope Token
)

response = client.chat.completions.create(
    model='Qwen/QwQ-32B-GGUF', # ModelScope Model-Id
    messages=[
        {
            'role': 'system',
            'content': 'You are a helpful assistant.'
        },
        {
            'role': 'user',
            'content': '你好'
        }
    ],
    stream=True
)

for chunk in response:
    print(chunk.choices[0].delta.content, end='', flush=True)