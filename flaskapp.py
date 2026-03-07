import requests
from flask import Flask, request
from dotenv import load_dotenv
import os

load_dotenv('.env')
app = Flask(__name__)

# 配置你的 Telegram 信息
TG_TOKEN = os.getenv("TG_TOKEN")
TG_CHAT_ID = "7198531433"

PROXIES = {
    "http": "http://127.0.0.1:7890",
    "https": "http://127.0.0.1:7890",
}

def send_tg_message(text):
    url = f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"
    payload = {
        "chat_id": TG_CHAT_ID,
        "text": text,
        "parse_mode": "Markdown"
    }
    try:
        # 加上 proxies 参数确保能翻过网络限制
        response = requests.post(url, json=payload, proxies=PROXIES, timeout=10)
        if response.status_code == 200:
            print(f"✅ Telegram 消息推送成功")
        else:
            print(f"❌ 发送失败，状态码: {response.status_code}, 详情: {response.text}")
    except Exception as e:
        print(f"❌ 网络异常，请检查代理是否运行: {e}")

@app.route('/', methods=['POST'])
def alert_receiver():
    data = request.json
    # 解析来自 Alertmanager 的 Webhook 数据
    for alert in data.get('alerts', []):
        status = alert.get('status')
        labels = alert.get('labels', {})
        annotations = alert.get('annotations', {})
        
        alert_name = labels.get('alertname', '未知告警')
        summary = annotations.get('summary', '量化预警触发')
        # 修正：Prometheus 发送的数值通常在描述信息或 labels 中，这里根据之前的 yml 习惯匹配
        full_description = annotations.get('description', '无详细描述')
    
        # 方案 B：如果非要提取数值，使用正则匹配（更稳健）
        import re
        # 匹配字符串中的数字和小数点
        value_match = re.search(r'(\d+\.\d+)', full_description)
        value = value_match.group(1) if value_match else "未知"

        if status == "firing":
            # --- 根据告警名称匹配图标，实现视觉分级 ---
            if "ExtremeOverheat" in alert_name:
                icon = "🔥【极度过热·减仓预警】"
                color_tag = "🔴"
            elif "3_4" in alert_name:
                icon = "⚠️【3/4回落·分歧点】"
                color_tag = "🟡"
            elif "2_3" in alert_name:
                icon = "📉【2/3回落·关键点】"
                color_tag = "🟠"
            elif "1.5T" in alert_name:
                icon = "🚨【1.5w亿·底部红线】"
                color_tag = "✅"
            else:
                icon = "🔔【量化审计预警】"
                color_tag = "⚪️"

            # 构造 Markdown 消息格式
            message = (
                f"{icon} \n\n"
                f"{color_tag} *告警项*: `{alert_name}` \n"
                f"📊 *当前成交额*: `{value}` 万亿 \n"
                f"📝 *逻辑提示*: {summary} \n"
                f"⏳ *状态*: 🔥 FIRING (正在触发) \n\n"
                f"🤖 *Dify 联动指令*: \n"
                f"请在 Dify 询问：`成交额在 {value} 万亿时，历史复盘对资源股的换手率审计结论是什么？`"
            )
            
            send_tg_message(message)
            print(f"✅ Telegram 分级消息已发出: {alert_name}")

        elif status == "resolved":
            # 增加恢复提醒，让你知道指标已经离开了告警区
            res_message = f"🟢 *【预警解除】*: `{alert_name}` 已恢复正常。"
            send_tg_message(res_message)

    return "OK", 200

if __name__ == '__main__':
    # 确保监听 0.0.0.0 端口
    app.run(host='0.0.0.0', port=5001)