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
    for alert in data.get('alerts', []):
        status = alert.get('status')
        labels = alert.get('labels', {})
        annotations = alert.get('annotations', {})
        
        alert_name = labels.get('alertname', '未知告警')
        summary = annotations.get('summary', '量化预警触发')
        full_description = annotations.get('description', '无详细描述')
        category = labels.get('category', 'unknown')
    
        import re
        # 匹配数字（支持整数和小数）
        value_match = re.search(r'(\d+\.?\d*)', full_description)
        value = value_match.group(1) if value_match else "未知"

        if status == "firing":
            # --- 成交额类告警 ---
            if "ExtremeOverheat" in alert_name:
                icon = "🔥【极度过热·减仓预警】"
                color_tag = "🔴"
                value_unit = "万亿"
                dify_prompt = f"成交额在 {value} 万亿时，历史复盘对资源股的换手率审计结论是什么？"
            elif "3_4" in alert_name:
                icon = "⚠️【3/4回落·分歧点】"
                color_tag = "🟡"
                value_unit = "万亿"
                dify_prompt = f"成交额在 {value} 万亿时，历史复盘对资源股的换手率审计结论是什么？"
            elif "2_3" in alert_name:
                icon = "📉【2/3回落·关键点】"
                color_tag = "🟠"
                value_unit = "万亿"
                dify_prompt = f"成交额在 {value} 万亿时，历史复盘对资源股的换手率审计结论是什么？"
            elif "1.5T" in alert_name:
                icon = "🚨【1.5w亿·底部红线】"
                color_tag = "✅"
                value_unit = "万亿"
                dify_prompt = f"成交额在 {value} 万亿时，历史复盘对资源股的换手率审计结论是什么？"
            # --- 市场结构类告警（新增）---
            elif "GrowthKc50Ratio" in alert_name or category == "market_structure":
                icon = "📊【成长风格占比预警】"
                color_tag = "🔵"
                value_unit = "%"
                dify_prompt = f"创业板占比达到 {value}% 时，骑行客的历史观点是什么？"
            # --- 估值类告警（新增）---
            elif "CybFin180Ratio" in alert_name or category == "valuation":
                if "Undervalued" in alert_name:
                    icon = "📊【估值低估·加仓区】"
                    color_tag = "🟢"
                    dify_prompt = f"创业板/180金融PE比值为 {value}，处于历史低估区间，骑行客对这类行情的观点是什么？"
                elif "High_12" in alert_name:
                    icon = "⚠️【估值偏高·谨慎区】"
                    color_tag = "🟡"
                    dify_prompt = f"创业板/180金融PE比值为 {value}，超过12倍，骑行客的历史观点是什么？"
                elif "VeryHigh_15" in alert_name:
                    icon = "🔥【估值严重高估·减仓区】"
                    color_tag = "🟠"
                    dify_prompt = f"创业板/180金融PE比值为 {value}，超过15倍，骑行客的历史观点是什么？"
                elif "Extreme_20" in alert_name:
                    icon = "🚨【估值极端高估·逃顶区】"
                    color_tag = "🔴"
                    dify_prompt = f"创业板/180金融PE比值为 {value}，超过20倍，骑行客的历史观点是什么？"
                else:
                    icon = "📊【估值预警】"
                    color_tag = "🔵"
                    dify_prompt = f"创业板/180金融PE比值为 {value}，相关历史规律是什么？"
                value_unit = "倍"
            else:
                icon = "🔔【量化审计预警】"
                color_tag = "⚪️"
                value_unit = ""
                dify_prompt = f"指标值为 {value}，相关历史规律是什么？"

            # 构造 Markdown 消息格式
            message = (
                f"{icon} \n\n"
                f"{color_tag} *告警项*: `{alert_name}` \n"
                f"📊 *当前值*: `{value}` {value_unit} \n"
                f"📝 *逻辑提示*: {summary} \n"
                f"⏳ *状态*: 🔥 FIRING (正在触发) \n\n"
                f"🤖 *Dify 联动指令*: \n请在 Dify 询问：`{dify_prompt}`"
            )
            
            send_tg_message(message)
            print(f"✅ Telegram 分级消息已发出: {alert_name}")

        elif status == "resolved":
            res_message = f"🟢 *【预警解除】*: `{alert_name}` 已恢复正常。"
            send_tg_message(res_message)

    return "OK", 200

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5001)
