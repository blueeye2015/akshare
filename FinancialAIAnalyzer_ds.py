import pandas as pd
import json
from typing import Dict, List
import requests  # 或其他 HTTP 客户端库
from sqlalchemy import create_engine
from openai import OpenAI, APIConnectionError

class FinancialAIAnalyzer:
    def __init__(self, db_params, api_key):
        self.db_params = db_params
        self.api_key = api_key
        self.engine = create_engine(f"postgresql://{db_params['user']}:{db_params['password']}@{db_params['host']}:{db_params['port']}/{db_params['database']}")

    def get_financial_data(self, symbol: str, periods: int = 10) -> pd.DataFrame:
        """从数据库获取财务数据"""
        query = f"""
        WITH latest_data AS (
            SELECT *,
                   ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY report_date DESC) cnt
            FROM public.financial_indicators 
            WHERE  RIGHT(TO_CHAR(report_date, 'YYYY-MM-DD') ,5)='12-31' AND symbol = '{symbol}'
        )
        SELECT 
            symbol,
            report_date,
            roe,
            weighted_roe,
            net_profit_margin,
            gross_profit_margin,
            total_asset_turnover,
            accounts_receivable_days,
            inventory_days,
            asset_liability_ratio,
            current_ratio,
            quick_ratio,
            ocf_to_revenue,
            cash_flow_ratio,
            revenue_growth,
            net_profit_growth,
            total_asset_growth
        FROM latest_data
        WHERE cnt <= {periods}
        ORDER BY report_date DESC
        """
        df = pd.read_sql(query, self.engine)
        # 确保 report_date 是 datetime 格式
        df['report_date'] = pd.to_datetime(df['report_date'])
        return df

    def prepare_data_for_ai(self, df: pd.DataFrame) -> Dict:
        """将财务数据转换为适合AI分析的格式"""
        latest = df.iloc[0]  # 最新一期数据
        
        # 处理可能的 NaN 值
        def safe_round(value, decimals=2):
            try:
                if pd.isna(value):
                    return None
                return round(float(value), decimals)
            except:
                return None
        # 计算同比变化
        yoy_changes = {
            'revenue_growth': latest['revenue_growth'],
            'net_profit_growth': latest['net_profit_growth'],
            'total_asset_growth': latest['total_asset_growth']
        }
        
        # 构建财务指标数据
        financial_metrics = {
            "基本信息": {
                "股票代码": latest['symbol'],
                "报告期": latest['report_date'].strftime('%Y-%m-%d'),
            },
            "盈利能力指标": {
                "ROE(%)": latest['roe'],
                "净利率(%)": latest['net_profit_margin'],
                "毛利率(%)": latest.get('gross_profit_margin'),
                "加权ROE(%)": latest['weighted_roe']
            },
            "运营能力指标": {
                "总资产周转率": latest['total_asset_turnover'],
                "应收账款周转天数": latest['accounts_receivable_days'],
                "存货周转天数": latest['inventory_days']
            },
            "偿债能力指标": {
                "资产负债率(%)": latest['asset_liability_ratio'],
                "流动比率": latest['current_ratio'],
                "速动比率": latest['quick_ratio']
            },
            "成长能力指标": yoy_changes,
            "现金流指标": {
                "经营活动现金流量": latest['ocf_to_revenue'],
                "现金流量比率": latest['cash_flow_ratio']
            }
        }
        
        # 添加历史趋势数据（确保数据可序列化）
        trends = {
            'roe_trend': [safe_round(x) for x in df['roe'].tolist()],
            'profit_margin_trend': [safe_round(x) for x in df['net_profit_margin'].tolist()],
            'asset_liability_trend': [safe_round(x) for x in df['asset_liability_ratio'].tolist()],
            'report_dates': df['report_date'].dt.strftime('%Y-%m-%d').tolist()
        }
        
        return {
            'metrics': financial_metrics,
            'trends': trends
        }

    def generate_ai_prompt(self, data: Dict) -> str:
        """生成用于AI分析的prompt"""
        return f"""作为一个专业的财务分析师，请基于以下财务数据对该公司进行深入分析：

                1. 财务指标数据：
                {json.dumps(data['metrics'], ensure_ascii=False, indent=2)}

                2. 关键指标历史趋势：
                {json.dumps(data['trends'], ensure_ascii=False, indent=2)}

                请从以下几个方面进行分析：
                1. 公司整体经营状况评估
                - 分析盈利能力和经营效率
                - 评估业务增长情况

                2. 主要财务风险分析
                - 偿债风险评估
                - 运营风险评估
                - 现金流风险评估

                3. 现金流状况评估
                - 经营现金流质量
                - 现金流结构分析

                4. 发展趋势判断
                - 基于历史数据的趋势分析
                - 关键指标变动原因分析

                5. 需要重点关注的问题
                - 列出潜在风险点
                - 提供改善建议

                请给出详细的分析结论和建议。回答要求：
                1. 分析要客观、专业，基于数据说话
                2. 对异常指标要重点分析原因
                3. 结合历史趋势给出合理的判断
                4. 提供具体的改善建议"""

    def call_ai_api(self, prompt: str) -> str:
        """调用AI API进行分析"""
        
        
        client = OpenAI(
        base_url="https://api.deepseek.com/v1",  # 或 "https://api.deepseek.com"
        api_key=self.api_key
        )
        
        response = client.chat.completions.create(
                model="deepseek-reasoner",
                messages=[
                    {"role": "user", "content": prompt}
                ]
            )
        
        
        return response.choices[0].message.content
       

    def analyze_company(self, symbol: str) -> str:
        """完整的分析流程"""
        try:
            # 1. 获取数据
            df = self.get_financial_data(symbol)
            if df.empty:
                return "未找到该公司数据"

            # 2. 准备数据
            data = self.prepare_data_for_ai(df)

            # 3. 生成prompt
            prompt = self.generate_ai_prompt(data)

            # 4. 调用AI获取分析结果
            analysis_result = self.call_ai_api(prompt)

            return analysis_result

        except Exception as e:
            return f"分析过程中出现错误: {str(e)}"

def main():
    # 配置参数
    db_params = {
        'host': 'localhost',
        'port': 5432,
        'user': 'postgres',
        'password': '12',
        'database': 'Financialdata'
    }
    
    api_key = "sk-db3726af7480474e964c7d02369f7dc2"
    
    # 创建分析器实例
    analyzer = FinancialAIAnalyzer(db_params, api_key)
    
    # 分析特定公司
    symbol = '300068'
    result = analyzer.analyze_company(symbol)
    
    # 打印分析结果
    print(result)

if __name__ == "__main__":
    main()
