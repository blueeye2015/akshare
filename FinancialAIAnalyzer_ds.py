import pandas as pd
import json
import time
from typing import Dict, List
import requests  # 或其他 HTTP 客户端库
from sqlalchemy import create_engine
from openai import OpenAI, APIConnectionError

class FinancialAIAnalyzer:
    def __init__(self, db_params, api_key):
        self.db_params = db_params
        self.api_key = api_key
        self.engine = create_engine(f"postgresql://{db_params['user']}:{db_params['password']}@{db_params['host']}:{db_params['port']}/{db_params['database']}")

    def get_financial_data(self, symbol: str, periods: int = 40) -> Dict[str, pd.DataFrame]:
        """从数据库获取三张财务报表数据，并计算单季数据
        
        Args:
            symbol: 股票代码
            periods: 获取的期数（默认40期，约10年的季报数据）
        """
        
        # 资产负债表查询 - 获取所有季报数据
        balance_query = f"""
        SELECT *
        FROM financial_statement
        WHERE security_code = '{symbol}'
        ORDER BY report_date DESC
        LIMIT {periods}
        """
        
        # 现金流量表查询
        cashflow_query = f"""
        SELECT *
        FROM cash_flow_sheet
        WHERE security_code = '{symbol}'
        ORDER BY report_date DESC
        LIMIT {periods}
        """
        
        # 利润表查询
        profit_query = f"""
        SELECT *
        FROM profit_sheet
        WHERE security_code = '{symbol}'
        ORDER BY report_date DESC
        LIMIT {periods}
        """
        
        # 获取数据
        balance_df = pd.read_sql(balance_query, self.engine)
        cashflow_df = pd.read_sql(cashflow_query, self.engine)
        profit_df = pd.read_sql(profit_query, self.engine)
        
        # 统一日期格式
        for df in [balance_df, cashflow_df, profit_df]:
            df['report_date'] = pd.to_datetime(df['report_date'])
            # 添加季度列
            df['quarter'] = df['report_date'].dt.quarter
            df['year'] = df['report_date'].dt.year
        
        # 计算单季数据
        def calculate_quarterly_data(df: pd.DataFrame, cumulative_columns: List[str]) -> pd.DataFrame:
            """计算单季数据
            
            Args:
                df: 原始DataFrame
                cumulative_columns: 需要计算单季的累计列名列表
            """
            # 按年份和季度排序
            df = df.sort_values(['year', 'quarter'])
            
            # 创建单季数据DataFrame
            quarterly_df = df.copy()
            
            for col in cumulative_columns:
                if col not in df.columns:
                    continue
                    
                # 计算单季数据
                quarterly_df[f'{col}_quarterly'] = df[col]
                
                # Q1保持不变
                mask_q1 = quarterly_df['quarter'] == 1
                # Q2-Q4需要减去上一季度的累计值
                mask_not_q1 = quarterly_df['quarter'] != 1
                
                quarterly_df.loc[mask_not_q1, f'{col}_quarterly'] = (
                    quarterly_df.loc[mask_not_q1, col].values - 
                    quarterly_df.groupby('year')[col].shift(1).loc[mask_not_q1].values
                )
            
            quarterly_df = quarterly_df.sort_values('report_date', ascending=False)    
            return quarterly_df
        
        # 利润表需要计算单季的列
        profit_cumulative_columns = [
            'total_operate_income', 'operate_income', 'total_operate_cost',
            'operate_cost', 'sale_expense', 'manage_expense', 'finance_expense',
            'operate_profit', 'total_profit', 'income_tax', 'netprofit',
            'parent_netprofit', 'deduct_parent_netprofit'
        ]
        
        # 现金流量表需要计算单季的列
        cashflow_cumulative_columns = [
            'sales_services', 'tax_refund', 'other_operate_received',
            'total_operate_received', 'goods_services_received', 'employee_received',
            'tax_payments', 'other_operate_payments', 'total_operate_payments',
            'operate_net_cash_flow', 'invest_withdrawal', 'invest_income',
            'fix_asset_disposal', 'total_invest_received', 'fix_asset_acquisition',
            'invest_payments', 'total_invest_payments', 'invest_net_cash_flow',
            'accept_invest_received', 'loan_received', 'total_finance_received',
            'loan_repayment', 'dividend_interest_payments', 'total_finance_payments',
            'finance_net_cash_flow', 'cash_equivalent_increase'
        ]

        balance_cumulative_columns  = [
            # 重要资产项目
            'monetaryfunds',          # 货币资金：反映企业流动性
            'accounts_rece',          # 应收账款：反映企业营运能力
            'inventory',              # 存货：反映企业营运效率
            'current_asset_balance',  # 流动资产合计
            'fixed_asset',           # 固定资产：反映企业实力
            'goodwill',              # 商誉：反映企业并购风险
            'intangible_asset',      # 无形资产：反映企业科技创新能力
            'noncurrent_asset_balance', # 非流动资产合计
            
            # 重要负债项目
            'short_loan',            # 短期借款：反映短期偿债压力
            'accounts_payable',      # 应付账款：反映企业商业信用
            'current_liab_balance',  # 流动负债合计
            'long_loan',             # 长期借款：反映长期债务状况
            'noncurrent_liab_balance', # 非流动负债合计
            'liab_balance',          # 负债合计
            
            # 核心所有者权益项目
            'share_capital',         # 股本
            'unassign_rpofit',      # 未分配利润：反映盈利积累
            'equity_balance',        # 所有者权益合计
            
            # 总计项目
            'total_assets',          # 资产总计
            'total_liabilities',     # 负债总计
            'total_equity'           # 所有者权益总计
        ]

        
        # 计算单季数据
        profit_quarterly = calculate_quarterly_data(profit_df, profit_cumulative_columns)
        cashflow_quarterly = calculate_quarterly_data(cashflow_df, cashflow_cumulative_columns)
        balance_quarterly = calculate_quarterly_data(balance_df, balance_cumulative_columns)
        
        # 添加季度标识
        for df in [balance_quarterly, cashflow_quarterly, profit_quarterly]:
            df['period'] = df.apply(lambda x: f"{x['year']}Q{x['quarter']}", axis=1)
        
        return {
            'balance': balance_quarterly,
            'cashflow': cashflow_quarterly,
            'profit': profit_quarterly,
            'balance_raw': balance_df,
            'cashflow_raw': cashflow_df,
            'profit_raw': profit_df
        }

    def prepare_data_for_ai(self, data_dict: Dict[str, pd.DataFrame]) -> Dict:
        """将三张财务报表数据转换为适合AI分析的格式"""
        balance_df = data_dict['balance']
        cashflow_df = data_dict['cashflow']
        profit_df = data_dict['profit']
        
        # print("资产负债表最新报告期:", balance_df['report_date'].iloc[0])
        # print("利润表最新报告期:", profit_df['report_date'].iloc[0])
        # print("现金流量表最新报告期:", cashflow_df['report_date'].iloc[0])
        
        latest = {
            'balance': balance_df.iloc[0],#获取最后一行数据
            'cashflow': cashflow_df.iloc[0],
            'profit': profit_df.iloc[0] 
        }
        
        # 基础财务指标
        financial_metrics = {
            "基本信息": {
                "股票代码": latest['balance']['symbol'],
                "报告期": latest['balance']['report_date'].strftime('%Y-%m-%d'),
                "公司名称": latest['balance']['security_name_abbr']
            },
            "资产结构": {
                "货币资金": latest['balance']['monetaryfunds'],          # 反映流动性
                "应收账款": latest['balance']['accounts_rece'],          # 反映营运能力
                "存货": latest['balance']['inventory'],                  # 反映营运效率
                "流动资产合计": latest['balance']['current_asset_balance']
            },
            "非流动资产": {
                "固定资产": latest['balance']['fixed_asset'],           # 反映实物资产规模
                "无形资产": latest['balance']['intangible_asset'],      # 反映科技创新能力
                "商誉": latest['balance']['goodwill'],                  # 反映并购风险
                "非流动资产合计": latest['balance']['noncurrent_asset_balance']
            },
            "负债结构": {
                "短期借款": latest['balance']['short_loan'],            # 短期偿债压力
                "应付账款": latest['balance']['accounts_payable'],      # 商业信用
                "流动负债合计": latest['balance']['current_liab_balance']
            },
            "非流动负债": {
                "长期借款": latest['balance']['long_loan'],             # 长期债务状况
                "非流动负债合计": latest['balance']['noncurrent_liab_balance'],
                "负债合计": latest['balance']['liab_balance']
            },
            "所有者权益": {
                "股本": latest['balance']['share_capital'],
                "未分配利润": latest['balance']['unassign_rpofit'],     # 反映盈利积累
                "所有者权益合计": latest['balance']['equity_balance']
            },
            "资产负债总计": {
                "资产总计": latest['balance']['total_assets'],
                "负债总计": latest['balance']['total_liabilities'],
                "所有者权益总计": latest['balance']['total_equity']
            },
            "季度经营情况": {
                "营业总收入(单季)": latest['profit']['total_operate_income_quarterly'],
                "营业成本(单季)": latest['profit']['operate_cost_quarterly'],
                "销售费用(单季)": latest['profit']['sale_expense_quarterly'],
                "管理费用(单季)": latest['profit']['manage_expense_quarterly'],
                "财务费用(单季)": latest['profit']['finance_expense_quarterly'],
                "净利润(单季)": latest['profit']['netprofit_quarterly']
            },
            "季度现金流情况": {
                "经营活动现金流入(单季)": latest['cashflow']['total_operate_received_quarterly'],
                "经营活动现金流出(单季)": latest['cashflow']['total_operate_payments_quarterly'],
                "经营活动净现金流(单季)": latest['cashflow']['operate_net_cash_flow_quarterly'],
                "投资活动净现金流(单季)": latest['cashflow']['invest_net_cash_flow_quarterly'],
                "筹资活动净现金流(单季)": latest['cashflow']['finance_net_cash_flow_quarterly']
            }
        }
        
        # 计算季度环比增长
        def calculate_qoq_growth(df: pd.DataFrame, column: str) -> List[float]:
            """计算环比增长率"""
            values = df[f'{column}_quarterly'].values
            qoq_growth = [(values[i] - values[i+1]) / abs(values[i+1]) * 100 if values[i+1] != 0 else 0 
                        for i in range(len(values)-1)]
            return qoq_growth
        
        # 添加趋势数据
        trends = {
            'periods': profit_df['period'].tolist(),
            'quarterly_revenue': {
                'values': profit_df['total_operate_income_quarterly'].tolist(),
                'qoq_growth': calculate_qoq_growth(profit_df, 'total_operate_income')
            },
            'quarterly_profit': {
                'values': profit_df['netprofit_quarterly'].tolist(),
                'qoq_growth': calculate_qoq_growth(profit_df, 'netprofit')
            },
            'quarterly_cashflow': {
                'values': cashflow_df['operate_net_cash_flow_quarterly'].tolist(),
                'qoq_growth': calculate_qoq_growth(cashflow_df, 'operate_net_cash_flow')
            },
            'asset_trend': balance_df['current_asset_balance'].tolist()
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
                - 存货风险评估

                3. 现金流状况评估
                - 经营现金流质量
                - 现金流结构分析

                4. 发展趋势判断
                - 基于历史数据的趋势分析
                - 关键指标变动原因分析

                5. 需要重点关注的问题
                - 列出潜在风险点                

                请给出详细的分析结论。回答要求：
                1. 分析要客观、专业，基于数据说话
                2. 对异常指标要重点分析原因
                3. 结合历史趋势给出合理的判断
                4. 以中短期投资标的给出购买建议"""
                

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
            data_dict  = self.get_financial_data(symbol)
            # 检查数据是否为空
            if any(df.empty for df in [data_dict['balance'], data_dict['cashflow'], data_dict['profit']]):
                return "未找到该公司完整的财务数据"

            # 2. 准备数据
            data = self.prepare_data_for_ai(data_dict)

            # 3. 生成prompt
            prompt = self.generate_ai_prompt(data)

            # 4. 调用AI获取分析结果
            analysis_result = self.call_ai_api(prompt)

            return analysis_result

        except Exception as e:
            return f"分析过程中出现错误: {str(e)}"

def main():
    db_params = {
        'host': '192.168.50.149',
        'port': 5432,
        'user': 'postgres',
        'password': '12',
        'database': 'Financialdata'
    }
    
    api_key = "sk-db3726af7480474e964c7d02369f7dc2"
    
    analyzer = FinancialAIAnalyzer(db_params, api_key)
    
    print("财务报表AI分析系统")
    print("-" * 50)
    
    symbol = input("请输入股票代码（例如：002245）：")
    
    print("\n正在获取财务数据...")
    try:
        result = analyzer.analyze_company(symbol)
        
        # 保存分析结果到文件
        with open(f'analysis_{symbol}_{time.strftime("%Y%m%d_%H%M%S")}.txt', 'w', encoding='utf-8') as f:
            f.write(result)
            
        print("\n分析完成！结果已保存到文件。")
        
    except Exception as e:
        print(f"\n错误：{str(e)}")

if __name__ == "__main__":
    main()
