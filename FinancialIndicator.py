from sqlalchemy import create_engine, Column, String, Float, DateTime, Date
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy.dialects.postgresql import insert
from datetime import datetime
import pandas as pd
import akshare as ak
import logging
import argparse
import sys
from typing import Dict, List

# 设置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('financial_indicators.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# 创建 Base
Base = declarative_base()

class FinancialIndicator(Base):
    __tablename__ = 'financial_indicators'
    
    # 复合主键
    symbol = Column(String(10), primary_key=True)
    report_date = Column(Date, primary_key=True)
    
    # 每股指标
    eps_basic = Column(Float)
    eps_diluted = Column(Float)
    eps_adjusted = Column(Float)
    eps_excl_nonrecurring = Column(Float)
    bps_pre_adjusted = Column(Float)
    bps_post_adjusted = Column(Float)
    ocf_per_share = Column(Float)
    capital_reserve_per_share = Column(Float)
    undistributed_profit_per_share = Column(Float)
    bps_adjusted = Column(Float)
    
    # 盈利能力指标
    roa_profit = Column(Float)
    main_business_profit_rate = Column(Float)
    roa_net_profit = Column(Float)
    cost_expense_profit_rate = Column(Float)
    operating_profit_rate = Column(Float)
    main_business_cost_rate = Column(Float)
    net_profit_margin = Column(Float)
    return_on_share_capital = Column(Float)
    return_on_equity = Column(Float)
    return_on_assets = Column(Float)
    gross_profit_margin = Column(Float)
    three_fee_ratio = Column(Float)
    non_main_business_ratio = Column(Float)
    main_profit_ratio = Column(Float)
    dividend_payout_ratio = Column(Float)
    return_on_investment = Column(Float)
    main_business_profit = Column(Float)
    roe = Column(Float)
    weighted_roe = Column(Float)
    net_profit_excl_nonrecurring = Column(Float)
    
    # 成长能力指标
    revenue_growth = Column(Float)
    net_profit_growth = Column(Float)
    net_asset_growth = Column(Float)
    total_asset_growth = Column(Float)
    
    # 营运能力指标
    accounts_receivable_turnover = Column(Float)
    accounts_receivable_days = Column(Float)
    inventory_days = Column(Float)
    inventory_turnover = Column(Float)
    fixed_asset_turnover = Column(Float)
    total_asset_turnover = Column(Float)
    total_asset_days = Column(Float)
    current_asset_turnover = Column(Float)
    current_asset_days = Column(Float)
    equity_turnover = Column(Float)
    
    # 偿债能力指标
    current_ratio = Column(Float)
    quick_ratio = Column(Float)
    cash_ratio = Column(Float)
    interest_coverage = Column(Float)
    long_term_debt_to_working_capital = Column(Float)
    equity_ratio = Column(Float)
    long_term_debt_ratio = Column(Float)
    equity_to_fixed_assets = Column(Float)
    debt_to_equity = Column(Float)
    long_term_assets_to_long_term_funding = Column(Float)
    capitalization_ratio = Column(Float)
    fixed_asset_net_value_ratio = Column(Float)
    capital_fixation_ratio = Column(Float)
    property_ratio = Column(Float)
    liquidation_value_ratio = Column(Float)
    fixed_asset_proportion = Column(Float)
    asset_liability_ratio = Column(Float)
    total_assets = Column(Float)
    
    # 现金流量指标
    ocf_to_revenue = Column(Float)
    ocf_to_assets = Column(Float)
    ocf_to_net_profit = Column(Float)
    ocf_to_liability = Column(Float)
    cash_flow_ratio = Column(Float)
    
    # 投资项目
    short_term_equity_investment = Column(Float)
    short_term_bond_investment = Column(Float)
    short_term_other_investment = Column(Float)
    long_term_equity_investment = Column(Float)
    long_term_bond_investment = Column(Float)
    long_term_other_investment = Column(Float)
    
    # 应收账款
    accounts_receivable_within_1y = Column(Float)
    accounts_receivable_1_2y = Column(Float)
    accounts_receivable_2_3y = Column(Float)
    accounts_receivable_within_3y = Column(Float)
    
    # 预付账款
    prepayments_within_1y = Column(Float)
    prepayments_1_2y = Column(Float)
    prepayments_2_3y = Column(Float)
    prepayments_within_3y = Column(Float)
    
    # 其他应收款
    other_receivables_within_1y = Column(Float)
    other_receivables_1_2y = Column(Float)
    other_receivables_2_3y = Column(Float)
    other_receivables_within_3y = Column(Float)
    
    # 时间戳
    create_time = Column(DateTime, default=datetime.now)
    update_time = Column(DateTime, default=datetime.now, onupdate=datetime.now)


class FinancialIndicatorCollector:
    def __init__(self, db_config: Dict):
        self.db_url = f"postgresql://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}"
        self.engine = create_engine(self.db_url)
        self.Session = sessionmaker(bind=self.engine)
        
    def get_stock_list(self) -> List[str]:
        """获取股票列表"""
        try:
            df = ak.stock_a_indicator_lg(symbol="all")
            return df['code'].tolist()
        except Exception as e:
            logger.error(f"获取股票列表失败: {str(e)}")
            return []
            
    def process_data(self, df: pd.DataFrame, symbol: str) -> pd.DataFrame:
        """处理原始数据"""
        if df.empty:
            return pd.DataFrame()
            
        # 重命名列
        column_mapping = {
            '日期': 'report_date',
            # 每股指标
            '摊薄每股收益(元)': 'eps_basic',
            '加权每股收益(元)': 'eps_diluted',
            '每股收益_调整后(元)': 'eps_adjusted',
            '扣除非经常性损益后的每股收益(元)': 'eps_excl_nonrecurring',
            '每股净资产_调整前(元)': 'bps_pre_adjusted',
            '每股净资产_调整后(元)': 'bps_post_adjusted',
            '每股经营性现金流(元)': 'ocf_per_share',
            '每股资本公积金(元)': 'capital_reserve_per_share',
            '每股未分配利润(元)': 'undistributed_profit_per_share',
            '调整后的每股净资产(元)': 'bps_adjusted',
            
            # 盈利能力
            '总资产利润率(%)': 'roa_profit',
            '主营业务利润率(%)': 'main_business_profit_rate',
            '总资产净利润率(%)': 'roa_net_profit',
            '成本费用利润率(%)': 'cost_expense_profit_rate',
            '营业利润率(%)': 'operating_profit_rate',
            '主营业务成本率(%)': 'main_business_cost_rate',
            '销售净利率(%)': 'net_profit_margin',
            '股本报酬率(%)': 'return_on_share_capital',
            '净资产报酬率(%)': 'return_on_equity',
            '资产报酬率(%)': 'return_on_assets',
            '销售毛利率(%)': 'gross_profit_margin',
            '三项费用比重': 'three_fee_ratio',
            '非主营比重': 'non_main_business_ratio',
            '主营利润比重': 'main_profit_ratio',
            '股息发放率(%)': 'dividend_payout_ratio',
            '投资收益率(%)': 'return_on_investment',
            '主营业务利润(元)': 'main_business_profit',
            '净资产收益率(%)': 'roe',
            '加权净资产收益率(%)': 'weighted_roe',
            '扣除非经常性损益后的净利润(元)': 'net_profit_excl_nonrecurring',
            
            # 成长能力
            '主营业务收入增长率(%)': 'revenue_growth',
            '净利润增长率(%)': 'net_profit_growth',
            '净资产增长率(%)': 'net_asset_growth',
            '总资产增长率(%)': 'total_asset_growth',
            
            # 营运能力
            '应收账款周转率(次)': 'accounts_receivable_turnover',
            '应收账款周转天数(天)': 'accounts_receivable_days',
            '存货周转天数(天)': 'inventory_days',
            '存货周转率(次)': 'inventory_turnover',
            '固定资产周转率(次)': 'fixed_asset_turnover',
            '总资产周转率(次)': 'total_asset_turnover',
            '总资产周转天数(天)': 'total_asset_days',
            '流动资产周转率(次)': 'current_asset_turnover',
            '流动资产周转天数(天)': 'current_asset_days',
            '股东权益周转率(次)': 'equity_turnover',
            
            # 偿债能力
            '流动比率': 'current_ratio',
            '速动比率': 'quick_ratio',
            '现金比率(%)': 'cash_ratio',
            '利息支付倍数': 'interest_coverage',
            '长期债务与营运资金比率(%)': 'long_term_debt_to_working_capital',
            '股东权益比率(%)': 'equity_ratio',
            '长期负债比率(%)': 'long_term_debt_ratio',
            '股东权益与固定资产比率(%)': 'equity_to_fixed_assets',
            '负债与所有者权益比率(%)': 'debt_to_equity',
            '长期资产与长期资金比率(%)': 'long_term_assets_to_long_term_funding',
            '资本化比率(%)': 'capitalization_ratio',
            '固定资产净值率(%)': 'fixed_asset_net_value_ratio',
            '资本固定化比率(%)': 'capital_fixation_ratio',
            '产权比率(%)': 'property_ratio',
            '清算价值比率(%)': 'liquidation_value_ratio',
            '固定资产比重(%)': 'fixed_asset_proportion',
            '资产负债率(%)': 'asset_liability_ratio',
            '总资产(元)': 'total_assets',
            
            # 现金流量
            '经营现金净流量对销售收入比率(%)': 'ocf_to_revenue',
            '资产的经营现金流量回报率(%)': 'ocf_to_assets',
            '经营现金净流量与净利润的比率(%)': 'ocf_to_net_profit',
            '经营现金净流量对负债比率(%)': 'ocf_to_liability',
            '现金流量比率(%)': 'cash_flow_ratio',
            
            # 投资项目
            '短期股票投资(元)': 'short_term_equity_investment',
            '短期债券投资(元)': 'short_term_bond_investment',
            '短期其它经营性投资(元)': 'short_term_other_investment',
            '长期股票投资(元)': 'long_term_equity_investment',
            '长期债券投资(元)': 'long_term_bond_investment',
            '长期其它经营性投资(元)': 'long_term_other_investment',
            
            # 应收账款
            '1年以内应收帐款(元)': 'accounts_receivable_within_1y',
            '1-2年以内应收帐款(元)': 'accounts_receivable_1_2y',
            '2-3年以内应收帐款(元)': 'accounts_receivable_2_3y',
            '3年以内应收帐款(元)': 'accounts_receivable_within_3y',
            
            # 预付账款
            '1年以内预付货款(元)': 'prepayments_within_1y',
            '1-2年以内预付货款(元)': 'prepayments_1_2y',
            '2-3年以内预付货款(元)': 'prepayments_2_3y',
            '3年以内预付货款(元)': 'prepayments_within_3y',
            
            # 其他应收款
            '1年以内其它应收款(元)': 'other_receivables_within_1y',
            '1-2年以内其它应收款(元)': 'other_receivables_1_2y',
            '2-3年以内其它应收款(元)': 'other_receivables_2_3y',
            '3年以内其它应收款(元)': 'other_receivables_within_3y'
        }
        
        df = df.rename(columns=column_mapping)
        df['symbol'] = symbol
        
        # 转换日期格式
        df['report_date'] = pd.to_datetime(df['report_date']).dt.date
        
        # 只选择已映射的列
        columns = ['symbol', 'report_date'] + [v for v in column_mapping.values() if v != 'report_date']
        return df[columns]

        
    def save_to_database(self, df: pd.DataFrame):
        """保存数据到数据库"""
        if df.empty:
            return
            
        session = self.Session()
        try:
            # 获取所有列名（除了主键和时间戳）
            update_columns = [col for col in df.columns 
                            if col not in ['symbol', 'report_date', 'create_time', 'update_time']]
            
            # 构建 upsert 语句
            stmt = insert(FinancialIndicator).values(df.to_dict('records'))
            
            # 构建更新字典
            update_dict = {col: stmt.excluded[col] for col in update_columns}  # 使用字典索引而不是 getattr
            update_dict['update_time'] = datetime.now()
            
            # 添加 on_conflict_do_update
            stmt = stmt.on_conflict_do_update(
                constraint=FinancialIndicator.__table__.primary_key,
                set_=update_dict
            )
            
            session.execute(stmt)
            session.commit()
            logger.info(f"成功保存 {len(df)} 条记录")
        except Exception as e:
            session.rollback()
            logger.error(f"数据保存失败: {str(e)}")
            raise
        finally:
            session.close()

            
    def collect_data(self, start_year: str):
        """收集财务指标数据"""
        stock_list = self.get_stock_list()
        total = len(stock_list)
        
        for i, symbol in enumerate(stock_list, 1):
            logger.info(f"正在处理股票 {symbol} ({i}/{total})")
            try:
                # 获取财务指标数据
                df = ak.stock_financial_analysis_indicator(symbol=symbol, start_year=start_year)
                df = self.process_data(df, symbol)
                self.save_to_database(df)
                logger.info(f"成功保存 {symbol} 的数据，共 {len(df)} 条记录")
            except Exception as e:
                logger.error(f"处理股票 {symbol} 失败: {str(e)}")
                continue

def main():
    parser = argparse.ArgumentParser(description='财务指标数据采集工具')
    parser.add_argument(
        '--start_year',
        type=str,
        default="2008",
        help='开始年份 (默认: 2008)'
    )
    
    args = parser.parse_args()
    
    # 数据库配置
    db_config = {
        'host': '192.168.50.149',
        'port': 5432,
        'user': 'postgres',
        'password': '12',
        'database': 'Financialdata'
    }
    
    collector = FinancialIndicatorCollector(db_config)
    
    try:
        logger.info(f"开始采集 {args.start_year} 年至今的财务指标数据...")
        collector.collect_data(args.start_year)
    except KeyboardInterrupt:
        logger.info("程序被用户中断")
    except Exception as e:
        logger.error(f"程序运行出错: {str(e)}")
        raise
    finally:
        logger.info("程序运行完成")

if __name__ == "__main__":
    main()
