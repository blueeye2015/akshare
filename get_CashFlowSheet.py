from sqlalchemy import create_engine, Column, String, Float, DateTime, Integer
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy.dialects.postgresql import insert
from datetime import datetime
import pandas as pd
import akshare as ak
import logging
import argparse
import sys
import time
import random
from typing import Dict, List
from retrying import retry

# 设置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('cash_flow_sheet.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# 创建 Base
Base = declarative_base()

class CashFlowSheet(Base):
    __tablename__ = 'cash_flow_sheet'
    
    # 复合主键
    symbol = Column(String(10), primary_key=True)  # 股票代码
    report_date = Column(String(10), primary_key=True)  # 报告期
    
    # 基本信息
    security_code = Column(String(10))  # 证券代码
    security_name_abbr = Column(String(50))  # 证券简称
    
    # 经营活动产生的现金流量
    sales_services = Column(Float)  # 销售商品、提供劳务收到的现金
    tax_refund = Column(Float)  # 收到的税费返还
    other_operate_received = Column(Float)  # 收到其他与经营活动有关的现金
    total_operate_received = Column(Float)  # 经营活动现金流入小计
    goods_services_received = Column(Float)  # 购买商品、接受劳务支付的现金
    employee_received = Column(Float)  # 支付给职工以及为职工支付的现金
    tax_payments = Column(Float)  # 支付的各项税费
    other_operate_payments = Column(Float)  # 支付其他与经营活动有关的现金
    total_operate_payments = Column(Float)  # 经营活动现金流出小计
    operate_net_cash_flow = Column(Float)  # 经营活动产生的现金流量净额

    # 投资活动产生的现金流量
    invest_withdrawal = Column(Float)  # 收回投资收到的现金
    invest_income = Column(Float)  # 取得投资收益收到的现金
    fix_asset_disposal = Column(Float)  # 处置固定资产、无形资产和其他长期资产收回的现金净额
    subsidiary_received = Column(Float)  # 处置子公司及其他营业单位收到的现金净额
    other_invest_received = Column(Float)  # 收到其他与投资活动有关的现金
    total_invest_received = Column(Float)  # 投资活动现金流入小计
    fix_asset_acquisition = Column(Float)  # 购建固定资产、无形资产和其他长期资产支付的现金
    invest_payments = Column(Float)  # 投资支付的现金
    subsidiary_payments = Column(Float)  # 取得子公司及其他营业单位支付的现金净额
    other_invest_payments = Column(Float)  # 支付其他与投资活动有关的现金
    total_invest_payments = Column(Float)  # 投资活动现金流出小计
    invest_net_cash_flow = Column(Float)  # 投资活动产生的现金流量净额

    # 筹资活动产生的现金流量
    accept_invest_received = Column(Float)  # 吸收投资收到的现金
    subsidiary_accept_invest = Column(Float)  # 子公司吸收少数股东投资收到的现金
    loan_received = Column(Float)  # 取得借款收到的现金
    bond_issue = Column(Float)  # 发行债券收到的现金
    other_finance_received = Column(Float)  # 收到其他与筹资活动有关的现金
    total_finance_received = Column(Float)  # 筹资活动现金流入小计
    loan_repayment = Column(Float)  # 偿还债务支付的现金
    dividend_interest_payments = Column(Float)  # 分配股利、利润或偿付利息支付的现金
    subsidiary_dividend_payments = Column(Float)  # 子公司支付给少数股东的股利、利润
    other_finance_payments = Column(Float)  # 支付其他与筹资活动有关的现金
    total_finance_payments = Column(Float)  # 筹资活动现金流出小计
    finance_net_cash_flow = Column(Float)  # 筹资活动产生的现金流量净额

    # 汇率变动对现金的影响
    exchange_rate_effects = Column(Float)  # 汇率变动对现金及现金等价物的影响

    # 现金及现金等价物净增加
    cash_equivalent_increase = Column(Float)  # 现金及现金等价物净增加额
    begin_cash_equivalent = Column(Float)  # 期初现金及现金等价物余额
    end_cash_equivalent = Column(Float)  # 期末现金及现金等价物余额

    # 时间戳
    create_time = Column(DateTime, default=datetime.now)
    update_time = Column(DateTime, default=datetime.now, onupdate=datetime.now)

class CashFlowSheetCollector:
    def __init__(self, db_config: Dict):
        self.db_url = f"postgresql://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}"
        self.engine = create_engine(self.db_url)
        self.Session = sessionmaker(bind=self.engine)
               
        # 重试配置
        self.max_retries = 3
        self.min_wait = 1  # 最小等待时间（秒）
        self.max_wait = 5  # 最大等待时间（秒）

    @retry(stop_max_attempt_number=3, wait_random_min=2000, wait_random_max=5000)
    def process_data(self, df: pd.DataFrame, symbol: str) -> pd.DataFrame:
        """处理原始数据"""
        if df.empty:
            return pd.DataFrame()
            
        # 打印原始列名，用于调试
        logger.debug(f"Original columns: {df.columns.tolist()}")
        
        # 重命名列（根据实际的 akshare 返回数据调整）
        column_mapping = {
            '股票代码': 'security_code',
            '股票简称': 'security_name_abbr',
            '报告期': 'report_date',
            '销售商品、提供劳务收到的现金': 'sales_services',
            '收到的税费返还': 'tax_refund',
            '收到其他与经营活动有关的现金': 'other_operate_received',
            '经营活动现金流入小计': 'total_operate_received',
            '购买商品、接受劳务支付的现金': 'goods_services_received',
            '支付给职工以及为职工支付的现金': 'employee_received',
            '支付的各项税费': 'tax_payments',
            '支付其他与经营活动有关的现金': 'other_operate_payments',
            '经营活动现金流出小计': 'total_operate_payments',
            '经营活动产生的现金流量净额': 'operate_net_cash_flow',
            '收回投资收到的现金': 'invest_withdrawal',
            '取得投资收益收到的现金': 'invest_income',
            '处置固定资产、无形资产和其他长期资产收回的现金净额': 'fix_asset_disposal',
            '处置子公司及其他营业单位收到的现金净额': 'subsidiary_received',
            '收到其他与投资活动有关的现金': 'other_invest_received',
            '投资活动现金流入小计': 'total_invest_received',
            '购建固定资产、无形资产和其他长期资产支付的现金': 'fix_asset_acquisition',
            '投资支付的现金': 'invest_payments',
            '取得子公司及其他营业单位支付的现金净额': 'subsidiary_payments',
            '支付其他与投资活动有关的现金': 'other_invest_payments',
            '投资活动现金流出小计': 'total_invest_payments',
            '投资活动产生的现金流量净额': 'invest_net_cash_flow',
            '吸收投资收到的现金': 'accept_invest_received',
            '子公司吸收少数股东投资收到的现金': 'subsidiary_accept_invest',
            '取得借款收到的现金': 'loan_received',
            '发行债券收到的现金': 'bond_issue',
            '收到其他与筹资活动有关的现金': 'other_finance_received',
            '筹资活动现金流入小计': 'total_finance_received',
            '偿还债务支付的现金': 'loan_repayment',
            '分配股利、利润或偿付利息支付的现金': 'dividend_interest_payments',
            '子公司支付给少数股东的股利、利润': 'subsidiary_dividend_payments',
            '支付其他与筹资活动有关的现金': 'other_finance_payments',
            '筹资活动现金流出小计': 'total_finance_payments',
            '筹资活动产生的现金流量净额': 'finance_net_cash_flow',
            '汇率变动对现金及现金等价物的影响': 'exchange_rate_effects',
            '现金及现金等价物净增加额': 'cash_equivalent_increase',
            '期初现金及现金等价物余额': 'begin_cash_equivalent',
            '期末现金及现金等价物余额': 'end_cash_equivalent'
        }
        
        # 创建新的 DataFrame，用 0 填充缺失值
        result_df = pd.DataFrame()
        result_df['symbol'] = [symbol]
        result_df['report_date'] = df['报告期'].iloc[0] if '报告期' in df.columns else None
        result_df['security_code'] = df['股票代码'].iloc[0] if '股票代码' in df.columns else None
        result_df['security_name_abbr'] = df['股票简称'].iloc[0] if '股票简称' in df.columns else None
        
        # 对于其他列，如果存在则使用实际值，不存在则填充 0
        for original_col, new_col in column_mapping.items():
            if original_col in df.columns:
                result_df[new_col] = df[original_col].iloc[0]
            else:
                result_df[new_col] = 0
                logger.debug(f"Column {original_col} not found in data, filling with 0")
        
        return result_df

    def save_to_database(self, df: pd.DataFrame):
        """保存数据到数据库"""
        if df.empty:
            return
            
        session = self.Session()
        try:
            update_columns = [c.name for c in CashFlowSheet.__table__.columns 
                            if c.name not in ['symbol', 'report_date', 'create_time']]
            
            stmt = insert(CashFlowSheet).values(df.to_dict('records'))
            update_dict = {col: stmt.excluded[col] for col in update_columns}
            update_dict['update_time'] = datetime.now()
            
            stmt = stmt.on_conflict_do_update(
                constraint=CashFlowSheet.__table__.primary_key,
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

    def collect_data(self, symbol: str):
        """采集单个股票的数据"""
        try:
            df = ak.stock_cash_flow_sheet_by_report_em(symbol=symbol)
            df = self.process_data(df, symbol)
            self.save_to_database(df)
            logger.info(f"成功处理股票 {symbol}")
        except Exception as e:
            logger.error(f"处理股票 {symbol} 失败: {str(e)}")
            raise
    
    def get_stock_list(self) -> List[str]:
        """获取A股股票列表"""
        try:
            # 使用 akshare 获取股票列表
            stock_df = ak.stock_info_a_code_name()
            
            # 添加市场标识
            def add_market_prefix(code):
                if code.startswith('6'):
                    return f"SH{code}"
                return f"SZ{code}"
            
            stock_list = [add_market_prefix(code) for code in stock_df['code']]
            logger.info(f"成功获取股票列表，共 {len(stock_list)} 只股票")
            return stock_list
        except Exception as e:
            logger.error(f"获取股票列表失败: {str(e)}")
            raise
    
    def collect_single_stock(self, symbol: str):
        """采集单个股票的数据（带重试机制）"""
        try:
            # 获取数据并打印列名（用于调试）
            df = ak.stock_cash_flow_sheet_by_report_em(symbol=symbol)
            if df.empty:
                logger.warning(f"股票 {symbol} 返回空数据")
                return
                
            logger.debug(f"获取到的数据列名: {df.columns.tolist()}")
            
            df = self.process_data(df, symbol)
            if not df.empty:
                self.save_to_database(df)
                logger.info(f"成功处理股票 {symbol}")
            else:
                logger.warning(f"股票 {symbol} 处理后数据为空")
        except Exception as e:
            logger.error(f"处理股票 {symbol} 失败: {str(e)}")
            raise


    def collect_all_stocks(self, batch_size: int = 50):
        """批量采集所有股票数据"""
        # 获取股票列表
        stock_list = self.get_stock_list()
        total_stocks = len(stock_list)
        
        # 批量处理
        for i in range(0, total_stocks, batch_size):
            batch = stock_list[i:i+batch_size]
            logger.info(f"开始处理第 {i+1}-{min(i+batch_size, total_stocks)} 只股票")
            
            for symbol in batch:
                try:
                    self.collect_single_stock(symbol)
                    time.sleep(random.uniform(1, 3))  # 每个请求后随机等待
                except Exception as e:
                    logger.error(f"处理股票 {symbol} 时发生错误: {str(e)}")
                    continue
            
            # 每批次处理完后额外等待
            logger.info(f"批次处理完成，等待 10 秒后继续...")
            time.sleep(10) 
            
def main():
    parser = argparse.ArgumentParser(description='现金流量表数据采集工具')
    parser.add_argument('--symbols', nargs='+', required=True, help='股票代码列表')
    
    try:
        args = parser.parse_args()
    except SystemExit:
        parser.print_help()
        sys.exit(1)

    # 数据库配置
    db_config = {
        'host': 'localhost',
        'port': 5432,
        'user': 'postgres',
        'password': '12',
        'database': 'Financialdata'
    }
    
    collector = CashFlowSheetCollector(db_config)
    
    try:
        # 设置批量处理大小
        batch_size = 50
        
        # 开始采集数据
        logger.info("开始采集现金流量表数据...")
        #collector.collect_all_stocks(batch_size)
        for symbol in args.symbols:
            logger.info(f"开始处理股票 {symbol}")
            collector.collect_data(symbol)
        
        
    except KeyboardInterrupt:
        logger.info("程序被用户中断")
    except Exception as e:
        logger.error(f"程序运行出错: {str(e)}")
        raise
    finally:
        logger.info("程序运行完成")

if __name__ == "__main__":
    main()
