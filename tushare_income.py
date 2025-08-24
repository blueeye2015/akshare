import tushare as ts
import akshare as ak
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
import logging
from typing import List, Dict, Optional
import time
import random
import multiprocessing
import argparse
import sys
from datetime import datetime, timedelta
from functools import wraps

# 设置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('income.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def retry_on_exception(retries=3, delay=5, backoff=2, exceptions=(Exception,)):
    """重试装饰器"""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            retry_delay = delay
            last_exception = None
            
            for retry in range(retries + 1):
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    last_exception = e
                    if retry == retries:
                        raise last_exception
                    
                    wait_time = retry_delay + random.uniform(0, 1)
                    logger.warning(f"尝试 {retry + 1}/{retries} 失败: {str(e)}. "
                                 f"{wait_time:.2f} 秒后重试...")
                    
                    time.sleep(wait_time)
                    retry_delay *= backoff
                    
            raise last_exception
        return wrapper
    return decorator

class IncomeCollector:
    def __init__(self, db_params: dict, tushare_token: str):
        self.db_params = db_params
        self.table_name = 'income'
        self.pro = ts.pro_api(tushare_token)
        
    def get_db_connection(self):
        return psycopg2.connect(**self.db_params)

    def init_table(self):
        """初始化数据表"""
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS {} (
            ts_code VARCHAR(10),
            ann_date DATE,
            f_ann_date DATE,
            end_date DATE,
            report_type VARCHAR(4),
            comp_type VARCHAR(4),
            end_type VARCHAR(4),
            basic_eps DECIMAL(10,4),
            diluted_eps DECIMAL(10,4),
            total_revenue DECIMAL(20,4),
            revenue DECIMAL(20,4),
            int_income DECIMAL(20,4),
            prem_earned DECIMAL(20,4),
            comm_income DECIMAL(20,4),
            n_commis_income DECIMAL(20,4),
            n_oth_income DECIMAL(20,4),
            n_oth_b_income DECIMAL(20,4),
            prem_income DECIMAL(20,4),
            out_prem DECIMAL(20,4),
            une_prem_reser DECIMAL(20,4),
            reins_income DECIMAL(20,4),
            n_sec_tb_income DECIMAL(20,4),
            n_sec_uw_income DECIMAL(20,4),
            n_asset_mg_income DECIMAL(20,4),
            oth_b_income DECIMAL(20,4),
            fv_value_chg_gain DECIMAL(20,4),
            invest_income DECIMAL(20,4),
            ass_invest_income DECIMAL(20,4),
            forex_gain DECIMAL(20,4),
            total_cogs DECIMAL(20,4),
            oper_cost DECIMAL(20,4),
            int_exp DECIMAL(20,4),
            comm_exp DECIMAL(20,4),
            biz_tax_surchg DECIMAL(20,4),
            sell_exp DECIMAL(20,4),
            admin_exp DECIMAL(20,4),
            fin_exp DECIMAL(20,4),
            assets_impair_loss DECIMAL(20,4),
            prem_refund DECIMAL(20,4),
            compens_payout DECIMAL(20,4),
            reser_insur_liab DECIMAL(20,4),
            div_payt DECIMAL(20,4),
            reins_exp DECIMAL(20,4),
            oper_exp DECIMAL(20,4),
            compens_payout_refu DECIMAL(20,4),
            insur_reser_refu DECIMAL(20,4),
            reins_cost_refund DECIMAL(20,4),
            other_bus_cost DECIMAL(20,4),
            operate_profit DECIMAL(20,4),
            non_oper_income DECIMAL(20,4),
            non_oper_exp DECIMAL(20,4),
            nca_disploss DECIMAL(20,4),
            total_profit DECIMAL(20,4),
            income_tax DECIMAL(20,4),
            n_income DECIMAL(20,4),
            n_income_attr_p DECIMAL(20,4),
            minority_gain DECIMAL(20,4),
            oth_compr_income DECIMAL(20,4),
            t_compr_income DECIMAL(20,4),
            compr_inc_attr_p DECIMAL(20,4),
            compr_inc_attr_m_s DECIMAL(20,4),
            ebit DECIMAL(20,4),
            ebitda DECIMAL(20,4),
            insurance_exp DECIMAL(20,4),
            undist_profit DECIMAL(20,4),
            distable_profit DECIMAL(20,4),
            rd_exp DECIMAL(20,4),
            fin_exp_int_exp DECIMAL(20,4),
            fin_exp_int_inc DECIMAL(20,4),
            transfer_surplus_rese DECIMAL(20,4),
            transfer_housing_imprest DECIMAL(20,4),
            transfer_oth DECIMAL(20,4),
            adj_lossgain DECIMAL(20,4),
            withdra_legal_surplus DECIMAL(20,4),
            withdra_legal_pubfund DECIMAL(20,4),
            withdra_biz_devfund DECIMAL(20,4),
            withdra_rese_fund DECIMAL(20,4),
            withdra_oth_ersu DECIMAL(20,4),
            workers_welfare DECIMAL(20,4),
            distr_profit_shrhder DECIMAL(20,4),
            prfshare_payable_dvd DECIMAL(20,4),
            comshare_payable_dvd DECIMAL(20,4),
            capit_comstock_div DECIMAL(20,4),
            net_after_nr_lp_correct DECIMAL(20,4),
            credit_impa_loss DECIMAL(20,4),
            net_expo_hedging_benefits DECIMAL(20,4),
            oth_impair_loss_assets DECIMAL(20,4),
            total_opcost DECIMAL(20,4),
            amodcost_fin_assets DECIMAL(20,4),
            oth_income DECIMAL(20,4),
            asset_disp_income DECIMAL(20,4),
            continued_net_profit DECIMAL(20,4),
            end_net_profit DECIMAL(20,4),
            update_flag VARCHAR(1),
            PRIMARY KEY (ts_code, end_date)
        )
        """.format(self.table_name)
        
        with self.get_db_connection() as conn:
            with conn.cursor() as cur:
                # 先检查表是否存在
                cur.execute(f"""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_name = '{self.table_name}'
                    )
                """)
                table_exists = cur.fetchone()[0]
                
                if table_exists:
                    logger.info(f"表 {self.table_name} 已存在，跳过创建")
                else:
                    # 创建新表
                    cur.execute(create_table_sql)
                    conn.commit()
                    logger.info("数据表初始化完成")

    def add_missing_columns(self, df: pd.DataFrame):
        """添加缺失的列到数据库表"""
        if df.empty:
            return
            
        with self.get_db_connection() as conn:
            with conn.cursor() as cur:
                # 获取当前表的列
                cur.execute(f"""
                    SELECT column_name 
                    FROM information_schema.columns 
                    WHERE table_name = '{self.table_name}'
                """)
                existing_columns = [row[0] for row in cur.fetchall()]
                
                # 检查是否有缺失的列
                df_columns = df.columns.tolist()
                missing_columns = [col for col in df_columns if col.lower() not in existing_columns]
                
                # 添加缺失的列
                for col in missing_columns:
                    try:
                        alter_sql = f"ALTER TABLE {self.table_name} ADD COLUMN {col} DECIMAL(20,4)"
                        cur.execute(alter_sql)
                        conn.commit()
                        logger.info(f"添加了缺失的列: {col}")
                    except Exception as e:
                        conn.rollback()
                        logger.error(f"添加列 {col} 失败: {str(e)}")

    @retry_on_exception(retries=3, delay=5, backoff=2)
    def get_all_stocks(self) -> List[str]:
        """获取所有股票代码"""
        try:
            df = ak.stock_info_a_code_name()
            stock_list = [
                f"{code}.{'SH' if str(code).startswith('6') else 'SZ'}"
                for code in df['code'].tolist()
                if str(code).startswith(('0', '3', '6'))
            ]
            logger.info(f"成功获取 {len(stock_list)} 个股票代码")
            return stock_list
        except Exception as e:
            logger.error(f"获取股票列表失败: {str(e)}")
            return []

    def get_report_periods(self, start_date: str, end_date: str) -> List[str]:
        """获取报告期列表"""
        # 季度末日期通常是3月31日、6月30日、9月30日和12月31日
        periods = []
        start_year = int(start_date[:4])
        end_year = int(end_date[:4])
        
        for year in range(start_year, end_year + 1):
            for quarter_end in ['0331', '0630', '0930', '1231']:
                period = f"{year}{quarter_end}"
                if start_date <= period <= end_date:
                    periods.append(period)
        
        return periods

    def get_latest_period(self) -> Optional[str]:
        """获取数据库中最新的报告期"""
        with self.get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(f"""
                    SELECT end_date 
                    FROM {self.table_name}
                    ORDER BY end_date DESC
                    LIMIT 1
                """)
                result = cur.fetchone()
                if result:
                    # 将日期转换为字符串格式 YYYYMMDD
                    return result[0].strftime('%Y%m%d')
                return None

    @retry_on_exception(retries=3, delay=10, backoff=1.5)
    def fetch_income_data(self, ts_code: str, period: str) -> pd.DataFrame:
        """获取指定股票和报告期的利润表数据"""
        try:
            logger.info(f"获取 {ts_code} 在 {period} 期间的利润表数据")
            df = self.pro.income(ts_code=ts_code, period=period)
            
            # 重要：每次请求后都要延时，避免触发频率限制
            # 200次/分钟 = 每0.3秒一次请求是安全的
            time.sleep(0.5)  # 增加到0.5秒更安全
            
            return df
        except Exception as e:
            if "每分钟最多访问" in str(e):
                logger.warning(f"触发频率限制，等待60秒后重试...")
                time.sleep(60)  # 等待1分钟让频率限制重置
            logger.error(f"获取 {ts_code} 在 {period} 利润表数据失败: {str(e)}")
            raise


    def process_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """处理数据"""
        if df.empty:
            return pd.DataFrame()
        
        # 转换日期格式
        date_columns = ['ann_date', 'f_ann_date', 'end_date']
        for col in date_columns:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col]).dt.date
        
        return df

    def save_to_db(self, df: pd.DataFrame):
        """保存数据到数据库"""
        if df.empty:
            return
            
        # 检查并添加缺失的列
        self.add_missing_columns(df)
        
        # 确保数据没有重复的主键
        if len(df) != len(df.drop_duplicates(subset=['ts_code', 'end_date'])):
            logger.warning(f"检测到重复的主键，正在删除重复项...")
            df = df.drop_duplicates(subset=['ts_code', 'end_date'])
            
        with self.get_db_connection() as conn:
            with conn.cursor() as cur:
                columns = df.columns.tolist()
                values = [tuple(x) for x in df.values]
                
                # 使用临时表策略来避免冲突
                try:
                    # 创建临时表
                    temp_table = f"{self.table_name}_temp_{int(time.time())}"
                    cur.execute(f"CREATE TEMP TABLE {temp_table} AS SELECT * FROM {self.table_name} WITH NO DATA")
                    
                    # 批量插入数据到临时表
                    execute_values(cur, f"INSERT INTO {temp_table} ({','.join(columns)}) VALUES %s", values)
                    
                    # 从临时表更新主表，使用单独的INSERT和UPDATE语句
                    update_stmt = f"""
                        UPDATE {self.table_name} m
                        SET {','.join(f"{col}=t.{col}" for col in columns if col not in ['ts_code', 'end_date'])}
                        FROM {temp_table} t
                        WHERE m.ts_code = t.ts_code AND m.end_date = t.end_date
                    """
                    cur.execute(update_stmt)
                    
                    insert_stmt = f"""
                        INSERT INTO {self.table_name}
                        SELECT t.*
                        FROM {temp_table} t
                        LEFT JOIN {self.table_name} m ON t.ts_code = m.ts_code AND t.end_date = m.end_date
                        WHERE m.ts_code IS NULL
                    """
                    cur.execute(insert_stmt)
                    
                    # 删除临时表
                    cur.execute(f"DROP TABLE {temp_table}")
                    
                    conn.commit()
                    logger.info(f"成功保存 {len(df)} 条记录")
                    
                except Exception as e:
                    conn.rollback()
                    logger.error(f"保存数据失败: {str(e)}")
                    raise

def process_stock_period(db_params: Dict, tushare_token: str, ts_code: str, period: str):
    """处理单个股票和报告期的数据采集"""
    try:
        collector = IncomeCollector(db_params, tushare_token)
        
        # 获取数据
        df = collector.fetch_income_data(ts_code, period)
        
        if df is not None and not df.empty:
            df = collector.process_data(df)
            collector.save_to_db(df)
            logger.info(f"成功处理 {ts_code} 在 {period} 期间的利润表数据，共 {len(df)} 条记录")
        else:
            logger.warning(f"股票 {ts_code} 在期间 {period} 没有可用数据")
            
    except Exception as e:
        logger.error(f"处理股票 {ts_code} 在期间 {period} 时出错: {str(e)}")

def chunk_list(lst, n):
    """将列表分成n个大小相近的块"""
    k, m = divmod(len(lst), n)
    return [lst[i * k + min(i, m):(i + 1) * k + min(i + 1, m)] for i in range(n)]

def process_period_stocks(db_params: Dict, tushare_token: str, period: str, stock_list: List[str], worker_id: int, total_workers: int):
    """处理单个报告期的多个股票数据采集"""
    try:
        # 将股票列表分成多个块，每个worker处理一部分
        stock_chunks = chunk_list(stock_list, total_workers)
        if worker_id < len(stock_chunks):
            stocks_to_process = stock_chunks[worker_id]
            logger.info(f"Worker {worker_id}: 开始处理 {period} 期间的 {len(stocks_to_process)} 只股票")
            
            for i, ts_code in enumerate(stocks_to_process):
                try:
                    process_stock_period(db_params, tushare_token, ts_code, period)
                    # 每处理10只股票记录一次进度
                    if (i + 1) % 10 == 0:
                        logger.info(f"Worker {worker_id}: {period} 期间已处理 {i+1}/{len(stocks_to_process)} 只股票")
                except Exception as e:
                    logger.error(f"Worker {worker_id}: 处理股票 {ts_code} 在 {period} 期间时出错: {str(e)}")
                    continue
                    
            logger.info(f"Worker {worker_id}: 成功完成 {period} 期间的所有股票处理")
        else:
            logger.warning(f"Worker {worker_id}: 没有分配到股票任务")
            
    except Exception as e:
        logger.error(f"Worker {worker_id} 处理期间 {period} 时出错: {str(e)}")

def main():
    parser = argparse.ArgumentParser(description='股票利润表数据采集工具')
    parser.add_argument(
        '--start_date',
        type=str,
        help='开始日期 (YYYYMMDD)'
    )
    parser.add_argument(
        '--end_date',
        type=str,
        default=datetime.now().strftime('%Y%m%d'),
        help='结束日期 (YYYYMMDD)'
    )
    parser.add_argument(
        '--tushare_token',
        type=str,
        required=True,
        help='Tushare API token'
    )
    parser.add_argument(
        '--processes',
        type=int,
        default=4,
        help='并行进程数'
    )
    
    try:
        args = parser.parse_args()
    except SystemExit:
        parser.print_help()
        sys.exit(1)

    # 数据库连接参数
    db_params = {
        'host': '192.168.50.149',
        'port': 5432,
        'user': 'postgres',
        'password': '12',
        'database': 'Financialdata'
    }
    
    try:
        collector = IncomeCollector(db_params, args.tushare_token)
        collector.init_table()
        
        # 获取所有股票列表
        stock_list = collector.get_all_stocks()
        if not stock_list:
            logger.error("没有找到股票列表，程序退出")
            return
            
        logger.info(f"共获取到 {len(stock_list)} 只股票")
        
        # 确定开始日期
        start_date = args.start_date
        if not start_date:
            latest_period = collector.get_latest_period()
            if latest_period:
                # 如果有最新期间，则从下一个季度开始
                year = int(latest_period[:4])
                month = int(latest_period[4:6])
                
                if month == 3:
                    start_date = f"{year}0401"
                elif month == 6:
                    start_date = f"{year}0701"
                elif month == 9:
                    start_date = f"{year}1001"
                else:  # month == 12
                    start_date = f"{year+1}0101"
            else:
                # 默认从2000年开始
                start_date = "20000101"
        
        # 获取需要处理的报告期
        periods = collector.get_report_periods(start_date, args.end_date)
        
        if not periods:
            logger.error("没有找到需要处理的报告期")
            return
        
        logger.info(f"开始处理 {len(periods)} 个报告期的利润表数据")
        
        # 调整进程数
        num_processes = min(args.processes, len(periods) * len(stock_list))
        
        # 创建任务列表
        tasks = []
        for period in periods:
            for worker_id in range(args.processes):
                tasks.append((db_params, args.tushare_token, period, stock_list, worker_id, args.processes))
        
        # 创建进程池执行任务
        with multiprocessing.Pool(processes=num_processes) as pool:
            pool.starmap(process_period_stocks, tasks)
            
        logger.info("所有报告期和股票处理完成")
        
    except KeyboardInterrupt:
        logger.info("程序被用户中断")
    except Exception as e:
        logger.error(f"程序运行出错: {str(e)}")
        raise
    finally:
        logger.info("程序运行完成")

if __name__ == "__main__":
    multiprocessing.freeze_support()
    main()
