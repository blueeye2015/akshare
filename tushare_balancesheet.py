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
        logging.FileHandler('balancesheet.log'),
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

class BalanceSheetCollector:
    def __init__(self, db_params: dict, tushare_token: str):
        self.db_params = db_params
        self.table_name = 'balancesheet'
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
            total_share DECIMAL(20,4),
            cap_rese DECIMAL(20,4),
            undistr_porfit DECIMAL(20,4),
            surplus_rese DECIMAL(20,4),
            special_rese DECIMAL(20,4),
            money_cap DECIMAL(20,4),
            trad_asset DECIMAL(20,4),
            notes_receiv DECIMAL(20,4),
            accounts_receiv DECIMAL(20,4),
            oth_receiv DECIMAL(20,4),
            prepayment DECIMAL(20,4),
            div_receiv DECIMAL(20,4),
            int_receiv DECIMAL(20,4),
            inventories DECIMAL(20,4),
            amor_exp DECIMAL(20,4),
            nca_within_1y DECIMAL(20,4),
            sett_rsrv DECIMAL(20,4),
            loanto_oth_bank_fi DECIMAL(20,4),
            premium_receiv DECIMAL(20,4),
            reinsur_receiv DECIMAL(20,4),
            reinsur_res_receiv DECIMAL(20,4),
            pur_resale_fa DECIMAL(20,4),
            oth_cur_assets DECIMAL(20,4),
            total_cur_assets DECIMAL(20,4),
            fa_avail_for_sale DECIMAL(20,4),
            htm_invest DECIMAL(20,4),
            lt_eqt_invest DECIMAL(20,4),
            lt_rec DECIMAL(20,4),
            fix_assets DECIMAL(20,4),
            cip DECIMAL(20,4),
            const_materials DECIMAL(20,4),
            fixed_assets_disp DECIMAL(20,4),
            produc_bio_assets DECIMAL(20,4),
            oil_and_gas_assets DECIMAL(20,4),
            intan_assets DECIMAL(20,4),
            r_and_d DECIMAL(20,4),
            goodwill DECIMAL(20,4),
            lt_amor_exp DECIMAL(20,4),
            defer_tax_assets DECIMAL(20,4),
            decr_in_disbur DECIMAL(20,4),
            oth_nca DECIMAL(20,4),
            total_nca DECIMAL(20,4),
            cash_reser_cb DECIMAL(20,4),
            depos_in_oth_bfi DECIMAL(20,4),
            prec_metals DECIMAL(20,4),
            deriv_assets DECIMAL(20,4),
            rr_reins_une_prem DECIMAL(20,4),
            rr_reins_outstd_cla DECIMAL(20,4),
            rr_reins_lins_liab DECIMAL(20,4),
            rr_reins_lthins_liab DECIMAL(20,4),
            refund_depos DECIMAL(20,4),
            ph_pledge_loans DECIMAL(20,4),
            refund_cap_depos DECIMAL(20,4),
            indep_acct_assets DECIMAL(20,4),
            client_depos DECIMAL(20,4),
            client_prov DECIMAL(20,4),
            transac_seat_fee DECIMAL(20,4),
            invest_as_receiv DECIMAL(20,4),
            total_assets DECIMAL(20,4),
            lt_borr DECIMAL(20,4),
            st_borr DECIMAL(20,4),
            cb_borr DECIMAL(20,4),
            depos_ib_deposits DECIMAL(20,4),
            loan_oth_bank DECIMAL(20,4),
            trading_fl DECIMAL(20,4),
            notes_payable DECIMAL(20,4),
            acct_payable DECIMAL(20,4),
            adv_receipts DECIMAL(20,4),
            sold_for_repur_fa DECIMAL(20,4),
            comm_payable DECIMAL(20,4),
            payroll_payable DECIMAL(20,4),
            taxes_payable DECIMAL(20,4),
            int_payable DECIMAL(20,4),
            div_payable DECIMAL(20,4),
            oth_payable DECIMAL(20,4),
            acc_exp DECIMAL(20,4),
            deferred_inc DECIMAL(20,4),
            st_bonds_payable DECIMAL(20,4),
            payable_to_reinsurer DECIMAL(20,4),
            rsrv_insur_cont DECIMAL(20,4),
            acting_trading_sec DECIMAL(20,4),
            acting_uw_sec DECIMAL(20,4),
            non_cur_liab_due_1y DECIMAL(20,4),
            oth_cur_liab DECIMAL(20,4),
            total_cur_liab DECIMAL(20,4),
            bond_payable DECIMAL(20,4),
            lt_payable DECIMAL(20,4),
            specific_payables DECIMAL(20,4),
            estimated_liab DECIMAL(20,4),
            defer_tax_liab DECIMAL(20,4),
            defer_inc_non_cur_liab DECIMAL(20,4),
            oth_ncl DECIMAL(20,4),
            total_ncl DECIMAL(20,4),
            depos_oth_bfi DECIMAL(20,4),
            deriv_liab DECIMAL(20,4),
            depos DECIMAL(20,4),
            agency_bus_liab DECIMAL(20,4),
            oth_liab DECIMAL(20,4),
            prem_receiv_adva DECIMAL(20,4),
            depos_received DECIMAL(20,4),
            ph_invest DECIMAL(20,4),
            reser_une_prem DECIMAL(20,4),
            reser_outstd_claims DECIMAL(20,4),
            reser_lins_liab DECIMAL(20,4),
            reser_lthins_liab DECIMAL(20,4),
            indept_acc_liab DECIMAL(20,4),
            pledge_borr DECIMAL(20,4),
            indem_payable DECIMAL(20,4),
            policy_div_payable DECIMAL(20,4),
            total_liab DECIMAL(20,4),
            treasury_share DECIMAL(20,4),
            ordin_risk_reser DECIMAL(20,4),
            forex_differ DECIMAL(20,4),
            invest_loss_unconf DECIMAL(20,4),
            minority_int DECIMAL(20,4),
            total_hldr_eqy_exc_min_int DECIMAL(20,4),
            total_hldr_eqy_inc_min_int DECIMAL(20,4),
            total_liab_hldr_eqy DECIMAL(20,4),
            lt_payroll_payable DECIMAL(20,4),
            oth_comp_income DECIMAL(20,4),
            oth_eqt_tools DECIMAL(20,4),
            oth_eqt_tools_p_shr DECIMAL(20,4),
            lending_funds DECIMAL(20,4),
            acc_receivable DECIMAL(20,4),
            st_fin_payable DECIMAL(20,4),
            payables DECIMAL(20,4),
            hfs_assets DECIMAL(20,4),
            hfs_sales DECIMAL(20,4),
            cost_fin_assets DECIMAL(20,4),
            fair_value_fin_assets DECIMAL(20,4),
            cip_total DECIMAL(20,4),
            oth_pay_total DECIMAL(20,4),
            long_pay_total DECIMAL(20,4),
            debt_invest DECIMAL(20,4),
            oth_debt_invest DECIMAL(20,4),
            oth_eq_invest DECIMAL(20,4),
            oth_illiq_fin_assets DECIMAL(20,4),
            oth_eq_ppbond DECIMAL(20,4),
            receiv_financing DECIMAL(20,4),
            use_right_assets DECIMAL(20,4),
            lease_liab DECIMAL(20,4),
            contract_assets DECIMAL(20,4),
            contract_liab DECIMAL(20,4),
            accounts_receiv_bill DECIMAL(20,4),
            accounts_pay DECIMAL(20,4),
            oth_rcv_total DECIMAL(20,4),
            fix_assets_total DECIMAL(20,4),
            update_flag VARCHAR(1),
            PRIMARY KEY (ts_code, end_date)
        )
        """.format(self.table_name)
        
        with self.get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(create_table_sql)
                conn.commit()
                logger.info("数据表初始化完成")

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

    @retry_on_exception(retries=3, delay=5, backoff=2)
    def get_trade_cal(self, start_date: str, end_date: str) -> List[str]:
        """获取交易日历"""
        try:
            df = self.pro.trade_cal(
                exchange='SSE',
                start_date=start_date,
                end_date=end_date,
                is_open='1'
            )
            return df['cal_date'].tolist()
        except Exception as e:
            logger.error(f"获取交易日历失败: {str(e)}")
            return []

    @retry_on_exception(retries=3, delay=5, backoff=2)
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

    @retry_on_exception(retries=3, delay=5, backoff=2)
    def fetch_balancesheet_data(self, ts_code: str, period: str) -> pd.DataFrame:
        """获取指定股票和报告期的资产负债表数据"""
        try:
            logger.info(f"获取 {ts_code} 在 {period} 期间的资产负债表数据")
            df = self.pro.balancesheet(ts_code=ts_code, period=period)
            # 添加延时以避免频率限制
            time.sleep(0.3)
            return df
        except Exception as e:
            logger.error(f"获取 {ts_code} 在 {period} 资产负债表数据失败: {str(e)}")
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
        
        # 确保数据没有重复的主键
        if len(df) != len(df.drop_duplicates(subset=['ts_code', 'end_date'])):
            logger.warning(f"检测到重复的主键，正在删除重复项...")
            df = df.drop_duplicates(subset=['ts_code', 'end_date'])
                
        with self.get_db_connection() as conn:
            with conn.cursor() as cur:
                columns = df.columns.tolist()
                values = [tuple(x) for x in df.values]
                
                insert_stmt = f"""
                    INSERT INTO {self.table_name} ({','.join(columns)})
                    VALUES %s
                    ON CONFLICT (ts_code, end_date)
                    DO UPDATE SET
                    {','.join(f"{col}=EXCLUDED.{col}" for col in columns 
                             if col not in ['ts_code', 'end_date'])}
                """
                
                try:
                    execute_values(cur, insert_stmt, values)
                    conn.commit()
                    logger.info(f"成功保存 {len(df)} 条记录")
                except Exception as e:
                    conn.rollback()
                    logger.error(f"保存数据失败: {str(e)}")
                    raise

def process_stock_period(db_params: Dict, tushare_token: str, ts_code: str, period: str):
    """处理单个股票和报告期的数据采集"""
    try:
        collector = BalanceSheetCollector(db_params, tushare_token)
        
        # 获取数据
        df = collector.fetch_balancesheet_data(ts_code, period)
        
        if df is not None and not df.empty:
            df = collector.process_data(df)
            collector.save_to_db(df)
            logger.info(f"成功处理 {ts_code} 在 {period} 期间的资产负债表数据，共 {len(df)} 条记录")
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
    parser = argparse.ArgumentParser(description='股票资产负债表数据采集工具')
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
        collector = BalanceSheetCollector(db_params, args.tushare_token)
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
        
        logger.info(f"开始处理 {len(periods)} 个报告期的资产负债表数据")
        
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
