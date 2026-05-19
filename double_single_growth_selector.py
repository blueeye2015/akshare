#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
双增/单增选股器

双增策略：基于正式季度报告（profit_sheet）
  - LTM扣非PEG < max_peg（默认1）
  - 季度扣非增速 > min_profit_growth（默认50%）
  - 季度营收增速 > min_revenue_growth（默认10%，可选上限）

单增策略：基于业绩快报/预告（performance_express / performance_forecast）
  - LTM扣非PEG < max_peg（默认1）
  - 季度扣非增速 > min_profit_growth（默认50%）
  - 无季度营收增速条件

说明：
  - LTM = Last Twelve Months，最近4个季度单季之和
  - PEG = PE_TTM / (LTM扣非净利润增速 × 100)
  - 季度扣非增速 = 最新单季度扣非净利润 vs 去年同期单季度的增速
  - 季度营收增速 = 最新单季度营业收入 vs 去年同期单季度的增速
"""

import pandas as pd
import numpy as np
import pywencai
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime
from dataclasses import dataclass, field
from typing import List, Optional, Dict, Tuple
import logging
import argparse
import os
from urllib.parse import urlparse
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# 配置加载（从 .env 读取 DB_DSN1）
# ---------------------------------------------------------------------------
def load_db_config() -> Dict:
    """从 .env 文件读取 DB_DSN1 并解析为 psycopg2 配置字典"""
    env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '.env')
    load_dotenv(env_path)

    dsn = os.getenv('DB_DSN1')
    if not dsn:
        raise ValueError("环境变量 DB_DSN1 未设置，请检查 .env 文件")

    # 解析 postgresql://user:password@host:port/database
    parsed = urlparse(dsn)
    db_config = {
        'host': parsed.hostname or '127.0.0.1',
        'port': parsed.port or 5432,
        'database': parsed.path.lstrip('/') if parsed.path else 'Financialdata',
        'user': parsed.username or 'postgres',
        'password': parsed.password or ''
    }
    logger.info(f"数据库配置已加载: {db_config['host']}:{db_config['port']}/{db_config['database']}")
    return db_config


# ---------------------------------------------------------------------------
# 选股条件
# ---------------------------------------------------------------------------
@dataclass
class GrowthCriteria:
    """双增/单增选股条件"""
    strategy: str = "double"               # "double" 或 "single"
    max_peg: float = 1.0                   # LTM扣非PEG上限
    min_profit_growth: float = 0.50        # 季度扣非增速下限（50%）
    min_revenue_growth: float = 0.10       # 季度营收增速下限（10%，仅双增）
    max_revenue_growth: Optional[float] = None  # 季度营收增速上限（可选，仅双增）
    min_market_cap: Optional[float] = None  # 最小市值（亿元），None表示不限制
    max_market_cap: Optional[float] = None  # 最大市值（亿元），None表示不限制
    max_deduct_pe: float = 50.0            # 扣非PE上限（市值/LTM扣非净利润）
    top_n: Optional[int] = None            # 返回前N只，None表示不限制
    exclude_industries: List[str] = field(default_factory=lambda: ['证券'])  # 排除行业列表


# ---------------------------------------------------------------------------
# 选股器主体
# ---------------------------------------------------------------------------
class DoubleSingleGrowthSelector:
    def __init__(self, db_config: Dict):
        self.db_config = db_config
        self.conn = None
        self._tushare_pro = None

    # --- 数据库连接 ---
    def connect(self):
        self.conn = psycopg2.connect(**self.db_config)
        logger.info("数据库连接成功")

    def disconnect(self):
        if self.conn:
            self.conn.close()
            logger.info("数据库连接已关闭")

    def get_latest_trade_date(self) -> Optional[str]:
        """获取最新交易日期"""
        query = "SELECT MAX(trade_date) as latest_date FROM daily_basic"
        with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query)
            result = cur.fetchone()
            if result and result['latest_date']:
                return result['latest_date'].strftime('%Y-%m-%d')
        return None

    # --- 基本面数据（市值、PE） ---
    def fetch_fundamental_data(self, trade_date: str) -> pd.DataFrame:
        """获取每日基本面数据：市值、PE_TTM、行业"""
        query = """
        SELECT
            d.ts_code,
            LEFT(d.ts_code, 6) as symbol,
            s.name,
            s.industry,
            d.total_mv / 10000 as market_cap,   -- 亿元
            d.pe_ttm,
            d.pb,
            d.turnover_rate
        FROM daily_basic d
        LEFT JOIN stock_basic s ON d.ts_code = s.ts_code
        WHERE d.trade_date = %s
          AND d.pe_ttm > 0
          AND d.total_mv IS NOT NULL
        """
        with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, (trade_date,))
            df = pd.DataFrame(cur.fetchall())
        logger.info(f"[基本面] 获取到 {len(df)} 只股票")
        return df

    # --- profit_sheet 季度数据计算（双增核心） ---
    def fetch_profit_sheet_quarterly(self, trade_date: str) -> pd.DataFrame:
        """
        从 profit_sheet 获取扣非净利润+营业收入，计算：
          - 最新单季度扣非增速
          - 最新单季度营收增速
          - LTM扣非增速
        """
        query = """
        SELECT
            LEFT(symbol, 6) as symbol,
            report_date,
            deduct_parent_netprofit,
            operate_income
        FROM profit_sheet
        WHERE report_date <= %s
          AND deduct_parent_netprofit IS NOT NULL
        ORDER BY symbol, report_date DESC
        """
        with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, (trade_date,))
            df = pd.DataFrame(cur.fetchall())

        if df.empty:
            logger.warning("[profit_sheet] 无数据")
            return pd.DataFrame()

        results = []
        for symbol, group in df.groupby('symbol'):
            metrics = self._calc_quarterly_metrics(group)
            if metrics:
                metrics['symbol'] = symbol
                results.append(metrics)

        result_df = pd.DataFrame(results)
        logger.info(f"[profit_sheet] 有效计算 {len(result_df)} 只股票")
        return result_df

    def _calc_quarterly_metrics(self, group: pd.DataFrame) -> Optional[Dict]:
        """
        对一个股票的 profit_sheet 数据计算季度指标
        要求最近5个季度数据连续（用于单季同比），最近8个季度用于LTM同比
        """
        group = group.sort_values('report_date', ascending=False).reset_index(drop=True)
        if len(group) < 5:
            return None

        # 解析每条记录
        records = []
        for _, row in group.iterrows():
            rd_raw = row['report_date']
            # 统一处理为 YYYY-MM-DD 字符串（数据库可能返回 datetime 对象）
            if hasattr(rd_raw, 'strftime'):
                rd = rd_raw.strftime('%Y-%m-%d')
            else:
                rd = str(rd_raw).split()[0]

            profit = float(row['deduct_parent_netprofit']) if row['deduct_parent_netprofit'] is not None else None
            revenue = float(row['operate_income']) if row['operate_income'] is not None else None

            # 判断季度
            if rd.endswith('03-31'):
                q = 1
            elif rd.endswith('06-30'):
                q = 2
            elif rd.endswith('09-30'):
                q = 3
            elif rd.endswith('12-31'):
                q = 4
            else:
                continue
            records.append({'report_date': rd, 'quarter': q, 'year': int(rd[:4]),
                            'cum_profit': profit, 'cum_revenue': revenue})

        if len(records) < 5:
            return None

        # 计算单季度值
        for i, rec in enumerate(records):
            if rec['quarter'] == 1:
                rec['q_profit'] = rec['cum_profit']
                rec['q_revenue'] = rec['cum_revenue']
            elif i < len(records) - 1:
                prev = records[i + 1]
                if rec['cum_profit'] is not None and prev['cum_profit'] is not None:
                    rec['q_profit'] = rec['cum_profit'] - prev['cum_profit']
                else:
                    rec['q_profit'] = None
                if rec['cum_revenue'] is not None and prev['cum_revenue'] is not None:
                    rec['q_revenue'] = rec['cum_revenue'] - prev['cum_revenue']
                else:
                    rec['q_revenue'] = None
            else:
                rec['q_profit'] = None
                rec['q_revenue'] = None

        # 连续性检查：最近5个季度必须是连续的 Q(n),Q(n-1),...,Q(n-4)
        latest = records[0]
        expected = []
        q, y = latest['quarter'], latest['year']
        for _ in range(5):
            expected.append((y, q))
            q -= 1
            if q == 0:
                q = 4
                y -= 1
        for i in range(5):
            if records[i]['quarter'] != expected[i][1] or records[i]['year'] != expected[i][0]:
                return None  # 不连续，跳过

        # 最新单季度 vs 去年同期
        latest_q_profit = records[0]['q_profit']
        latest_q_revenue = records[0]['q_revenue']
        yoy_q_profit = records[4]['q_profit']
        yoy_q_revenue = records[4]['q_revenue']

        # LTM = 最近4个季度单季之和
        ltm_profit = sum(r['q_profit'] for r in records[:4] if r['q_profit'] is not None)
        ltm_cnt = sum(1 for r in records[:4] if r['q_profit'] is not None)

        # 去年同期LTM（需要第5~8个季度）
        yoy_ltm_profit = None
        yoy_ltm_cnt = 0
        if len(records) >= 8:
            yoy_ltm_profit = sum(r['q_profit'] for r in records[4:8] if r['q_profit'] is not None)
            yoy_ltm_cnt = sum(1 for r in records[4:8] if r['q_profit'] is not None)

        # 增速计算
        quarter_profit_growth = None
        if latest_q_profit is not None and yoy_q_profit is not None and yoy_q_profit != 0:
            quarter_profit_growth = (latest_q_profit - yoy_q_profit) / abs(yoy_q_profit)

        quarter_revenue_growth = None
        if latest_q_revenue is not None and yoy_q_revenue is not None and yoy_q_revenue != 0:
            quarter_revenue_growth = (latest_q_revenue - yoy_q_revenue) / abs(yoy_q_revenue)

        ltm_profit_growth = None
        if ltm_cnt == 4 and yoy_ltm_cnt == 4 and yoy_ltm_profit is not None and yoy_ltm_profit != 0:
            ltm_profit_growth = (ltm_profit - yoy_ltm_profit) / abs(yoy_ltm_profit)

        return {
            'latest_report_date': records[0]['report_date'],
            'quarter_profit_growth': quarter_profit_growth,
            'quarter_revenue_growth': quarter_revenue_growth,
            'ltm_profit_growth': ltm_profit_growth,
            'latest_q_profit': latest_q_profit,
            'latest_q_revenue': latest_q_revenue,
            'ltm_profit': ltm_profit,
            'data_source': 'profit_sheet'
        }

    # --- performance_forecast 单季度扣非增速（单增核心） ---
    def fetch_forecast_quarterly_growth(self, trade_date: str) -> pd.DataFrame:
        """
        从 performance_forecast 获取扣非预告，结合 profit_sheet 历史数据计算单季度扣非增速。
        逻辑：
          forecast_value 是累计值（对应 report_period），
          单季度 = forecast_value - profit_sheet 上一期累计值
          去年同期单季度 = profit_sheet 去年同期累计 - 去年同期上一期累计
        """
        current_year = str(trade_date)[:4]

        # 1) 获取本年度扣非预告（最新一期）
        query = """
        SELECT
            LEFT(symbol, 6) as symbol,
            report_period,
            forecast_value,
            last_year_value,
            change_rate,
            announce_date
        FROM performance_forecast
        WHERE report_period LIKE %s
          AND forecast_indicator = '扣除非经常性损益后的净利润'
          AND forecast_value IS NOT NULL
        """
        with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, (f'{current_year}%',))
            df_fc = pd.DataFrame(cur.fetchall())

        if df_fc.empty:
            logger.warning("[performance_forecast] 无扣非预告数据")
            return pd.DataFrame()

        # 取每只股票最新的一条预告
        df_fc = df_fc.sort_values('announce_date', ascending=False)
        df_fc = df_fc.drop_duplicates(subset=['symbol'], keep='first')

        symbols = df_fc['symbol'].tolist()
        if not symbols:
            return pd.DataFrame()

        # 2) 批量获取这些股票的 profit_sheet 历史数据（用于差分）
        query_hist = """
        SELECT
            LEFT(symbol, 6) as symbol,
            report_date,
            deduct_parent_netprofit
        FROM profit_sheet
        WHERE LEFT(symbol, 6) = ANY(%s)
          AND deduct_parent_netprofit IS NOT NULL
        ORDER BY symbol, report_date DESC
        """
        with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query_hist, (symbols,))
            df_hist = pd.DataFrame(cur.fetchall())

        # 3) 逐条计算单季度增速
        results = []
        for _, row in df_fc.iterrows():
            symbol = row['symbol']
            rp = str(row['report_period'])          # e.g. '20251231'
            forecast_val = self._to_float(row['forecast_value'])
            change_rate = self._to_float(row['change_rate'])

            if forecast_val is None or len(rp) != 8:
                continue

            # 确定所需的历史报告期
            prev_rd, yoy_rd, yoy_prev_rd = self._get_related_periods(rp)
            if not prev_rd:
                continue

            hist = df_hist[df_hist['symbol'] == symbol]
            if hist.empty:
                continue

            hist = hist.copy()
            hist['rd_fmt'] = hist['report_date'].astype(str).str.replace('-', '')

            # profit_sheet 上一期累计值
            prev_cum = self._lookup_profit(hist, prev_rd)
            # 去年同期累计值
            yoy_cum = self._lookup_profit(hist, yoy_rd)
            # 去年同期上一期累计值
            yoy_prev_cum = self._lookup_profit(hist, yoy_prev_rd)

            # 计算单季度值（单位统一为 profit_sheet 单位，forecast_val 原始是元需转换）
            # 根据项目现有逻辑：profit_sheet 存的是"万元"级别（原始元/10000）
            # performance_forecast 的 forecast_value 原始是元，同样 /10000
            forecast_val_wan = forecast_val / 10000.0

            if prev_cum is not None:
                q_profit = forecast_val_wan - prev_cum
            else:
                # 如果是Q1且没有上一期，forecast 本身就是单季度
                if rp.endswith('0331'):
                    q_profit = forecast_val_wan
                else:
                    continue

            # 去年同期单季度
            if yoy_cum is not None and yoy_prev_cum is not None:
                yoy_q_profit = yoy_cum - yoy_prev_cum
            elif rp.endswith('0331') and yoy_cum is not None:
                yoy_q_profit = yoy_cum
            else:
                continue

            if yoy_q_profit == 0:
                continue

            quarter_profit_growth = (q_profit - yoy_q_profit) / abs(yoy_q_profit)

            results.append({
                'symbol': symbol,
                'report_period': rp,
                'quarter_profit_growth': quarter_profit_growth,
                'change_rate': change_rate,
                'forecast_value_wan': forecast_val_wan,
                'data_source': 'performance_forecast'
            })

        result_df = pd.DataFrame(results)
        logger.info(f"[performance_forecast] 有效计算 {len(result_df)} 只股票")
        return result_df

    @staticmethod
    def _get_related_periods(report_period: str) -> Tuple[Optional[str], Optional[str], Optional[str]]:
        """
        给定报告期（如20251231），返回所需的历史报告期：
        (上一期累计, 去年同期累计, 去年同期上一期累计)
        """
        if len(report_period) != 8:
            return None, None, None
        y = report_period[:4]
        suffix = report_period[4:]

        if suffix == '0331':
            return (f"{int(y)-1}1231", f"{int(y)-1}0331", f"{int(y)-2}1231")
        elif suffix == '0630':
            return (f"{y}0331", f"{int(y)-1}0630", f"{int(y)-1}0331")
        elif suffix == '0930':
            return (f"{y}0630", f"{int(y)-1}0930", f"{int(y)-1}0630")
        elif suffix == '1231':
            return (f"{y}0930", f"{int(y)-1}1231", f"{int(y)-1}0930")
        else:
            return None, None, None

    @staticmethod
    def _lookup_profit(hist_df: pd.DataFrame, report_date_fmt: str) -> Optional[float]:
        """在历史数据中查找指定报告期的扣非净利润"""
        row = hist_df[hist_df['rd_fmt'] == report_date_fmt]
        if row.empty:
            return None
        val = row.iloc[0]['deduct_parent_netprofit']
        return float(val) if val is not None else None

    @staticmethod
    def _to_float(val) -> Optional[float]:
        try:
            return float(val)
        except (TypeError, ValueError):
            return None

    # --- performance_express 补充数据 ---
    def fetch_express_growth(self, trade_date: str) -> pd.DataFrame:
        """
        从 performance_express 获取净利润同比增速（累计同比）和营收同比增速。
        作为单增策略的备用数据源。
        """
        current_year = str(trade_date)[:4]
        query = """
        SELECT
            LEFT(symbol, 6) as symbol,
            report_period,
            net_profit_yoy_change,
            net_profit_qoq_change,
            revenue_yoy_change,
            announce_date,
            ROW_NUMBER() OVER (PARTITION BY LEFT(symbol, 6) ORDER BY announce_date DESC) as rn
        FROM performance_express
        WHERE report_period LIKE %s
        """
        with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, (f'{current_year}%',))
            df = pd.DataFrame(cur.fetchall())

        if df.empty:
            return pd.DataFrame()

        # 取最新
        df = df[df['rn'] == 1].drop('rn', axis=1)

        # akshare 返回的百分比字段如 50.5 表示 50.5%，转为小数
        for col in ['net_profit_yoy_change', 'net_profit_qoq_change', 'revenue_yoy_change']:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
                # 如果值明显大于1（如50），说明是百分比数值，除以100
                df[col] = df[col].apply(lambda x: x / 100.0 if pd.notna(x) and abs(x) > 1 else x)

        logger.info(f"[performance_express] 获取到 {len(df)} 只股票")
        return df

    # --- LTM 增速补充（用于单增的PEG计算） ---
    def fetch_ltm_from_profit_sheet(self, trade_date: str, symbols: List[str]) -> pd.DataFrame:
        """只为指定股票列表计算 LTM扣非增速（用于单增的PEG）"""
        if not symbols:
            return pd.DataFrame()

        query = """
        SELECT
            LEFT(symbol, 6) as symbol,
            report_date,
            deduct_parent_netprofit
        FROM profit_sheet
        WHERE report_date <= %s
          AND LEFT(symbol, 6) = ANY(%s)
          AND deduct_parent_netprofit IS NOT NULL
        ORDER BY symbol, report_date DESC
        """
        with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, (trade_date, symbols))
            df = pd.DataFrame(cur.fetchall())

        results = []
        for symbol, group in df.groupby('symbol'):
            metrics = self._calc_quarterly_metrics(group)
            if metrics and metrics.get('ltm_profit_growth') is not None:
                results.append({
                    'symbol': symbol,
                    'ltm_profit_growth': metrics['ltm_profit_growth']
                })

        return pd.DataFrame(results)

    # -----------------------------------------------------------------------
    # 选股主入口
    # -----------------------------------------------------------------------
    def _get_tushare_pro(self):
        """懒加载 tushare pro api"""
        if self._tushare_pro is None:
            tushare_token = os.getenv('TUSHARE') or os.getenv('TUSHARE_TOKEN')
            if tushare_token:
                import tushare as ts
                self._tushare_pro = ts.pro_api(tushare_token)
                logger.info("Tushare Pro 客户端初始化成功")
            else:
                logger.warning("环境变量 TUSHARE/TUSHARE_TOKEN 未设置")
        return self._tushare_pro

    def _fetch_main_business(self, ts_codes: List[str]) -> pd.DataFrame:
        """
        获取主营业务构成。优先从数据库 fina_mainbz 查，缺失的调 tushare 补全并入库。
        取每只股票最近一期报告中，按产品收入最高的前2项拼接。
        """
        if not ts_codes:
            return pd.DataFrame()

        # 1) 先从数据库查已有数据
        existing_df = self._query_mainbiz_from_db(ts_codes)
        existing_codes = set(existing_df['ts_code'].unique()) if not existing_df.empty else set()
        missing_codes = [tc for tc in ts_codes if tc not in existing_codes]

        logger.info(f"主营业务 DB 命中: {len(existing_codes)}/{len(ts_codes)}, 需补全: {len(missing_codes)}")

        # 2) 缺失的调 tushare 获取
        fetched_df = pd.DataFrame()
        if missing_codes:
            pro = self._get_tushare_pro()
            if pro is not None:
                fetched_df = self._sync_mainbiz_from_tushare(pro, missing_codes)

        # 3) 合并
        if not existing_df.empty and not fetched_df.empty:
            df_all = pd.concat([existing_df, fetched_df], ignore_index=True)
        elif not existing_df.empty:
            df_all = existing_df.copy()
        elif not fetched_df.empty:
            df_all = fetched_df.copy()
        else:
            return pd.DataFrame()

        # 4) 处理：取每只股票最新一期，收入最高的前2项
        results = []
        for ts_code, group in df_all.groupby('ts_code'):
            group['bz_sales_num'] = pd.to_numeric(group['bz_sales'], errors='coerce')
            latest_period = group['end_date'].max()
            latest = group[group['end_date'] == latest_period]
            top2 = latest.nlargest(2, 'bz_sales_num')
            items = [str(x).strip() for x in top2['bz_item'].tolist() if pd.notna(x)]
            main_biz = ' + '.join(items) if items else 'N/A'
            results.append({'ts_code': ts_code, 'main_business': main_biz})

        return pd.DataFrame(results)

    def _query_mainbiz_from_db(self, ts_codes: List[str]) -> pd.DataFrame:
        """从数据库查询 fina_mainbz 数据"""
        if not ts_codes or self.conn is None:
            return pd.DataFrame()
        query = """
        SELECT ts_code, end_date, bz_item, bz_sales, bz_profit, bz_cost, curr_type
        FROM fina_mainbz
        WHERE ts_code = ANY(%s)
        """
        try:
            with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(query, (ts_codes,))
                df = pd.DataFrame(cur.fetchall())
            return df
        except Exception as e:
            logger.warning(f"DB 查询 fina_mainbz 失败: {e}")
            return pd.DataFrame()

    def _sync_mainbiz_from_tushare(self, pro, ts_codes: List[str]) -> pd.DataFrame:
        """从 tushare 同步 fina_mainbz，严格频率控制，入库后返回"""
        import time
        end_date = datetime.now().strftime('%Y%m%d')
        start_date = (datetime.now() - pd.Timedelta(days=730)).strftime('%Y%m%d')

        all_data = []
        call_times = []  # 记录每次调用时间

        for tc in ts_codes:
            now = time.time()
            # 清理1分钟前的记录
            call_times = [t for t in call_times if now - t < 60]
            # 如果1分钟内已调用55次，sleep到下一分钟
            if len(call_times) >= 55:
                sleep_sec = 60 - (now - call_times[0]) + 1
                if sleep_sec > 0:
                    logger.info(f"tushare 频率控制: 暂停 {sleep_sec:.0f} 秒")
                    time.sleep(sleep_sec)
                call_times = []

            try:
                df = pro.fina_mainbz(ts_code=tc, type='P', start_date=start_date, end_date=end_date)
                call_times.append(time.time())
                if df is not None and not df.empty:
                    all_data.append(df)
            except Exception as e:
                if '频率超限' in str(e):
                    logger.warning("tushare 频率超限，暂停60秒重试...")
                    time.sleep(60)
                    call_times = []
                    try:
                        df = pro.fina_mainbz(ts_code=tc, type='P', start_date=start_date, end_date=end_date)
                        call_times.append(time.time())
                        if df is not None and not df.empty:
                            all_data.append(df)
                    except Exception as e2:
                        logger.warning(f"重试失败 {tc}: {e2}")
                else:
                    logger.warning(f"fina_mainbz 查询失败 {tc}: {e}")

        if not all_data:
            return pd.DataFrame()

        df_all = pd.concat(all_data, ignore_index=True)
        self._save_mainbiz_to_db(df_all)
        return df_all

    def _save_mainbiz_to_db(self, df: pd.DataFrame):
        """将 fina_mainbz 数据写入数据库"""
        if df.empty or self.conn is None:
            return
        try:
            with self.conn.cursor() as cur:
                for _, row in df.iterrows():
                    cur.execute("""
                        INSERT INTO fina_mainbz (ts_code, end_date, bz_item, bz_code, bz_sales, bz_profit, bz_cost, curr_type)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (ts_code, end_date, bz_item) DO UPDATE SET
                            bz_code = EXCLUDED.bz_code,
                            bz_sales = EXCLUDED.bz_sales,
                            bz_profit = EXCLUDED.bz_profit,
                            bz_cost = EXCLUDED.bz_cost,
                            curr_type = EXCLUDED.curr_type,
                            update_time = CURRENT_TIMESTAMP
                    """, (
                        row.get('ts_code'), row.get('end_date'), row.get('bz_item'),
                        row.get('bz_code'), row.get('bz_sales'), row.get('bz_profit'),
                        row.get('bz_cost'), row.get('curr_type')
                    ))
            self.conn.commit()
            logger.info(f"fina_mainbz 数据已入库: {len(df)} 条")
        except Exception as e:
            logger.warning(f"fina_mainbz 入库失败: {e}")
            self.conn.rollback()

    def select(self, criteria: GrowthCriteria, trade_date: str = None,
               enrich_concepts: bool = False) -> pd.DataFrame:
        """统一选股入口
        
        Args:
            criteria: 选股条件
            trade_date: 交易日期
            enrich_concepts: 是否通过 pywencai 获取所属概念（较慢，按需开启）
        """
        if criteria.strategy == 'double':
            df = self.select_double_growth(criteria, trade_date)
        else:
            df = self.select_single_growth(criteria, trade_date)

        #  enrichment: 主营业务
        if not df.empty and 'ts_code' in df.columns:
            logger.info("正在获取主营业务构成 (fina_mainbz)...")
            df_mainbiz = self._fetch_main_business(df['ts_code'].tolist())
            if not df_mainbiz.empty:
                df = df.merge(df_mainbiz, on='ts_code', how='left')
                logger.info(f"主营业务数据已合并: {len(df)} 只")

        #  enrichment: 所属概念
        if enrich_concepts and not df.empty and 'symbol' in df.columns:
            logger.info("正在获取所属概念 (pywencai)...")
            df_concepts = self._fetch_concepts(df['symbol'].tolist())
            if not df_concepts.empty:
                df = df.merge(df_concepts, on='symbol', how='left')
                logger.info(f"所属概念数据已合并: {len(df)} 只")

        return df

    # -----------------------------------------------------------------------
    # 概念数据获取
    # -----------------------------------------------------------------------
    def _fetch_concepts(self, symbols: List[str], batch_size: int = 50) -> pd.DataFrame:
        """
        通过 pywencai 批量获取股票所属概念。
        pywencai.get(query='所属概念', find=['300750', '603045'], loop=True)
        返回 DataFrame，含 'code' 和 '所属概念' 列。
        """
        if not symbols:
            return pd.DataFrame()

        import time
        all_results = []

        # 去重并转为字符串
        symbols = list(dict.fromkeys(symbols))
        total = len(symbols)

        for i in range(0, total, batch_size):
            batch = symbols[i:i + batch_size]
            try:
                t1 = time.time()
                df = pywencai.get(query='所属概念', find=batch, loop=True)
                t2 = time.time()
                if isinstance(df, pd.DataFrame) and not df.empty:
                    # 提取 code 和所属概念
                    if 'code' in df.columns and '所属概念' in df.columns:
                        all_results.append(df[['code', '所属概念']].copy())
                    elif '股票代码' in df.columns and '所属概念' in df.columns:
                        # 提取6位代码
                        df['code'] = df['股票代码'].str.extract(r'(\d{6})')
                        all_results.append(df[['code', '所属概念']].copy())
                logger.info(f"  pywencai 概念查询 [{i+1}-{min(i+batch_size, total)}/{total}] "
                            f"返回 {len(df) if isinstance(df, pd.DataFrame) else 0} 条, 耗时 {t2-t1:.1f}s")
            except Exception as e:
                logger.warning(f"pywencai 概念查询失败 batch {i+1}-{min(i+batch_size, total)}: {e}")
            time.sleep(0.5)  # 轻微限速

        if not all_results:
            return pd.DataFrame()

        df_all = pd.concat(all_results, ignore_index=True)
        df_all = df_all.rename(columns={'所属概念': 'concepts', 'code': 'symbol'})
        df_all = df_all.drop_duplicates(subset=['symbol'], keep='first')
        # pywencai 可能返回额外股票，过滤只保留传入的 symbol
        symbol_set = set(str(s) for s in symbols)
        df_all = df_all[df_all['symbol'].isin(symbol_set)]
        logger.info(f"所属概念获取完成: {len(df_all)}/{total} 只股票")
        return df_all

    # -----------------------------------------------------------------------
    # 分类筛选
    # -----------------------------------------------------------------------
    def filter_by_concepts(self, df: pd.DataFrame,
                           include: Optional[List[str]] = None,
                           exclude: Optional[List[str]] = None) -> pd.DataFrame:
        """
        按所属概念关键词筛选股票。

        Args:
            df: 选股结果 DataFrame，需含 'concepts' 列
            include: 概念关键词列表，任一匹配即入选（OR 关系）
            exclude: 概念关键词列表，任一匹配即排除
        """
        if df.empty or 'concepts' not in df.columns:
            return df

        result = df.copy()
        if include:
            pattern = '|'.join(include)
            mask = result['concepts'].astype(str).str.contains(pattern, na=False, regex=True)
            result = result[mask]
            logger.info(f"概念筛选(含: {include}): {len(result)} 只")

        if exclude and not result.empty:
            pattern = '|'.join(exclude)
            mask = ~result['concepts'].astype(str).str.contains(pattern, na=False, regex=True)
            result = result[mask]
            logger.info(f"概念筛选(排除: {exclude}): {len(result)} 只")

        return result

    def filter_by_industry(self, df: pd.DataFrame,
                           include: Optional[List[str]] = None,
                           exclude: Optional[List[str]] = None) -> pd.DataFrame:
        """按行业关键词筛选股票（'industry' 列）"""
        if df.empty or 'industry' not in df.columns:
            return df

        result = df.copy()
        if include:
            pattern = '|'.join(include)
            mask = result['industry'].astype(str).str.contains(pattern, na=False, regex=True)
            result = result[mask]
            logger.info(f"行业筛选(含: {include}): {len(result)} 只")

        if exclude and not result.empty:
            pattern = '|'.join(exclude)
            mask = ~result['industry'].astype(str).str.contains(pattern, na=False, regex=True)
            result = result[mask]
            logger.info(f"行业筛选(排除: {exclude}): {len(result)} 只")

        return result

    def filter_by_main_business(self, df: pd.DataFrame,
                                include: Optional[List[str]] = None,
                                exclude: Optional[List[str]] = None) -> pd.DataFrame:
        """按主营业务关键词筛选股票（'main_business' 列）"""
        if df.empty or 'main_business' not in df.columns:
            return df

        result = df.copy()
        if include:
            pattern = '|'.join(include)
            mask = result['main_business'].astype(str).str.contains(pattern, na=False, regex=True)
            result = result[mask]
            logger.info(f"主营业务筛选(含: {include}): {len(result)} 只")

        if exclude and not result.empty:
            pattern = '|'.join(exclude)
            mask = ~result['main_business'].astype(str).str.contains(pattern, na=False, regex=True)
            result = result[mask]
            logger.info(f"主营业务筛选(排除: {exclude}): {len(result)} 只")

        return result

    def classify_summary(self, df: pd.DataFrame,
                         by: str = 'industry',
                         top_k: int = 20) -> pd.DataFrame:
        """
        按指定维度分类统计。

        Args:
            df: 选股结果 DataFrame
            by: 'industry' | 'concepts' | 'main_business'
            top_k: 返回前K个分类
        """
        if df.empty or by not in df.columns:
            return pd.DataFrame()

        if by == 'concepts':
            # 概念是分号分隔的，需要展开
            all_concepts = []
            for _, row in df.iterrows():
                concepts_str = str(row['concepts']) if pd.notna(row['concepts']) else ''
                for c in concepts_str.split(';'):
                    c = c.strip()
                    if c and c != 'nan':
                        all_concepts.append(c)
            summary = pd.Series(all_concepts).value_counts().head(top_k).reset_index()
            summary.columns = ['concept', 'count']
        else:
            summary = df[by].value_counts().head(top_k).reset_index()
            summary.columns = [by, 'count']

        return summary

    def select_double_growth(self, criteria: GrowthCriteria, trade_date: str = None) -> pd.DataFrame:
        """
        双增选股：基于 profit_sheet 正式财报
        """
        if trade_date is None:
            trade_date = self.get_latest_trade_date()
        logger.info(f"=== 双增选股 | 日期: {trade_date} ===")

        # 1) 基本面
        df_basic = self.fetch_fundamental_data(trade_date)
        df_basic = self._apply_market_cap_filter(df_basic, criteria)
        df_basic = self._apply_industry_exclude_filter(df_basic, criteria)
        if df_basic.empty:
            return pd.DataFrame()

        # 2) profit_sheet 季度数据
        df_growth = self.fetch_profit_sheet_quarterly(trade_date)
        if df_growth.empty:
            logger.warning("无 profit_sheet 季度数据")
            return pd.DataFrame()

        # 3) 合并
        df = df_basic.merge(df_growth, on='symbol', how='inner')
        logger.info(f"合并后: {len(df)} 只")

        # 4) 计算PEG
        df = self._calc_peg(df)

        # 5) 计算扣非PE并过滤
        df = self._calc_deduct_pe(df)
        df = self._apply_deduct_pe_filter(df, criteria)
        if df.empty:
            return pd.DataFrame()

        # 6) 应用增速筛选条件
        df = self._apply_growth_filters(df, criteria)
        if df.empty:
            return pd.DataFrame()

        # 6) 按PEG升序排序
        df = df.sort_values('peg', ascending=True)
        if criteria.top_n is not None:
            df = df.head(criteria.top_n)
        return df

    def select_single_growth(self, criteria: GrowthCriteria, trade_date: str = None) -> pd.DataFrame:
        """
        单增选股：季度扣非增速 > 阈值，无营收条件
        数据源优先级：
          1. performance_forecast（扣非预告，最前瞻）
          2. performance_express（快报补充）
          3. profit_sheet（正式财报兜底，不检查营收即可）
        LTM增速统一从 profit_sheet 计算（用于PEG）
        """
        if trade_date is None:
            trade_date = self.get_latest_trade_date()
        logger.info(f"=== 单增选股 | 日期: {trade_date} ===")

        # 1) 基本面
        df_basic = self.fetch_fundamental_data(trade_date)
        df_basic = self._apply_market_cap_filter(df_basic, criteria)
        df_basic = self._apply_industry_exclude_filter(df_basic, criteria)
        if df_basic.empty:
            return pd.DataFrame()

        # 2) 获取 forecast / express 增速
        df_forecast = self.fetch_forecast_quarterly_growth(trade_date)
        df_express = self.fetch_express_growth(trade_date)

        # 3) 合并快报/预报数据
        df_growth_forecast = self._merge_growth_data(df_forecast, df_express)

        # 4) 从 profit_sheet 获取所有股票的季度数据（作为兜底）
        df_growth_profit = self.fetch_profit_sheet_quarterly(trade_date)

        # 5) 合并：forecast/express 优先，profit_sheet 补充缺失的
        if df_growth_forecast.empty:
            df_growth = df_growth_profit[['symbol', 'quarter_profit_growth', 'ltm_profit_growth', 'data_source']].copy()
        else:
            # profit_sheet 补充 forecast 没有覆盖的股票
            forecast_symbols = set(df_growth_forecast['symbol'])
            df_profit_fill = df_growth_profit[~df_growth_profit['symbol'].isin(forecast_symbols)]
            if not df_profit_fill.empty:
                df_profit_fill = df_profit_fill[['symbol', 'quarter_profit_growth', 'ltm_profit_growth', 'data_source']].copy()
                df_growth = pd.concat([df_growth_forecast, df_profit_fill], ignore_index=True)
            else:
                df_growth = df_growth_forecast.copy()
            # 对于 forecast 有的股票，如果其 LTM 为空，用 profit_sheet 补充
            if not df_growth_profit.empty:
                ltm_map = df_growth_profit[['symbol', 'ltm_profit_growth']].copy()
                df_growth = df_growth.merge(ltm_map, on='symbol', how='left', suffixes=('', '_ps'))
                df_growth['ltm_profit_growth'] = df_growth['ltm_profit_growth'].fillna(df_growth['ltm_profit_growth_ps'])
                df_growth = df_growth.drop(columns=['ltm_profit_growth_ps'], errors='ignore')

        if df_growth.empty:
            logger.warning("无任何业绩增速数据")
            return pd.DataFrame()

        # 6) 合并基本面
        df = df_basic.merge(df_growth, on='symbol', how='inner')
        logger.info(f"合并后: {len(df)} 只")

        # 7) 计算PEG
        df = self._calc_peg(df)

        # 8) 确保 ltm_profit 存在并计算扣非PE
        if 'ltm_profit' not in df.columns and not df_growth_profit.empty:
            ltm_profit_map = df_growth_profit[['symbol', 'ltm_profit']].copy()
            df = df.merge(ltm_profit_map, on='symbol', how='left')
        df = self._calc_deduct_pe(df)
        df = self._apply_deduct_pe_filter(df, criteria)
        if df.empty:
            return pd.DataFrame()

        # 9) 应用增速筛选（单增无营收条件）
        df = self._apply_profit_filter(df, criteria)
        if df.empty:
            return pd.DataFrame()

        # 10) 排序
        df = df.sort_values('peg', ascending=True)
        if criteria.top_n is not None:
            df = df.head(criteria.top_n)
        return df

    def select_single_growth_fast(self, criteria: GrowthCriteria, trade_date: str = None) -> pd.DataFrame:
        """
        单增选股（快速版）：仅基于 profit_sheet，跳过 forecast/express 合并逻辑。
        用于回测等批量场景，速度接近双增。
        """
        if trade_date is None:
            trade_date = self.get_latest_trade_date()
        logger.info(f"=== 单增选股(快速版) | 日期: {trade_date} ===")

        # 1) 基本面
        df_basic = self.fetch_fundamental_data(trade_date)
        df_basic = self._apply_market_cap_filter(df_basic, criteria)
        df_basic = self._apply_industry_exclude_filter(df_basic, criteria)
        if df_basic.empty:
            return pd.DataFrame()

        # 2) profit_sheet 季度数据（唯一数据源）
        df_growth = self.fetch_profit_sheet_quarterly(trade_date)
        if df_growth.empty:
            logger.warning("无 profit_sheet 季度数据")
            return pd.DataFrame()

        # 3) 合并
        df = df_basic.merge(df_growth, on='symbol', how='inner')
        logger.info(f"合并后: {len(df)} 只")

        # 4) 计算PEG
        df = self._calc_peg(df)

        # 5) 计算扣非PE并过滤
        df = self._calc_deduct_pe(df)
        df = self._apply_deduct_pe_filter(df, criteria)
        if df.empty:
            return pd.DataFrame()

        # 6) 应用增速筛选（单增无营收条件）
        df = self._apply_profit_filter(df, criteria)
        if df.empty:
            return pd.DataFrame()

        # 7) 排序
        df = df.sort_values('peg', ascending=True)
        if criteria.top_n is not None:
            df = df.head(criteria.top_n)
        return df

    # --- 辅助方法 ---
    def _apply_market_cap_filter(self, df: pd.DataFrame, criteria: GrowthCriteria) -> pd.DataFrame:
        """市值过滤（None表示不限制）"""
        if df.empty:
            return df
        mask = pd.Series(True, index=df.index)
        if criteria.min_market_cap is not None:
            mask &= df['market_cap'] >= criteria.min_market_cap
        if criteria.max_market_cap is not None:
            mask &= df['market_cap'] <= criteria.max_market_cap
        filtered = df[mask]
        cap_range = f"{criteria.min_market_cap if criteria.min_market_cap is not None else '无下限'}-{criteria.max_market_cap if criteria.max_market_cap is not None else '无上限'}"
        logger.info(f"市值过滤 ({cap_range}亿): {len(filtered)} 只")
        return filtered

    def _apply_industry_exclude_filter(self, df: pd.DataFrame, criteria: GrowthCriteria) -> pd.DataFrame:
        """排除指定行业的股票"""
        if df.empty or not criteria.exclude_industries:
            return df
        mask = pd.Series(True, index=df.index)
        for ind in criteria.exclude_industries:
            mask &= ~df['industry'].astype(str).str.contains(ind, na=False, regex=False)
        filtered = df[mask]
        logger.info(f"排除行业 {criteria.exclude_industries}: {len(filtered)}/{len(df)} 只")
        return filtered

    def _calc_peg(self, df: pd.DataFrame) -> pd.DataFrame:
        """计算 PEG = PE_TTM / (LTM扣非增速 × 100)"""
        df = df.copy()
        pe = pd.to_numeric(df['pe_ttm'], errors='coerce')
        ltm = pd.to_numeric(df['ltm_profit_growth'], errors='coerce')
        df['peg'] = np.where(
            (ltm.notna()) & (ltm > 0),
            pe / (ltm * 100),
            np.inf
        )
        return df

    def _calc_deduct_pe(self, df: pd.DataFrame) -> pd.DataFrame:
        """计算扣非PE = 市值(亿元) × 10000 / LTM扣非净利润(万元)"""
        df = df.copy()
        if 'ltm_profit' in df.columns:
            market_cap = pd.to_numeric(df['market_cap'], errors='coerce')
            ltm_profit = pd.to_numeric(df['ltm_profit'], errors='coerce')
            df['deduct_pe'] = np.where(
                (ltm_profit.notna()) & (ltm_profit > 0),
                market_cap * 10000 / ltm_profit,
                np.inf
            )
        else:
            df['deduct_pe'] = np.inf
        return df

    def _apply_deduct_pe_filter(self, df: pd.DataFrame, criteria: GrowthCriteria) -> pd.DataFrame:
        """扣非PE上限过滤"""
        before = len(df)
        df = df[df['deduct_pe'] < criteria.max_deduct_pe]
        logger.info(f"扣非PE < {criteria.max_deduct_pe}: {len(df)}/{before}")
        return df

    def _apply_growth_filters(self, df: pd.DataFrame, criteria: GrowthCriteria) -> pd.DataFrame:
        """双增：应用全部筛选条件"""
        # PEG
        before = len(df)
        df = df[df['peg'] < criteria.max_peg]
        logger.info(f"PEG < {criteria.max_peg}: {len(df)}/{before}")

        if df.empty:
            return df

        # 季度扣非增速
        before = len(df)
        df = df[df['quarter_profit_growth'] > criteria.min_profit_growth]
        logger.info(f"扣非增速 > {criteria.min_profit_growth*100:.0f}%: {len(df)}/{before}")

        if df.empty:
            return df

        # 季度营收增速
        before = len(df)
        df = df[df['quarter_revenue_growth'] > criteria.min_revenue_growth]
        logger.info(f"营收增速 > {criteria.min_revenue_growth*100:.0f}%: {len(df)}/{before}")

        if criteria.max_revenue_growth is not None and not df.empty:
            before = len(df)
            df = df[df['quarter_revenue_growth'] < criteria.max_revenue_growth]
            logger.info(f"营收增速 < {criteria.max_revenue_growth*100:.0f}%: {len(df)}/{before}")

        return df

    def _apply_profit_filter(self, df: pd.DataFrame, criteria: GrowthCriteria) -> pd.DataFrame:
        """单增：只应用PEG和扣非增速筛选"""
        # PEG
        before = len(df)
        df = df[df['peg'] < criteria.max_peg]
        logger.info(f"PEG < {criteria.max_peg}: {len(df)}/{before}")

        if df.empty:
            return df

        # 季度扣非增速
        before = len(df)
        df = df[df['quarter_profit_growth'] > criteria.min_profit_growth]
        logger.info(f"扣非增速 > {criteria.min_profit_growth*100:.0f}%: {len(df)}/{before}")

        return df

    def _merge_growth_data(self, df_forecast: pd.DataFrame, df_express: pd.DataFrame) -> pd.DataFrame:
        """
        合并 forecast 和 express 的增速数据。
        forecast 优先（更精确，因为是扣非数据），express 补充缺失的股票。
        """
        if df_forecast.empty and df_express.empty:
            return pd.DataFrame()

        if df_forecast.empty:
            # 只用 express，但 express 没有严格扣非，用 net_profit_yoy_change 作为近似
            df = df_express[['symbol', 'net_profit_yoy_change']].copy()
            df = df.rename(columns={'net_profit_yoy_change': 'quarter_profit_growth'})
            df['data_source'] = 'performance_express'
            return df

        if df_express.empty:
            return df_forecast[['symbol', 'quarter_profit_growth', 'data_source']].copy()

        # forecast 优先
        df_result = df_forecast[['symbol', 'quarter_profit_growth', 'data_source']].copy()
        forecast_symbols = set(df_result['symbol'])

        # express 补充 forecast 没有的股票
        df_express_new = df_express[~df_express['symbol'].isin(forecast_symbols)]
        if not df_express_new.empty:
            df_express_new = df_express_new[['symbol', 'net_profit_yoy_change']].copy()
            df_express_new = df_express_new.rename(columns={'net_profit_yoy_change': 'quarter_profit_growth'})
            df_express_new['data_source'] = 'performance_express'
            df_result = pd.concat([df_result, df_express_new], ignore_index=True)

        return df_result

    # -----------------------------------------------------------------------
    # 报告生成
    # -----------------------------------------------------------------------
    def generate_report(self, df: pd.DataFrame, criteria: GrowthCriteria,
                        output_file: Optional[str] = None):
        """生成选股报告"""
        report = []
        report.append("=" * 90)
        report.append(f"{'双增' if criteria.strategy == 'double' else '单增'}选股策略报告")
        report.append("=" * 90)
        report.append(f"生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report.append(f"策略类型: {criteria.strategy}")
        report.append(f"LTM扣非PEG: < {criteria.max_peg}")
        report.append(f"扣非PE: < {criteria.max_deduct_pe}")
        report.append(f"季度扣非增速: > {criteria.min_profit_growth*100:.0f}%")
        if criteria.strategy == 'double':
            line = f"季度营收增速: > {criteria.min_revenue_growth*100:.0f}%"
            if criteria.max_revenue_growth is not None:
                line += f", < {criteria.max_revenue_growth*100:.0f}%"
            report.append(line)
        min_cap_str = f"{criteria.min_market_cap}亿" if criteria.min_market_cap is not None else "无下限"
        max_cap_str = f"{criteria.max_market_cap}亿" if criteria.max_market_cap is not None else "无上限"
        report.append(f"市值范围: {min_cap_str}-{max_cap_str}")
        report.append("")

        if df.empty:
            report.append("⚠️  没有符合条件的股票")
        else:
            display_cols = [
                'symbol', 'name', 'industry', 'main_business', 'concepts',
                'market_cap', 'pe_ttm', 'deduct_pe', 'peg',
                'quarter_profit_growth', 'quarter_revenue_growth',
                'ltm_profit_growth', 'data_source'
            ]
            available_cols = [c for c in display_cols if c in df.columns]
            df_disp = df[available_cols].copy()

            # 格式化百分比
            for col in ['quarter_profit_growth', 'quarter_revenue_growth', 'ltm_profit_growth']:
                if col in df_disp.columns:
                    df_disp[col] = df_disp[col].apply(lambda x: f"{x*100:.2f}%" if pd.notna(x) else "N/A")

            if 'peg' in df_disp.columns:
                df_disp['peg'] = df_disp['peg'].apply(lambda x: f"{x:.3f}" if pd.notna(x) else "N/A")
            if 'market_cap' in df_disp.columns:
                df_disp['market_cap'] = df_disp['market_cap'].apply(lambda x: f"{x:.2f}" if pd.notna(x) else "N/A")
            if 'deduct_pe' in df_disp.columns:
                df_disp['deduct_pe'] = df_disp['deduct_pe'].apply(lambda x: f"{x:.2f}" if pd.notna(x) else "N/A")
            if 'pe_ttm' in df_disp.columns:
                df_disp['pe_ttm'] = df_disp['pe_ttm'].apply(lambda x: f"{x:.2f}" if pd.notna(x) else "N/A")

            top_info = f"（Top {criteria.top_n}）" if criteria.top_n is not None else ""
            report.append(f"共选出 {len(df)} 只股票{top_info}：")
            report.append("")
            report.append(df_disp.to_string(index=False))

        report.append("")
        report.append("=" * 90)
        report_text = "\n".join(report)
        print(report_text)

        if output_file:
            with open(output_file, 'w', encoding='utf-8') as f:
                f.write(report_text)
            logger.info(f"报告已保存: {output_file}")


# ---------------------------------------------------------------------------
# 命令行入口
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(
        description='双增/单增选股器',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  # 双增选股（默认）
  python double_single_growth_selector.py --strategy double

  # 单增选股
  python double_single_growth_selector.py --strategy single

  # 自定义参数
  python double_single_growth_selector.py --strategy double --peg 0.8 --profit-growth 0.6 --revenue-growth 0.15

  # 营收增速限制在10%-20%之间
  python double_single_growth_selector.py --strategy double --revenue-growth 0.10 --max-revenue-growth 0.20
        """
    )
    parser.add_argument('--strategy', type=str, default='double', choices=['double', 'single'],
                        help='策略类型: double(双增), single(单增)')
    parser.add_argument('--peg', type=float, default=1.0, help='LTM扣非PEG上限，默认1.0')
    parser.add_argument('--profit-growth', type=float, default=0.50,
                        help='季度扣非增速下限，默认0.50（50%%）')
    parser.add_argument('--revenue-growth', type=float, default=0.10,
                        help='季度营收增速下限（仅双增），默认0.10（10%%）')
    parser.add_argument('--max-revenue-growth', type=float, default=None,
                        help='季度营收增速上限（仅双增），默认无上限')
    parser.add_argument('--min-cap', type=float, default=None, help='最小市值（亿元），默认不限制')
    parser.add_argument('--max-cap', type=float, default=None, help='最大市值（亿元），默认不限制')
    parser.add_argument('--max-deduct-pe', type=float, default=50.0, help='扣非PE上限，默认50')
    parser.add_argument('--top', type=int, default=None, help='返回前N只，默认不限制')
    parser.add_argument('--date', type=str, default=None, help='指定交易日期 YYYY-MM-DD')
    parser.add_argument('--output', type=str, default=None, help='输出文件名前缀')
    parser.add_argument('--enrich-concepts', action='store_true',
                        help='通过 pywencai 获取所属概念（较慢，按需开启）')
    parser.add_argument('--filter-concepts', type=str, default=None,
                        help='概念关键词过滤，逗号分隔（如"锂电池,新能源"）')
    parser.add_argument('--filter-industry', type=str, default=None,
                        help='行业关键词过滤，逗号分隔')
    parser.add_argument('--filter-business', type=str, default=None,
                        help='主营业务关键词过滤，逗号分隔')

    args = parser.parse_args()

    db_config = load_db_config()
    selector = DoubleSingleGrowthSelector(db_config)

    try:
        selector.connect()

        criteria = GrowthCriteria(
            strategy=args.strategy,
            max_peg=args.peg,
            min_profit_growth=args.profit_growth,
            min_revenue_growth=args.revenue_growth,
            max_revenue_growth=args.max_revenue_growth,
            min_market_cap=args.min_cap,
            max_market_cap=args.max_cap,
            max_deduct_pe=args.max_deduct_pe,
            top_n=args.top
        )

        # 执行选股
        results = selector.select(criteria, trade_date=args.date,
                                   enrich_concepts=args.enrich_concepts)

        # 分类筛选
        if args.filter_concepts and not results.empty:
            keywords = [k.strip() for k in args.filter_concepts.split(',')]
            results = selector.filter_by_concepts(results, include=keywords)
        if args.filter_industry and not results.empty:
            keywords = [k.strip() for k in args.filter_industry.split(',')]
            results = selector.filter_by_industry(results, include=keywords)
        if args.filter_business and not results.empty:
            keywords = [k.strip() for k in args.filter_business.split(',')]
            results = selector.filter_by_main_business(results, include=keywords)

        # 分类统计
        if not results.empty:
            print("\n" + "=" * 60)
            print("📊 分类统计")
            print("=" * 60)
            if 'industry' in results.columns:
                industry_summary = selector.classify_summary(results, by='industry', top_k=15)
                if not industry_summary.empty:
                    print("\n【行业分布 Top 15】")
                    print(industry_summary.to_string(index=False))
            if 'concepts' in results.columns:
                concept_summary = selector.classify_summary(results, by='concepts', top_k=15)
                if not concept_summary.empty:
                    print("\n【所属概念分布 Top 15】")
                    print(concept_summary.to_string(index=False))

        # 输出
        prefix = args.output or f"{args.strategy}_growth_selection"
        report_file = f"{prefix}_report.txt"
        csv_file = f"{prefix}.csv"

        selector.generate_report(results, criteria, output_file=report_file)
        results.to_csv(csv_file, index=False, encoding='utf-8-sig')

        print(f"\n✅ 报告: {report_file}")
        print(f"✅ CSV:  {csv_file}")

    except Exception as e:
        logger.error(f"选股失败: {e}", exc_info=True)
        raise
    finally:
        selector.disconnect()


if __name__ == '__main__':
    main()
