#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
炒作选股程序 - 基于骑行客"女神的三围"量化策略

核心逻辑（回归分析结果）：
1. 业绩增速（扣非净利）：正相关(0.79)，越高越好
2. 估值（扣非市盈率PE）：正相关(0.75)，估值越高反而越好
3. 市值大小：负相关(0.65)，市值越小越好
4. 此前一年涨幅：负相关(0.72)，此前一年涨幅越小越好

最强爆发力组合 = 小市值 + 此前一年的失意者 + 高业绩增速
"""

import pandas as pd
import numpy as np
import psycopg2
import pywencai
from psycopg2.extras import RealDictCursor
from datetime import datetime, timedelta
from dataclasses import dataclass
from typing import List, Optional, Dict, Tuple, Set, Union
import yaml
import logging
import re
import argparse
import json

# 尝试导入 mem0，如果不存在则忽略
try:
    from mem0 import Memory
    MEM0_AVAILABLE = True
except ImportError:
    MEM0_AVAILABLE = False
    logger = logging.getLogger(__name__)
    logger.warning("mem0 未安装，会话记忆功能不可用")

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


@dataclass
class SelectionCriteria:
    """选股条件配置"""
    # 概念板块筛选（问财查询，支持单个或多个概念）
    # 单概念: concept_sectors="煤炭"
    # 多概念: concept_sectors=["煤炭", "煤化工", "煤电"]
    concept_sectors: Optional[Union[str, List[str]]] = None  # None表示不限制
    
    # 市值限制（亿元）
    max_market_cap: float = 500  # 默认最大500亿，小市值策略可设为100
    min_market_cap: float = 10   # 最小10亿，过滤掉流动性过差的
    
    # 业绩增速阈值
    min_qoq_growth: float = 0.20      # 季度环比最低20%
    min_yoy_growth: float = 0.30      # 同比增速最低30%
    
    # 此前一年涨幅阈值（负相关，越小越好）
    max_past_year_return: float = 0.30  # 过去一年涨幅不超过30%
    
    # 评分权重（骑行客"女神的三围"策略优化版）
    # 小市值 + 此前一年的失意者 + 高业绩增速
    weight_qoq: float = 0.15      # 季度环比增速权重（短期爆发）
    weight_yoy: float = 0.15      # 同比增速权重（中期增长）
    weight_pe: float = 0.15       # PE估值权重（炒作容忍度）
    weight_market_cap: float = 0.25  # 市值权重（小盘弹性，核心因子）
    weight_past_return: float = 0.30   # 过去一年涨幅权重（低位优势，核心因子）
    
    # 评分方向（1=正向，-1=负向）
    direction_pe: int = 1         # PE正向（越高越好）
    direction_market_cap: int = -1   # 市值负向（越小越好）
    direction_past_return: int = -1  # 涨幅负向（越小越好）


class SessionMemory:
    """选股会话记忆管理 - 基于 mem0"""
    
    def __init__(self, user_id: str = "quant_user"):
        self.user_id = user_id
        self.memory = None
        self.session_id = None
        
        if MEM0_AVAILABLE:
            try:
                # 配置 mem0 使用本地存储
                config = {
                    "vector_store": {
                        "provider": "qdrant",
                        "config": {
                            "path": "/data/akshare/mem0_db",
                        }
                    }
                }
                self.memory = Memory.from_config(config)
                logger.info("mem0 记忆系统初始化成功")
            except Exception as e:
                logger.warning(f"mem0 初始化失败: {e}，记忆功能不可用")
                self.memory = None
        else:
            logger.info("mem0 未安装，跳过记忆功能")
    
    def start_session(self, criteria: SelectionCriteria) -> str:
        """开始新的选股会话"""
        self.session_id = f"选股_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        # 记录选股条件
        concept_str = self._format_concepts(criteria.concept_sectors)
        condition = f"选股条件: 概念={concept_str}, 市值{criteria.min_market_cap}-{criteria.max_market_cap}亿, 季度环比≥{criteria.min_qoq_growth*100:.0f}%, 同比≥{criteria.min_yoy_growth*100:.0f}%"
        
        self.save(f"开始选股会话 [{self.session_id}]: {condition}", metadata={"type": "session_start", "session_id": self.session_id})
        logger.info(f"选股会话已记录: {self.session_id}")
        return self.session_id
    
    def save(self, message: str, metadata: dict = None):
        """保存记忆"""
        if self.memory:
            try:
                meta = metadata or {}
                meta["session_id"] = self.session_id
                meta["timestamp"] = datetime.now().isoformat()
                self.memory.add(message, user_id=self.user_id, metadata=meta)
            except Exception as e:
                logger.debug(f"保存记忆失败: {e}")
    
    def save_results(self, df: pd.DataFrame, top_n: int = 20):
        """保存选股结果"""
        if df.empty or not self.memory:
            return
        
        # 提取 TopN 股票代码和名称
        top_stocks = []
        for _, row in df.head(top_n).iterrows():
            stock_info = f"{row.get('ts_code', 'N/A')} {row.get('name', 'N/A')}"
            top_stocks.append(stock_info)
        
        result_str = f"选股结果 [{self.session_id}]: " + "; ".join(top_stocks)
        self.save(result_str, metadata={"type": "selection_result", "session_id": self.session_id, "count": len(df)})
    
    def find_similar_session(self, concepts: List[str]) -> Optional[Dict]:
        """查找相似条件的选股历史"""
        if not self.memory or not concepts:
            return None
        
        try:
            # 搜索相关记忆
            query = f"选股 {concepts} 概念"
            results = self.memory.search(query, user_id=self.user_id, limit=5)
            
            if not results:
                return None
            
            # 找最近的选股会话
            for r in results:
                meta = r.get("metadata", {})
                if meta.get("type") == "session_start":
                    return {
                        "session_id": meta.get("session_id"),
                        "memory": r.get("memory"),
                        "timestamp": meta.get("timestamp")
                    }
            return None
        except Exception as e:
            logger.debug(f"搜索记忆失败: {e}")
            return None
    
    def get_session_history(self, session_id: str) -> List[str]:
        """获取指定会话的所有记忆"""
        if not self.memory:
            return []
        
        try:
            # 获取所有记忆并过滤
            all_memories = self.memory.get_all(user_id=self.user_id)
            session_memories = []
            for mem in all_memories:
                if mem.get("metadata", {}).get("session_id") == session_id:
                    session_memories.append(mem.get("memory", ""))
            return session_memories
        except Exception as e:
            logger.debug(f"获取会话历史失败: {e}")
            return []
    
    @staticmethod
    def _format_concepts(concepts) -> str:
        """格式化概念列表"""
        if not concepts:
            return "全市场"
        if isinstance(concepts, str):
            return concepts
        return ",".join(concepts)


class ChaoZuoSelector:
    """炒作选股器"""
    
    def __init__(self, db_config: Dict):
        self.db_config = db_config
        self.conn = None
        
    def connect(self):
        """连接数据库"""
        self.conn = psycopg2.connect(**self.db_config)
        logger.info("数据库连接成功")
        
    def disconnect(self):
        """断开连接"""
        if self.conn:
            self.conn.close()
            logger.info("数据库连接已关闭")
    
    def get_concept_sector_stocks(self, concept_names: Union[str, List[str]]) -> Set[str]:
        """
        使用问财获取概念板块的股票列表（支持单个或多个概念）
        
        Args:
            concept_names: 概念板块名称，如 "煤炭" 或 ["煤炭", "煤化工", "煤电"]
            
        Returns:
            股票代码集合（6位数字格式，如 000001, 600000）
        """
        # 统一转换为列表
        if isinstance(concept_names, str):
            concept_list = [concept_names]
        else:
            concept_list = concept_names
        
        all_stocks = set()
        
        for concept_name in concept_list:
            logger.info(f"使用问财获取【{concept_name}】概念板块股票...")
            
            try:
                # 使用问财查询概念板块
                res = pywencai.get(query=concept_name, sort_key='', sort_order='asc')
                
                if res is None or res.empty:
                    logger.warning(f"问财返回空数据，概念【{concept_name}】可能没有结果")
                    continue
                
                # 提取股票代码
                stock_codes = set()
                
                # 问财返回的列可能包含 'code', '股票代码', 'ts_code' 等
                code_col = None
                for col in ['code', '股票代码', 'ts_code', 'symbol']:
                    if col in res.columns:
                        code_col = col
                        break
                
                if code_col is None:
                    logger.warning(f"无法找到股票代码列，可用列: {list(res.columns)}")
                    continue
                
                for code in res[code_col]:
                    if pd.isna(code):
                        continue
                    # 统一转换为6位数字格式
                    code_str = str(code).strip()
                    # 处理带后缀的格式（如 000001.SZ）
                    if '.' in code_str:
                        code_str = code_str.split('.')[0]
                    # 只保留6位数字
                    if re.match(r'^\d{6}$', code_str):
                        stock_codes.add(code_str)
                
                logger.info(f"问财返回【{concept_name}】概念板块共 {len(stock_codes)} 只股票")
                all_stocks.update(stock_codes)
                
            except Exception as e:
                logger.error(f"问财查询【{concept_name}】失败: {e}")
                continue
        
        logger.info(f"所有概念板块合并后共 {len(all_stocks)} 只独特股票")
        return all_stocks
    
    def filter_by_concept_sector(self, df: pd.DataFrame, concept_names: Optional[Union[str, List[str]]]) -> pd.DataFrame:
        """
        根据概念板块过滤股票（支持单个或多个概念）
        
        Args:
            df: 股票DataFrame
            concept_names: 概念板块名称，可以是字符串或列表，None则不过滤
            
        Returns:
            过滤后的DataFrame
        """
        if not concept_names:
            return df
        
        # 获取概念板块股票
        concept_stocks = self.get_concept_sector_stocks(concept_names)
        
        if not concept_stocks:
            concept_str = concept_names if isinstance(concept_names, str) else ', '.join(concept_names)
            logger.warning(f"概念板块【{concept_str}】未获取到股票，返回空结果")
            return df.iloc[0:0]  # 返回空DataFrame
        
        # 提取df中的security_code用于匹配（6位数字）
        if 'security_code' in df.columns:
            df_filtered = df[df['security_code'].isin(concept_stocks)]
        elif 'ts_code' in df.columns:
            # 从ts_code提取6位代码
            df['temp_code'] = df['ts_code'].str.split('.').str[0]
            df_filtered = df[df['temp_code'].isin(concept_stocks)]
            df_filtered = df_filtered.drop(columns=['temp_code'])
        else:
            logger.warning("DataFrame中没有股票代码列，无法过滤概念板块")
            return df
        
        logger.info(f"概念板块过滤后剩余 {len(df_filtered)} 只")
        return df_filtered
    
    def get_latest_trade_date(self) -> str:
        """获取最新交易日期"""
        query = """
        SELECT MAX(trade_date) as latest_date FROM daily_basic
        """
        with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query)
            result = cur.fetchone()
            return result['latest_date'].strftime('%Y-%m-%d') if result['latest_date'] else None
    
    def fetch_fundamental_data(self, trade_date: str) -> pd.DataFrame:
        """
        获取基本面数据
        
        需要的字段：
        - ts_code: 股票代码
        - name: 股票名称
        - market_cap: 总市值（亿元）
        - pe_ttm: 市盈率TTM
        """
        query = """
        SELECT 
            d.ts_code,
            d.security_code,
            s.name,
            d.total_mv / 10000 as market_cap,  -- 总市值转换为亿元
            d.pe_ttm,
            d.pb,
            d.turnover_rate,
            d.volume_ratio
        FROM daily_basic d
        LEFT JOIN stock_basic s ON d.ts_code = s.ts_code
        WHERE d.trade_date = %s
          AND d.pe_ttm > 0  -- 过滤亏损股
          AND d.total_mv IS NOT NULL
        ORDER BY d.total_mv ASC
        """
        
        with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, (trade_date,))
            rows = cur.fetchall()
            
        df = pd.DataFrame(rows)
        logger.info(f"获取到 {len(df)} 只股票的基本面数据")
        return df
    
    def fetch_profit_growth_data(self, trade_date: str) -> pd.DataFrame:
        """
        获取业绩增长数据（使用扣非净利润计算季度环比和同比增速）
        
        数据源优先级：
        1. profit_sheet（正式财报）- deduct_parent_netprofit
        2. performance_express（业绩快报）- deduct_profit 或扣非相关字段
        3. performance_forecast（业绩预告）- profit_deduct_min/max 取中值
        
        计算：
        - 季度环比 = (本期 - 上期) / |上期|
        - 同比增速 = (本期 - 去年同期) / |去年同期|
        """
        
        # 步骤1：从 profit_sheet 获取扣非净利润
        logger.info("[业绩数据] 从 profit_sheet 获取扣非净利润...")
        df_profit = self._fetch_from_profit_sheet(trade_date)
        
        # 步骤2：检查是否需要补充快报/预告数据
        # 获取最新报告日期
        latest_date = self._get_latest_report_date(trade_date)
        logger.info(f"[业绩数据] profit_sheet 最新报告期: {latest_date}")
        
        # 如果最新报告期早于当年最新季度，尝试获取快报/预告
        if latest_date:
            latest_year = int(str(latest_date)[:4])
            trade_year = int(str(trade_date)[:4])
            
            # 如果profit_sheet数据不是最新的，尝试获取快报/预告
            if latest_year < trade_year or (latest_year == trade_year and self._is_old_report(latest_date, trade_date)):
                logger.info("[业绩数据] 尝试从快报/预告表补充最新业绩...")
                df_express = self._fetch_from_performance_express(trade_date)
                df_forecast = self._fetch_from_performance_forecast(trade_date)
                
                # 合并数据：profit_sheet 为主，快报/预告补充缺失的
                df_profit = self._merge_performance_data(df_profit, df_express, df_forecast)
        
        return df_profit
    
    def _get_latest_report_date(self, trade_date: str) -> Optional[str]:
        """获取 profit_sheet 最新报告期"""
        try:
            query = """
            SELECT MAX(report_date) as max_date 
            FROM profit_sheet 
            WHERE report_date <= %s 
              AND deduct_parent_netprofit IS NOT NULL
            """
            with self.conn.cursor() as cur:
                cur.execute(query, (trade_date,))
                result = cur.fetchone()
                # report_date 在数据库中是字符串格式(如'20251231')，直接返回
                if result and result[0]:
                    max_date = result[0]
                    # 如果是 datetime/date 对象，转字符串
                    if hasattr(max_date, 'strftime'):
                        return max_date.strftime('%Y%m%d')
                    # 否则直接返回字符串
                    return str(max_date)
                return None
        except Exception as e:
            logger.warning(f"获取最新报告期失败: {e}")
            return None
    
    def _is_old_report(self, report_date: str, trade_date: str) -> bool:
        """判断报告期是否太旧（超过4个月）"""
        try:
            from datetime import datetime
            r_date = datetime.strptime(str(report_date), '%Y%m%d')
            t_date = datetime.strptime(str(trade_date), '%Y-%m-%d')
            # 如果报告期距交易日期超过120天，认为是旧数据
            return (t_date - r_date).days > 120
        except:
            return False
    
    def _fetch_from_profit_sheet(self, trade_date: str) -> pd.DataFrame:
        """从 profit_sheet 获取扣非净利润及增速（单季度计算）"""
        query = """
        WITH quarterly_data AS (
            SELECT 
                symbol,
                deduct_parent_netprofit,
                report_date,
                ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY report_date DESC) as rn_desc
            FROM profit_sheet
            WHERE report_date <= %s
              AND deduct_parent_netprofit IS NOT NULL
        ),
        -- 今年Q3单季度 = Q3累计 - Q2累计 (rn=1 - rn=2)
        current_q3 AS (
            SELECT 
                a.symbol, 
                a.deduct_parent_netprofit - b.deduct_parent_netprofit as current_q3_profit
            FROM quarterly_data a
            JOIN quarterly_data b ON a.symbol = b.symbol
            WHERE a.rn_desc = 1 AND b.rn_desc = 2
        ),
        -- 今年Q2累计 (用于环比计算)
        current_q2 AS (
            SELECT symbol, deduct_parent_netprofit as current_q2_profit
            FROM quarterly_data WHERE rn_desc = 2
        ),
        -- 去年Q3单季度 = 去年Q3累计 - 去年Q2累计 (rn=5 - rn=6)
        yoy_q3 AS (
            SELECT 
                a.symbol, 
                a.deduct_parent_netprofit - b.deduct_parent_netprofit as yoy_q3_profit
            FROM quarterly_data a
            JOIN quarterly_data b ON a.symbol = b.symbol
            WHERE a.rn_desc = 5 AND b.rn_desc = 6
        ),
        -- 最新报告期日期
        latest_date AS (
            SELECT symbol, report_date as current_date
            FROM quarterly_data WHERE rn_desc = 1
        )
        SELECT 
            LEFT(c.symbol, 6) as symbol,
            c.current_q3_profit as current_profit,
            d.current_date,
            q2.current_q2_profit as prev_profit,
            y.yoy_q3_profit as yoy_profit,
            -- 季度环比：(今年Q3单季度 - 今年Q2累计) / |今年Q2累计|
            CASE WHEN q2.current_q2_profit IS NULL OR q2.current_q2_profit = 0 THEN NULL
                 ELSE (c.current_q3_profit - q2.current_q2_profit) / ABS(q2.current_q2_profit) END as qoq_growth,
            -- 同比增速：(今年Q3单季度 - 去年Q3单季度) / |去年Q3单季度|
            CASE WHEN y.yoy_q3_profit IS NULL OR y.yoy_q3_profit = 0 THEN NULL
                 ELSE (c.current_q3_profit - y.yoy_q3_profit) / ABS(y.yoy_q3_profit) END as yoy_growth,
            'profit_sheet' as data_source
        FROM current_q3 c
        LEFT JOIN current_q2 q2 ON c.symbol = q2.symbol
        LEFT JOIN yoy_q3 y ON c.symbol = y.symbol
        LEFT JOIN latest_date d ON c.symbol = d.symbol
        """
        
        try:
            with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(query, (trade_date,))
                df = pd.DataFrame(cur.fetchall())
                # 转换日期格式为字符串以便比较
                if 'current_date' in df.columns and not df.empty:
                    df['current_date'] = df['current_date'].astype(str)
                logger.info(f"[profit_sheet] 获取到 {len(df)} 只股票")
                return df
        except Exception as e:
            logger.error(f"[profit_sheet] 查询失败: {e}")
            return pd.DataFrame()
    
    def _fetch_from_performance_express(self, trade_date: str) -> pd.DataFrame:
        """从 performance_express（业绩快报）获取扣非净利润
        
        注：经查询，该表没有扣非净利润字段，此方法弃用，返回空DataFrame
        """
        logger.debug("[performance_express] 业绩快报表无扣非净利润字段，跳过")
        return pd.DataFrame()
    
    def _fetch_from_performance_forecast(self, trade_date: str) -> pd.DataFrame:
        """从 performance_forecast（业绩预告）获取扣非净利润及同比增速
        
        重要：
        1. performance_forecast 存储的是全年累计值（单位：元），需要除以10000转为万元
        2. Q4单季度 = 预告全年累计 - profit_sheet三季度累计
        3. 去年Q4单季度 = profit_sheet去年年报 - profit_sheet去年Q3（不用预告数据）
        """
        try:
            # 步骤1：获取最新预告数据（全年累计，单位：元）
            current_year = trade_date[:4]  # '2026'
            last_year = str(int(current_year) - 1)  # '2025'
            
            query_current = """
            SELECT 
                symbol,
                forecast_value / 10000.0 as full_year_profit,  -- 转为万元
                report_period,
                announce_date,
                ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY announce_date DESC) as rn
            FROM performance_forecast
            WHERE report_period LIKE %s
              AND forecast_indicator = '扣除非经常性损益后的净利润'
              AND forecast_value IS NOT NULL
            """
            
            with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
                # 获取今年预告 (2025年报)
                cur.execute(query_current, (f'{last_year}%',))
                df_current = pd.DataFrame(cur.fetchall())
                df_current = df_current[df_current['rn'] == 1].drop('rn', axis=1)
                
                if df_current.empty:
                    logger.info("[performance_forecast] 无今年预告数据")
                    return pd.DataFrame()
                
                # 统一symbol格式（去掉后缀）
                df_current['symbol'] = df_current['symbol'].str[:6]
                
                # 获取候选股票列表
                symbols_list = df_current['symbol'].tolist()
                
                # 获取今年Q3累计（9月30日）- 用于计算今年Q4
                query_q3 = """
                SELECT 
                    LEFT(symbol, 6) as symbol,
                    deduct_parent_netprofit::float as q3_cumulative
                FROM profit_sheet
                WHERE report_date LIKE %s
                  AND deduct_parent_netprofit IS NOT NULL
                  AND LEFT(symbol, 6) = ANY(%s)
                """
                q3_date = f'{last_year}-09-30%'
                cur.execute(query_q3, (q3_date, symbols_list))
                df_q3 = pd.DataFrame(cur.fetchall())
                
                # 获取去年Q4单季度（用profit_sheet的2024年报 - 2024Q3）
                query_q4_last = """
                WITH last_year_data AS (
                    SELECT 
                        LEFT(symbol, 6) as symbol,
                        MAX(CASE WHEN report_date LIKE %s THEN deduct_parent_netprofit::float END) as fy_2024,
                        MAX(CASE WHEN report_date LIKE %s THEN deduct_parent_netprofit::float END) as q3_2024
                    FROM profit_sheet
                    WHERE (report_date LIKE %s OR report_date LIKE %s)
                      AND deduct_parent_netprofit IS NOT NULL
                      AND LEFT(symbol, 6) = ANY(%s)
                    GROUP BY LEFT(symbol, 6)
                )
                SELECT 
                    symbol,
                    (fy_2024 - q3_2024) as ly_q4_single
                FROM last_year_data
                WHERE fy_2024 IS NOT NULL AND q3_2024 IS NOT NULL
                """
                fy_pattern = f'{int(last_year)-1}-12-31%'  # 2024年报
                q3_pattern = f'{int(last_year)-1}-09-30%'  # 2024Q3
                cur.execute(query_q4_last, (fy_pattern, q3_pattern, fy_pattern, q3_pattern, symbols_list))
                df_q4_last = pd.DataFrame(cur.fetchall())
            
            # 步骤2：合并计算
            df = df_current.copy()
            
            # 合并今年Q3
            if not df_q3.empty:
                df = df.merge(df_q3, on='symbol', how='left')
            else:
                df['q3_cumulative'] = None
            
            # 合并去年Q4（来自profit_sheet）
            if not df_q4_last.empty:
                df = df.merge(df_q4_last, on='symbol', how='left')
            else:
                df['ly_q4_single'] = None
            
            # 计算今年Q4单季度（万元 - 万元）
            df['q4_single'] = df['full_year_profit'].astype(float) - df['q3_cumulative'].astype(float)
            
            # 计算同比增速
            def calc_yoy(row):
                if pd.notna(row['ly_q4_single']) and row['ly_q4_single'] != 0:
                    return (row['q4_single'] - row['ly_q4_single']) / abs(row['ly_q4_single'])
                return None
            
            df['yoy_growth'] = df.apply(calc_yoy, axis=1)
            
            # 格式化输出
            df['current_profit'] = df['q4_single']
            df['data_source'] = 'performance_forecast'
            
            # 打印样例
            sample = df[df['symbol'].isin(['002530', '300921'])][['symbol', 'full_year_profit', 'q3_cumulative', 'q4_single', 'ly_q4_single', 'yoy_growth']]
            if not sample.empty:
                logger.info(f"[performance_forecast] 样例数据: {sample.to_string(index=False)}")
                logger.info(f"[performance_forecast] 样例数据: {sample.to_string(index=False)}")
            
            yoy_count = df['yoy_growth'].notna().sum()
            logger.info(f"[performance_forecast] 获取到 {len(df)} 只股票，其中 {yoy_count} 只有效Q4单季度同比增速")
            
            return df[['symbol', 'full_year_profit', 'current_profit', 'report_period', 
                       'data_source', 'q3_cumulative', 'yoy_growth']]
            
        except Exception as e:
            logger.warning(f"[performance_forecast] 查询失败: {e}")
            import traceback
            logger.debug(traceback.format_exc())
            return pd.DataFrame()

    def _merge_performance_data(self, df_profit: pd.DataFrame, 
                                df_express: pd.DataFrame, 
                                df_forecast: pd.DataFrame) -> pd.DataFrame:
        """合并多数据源，profit_sheet 为主，快报/预告补充或覆盖
        
        关键修复：对于预告数据，如果报告期比 profit_sheet 新，则覆盖并重新计算同比增速
        """
        if df_profit.empty:
            logger.warning("[合并] profit_sheet 为空，尝试使用快报/预告数据")
            df_result = pd.concat([df_express, df_forecast], ignore_index=True)
            df_result = df_result.drop_duplicates(subset=['symbol'], keep='first')
        else:
            df_result = df_profit.copy()
            
            # 补充快报数据（profit_sheet 中没有的股票）
            if not df_express.empty:
                existing_symbols = set(df_result['symbol'])
                df_express_new = df_express[~df_express['symbol'].isin(existing_symbols)]
                if not df_express_new.empty:
                    logger.info(f"[合并] 从快报补充 {len(df_express_new)} 只股票")
                    df_result = pd.concat([df_result, df_express_new], ignore_index=True)
            
            # 处理预告数据：如果预告的报告期比 profit_sheet 新，则覆盖
            if not df_forecast.empty:
                # 获取 profit_sheet 中各股票的最新报告期
                profit_max_dates = df_profit.groupby('symbol')['current_date'].max() if 'current_date' in df_profit.columns else pd.Series()
                
                stocks_to_update = []
                stocks_to_add = []
                
                for _, row in df_forecast.iterrows():
                    symbol = row['symbol']
                    forecast_period = row.get('report_period', '')
                    
                    if symbol in profit_max_dates.index:
                        # 比较报告期，如果预告更新则覆盖
                        profit_period = str(profit_max_dates[symbol])
                        if str(forecast_period) > str(profit_period):
                            stocks_to_update.append(symbol)
                    else:
                        # profit_sheet 中没有，新增
                        stocks_to_add.append(symbol)
                
                # 覆盖更新的股票
                if stocks_to_update:
                    logger.info(f"[合并] 用预告数据覆盖 {len(stocks_to_update)} 只股票的旧财报数据")
                    # 先保存profit_sheet的环比数据
                    qoq_data = df_result[df_result['symbol'].isin(stocks_to_update)][['symbol', 'qoq_growth']].copy()
                    # 删除旧数据
                    df_result = df_result[~df_result['symbol'].isin(stocks_to_update)]
                    # 添加新数据
                    df_forecast_update = df_forecast[df_forecast['symbol'].isin(stocks_to_update)].copy()
                    # 合并保存的环比数据（预告数据没有环比）
                    if not qoq_data.empty:
                        df_forecast_update = df_forecast_update.merge(qoq_data, on='symbol', how='left')
                    df_result = pd.concat([df_result, df_forecast_update], ignore_index=True)
                
                # 添加全新的股票
                if stocks_to_add:
                    logger.info(f"[合并] 从预告补充 {len(stocks_to_add)} 只新股票")
                    df_forecast_new = df_forecast[df_forecast['symbol'].isin(stocks_to_add)].copy()
                    df_result = pd.concat([df_result, df_forecast_new], ignore_index=True)
        
        # 最终处理：不填充为0，保持NaN
        if 'qoq_growth' not in df_result.columns:
            df_result['qoq_growth'] = None
        if 'yoy_growth' not in df_result.columns:
            df_result['yoy_growth'] = None
        
        # 最终去重（保险起见）
        before_dedup = len(df_result)
        df_result = df_result.drop_duplicates(subset=['symbol'], keep='last')
        after_dedup = len(df_result)
        if before_dedup != after_dedup:
            logger.info(f"[合并] 去重后: {after_dedup} 只股票 (去重 {before_dedup - after_dedup} 只)")
        
        logger.info(f"[合并] 最终业绩数据: {len(df_result)} 只股票")
        return df_result
    
    def fetch_past_year_return(self, trade_date: str) -> pd.DataFrame:
        """获取过去一年涨幅（修复：使用 symbol 而非 ts_code，返回6位代码）"""
        date_obj = datetime.strptime(trade_date, '%Y-%m-%d')
        one_year_ago = (date_obj - timedelta(days=365)).strftime('%Y-%m-%d')
        
        query = """
        WITH current_price AS (
            SELECT symbol, close as current_close
            FROM stock_history
            WHERE trade_date = %s
              AND adjust_type = 'hfq'  -- 后复权
        ),
        past_price AS (
            SELECT DISTINCT ON (symbol) 
                symbol, close as past_close
            FROM stock_history
            WHERE trade_date <= %s
              AND adjust_type = 'hfq'
            ORDER BY symbol, trade_date DESC
        )
        SELECT 
            LEFT(c.symbol, 6) as symbol,  -- 转换为6位代码
            (c.current_close - p.past_close) / p.past_close as past_year_return
        FROM current_price c
        JOIN past_price p ON c.symbol = p.symbol
        WHERE p.past_close > 0
        """
        
        try:
            with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(query, (trade_date, one_year_ago))
                rows = cur.fetchall()
                df = pd.DataFrame(rows)
                logger.info(f"获取过去一年涨幅数据：{len(df)} 条")
                return df
        except Exception as e:
            logger.error(f"获取过去一年涨幅失败: {e}")
            return pd.DataFrame()
    
    def calculate_scores(self, df: pd.DataFrame, criteria: SelectionCriteria) -> pd.DataFrame:
        """
        计算各因子得分并加权汇总
        
        评分方法：在每个因子内做标准化排名得分（0-100分）
        """
        result = df.copy()
        
        # 1. 季度环比增速得分（正向，越高越好）
        if 'qoq_growth' in result.columns and result['qoq_growth'].notna().any():
            result['score_qoq'] = self._normalize_score(result['qoq_growth'], ascending=True)
        else:
            result['score_qoq'] = 50  # 默认值
            
        # 2. 同比增速得分（正向，越高越好）- 改名了 ltm_growth -> yoy_growth
        if 'yoy_growth' in result.columns and result['yoy_growth'].notna().any():
            result['score_yoy'] = self._normalize_score(result['yoy_growth'], ascending=True)
        else:
            result['score_yoy'] = 50  # 默认值
        
        # 3. PE估值得分（正向，越高越好 - 反映炒作容忍度）
        if 'pe_ttm' in result.columns:
            # 过滤极端值
            pe_filtered = result['pe_ttm'].clip(upper=200)
            result['score_pe'] = self._normalize_score(pe_filtered, ascending=True)
        else:
            result['score_pe'] = 50
            
        # 4. 市值得分（负向，越小越好）
        if 'market_cap' in result.columns:
            result['score_market_cap'] = self._normalize_score(result['market_cap'], ascending=False)
        else:
            result['score_market_cap'] = 50
            
        # 5. 过去一年涨幅得分（负向，越小越好）
        if 'past_year_return' in result.columns and result['past_year_return'].notna().any():
            result['score_past_return'] = self._normalize_score(result['past_year_return'], ascending=False)
        else:
            result['score_past_return'] = 50
        
        # 计算加权总分（使用 yoy_growth 而非 ltm_growth）
        result['total_score'] = (
            result['score_qoq'] * criteria.weight_qoq +
            result['score_yoy'] * criteria.weight_yoy +
            result['score_pe'] * criteria.weight_pe +
            result['score_market_cap'] * criteria.weight_market_cap +
            result['score_past_return'] * criteria.weight_past_return
        )
        
        return result
    
    def _normalize_score(self, series: pd.Series, ascending: bool = True) -> pd.Series:
        """
        将数据标准化为0-100分
        
        Args:
            series: 输入数据
            ascending: True表示越大越好，False表示越小越好
        """
        # 处理缺失值
        s = series.fillna(series.median())
        
        # 计算排名百分比
        if ascending:
            score = s.rank(pct=True) * 100
        else:
            score = (1 - s.rank(pct=True)) * 100
            
        return score
    
    def filter_stocks(self, df: pd.DataFrame, criteria: SelectionCriteria) -> pd.DataFrame:
        """根据硬性条件过滤股票"""
        filtered = df.copy()
        
        # 市值过滤
        if 'market_cap' in filtered.columns:
            filtered = filtered[
                (filtered['market_cap'] >= criteria.min_market_cap) &
                (filtered['market_cap'] <= criteria.max_market_cap)
            ]
            logger.info(f"市值过滤后剩余 {len(filtered)} 只")
        
        # 业绩增速过滤（使用 yoy_growth 而非 ltm_growth）
        if 'qoq_growth' in filtered.columns:
            filtered = filtered[filtered['qoq_growth'] >= criteria.min_qoq_growth]
            logger.info(f"季度增速过滤后剩余 {len(filtered)} 只")
            
        if 'yoy_growth' in filtered.columns:
            filtered = filtered[filtered['yoy_growth'] >= criteria.min_yoy_growth]
            logger.info(f"同比增速过滤后剩余 {len(filtered)} 只")
        
        # 过去一年涨幅过滤
        if 'past_year_return' in filtered.columns:
            filtered = filtered[filtered['past_year_return'] <= criteria.max_past_year_return]
            logger.info(f"涨幅过滤后剩余 {len(filtered)} 只")
        
        return filtered
    
    def select(self, criteria: Optional[SelectionCriteria] = None, 
               top_n: int = 20) -> pd.DataFrame:
        """
        执行选股
        
        Args:
            criteria: 选股条件，默认使用标准配置
            top_n: 返回前N只股票
            
        Returns:
            DataFrame包含选股结果和各项得分
        """
        if criteria is None:
            criteria = SelectionCriteria()
        
        # 获取最新交易日
        trade_date = self.get_latest_trade_date()
        logger.info(f"选股日期: {trade_date}")
        
        # ===== 步骤1：获取基本面数据 =====
        logger.info("[1/6] 获取基本面数据...")
        df_basic = self.fetch_fundamental_data(trade_date)
        
        # 提取security_code用于后续匹配
        if 'ts_code' in df_basic.columns:
            df_basic['security_code'] = df_basic['ts_code'].str.split('.').str[0]
        
        # ===== 步骤2：概念板块筛选（新增）=====
        if criteria.concept_sectors:
            concept_str = criteria.concept_sectors if isinstance(criteria.concept_sectors, str) else ', '.join(criteria.concept_sectors)
            logger.info(f"[2/6] 概念板块筛选: {concept_str}...")
            df_basic = self.filter_by_concept_sector(df_basic, criteria.concept_sectors)
            if df_basic.empty:
                logger.warning(f"概念板块【{concept_str}】没有符合条件的股票")
                return df_basic
        else:
            logger.info("[2/6] 概念板块筛选: 无限制")
        
        # ===== 步骤3：市值过滤 =====
        logger.info(f"[3/6] 市值过滤 ({criteria.min_market_cap}-{criteria.max_market_cap}亿)...")
        df_filtered = self.filter_stocks(df_basic, criteria)
        
        # ===== 步骤4：获取业绩增速 =====
        logger.info("[4/6] 获取业绩增速数据...")
        df_growth = self.fetch_profit_growth_data(trade_date)
        
        # ===== 步骤5：获取过去一年涨幅 =====
        logger.info("[5/6] 获取过去一年涨幅...")
        df_return = self.fetch_past_year_return(trade_date)
        
        # ===== 步骤6：合并数据、计算得分 =====
        logger.info("[6/6] 数据合并与评分...")
        df = df_filtered
        
        # 合并业绩增速（使用 security_code 匹配 symbol）
        if not df_growth.empty:
            df_growth_renamed = df_growth.rename(columns={'symbol': 'security_code'})
            df = df.merge(df_growth_renamed[['security_code', 'qoq_growth', 'yoy_growth']], 
                         on='security_code', how='left')
        else:
            df['qoq_growth'] = 0
            df['yoy_growth'] = 0
        
        # 合并涨幅数据（使用 security_code 匹配 symbol）
        if not df_return.empty:
            df_return_renamed = df_return.rename(columns={'symbol': 'security_code'})
            df = df.merge(df_return_renamed[['security_code', 'past_year_return']], 
                         on='security_code', how='left')
        else:
            df['past_year_return'] = 0
        
        # 填充缺失值
        df['qoq_growth'] = df['qoq_growth'].fillna(0)
        df['yoy_growth'] = df['yoy_growth'].fillna(0)
        df['past_year_return'] = df['past_year_return'].fillna(0)
        
        # 计算得分
        df_scored = self.calculate_scores(df, criteria)
        
        # 排序并返回Top N
        df_result = df_scored.sort_values('total_score', ascending=False).head(top_n)
        
        return df_result
    
    def generate_report(self, df: pd.DataFrame, criteria: SelectionCriteria,
                        output_file: Optional[str] = None):
        """生成选股报告"""
        report = []
        report.append("=" * 80)
        report.append("炒作选股策略报告 - 基于骑行客'女神的三围'")
        report.append("=" * 80)
        report.append(f"生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report.append("")
        
        # 显示选股条件
        report.append("【选股条件】")
        if criteria.concept_sectors:
            concept_str = criteria.concept_sectors if isinstance(criteria.concept_sectors, str) else ', '.join(criteria.concept_sectors)
            report.append(f"  概念板块: {concept_str}")
        report.append(f"  市值范围: {criteria.min_market_cap}-{criteria.max_market_cap}亿")
        report.append(f"  季度增速: ≥{criteria.min_qoq_growth*100:.0f}%")
        report.append(f"  同比增速: ≥{criteria.min_yoy_growth*100:.0f}%")
        report.append(f"  涨幅限制: ≤{criteria.max_past_year_return*100:.0f}%")
        report.append("")
        
        # 格式化显示数据
        df_display = df.copy()
        
        # 将小数形式的增速/涨幅转换为百分比显示
        for col in ['qoq_growth', 'yoy_growth', 'past_year_return']:
            if col in df_display.columns:
                df_display[col] = df_display[col].apply(lambda x: f"{x*100:.2f}%" if pd.notna(x) else "N/A")
        
        # 显示Top股票
        display_cols = ['ts_code', 'name', 'market_cap', 'pe_ttm', 
                       'qoq_growth', 'yoy_growth', 'past_year_return', 'total_score']
        
        # 只保留存在的列
        available_cols = [c for c in display_cols if c in df_display.columns]
        report.append("【选股结果】")
        report.append(df_display[available_cols].to_string(index=False))
        report.append("")
        report.append("=" * 80)
        
        report_text = "\n".join(report)
        print(report_text)
        
        if output_file:
            with open(output_file, 'w', encoding='utf-8') as f:
                f.write(report_text)
            logger.info(f"报告已保存至: {output_file}")


def load_db_config(config_file: str = "config.yaml") -> Dict:
    """加载数据库配置"""
    # 远程服务器的实际配置
    default_config = {
        'host': '127.0.0.1',
        'port': 5432,
        'database': 'Financialdata',
        'user': 'postgres',
        'password': '12'
    }
    
    try:
        with open(config_file, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
            if config and 'database' in config:
                return config['database']
    except FileNotFoundError:
        logger.info(f"配置文件 {config_file} 不存在，使用默认配置")
    
    return default_config


def parse_concepts(concept_str: str) -> List[str]:
    """解析概念字符串，逗号分隔"""
    if not concept_str:
        return None
    return [c.strip() for c in concept_str.split(',')]


def main():
    """主函数 - 支持命令行参数和会话记忆"""
    parser = argparse.ArgumentParser(
        description='炒作选股 - 基于骑行客"女神的三围"',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  # 全市场选股（无概念限制）
  python chaozuo_selector.py
  
  # 单概念选股
  python chaozuo_selector.py --concepts 煤炭
  
  # 多概念选股（逗号分隔，取并集）
  python chaozuo_selector.py --concepts "智谱AI,云计算"
  
  # 自定义参数
  python chaozuo_selector.py --concepts "智谱AI,云计算" --max-cap 200 --min-qoq 0.20 --top 20
        """
    )
    
    # 概念板块
    parser.add_argument('--concepts', type=str, default=None,
                        help='概念板块，多个用逗号分隔，如"智谱AI,云计算"')
    
    # 市值限制
    parser.add_argument('--max-cap', type=float, default=200,
                        help='最大市值（亿元），默认200')
    parser.add_argument('--min-cap', type=float, default=10,
                        help='最小市值（亿元），默认10')
    
    # 业绩增速
    parser.add_argument('--min-qoq', type=float, default=0.20,
                        help='季度环比增速最低值，默认0.20（20%%）')
    parser.add_argument('--min-yoy', type=float, default=0.30,
                        help='同比增速最低值，默认0.30（30%%）')
    
    # 涨幅限制
    parser.add_argument('--max-return', type=float, default=0.50,
                        help='过去一年涨幅上限，默认0.50（50%%）')
    
    # 输出数量
    parser.add_argument('--top', type=int, default=20,
                        help='返回前N只股票，默认20')
    
    # 输出文件
    parser.add_argument('--output', type=str, default='chaozuo_selection',
                        help='输出文件名前缀，默认chaozuo_selection')
    
    # 记忆功能开关
    parser.add_argument('--no-memory', action='store_true',
                        help='禁用会话记忆功能')
    
    args = parser.parse_args()
    
    # 加载配置
    db_config = load_db_config()
    
    # 初始化会话记忆
    session_memory = None
    if not args.no_memory and MEM0_AVAILABLE:
        try:
            session_memory = SessionMemory(user_id="quant_user")
            logger.info("会话记忆功能已启用")
        except Exception as e:
            logger.warning(f"记忆功能初始化失败: {e}")
    
    # 创建选股器
    selector = ChaoZuoSelector(db_config)
    
    try:
        # 连接数据库
        selector.connect()
        
        # 构建选股条件
        concept_list = parse_concepts(args.concepts)
        
        criteria = SelectionCriteria(
            concept_sectors=concept_list,
            max_market_cap=args.max_cap,
            min_market_cap=args.min_cap,
            min_qoq_growth=args.min_qoq,
            min_yoy_growth=args.min_yoy,
            max_past_year_return=args.max_return,
        )
        
        # 打印选股条件
        print("=" * 70)
        print("炒作选股 - 基于骑行客'女神的三围'")
        print("=" * 70)
        if concept_list:
            print(f"概念板块: {', '.join(concept_list)}")
        else:
            print("概念板块: 无限制（全市场）")
        print(f"市值范围: {args.min_cap}-{args.max_cap}亿")
        print(f"季度环比: ≥{args.min_qoq*100:.0f}%")
        print(f"同比增速: ≥{args.min_yoy*100:.0f}%")
        print(f"涨幅限制: ≤{args.max_return*100:.0f}%")
        print(f"输出数量: Top {args.top}")
        print("=" * 70)
        
        # 检查历史会话
        if session_memory and concept_list:
            similar = session_memory.find_similar_session(concept_list)
            if similar:
                print(f"\n💡 发现相似选股历史 [{similar['session_id']}]")
                print(f"   上次选股时间: {similar['timestamp']}")
                print(f"   查询该会话历史: python chaozuo_selector.py --history {similar['session_id']}")
        
        # 启动新会话
        if session_memory:
            session_id = session_memory.start_session(criteria)
            logger.info(f"选股会话: {session_id}")
        
        # 执行选股
        results = selector.select(criteria=criteria, top_n=args.top)
        
        if results.empty:
            print("\n⚠️  没有符合条件的股票")
            if session_memory:
                session_memory.save("选股结果: 无符合条件的股票", metadata={"type": "no_result"})
            return
        
        # 保存选股结果到记忆
        if session_memory:
            session_memory.save_results(results, top_n=args.top)
        
        # 生成报告
        report_file = f"{args.output}_report.txt"
        csv_file = f"{args.output}.csv"
        
        selector.generate_report(results, criteria=criteria, output_file=report_file)
        results.to_csv(csv_file, index=False, encoding='utf-8-sig')
        
        print(f"\n✅ 报告已保存: {report_file}")
        print(f"✅ CSV已保存: {csv_file}")
        
        # 提示会话信息
        if session_memory and session_memory.session_id:
            print(f"📌 本次选股会话ID: {session_memory.session_id}")
            print(f"   下次可直接问: '那几只股票现在怎么样了？'")
        
    except Exception as e:
        logger.error(f"选股过程出错: {e}", exc_info=True)
        raise
    finally:
        selector.disconnect()


if __name__ == "__main__":
    main()
