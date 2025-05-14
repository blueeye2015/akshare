
WITH latest_quarter AS (
    -- 获取最新季度日期
    SELECT MAX(report_date) as max_date
    FROM profit_sheet
),

quarterly_data AS (
    -- 计算单季度数据
    SELECT 
        security_code,
        security_name,
        report_date,
        CASE 
            -- 第一季度数据就是当期数据
            WHEN EXTRACT(MONTH FROM to_date(report_date, 'YYYY-MM-DD')) = 3 THEN deduct_parent_netprofit
            -- 其他季度需要用当期减去上期
            ELSE deduct_parent_netprofit - LAG(deduct_parent_netprofit) OVER (
                PARTITION BY security_code, EXTRACT(YEAR FROM to_date(report_date, 'YYYY-MM-DD'))
                ORDER BY report_date
            )
        END as quarterly_profit,
        CASE 
            WHEN EXTRACT(MONTH FROM to_date(report_date, 'YYYY-MM-DD')) = 3 THEN total_operate_income
            ELSE total_operate_income - LAG(total_operate_income) OVER (
                PARTITION BY security_code, EXTRACT(YEAR FROM to_date(report_date, 'YYYY-MM-DD'))
                ORDER BY report_date
            )
        END as quarterly_revenue
    FROM profit_sheet
),

profit_yoy AS (
    -- 计算单季度净利润同比增速
    SELECT 
        t1.security_code,
        t1.report_date,
        t1.quarterly_profit as current_profit,
        t2.quarterly_profit as last_year_profit,
        t1.security_name,
        CASE 
            WHEN t2.quarterly_profit != 0 THEN 
                ((t1.quarterly_profit - t2.quarterly_profit) / ABS(t2.quarterly_profit) * 100)
            ELSE NULL 
        END as profit_yoy
    FROM quarterly_data t1
    LEFT JOIN quarterly_data t2 ON t1.security_code = t2.security_code 
        AND to_date(t1.report_date, 'YYYY-MM-DD') - INTERVAL '1 year' = to_date(t2.report_date, 'YYYY-MM-DD')
    WHERE t1.report_date = (SELECT max_date FROM latest_quarter)
),

profit_qoq AS (
    -- 计算单季度净利润环比增速
    SELECT 
        t1.security_code,
        CASE 
            WHEN t2.quarterly_profit != 0 THEN 
                ((t1.quarterly_profit - t2.quarterly_profit) / ABS(t2.quarterly_profit) * 100)
            ELSE NULL 
        END as profit_qoq
    FROM quarterly_data t1
    LEFT JOIN quarterly_data t2 ON t1.security_code = t2.security_code 
        AND to_date(t1.report_date, 'YYYY-MM-DD') - INTERVAL '3 months' = to_date(t2.report_date, 'YYYY-MM-DD')
    WHERE t1.report_date = (SELECT max_date FROM latest_quarter)
),

revenue_yoy AS (
    -- 计算单季度营收同比增速
    SELECT 
        t1.security_code,
        CASE 
            WHEN t2.quarterly_revenue != 0 THEN 
                ((t1.quarterly_revenue - t2.quarterly_revenue) / ABS(t2.quarterly_revenue) * 100)
            ELSE NULL 
        END as revenue_yoy
    FROM quarterly_data t1
    LEFT JOIN quarterly_data t2 ON t1.security_code = t2.security_code 
        AND to_date(t1.report_date, 'YYYY-MM-DD') - INTERVAL '1 year' = to_date(t2.report_date, 'YYYY-MM-DD')
    WHERE t1.report_date = (SELECT max_date FROM latest_quarter)
),

revenue_qoq AS (
    -- 计算单季度营收环比增速
    SELECT 
        t1.security_code,
        CASE 
            WHEN t2.quarterly_revenue != 0 THEN 
                ((t1.quarterly_revenue - t2.quarterly_revenue) / ABS(t2.quarterly_revenue) * 100)
            ELSE NULL 
        END as revenue_qoq
    FROM quarterly_data t1
    LEFT JOIN quarterly_data t2 ON t1.security_code = t2.security_code 
        AND to_date(t1.report_date, 'YYYY-MM-DD') - INTERVAL '3 months' = to_date(t2.report_date, 'YYYY-MM-DD')
    WHERE t1.report_date = (SELECT max_date FROM latest_quarter)
),

ttm_profit AS (
    -- 计算TTM扣非净利润（使用单季度数据）
    SELECT 
        security_code,
        SUM(quarterly_profit) as ttm_deduct_profit
    FROM quarterly_data
    WHERE report_date >= (
        SELECT to_char(to_date(max_date, 'YYYY-MM-DD') - INTERVAL '9 months', 'YYYY-MM-DD')
        FROM latest_quarter
    )
    GROUP BY security_code
),

latest_market_value AS (
    -- 获取最新市值数据
    select symbol,total_market_value / 10000 as total_mv from public.stock_individual_info
)

-- 最终计算PEG
SELECT 
    py.security_code,
    py.security_name,
    py.current_profit,
    py.last_year_profit,
    py.profit_yoy as growth_rate,
    pq.profit_qoq as profit_qoq_rate,
    ry.revenue_yoy as revenue_growth_rate,
    rq.revenue_qoq as revenue_qoq_rate,
	lmv.total_mv,
	tp.ttm_deduct_profit,
    lmv.total_mv / NULLIF(tp.ttm_deduct_profit, 0) as pe_deduct,
    CASE 
        WHEN py.profit_yoy > 0 THEN 
            (lmv.total_mv / NULLIF(tp.ttm_deduct_profit, 0)) / py.profit_yoy
        ELSE NULL 
    END as peg
FROM profit_yoy py
JOIN profit_qoq pq ON py.security_code = pq.security_code
JOIN revenue_yoy ry ON py.security_code = ry.security_code
JOIN revenue_qoq rq ON py.security_code = rq.security_code
JOIN ttm_profit tp ON py.security_code = tp.security_code
JOIN latest_market_value lmv ON py.security_code = lmv.symbol
WHERE 
    -- 确保扣非净利润为正
    tp.ttm_deduct_profit > 0
    -- 确保净利润同比增速大于50%
    AND py.profit_yoy > 50
    -- 确保营收同比增速大于20%
    AND ry.revenue_yoy > 20
    -- 确保净利润环比为正
    AND pq.profit_qoq > 0
    -- 确保营收环比为正
    AND rq.revenue_qoq > 0
    -- PEG在0-0.5之间
    AND (lmv.total_mv / NULLIF(tp.ttm_deduct_profit, 0)) / py.profit_yoy BETWEEN 0 AND 0.5
ORDER BY peg ASC;