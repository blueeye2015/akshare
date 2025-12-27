DROP TABLE IF EXISTS micro_cap_index;
CREATE TABLE micro_cap_index (
    trade_date   date   PRIMARY KEY,
    cnt          int,          -- 当日成分股数量
    next_ret     numeric(10,6),-- 指数次日收益率（等权）
    nav          numeric(20,6) -- 累计净值
);


WITH base AS (
    /* 1. 取每日可交易股票 */
    SELECT
        b.trade_date,
        b.security_code,
        b.circ_mv,
        h.pct_change / 100 AS ret,          -- 当日收益
        LEAD(h.pct_change / 100) OVER (PARTITION BY b.security_code ORDER BY b.trade_date) AS next_ret_raw, -- 次日收益
        /* 涨跌停判断：收益>9.9% 或 <-9.9% 且振幅<1% 视为一字板 */
        CASE WHEN abs(h.pct_change) > 9.9 AND h.amplitude < 1.0 THEN 1 ELSE 0 END AS limit_flag,
        /* 上市天数：用首张行情日期算 */
        (b.trade_date -
         CASE
             WHEN i.listing_date ~ '^\d{8}$' THEN
                 TO_DATE(i.listing_date, 'YYYYMMDD')
             WHEN i.listing_date ~ '^\d{4}-\d{2}-\d{2}$' THEN
                 TO_DATE(i.listing_date, 'YYYY-MM-DD')
             ELSE NULL
         END) AS list_days
    FROM daily_basic b
    JOIN stock_history h
          ON b.security_code = h.symbol
         AND b.trade_date = h.trade_date
	LEFT JOIN stock_individual_info i
          ON b.security_code = i.symbol
    WHERE h.adjust_type = 'hfq'            -- 用后复权
      AND b.circ_mv > 0
),
filter AS (
    /* 2. 剔除 ST、停牌、一字板、上市<60 日 */
    SELECT
        trade_date,
        security_code,
        circ_mv,
        next_ret_raw,
        ret
    FROM base
    WHERE limit_flag = 0                   -- 剔除一字板
      AND list_days >= 60                  -- 上市满 60 日
      AND security_code NOT LIKE 'ST%'           -- 简单剔除 ST
      AND security_code NOT LIKE '%ST%'
),
ranked AS (
    /* 3. 每日按流通市值升序排名，取 10% 分位 */
    SELECT
        trade_date,
        security_code,
        next_ret_raw,
        ntile(10) OVER (PARTITION BY trade_date ORDER BY circ_mv) AS mv_rank
    FROM filter
),
pool AS (
    /* 4. 只留最小 10% 档（rank=1） */
    SELECT
        trade_date,
        security_code,
        next_ret_raw
    FROM ranked
    WHERE mv_rank = 1
),
index_ret AS (
    /* 5. 计算指数次日等权收益 */
    SELECT
        trade_date,
        COUNT(*) AS cnt,
        AVG(next_ret_raw) AS next_ret
    FROM pool
    GROUP BY trade_date
)
/* 6. 累乘净值并插入结果表 */
INSERT INTO micro_cap_index (trade_date, cnt, next_ret, nav)
SELECT
    trade_date,
    cnt,
    next_ret,
    exp(SUM(ln(1 + next_ret)) OVER (ORDER BY trade_date)) AS nav
FROM index_ret
ORDER BY trade_date;



-- 最近 10 年收益
SELECT
    trade_date,
    nav,
    cnt
FROM micro_cap_index
WHERE trade_date >= '2014-01-01'
ORDER BY trade_date;

-- 年化收益、夏普（近似）
SELECT
    COUNT(*) AS days,
    AVG(next_ret) * 252 AS ann_ret,
    STDDEV(next_ret) * SQRT(252) AS ann_vol,
    (AVG(next_ret) * 252) / (STDDEV(next_ret) * SQRT(252)) AS sharpe
FROM micro_cap_index
WHERE trade_date >= '2014-01-01';