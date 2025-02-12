
CREATE TABLE financial_indicators_ths (
    -- 主键
    symbol VARCHAR(10) NOT NULL,
    report_period VARCHAR(20) NOT NULL,
    
    -- 财务指标
    net_profit VARCHAR(50),                    -- 净利润
    net_profit_yoy NUMERIC,               -- 净利润同比增长率
    deducted_net_profit VARCHAR(50),          -- 扣非净利润
    deducted_net_profit_yoy NUMERIC,      -- 扣非净利润同比增长率
    total_revenue VARCHAR(50),                -- 营业总收入
    total_revenue_yoy NUMERIC,            -- 营业总收入同比增长率
    eps NUMERIC,                          -- 基本每股收益
    nav_per_share NUMERIC,                -- 每股净资产
    capital_reserve_per_share NUMERIC,    -- 每股资本公积金
    undistributed_profit_per_share NUMERIC, -- 每股未分配利润
    ocf_per_share NUMERIC,                -- 每股经营现金流
    net_profit_margin NUMERIC,            -- 销售净利率
    gross_profit_margin NUMERIC,          -- 销售毛利率
    roe NUMERIC,                          -- 净资产收益率
    roe_diluted NUMERIC,                  -- 净资产收益率-摊薄
    operating_cycle NUMERIC,              -- 营业周期
    inventory_turnover NUMERIC,           -- 存货周转率
    inventory_days NUMERIC,               -- 存货周转天数
    receivables_days NUMERIC,             -- 应收账款周转天数
    current_ratio NUMERIC,                -- 流动比率
    quick_ratio NUMERIC,                  -- 速动比率
    conservative_quick_ratio NUMERIC,     -- 保守速动比率
    equity_ratio NUMERIC,                 -- 产权比率
    debt_asset_ratio NUMERIC,             -- 资产负债率
    
    -- 时间戳
    create_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    update_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    
    -- 主键约束
    PRIMARY KEY (symbol, report_period)
);

-- 创建索引
CREATE INDEX idx_financial_indicators_symbol ON financial_indicators_ths(symbol);
CREATE INDEX idx_financial_indicators_report_period ON financial_indicators_ths(report_period);
CREATE INDEX idx_financial_indicators_update_time ON financial_indicators_ths(update_time);

-- 添加表注释
COMMENT ON TABLE financial_indicators_ths IS '同花顺财务指标数据表';

-- 添加字段注释
COMMENT ON COLUMN financial_indicators_ths.symbol IS '股票代码';
COMMENT ON COLUMN financial_indicators_ths.report_period IS '报告期';
COMMENT ON COLUMN financial_indicators_ths.net_profit IS '净利润';
COMMENT ON COLUMN financial_indicators_ths.net_profit_yoy IS '净利润同比增长率';
COMMENT ON COLUMN financial_indicators_ths.deducted_net_profit IS '扣非净利润';
COMMENT ON COLUMN financial_indicators_ths.deducted_net_profit_yoy IS '扣非净利润同比增长率';
COMMENT ON COLUMN financial_indicators_ths.total_revenue IS '营业总收入';
COMMENT ON COLUMN financial_indicators_ths.total_revenue_yoy IS '营业总收入同比增长率';
COMMENT ON COLUMN financial_indicators_ths.eps IS '基本每股收益';
COMMENT ON COLUMN financial_indicators_ths.nav_per_share IS '每股净资产';
COMMENT ON COLUMN financial_indicators_ths.capital_reserve_per_share IS '每股资本公积金';
COMMENT ON COLUMN financial_indicators_ths.undistributed_profit_per_share IS '每股未分配利润';
COMMENT ON COLUMN financial_indicators_ths.ocf_per_share IS '每股经营现金流';
COMMENT ON COLUMN financial_indicators_ths.net_profit_margin IS '销售净利率';
COMMENT ON COLUMN financial_indicators_ths.gross_profit_margin IS '销售毛利率';
COMMENT ON COLUMN financial_indicators_ths.roe IS '净资产收益率';
COMMENT ON COLUMN financial_indicators_ths.roe_diluted IS '净资产收益率-摊薄';
COMMENT ON COLUMN financial_indicators_ths.operating_cycle IS '营业周期';
COMMENT ON COLUMN financial_indicators_ths.inventory_turnover IS '存货周转率';
COMMENT ON COLUMN financial_indicators_ths.inventory_days IS '存货周转天数';
COMMENT ON COLUMN financial_indicators_ths.receivables_days IS '应收账款周转天数';
COMMENT ON COLUMN financial_indicators_ths.current_ratio IS '流动比率';
COMMENT ON COLUMN financial_indicators_ths.quick_ratio IS '速动比率';
COMMENT ON COLUMN financial_indicators_ths.conservative_quick_ratio IS '保守速动比率';
COMMENT ON COLUMN financial_indicators_ths.equity_ratio IS '产权比率';
COMMENT ON COLUMN financial_indicators_ths.debt_asset_ratio IS '资产负债率';
COMMENT ON COLUMN financial_indicators_ths.create_time IS '创建时间';
COMMENT ON COLUMN financial_indicators_ths.update_time IS '更新时间';

-- 创建业绩预告表
CREATE TABLE performance_forecast (
    -- 主键
    symbol VARCHAR(10),           -- 股票代码
    report_period VARCHAR(8),     -- 报告期 YYYYMMDD
    announce_date VARCHAR(10),    -- 公告日期 YYYY-MM-DD
    
    -- 业绩预告数据
    stock_name VARCHAR(50),       -- 股票简称
    forecast_indicator FLOAT,     -- 预测指标
    performance_change FLOAT,     -- 业绩变动
    forecast_value FLOAT,         -- 预测数值
    change_rate FLOAT,           -- 业绩变动幅度
    change_reason TEXT,          -- 业绩变动原因
    forecast_type VARCHAR(20),    -- 预告类型
    last_year_value FLOAT,       -- 上年同期值
    
    -- 时间戳
    create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,  -- 创建时间
    update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,  -- 更新时间
    
    -- 设置复合主键
    PRIMARY KEY (symbol, report_period, announce_date)
);

-- 创建索引
CREATE INDEX idx_performance_forecast_symbol ON performance_forecast(symbol);
CREATE INDEX idx_performance_forecast_report_period ON performance_forecast(report_period);
CREATE INDEX idx_performance_forecast_announce_date ON performance_forecast(announce_date);

-- 添加表注释
COMMENT ON TABLE performance_forecast IS '股票业绩预告数据表';

-- 添加字段注释
COMMENT ON COLUMN performance_forecast.symbol IS '股票代码';
COMMENT ON COLUMN performance_forecast.report_period IS '报告期 YYYYMMDD';
COMMENT ON COLUMN performance_forecast.announce_date IS '公告日期 YYYY-MM-DD';
COMMENT ON COLUMN performance_forecast.stock_name IS '股票简称';
COMMENT ON COLUMN performance_forecast.forecast_indicator IS '预测指标';
COMMENT ON COLUMN performance_forecast.performance_change IS '业绩变动';
COMMENT ON COLUMN performance_forecast.forecast_value IS '预测数值';
COMMENT ON COLUMN performance_forecast.change_rate IS '业绩变动幅度';
COMMENT ON COLUMN performance_forecast.change_reason IS '业绩变动原因';
COMMENT ON COLUMN performance_forecast.forecast_type IS '预告类型';
COMMENT ON COLUMN performance_forecast.last_year_value IS '上年同期值';
COMMENT ON COLUMN performance_forecast.create_time IS '创建时间';
COMMENT ON COLUMN performance_forecast.update_time IS '更新时间';
