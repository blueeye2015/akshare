
CREATE TABLE financial_indicators_ths (
    -- 主键
    symbol VARCHAR(10) NOT NULL,
    report_period VARCHAR(20) NOT NULL,
    
    -- 财务指标
    net_profit VARCHAR(50),                    -- 净利润
    net_profit_yoy VARCHAR(50),               -- 净利润同比增长率
    deducted_net_profit VARCHAR(50),          -- 扣非净利润
    deducted_net_profit_yoy VARCHAR(50),      -- 扣非净利润同比增长率
    total_revenue VARCHAR(50),                -- 营业总收入
    total_revenue_yoy VARCHAR(50),            -- 营业总收入同比增长率
    eps VARCHAR(50),                          -- 基本每股收益
    nav_per_share VARCHAR(50),                -- 每股净资产
    capital_reserve_per_share VARCHAR(50),    -- 每股资本公积金
    undistributed_profit_per_share VARCHAR(50), -- 每股未分配利润
    ocf_per_share VARCHAR(50),                -- 每股经营现金流
    net_profit_margin VARCHAR(50),            -- 销售净利率
    gross_profit_margin VARCHAR(50),          -- 销售毛利率
    roe VARCHAR(50),                          -- 净资产收益率
    roe_diluted VARCHAR(50),                  -- 净资产收益率-摊薄
    operating_cycle VARCHAR(50),              -- 营业周期
    inventory_turnover VARCHAR(50),           -- 存货周转率
    inventory_days VARCHAR(50),               -- 存货周转天数
    receivables_days VARCHAR(50),             -- 应收账款周转天数
    current_ratio VARCHAR(50),                -- 流动比率
    quick_ratio VARCHAR(50),                  -- 速动比率
    conservative_quick_ratio VARCHAR(50),     -- 保守速动比率
    equity_ratio VARCHAR(50),                 -- 产权比率
    debt_asset_ratio VARCHAR(50),             -- 资产负债率
    
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
    forecast_indicator VARCHAR(50),     -- 预测指标
    performance_change VARCHAR(50),     -- 业绩变动
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


-- 添加更新时间触发器
CREATE OR REPLACE FUNCTION update_timestamp_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.update_time = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

CREATE TRIGGER update_performance_forecast_timestamp
    BEFORE UPDATE ON performance_forecast
    FOR EACH ROW
    EXECUTE FUNCTION update_timestamp_column();


-- 创建更新时间触发器
CREATE OR REPLACE FUNCTION update_timestamp()
RETURNS TRIGGER AS $$
BEGIN
    NEW.update_time = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- 创建触发器
CREATE TRIGGER update_financial_indicators_timestamp
    BEFORE UPDATE ON financial_indicators_ths
    FOR EACH ROW
    EXECUTE FUNCTION update_timestamp();
	
-- 创建资产负债表
CREATE TABLE balance_sheet (
    -- 主键
    symbol VARCHAR(10) NOT NULL,  -- 股票代码
    report_date VARCHAR(20) NOT NULL,  -- 报告期
    
    -- 基础信息
    security_name VARCHAR(50),  -- 股票名称
    
    -- 资产负债表科目（单位：亿元）
    total_current_assets DECIMAL(20,4),  -- 流动资产合计
    total_current_liab DECIMAL(20,4),    -- 流动负债合计
    goodwill DECIMAL(20,4),              -- 商誉
    intangible_assets DECIMAL(20,4),     -- 无形资产
    long_loan DECIMAL(20,4),             -- 长期借款
    bonds_payable DECIMAL(20,4),         -- 应付债券
    long_payable DECIMAL(20,4),          -- 长期应付款
    special_payable DECIMAL(20,4),       -- 专项应付款
    predict_liab DECIMAL(20,4),          -- 预计负债
    defer_tax_liab DECIMAL(20,4),        -- 递延所得税负债
    develop_expense DECIMAL(20,4),        -- 开发支出
    long_rece DECIMAL(20,4),             -- 长期应收款
    total_parent_equity DECIMAL(20,4),   -- 归属于母公司股东权益合计
    preferred_stock DECIMAL(20,4),        -- 优先股
    perpetual_bond DECIMAL(20,4),        -- 永续债（其他权益工具）
    accounts_rece DECIMAL(20,4),         -- 应收账款
    
    -- 时间戳
    create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,  -- 创建时间
    update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,  -- 更新时间
    
    -- 设置主键
    PRIMARY KEY (symbol, report_date)
);

-- 创建索引
CREATE INDEX idx_balance_sheet_symbol ON balance_sheet(symbol);
CREATE INDEX idx_balance_sheet_report_date ON balance_sheet(report_date);
CREATE INDEX idx_balance_sheet_update_time ON balance_sheet(update_time);

-- 添加表注释
COMMENT ON TABLE balance_sheet IS '东方财富资产负债表数据';

-- 添加字段注释
COMMENT ON COLUMN balance_sheet.symbol IS '股票代码';
COMMENT ON COLUMN balance_sheet.report_date IS '报告期';
COMMENT ON COLUMN balance_sheet.security_name IS '股票名称';
COMMENT ON COLUMN balance_sheet.total_current_assets IS '流动资产合计（亿元）';
COMMENT ON COLUMN balance_sheet.total_current_liab IS '流动负债合计（亿元）';
COMMENT ON COLUMN balance_sheet.goodwill IS '商誉（亿元）';
COMMENT ON COLUMN balance_sheet.intangible_assets IS '无形资产（亿元）';
COMMENT ON COLUMN balance_sheet.long_loan IS '长期借款（亿元）';
COMMENT ON COLUMN balance_sheet.bonds_payable IS '应付债券（亿元）';
COMMENT ON COLUMN balance_sheet.long_payable IS '长期应付款（亿元）';
COMMENT ON COLUMN balance_sheet.special_payable IS '专项应付款（亿元）';
COMMENT ON COLUMN balance_sheet.predict_liab IS '预计负债（亿元）';
COMMENT ON COLUMN balance_sheet.defer_tax_liab IS '递延所得税负债（亿元）';
COMMENT ON COLUMN balance_sheet.develop_expense IS '开发支出（亿元）';
COMMENT ON COLUMN balance_sheet.long_rece IS '长期应收款（亿元）';
COMMENT ON COLUMN balance_sheet.total_parent_equity IS '归属于母公司股东权益合计（亿元）';
COMMENT ON COLUMN balance_sheet.preferred_stock IS '优先股（亿元）';
COMMENT ON COLUMN balance_sheet.perpetual_bond IS '永续债（其他权益工具）（亿元）';
COMMENT ON COLUMN balance_sheet.accounts_rece IS '应收账款（亿元）';
COMMENT ON COLUMN balance_sheet.create_time IS '创建时间';
COMMENT ON COLUMN balance_sheet.update_time IS '更新时间';

-- 创建更新时间触发器
CREATE OR REPLACE FUNCTION update_timestamp()
RETURNS TRIGGER AS $$
BEGIN
    NEW.update_time = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

CREATE TRIGGER update_balance_sheet_timestamp
    BEFORE UPDATE ON balance_sheet
    FOR EACH ROW
    EXECUTE FUNCTION update_timestamp();


	-- 创建利润表
CREATE TABLE profit_sheet (
    -- 主键
    symbol VARCHAR(10) NOT NULL,  -- 股票代码
    report_date VARCHAR(20) NOT NULL,  -- 报告期
    
    -- 基础信息
    security_name VARCHAR(50),  -- 股票名称
    
    -- 利润表主要科目（单位：亿元）
    total_operate_income DECIMAL(20,4),      -- 营业总收入
    operate_income DECIMAL(20,4),            -- 营业收入
    total_operate_cost DECIMAL(20,4),        -- 营业总成本
    operate_cost DECIMAL(20,4),              -- 营业成本
    sale_expense DECIMAL(20,4),              -- 销售费用
    manage_expense DECIMAL(20,4),            -- 管理费用
    finance_expense DECIMAL(20,4),           -- 财务费用
    operate_profit DECIMAL(20,4),            -- 营业利润
    total_profit DECIMAL(20,4),              -- 利润总额
    income_tax DECIMAL(20,4),                -- 所得税费用
    netprofit DECIMAL(20,4),                 -- 净利润
    parent_netprofit DECIMAL(20,4),          -- 归属于母公司股东的净利润
    deduct_parent_netprofit DECIMAL(20,4),   -- 扣除非经常性损益后的净利润
    basic_eps DECIMAL(20,4),                 -- 基本每股收益
    diluted_eps DECIMAL(20,4),               -- 稀释每股收益
    
    -- 时间戳
    create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,  -- 创建时间
    update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,  -- 更新时间
    
    -- 设置主键
    PRIMARY KEY (symbol, report_date)
);

-- 创建索引
CREATE INDEX idx_profit_sheet_symbol ON profit_sheet(symbol);
CREATE INDEX idx_profit_sheet_report_date ON profit_sheet(report_date);
CREATE INDEX idx_profit_sheet_update_time ON profit_sheet(update_time);

-- 创建更新时间触发器
CREATE OR REPLACE FUNCTION update_timestamp()
RETURNS TRIGGER AS $$
BEGIN
    NEW.update_time = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

CREATE TRIGGER update_profit_sheet_timestamp
    BEFORE UPDATE ON profit_sheet
    FOR EACH ROW
    EXECUTE FUNCTION update_timestamp();

-- 创建股票指标表
CREATE TABLE stock_indicator (
    -- 主键
    symbol VARCHAR(10),           -- 股票代码
    trade_date DATE,             -- 交易日期
    
    -- 指标数据
    pe NUMERIC(20,4),            -- 市盈率
    pe_ttm NUMERIC(20,4),        -- 市盈率TTM
    pb NUMERIC(20,4),            -- 市净率
    ps NUMERIC(20,4),            -- 市销率
    ps_ttm NUMERIC(20,4),        -- 市销率TTM
    dv_ratio NUMERIC(20,4),      -- 股息率
    dv_ttm NUMERIC(20,4),        -- 股息率TTM
    total_mv NUMERIC(20,4),      -- 总市值
    
    -- 时间戳
    create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- 设置复合主键
    PRIMARY KEY (symbol, trade_date)
);

-- 创建索引
CREATE INDEX idx_stock_indicator_symbol ON stock_indicator(symbol);
CREATE INDEX idx_stock_indicator_trade_date ON stock_indicator(trade_date);

-- 添加更新时间触发器
CREATE OR REPLACE FUNCTION update_timestamp_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.update_time = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

CREATE TRIGGER update_stock_indicator_timestamp
    BEFORE UPDATE ON stock_indicator
    FOR EACH ROW
    EXECUTE FUNCTION update_timestamp_column();

-- 添加表注释
COMMENT ON TABLE stock_indicator IS 'A股个股指标数据表';

-- 添加字段注释
COMMENT ON COLUMN stock_indicator.symbol IS '股票代码';
COMMENT ON COLUMN stock_indicator.trade_date IS '交易日期';
COMMENT ON COLUMN stock_indicator.pe IS '市盈率';
COMMENT ON COLUMN stock_indicator.pe_ttm IS '市盈率TTM';
COMMENT ON COLUMN stock_indicator.pb IS '市净率';
COMMENT ON COLUMN stock_indicator.ps IS '市销率';
COMMENT ON COLUMN stock_indicator.ps_ttm IS '市销率TTM';
COMMENT ON COLUMN stock_indicator.dv_ratio IS '股息率';
COMMENT ON COLUMN stock_indicator.dv_ttm IS '股息率TTM';
COMMENT ON COLUMN stock_indicator.total_mv IS '总市值';

CREATE TABLE financial_indicators (
    -- 复合主键
    symbol VARCHAR(10),
    report_date DATE,
    
    -- 每股指标
    eps_basic NUMERIC,  -- 摊薄每股收益
    eps_diluted NUMERIC,  -- 加权每股收益
    eps_adjusted NUMERIC,  -- 每股收益_调整后
    eps_excl_nonrecurring NUMERIC,  -- 扣除非经常性损益后的每股收益
    bps_pre_adjusted NUMERIC,  -- 每股净资产_调整前
    bps_post_adjusted NUMERIC,  -- 每股净资产_调整后
    ocf_per_share NUMERIC,  -- 每股经营性现金流
    capital_reserve_per_share NUMERIC,  -- 每股资本公积金
    undistributed_profit_per_share NUMERIC,  -- 每股未分配利润
    bps_adjusted NUMERIC,  -- 调整后的每股净资产

    -- 盈利能力指标
    roa_profit NUMERIC,  -- 总资产利润率
    main_business_profit_rate NUMERIC,  -- 主营业务利润率
    roa_net_profit NUMERIC,  -- 总资产净利润率
    cost_expense_profit_rate NUMERIC,  -- 成本费用利润率
    operating_profit_rate NUMERIC,  -- 营业利润率
    main_business_cost_rate NUMERIC,  -- 主营业务成本率
    net_profit_margin NUMERIC,  -- 销售净利率
    return_on_equity NUMERIC,  -- 净资产收益率
    weighted_roe NUMERIC,  -- 加权净资产收益率
    
    -- 成长能力指标
    revenue_growth NUMERIC,  -- 主营业务收入增长率
    net_profit_growth NUMERIC,  -- 净利润增长率
    net_asset_growth NUMERIC,  -- 净资产增长率
    total_asset_growth NUMERIC,  -- 总资产增长率
    
    -- 营运能力指标
    accounts_receivable_turnover NUMERIC,  -- 应收账款周转率
    inventory_turnover NUMERIC,  -- 存货周转率
    fixed_asset_turnover NUMERIC,  -- 固定资产周转率
    total_asset_turnover NUMERIC,  -- 总资产周转率
    
    -- 偿债能力指标
    current_ratio NUMERIC,  -- 流动比率
    quick_ratio NUMERIC,  -- 速动比率
    cash_ratio NUMERIC,  -- 现金比率
    interest_coverage NUMERIC,  -- 利息支付倍数
    debt_to_equity NUMERIC,  -- 负债与所有者权益比率
    asset_liability_ratio NUMERIC,  -- 资产负债率
    
    -- 现金流量指标
    ocf_to_revenue NUMERIC,  -- 经营现金净流量对销售收入比率
    ocf_to_asset NUMERIC,  -- 资产的经营现金流量回报率
    ocf_to_net_profit NUMERIC,  -- 经营现金净流量与净利润的比率
    
    -- 时间戳
    create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- 设置复合主键
    PRIMARY KEY (symbol, report_date)
);

-- 创建索引
CREATE INDEX idx_financial_indicators_symbol ON financial_indicators(symbol);
CREATE INDEX idx_financial_indicators_report_date ON financial_indicators(report_date);
