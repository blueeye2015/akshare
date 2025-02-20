--寻找相关行业头部企业
SELECT a.ts_code,a.name 股票名,a.industry 行业,a.market 板块,b.total_mv/10000 市值 
FROM public.stock_basic a inner join public.daily_basic b on a.ts_code=b.ts_code
where (industry like '%化工%' or industry like '%化纤%')and b.trade_date = '20210716'
order by b.total_mv desc
--扣除非经常性损益后的净利润ltm
with cte as (
SELECT * FROM (
SELECT *,Row_number() over (order by end_date desc) Cnt FROM (
SELECT end_date,(profit_dedt-
	CASE WHEN RIGHT(end_date,4)<>'0331' THEN Lag(profit_dedt,1)  OVER (ORDER BY end_date) ELSE 0 END)/100000000 扣非
FROM	(SELECT DISTINCT end_date,profit_dedt,ts_code FROM public.fina_indicator WHERE profit_dedt IS NOT NULL) A
	) B  --AND RIGHT(end_date,4)='1231'
) A  )
--按年	
--净利润ltm
,cte1 as (
SELECT * FROM (
SELECT *,Row_number() over (order by end_date desc) Cnt FROM (	
SELECT 	end_date,(total_revenue-CASE WHEN RIGHT(end_date,4)<>'0331' THEN Lag(total_revenue,1)  OVER (ORDER BY end_date) ELSE 0 END)/100000000 总营收,
		(n_income_attr_p-CASE WHEN RIGHT(end_date,4)<>'0331' THEN Lag(n_income_attr_p,1)  OVER (ORDER BY end_date) ELSE 0 END)/100000000 净利润
FROM (SELECT  DISTINCT end_date,total_revenue,n_income_attr_p	FROM public.income WHERE total_revenue IS NOT NULL AND n_income_attr_p IS NOT NULL) A
	) B
 --AND RIGHT(end_date,4)='1231'
	) A 
	) , cte2 as (
--现金流ltm
SELECT * FROM (
SELECT *,Row_number() over (order by end_date desc) Cnt FROM (	
SELECT 	end_date,
		(n_cashflow_act-CASE WHEN RIGHT(end_date,4)<>'0331' THEN Lag(n_cashflow_act,1)  OVER (ORDER BY end_date) ELSE 0 END)/100000000 现金流
FROM (SELECT DISTINCT end_date,n_cashflow_act	FROM public.cashflow) A
	) B
--AND RIGHT(end_date,4)='1231'
	) A 
		) 
select a.end_date,a.扣非,b.总营收,b.净利润,c.现金流 from cte a left join cte1 b on a.cnt= b.cnt 
left join cte2 c on a.cnt =c.cnt
SELECT trade_date,pe,total_mv/10000 FROM public.daily_basic WHERE trade_date IN (
select  max(trade_date) from public.daily_basic 
WHERE trade_date>='20100601' 
group by left(trade_date,6)
) AND SUBSTRING(trade_date , 5 , 2) IN ('03','06','09','12')
ORDER BY trade_date;


SELECT trade_date,PE_TTM,DV_TTM,total_share FROM public.daily_basic where trade_date>='20111007'
order by trade_date
WHERE trade_date IN (select  max(trade_date) from public.daily_basic GROUP BY LEFT(trade_date,4))
ORDER BY trade_date
--现金流
SELECT end_date,n_cashflow_act/100000000 FROM cashflow WHERE END_DATE>='20200101'
SELECT DISTINCT end_date,bz_item,bz_sales FROM public.fina_mainbz
WHERE end_date>='20190801' 
AND bz_sales>1000000000
ORDER BY end_date
LIMIT 100
select distinct end_date,eps from public.fina_indicator where eps is not null order by end_date
SELECT * FROM public.fina_mainbz WHERE "bz_item"='船舶造修'
SELECT * FROM public.shibor
SELECT  DISTINCT end_date,n_income,n_income_attr_p	FROM public.income1
WHERE end_date>='20190801' 
ORDER BY end_date
--按年计算
with cte as (
SELECT DISTINCT end_date,profit_dedt/100000000 扣非  FROM public.fina_indicator 
WHERE RIGHT(end_date,4)='1231' AND end_date>='20110101'),cte1 as (
SELECT DISTINCT end_date,total_hldr_eqy_exc_min_int/100000000 归母净资产 FROM public.balancesheet 
WHERE RIGHT(end_date,4)='1231' AND end_date>='20110101'),cte2 as (
SELECT  DISTINCT end_date,total_revenue/100000000 as 营业收入,n_income_attr_p/100000000 as 归母净利	FROM public.income 
	WHERE RIGHT(end_date,4)='1231' AND end_date>='20110101')
select a.end_date,a.扣非,c.营业收入,c.归母净利,b.归母净资产 from cte a left join cte1 b on a.end_date=b.end_date
left join cte2 c on a.end_date=c.end_date
SELECT * FROM (
SELECT *,Row_number() over (order by end_date desc) Cnt FROM (	
SELECT end_date,
		(total_hldr_eqy_inc_min_int-CASE WHEN RIGHT(end_date,4)<>'0331' THEN Lag(total_hldr_eqy_inc_min_int,1)  OVER (ORDER BY end_date) ELSE 0 END)/100000000
FROM (
SELECT DISTINCT end_date,total_hldr_eqy_inc_min_int FROM public.balancesheet 
	) A
	) B
SELECT DISTINCT end_date,profit_dedt/100000000 FROM public.fina_indicator
WHERE RIGHT(end_date,4)='1231'
order by end_date

select trade_date,pe,total_mv/10000 as total_mv,total_share/10000 as total_share from public.daily_basic
where trade_date in (select max(trade_date) from public.daily_basic
					group by left(trade_date,4))
order by trade_date

select trade_date,close from public.daily where trade_date in (
select max(trade_date) from public.daily group by left(trade_date,4) )
order by trade_date

select ex_date,stk_div,cash_div_tax
	from dividend where ex_date>='20100101'  order by  end_date

SELECT TS_CODE,NAME,sum(bz_sales),
array_agg(bz_item||'|')
FROM (
SELECT DISTINCT A.TS_CODE,B.NAME,A.bz_item,bz_sales
FROM fina_mainbz A INNER JOIN public.stock_basic B 
ON A.TS_CODE =B.TS_CODE
WHERE bz_item LIKE '%%'
AND END_DATE = '20191231'
) A GROUP BY TS_CODE,NAME
ORDER BY 3 desc

select array_agg(bz_item||'|') 
from fina_mainbz WHERE bz_item LIKE '%铝%'
AND END_DATE = '20191231'
AND ts_code = '600219.SH';


SELECT DISTINCT end_date,profit_dedt FROM public.fina_indicator
ORDER BY end_date

DELETE public.fina_indicator WHERE profit_dedt IN (1514.1,1891.8)
select * from public.forecast where ts_code = '002027.SZ'
select * 
FROM public.daily WHERE TS_CODE = '601677.SH'
select a.*,b.name from public.forecast a inner join public.stock_basic b on a.ts_code = b.ts_code where a.ts_code in (
select ts_code from public.stock_basic where name in ('思源电气','赞宇科技','振东制药','分众传媒','横店东磁','以岭药业','黑马股份','浙富控股')
	) and end_date = '20210331'

select ts_code from public.express where end_date='20201231' and ann_date<='20210402'
select count(*) from (
select *,row_number() over (partition by ts_code order by actual_date )  from public.disclosure_date 
where end_date='20201231' 
) a inner join public.stock_basic b on a.ts_code =b.ts_code
where row_number = 1 and actual_date is null

select * from public.index_basic where name like '%创业%'


SELECT * 
FROM (
select trade_date,sum(close) over (partition by ts_code order by trade_date 
								   ROWS BETWEEN 59 PRECEDING AND CURRENT ROW)/60 AS "60日均线"
,close		 
from public.index_daily
	) A WHERE trade_date >= '20200101'
) b where 
order by trade_date

select trade_date,close,round("60日均线"::numeric,2) from (
select trade_date,sum(close) over (partition by ts_code order by trade_date 
								   ROWS BETWEEN 59 PRECEDING AND CURRENT ROW)/60 AS "60日均线"
,close	
FROM public.pro_bar WHERE TS_CODE = '603010.SH' 
	) a where trade_date>='20200701'
	and close < round("60日均线"::numeric,2)
	order by 2
select trade_date,round("60日均线"::numeric,2)/close-1 from (
select trade_date,sum(close) over (partition by ts_code order by trade_date 
								   ROWS BETWEEN 59 PRECEDING AND CURRENT ROW)/60 AS "60日均线"
,close	
FROM public.pro_bar WHERE TS_CODE = '603010.SH' 
	) a where trade_date>='20200701'
	and round("60日均线"::numeric,2)/close-1>0
	order by 2
select trade_date,round("60日均线"::numeric,2)/close-1 from (
select trade_date,sum(close) over (partition by ts_code order by trade_date 
								   ROWS BETWEEN 59 PRECEDING AND CURRENT ROW)/60 AS "60日均线"
,close	
FROM public.pro_bar WHERE TS_CODE = '603010.SH' 
	) a where trade_date>='20200701'
	and round("60日均线"::numeric,2)/close-1>0
	order by 2
select * from	public.pro_bar
	select * FROM public.daily WHERE TS_CODE = '603010.SH' and trade_date='20210413'
select * from 	public.fina_mainbz a inner join public.stock_basic b on a.ts_code =b.ts_code
delete from	public.fina_indicator where profit_dedt = '22882001626'
delete from public.income where total_revenue='2691804098.75'
select DISTINCT end_date,profit_dedt
from public.fina_indicator WHERE profit_dedt IS NOT NULL order by end_date
SELECT  DISTINCT end_date,total_revenue,n_income_attr_p	FROM public.income WHERE total_revenue IS NOT NULL AND n_income_attr_p IS NOT NULL
ORDER BY 1
SELECT  DISTINCT end_date,n_cashflow_act	FROM public.cashflow
order by 1 
SELECT ts_code FROM public.stock_basic where symbol = '000059'
select * from public.stock_basic where name = '中国石化'
SELECT DISTINCT end_date,profit_dedt,ts_code FROM public.fina_indicator WHERE profit_dedt IS NOT NULL order by ts_code,end_date
TRUNCATE TABLE public.fina_indicator;
TRUNCATE TABLE public.income;
TRUNCATE TABLE public.cashflow;
TRUNCATE TABLE public.fina_mainbz;
SELECT * FROM (
SELECT DISTINCT B.NAME,end_date,n_income/100000000 AS 净利润,compr_inc_attr_p/100000000 AS 归属于母公司
FROM public.income A INNER JOIN public.stock_basic B ON A.TS_CODE =B.TS_CODE
WHERE RIGHT(end_date,4)='1231'
AND end_date>='20110101'
) A ORDER BY NAME,end_date

SELECT * FROM (
SELECT DISTINCT B.NAME,end_date, n_cashflow_act/100000000 as 经营活动产生的现金流量净额,
n_cashflow_inv_act/100000000 AS 投资活动产生的现金流量净额
FROM public.cashflow A INNER JOIN public.stock_basic B ON A.TS_CODE =B.TS_CODE
WHERE RIGHT(end_date,4)='1231'
AND end_date>='20110101'
	) A
ORDER BY NAME,end_date

SELECT b.name,end_date,a.经营活动产生的现金流量净额,a.投资活动产生的现金流量净额 
FROM (
SELECT *,Row_number() over (PARTITION BY ts_code order by end_date desc) Cnt FROM (
SELECT ts_code,end_date,(n_cashflow_act-
	CASE WHEN RIGHT(end_date,4)<>'0331' THEN Lag(n_cashflow_act,1)  OVER (PARTITION BY ts_code ORDER BY end_date) ELSE 0 END)/100000000 经营活动产生的现金流量净额,
	(n_cashflow_inv_act-
	CASE WHEN RIGHT(end_date,4)<>'0331' THEN Lag(n_cashflow_inv_act,1)  OVER (PARTITION BY ts_code ORDER BY end_date) ELSE 0 END)/100000000 投资活动产生的现金流量净额
FROM	(SELECT DISTINCT end_date,n_cashflow_act,n_cashflow_inv_act,ts_code FROM public.cashflow 
		 WHERE n_cashflow_act IS NOT NULL AND n_cashflow_inv_act IS NOT NULL) A
	) B WHERE end_date>='20200101' --AND RIGHT(end_date,4)='1231'
) A INNER JOIN public.stock_basic B ON A.TS_CODE =B.TS_CODE
WHERE Cnt <=4

select * from  byyear_count('20200101') 
as (end_date text,扣非 double precision,营业收入 double precision,
	归母净利 double precision,归母净资产 double precision)
select * from public.ltm_count() as ( end_date text,扣非 double precision,总营收 double precision,
	净利润 double precision,现金流 double precision)
	
	
select  first_value(close) over (order by trade_date asc) as "4.30日股价", --4.30日股价,
first_value(high) over (order by high desc) as 股价最高, --股价最高
first_value(low) over (order by low asc) as 股价最低,  --股价最低
first_value(close) over (order by trade_date desc) AS 最新收盘价,
MIN(close) FILTER (WHERE trade_date>='20210518') OVER (ORDER BY close ASC) as 最大回撤价格
from public.daily where trade_date>='20210430'
order by trade_date desc limit 1

select * from public.
SELECT *
,SUM("涨跌") OVER (ORDER BY TRADE_DATE ROWS BETWEEN CURRENT ROW AND 12 FOLLOWING)
FROM (
SELECT TRADE_DATE,CASE WHEN CAST(NEXT_WEEK_CLOSE AS FLOAT)-CAST(CLOSE AS FLOAT) > 0 THEN 0 ELSE 1 END AS "涨跌",
CLOSE,NEXT_WEEK_CLOSE
FROM (
SELECT TRADE_DATE,CLOSE,LEAD(CLOSE,1) OVER (PARTITION BY TS_CODE ORDER BY TRADE_DATE) AS NEXT_WEEK_CLOSE
FROM public.index_weekly
WHERE TS_CODE = '399006.SZ'
	) A 
	) B
ORDER BY 1
drop table profit_dedt
create table public.profit_dedt as
SELECT *,Row_number() over (PARTITION BY ts_code order by end_date desc) Cnt FROM (
SELECT ts_code,end_date,(profit_dedt-
	CASE WHEN RIGHT(end_date,4)<>'0331' THEN Lag(profit_dedt,1)  OVER (PARTITION BY ts_code ORDER BY end_date) ELSE 0 END)/100000000 扣非
FROM	(SELECT DISTINCT end_date,profit_dedt,ts_code FROM public.fina_indicator WHERE profit_dedt IS NOT NULL) A
	) B  --AND RIGHT(end_date,4)='1231'
	
create table public.total_revenue as
SELECT *,Row_number() over (PARTITION BY ts_code order by end_date desc) Cnt FROM (	
SELECT 	ts_code,end_date,(total_revenue-CASE WHEN RIGHT(end_date,4)<>'0331' THEN Lag(total_revenue,1)  OVER (PARTITION BY ts_code ORDER BY end_date) ELSE 0 END)/100000000 总营收,
		(n_income_attr_p-CASE WHEN RIGHT(end_date,4)<>'0331' THEN Lag(n_income_attr_p,1)  OVER (PARTITION BY ts_code ORDER BY end_date) ELSE 0 END)/100000000 净利润
FROM (SELECT  DISTINCT ts_code,end_date,total_revenue,n_income_attr_p	FROM public.income WHERE total_revenue IS NOT NULL AND n_income_attr_p IS NOT NULL) A
	) B

select distinct end_date from profit_dedt where end_date>='20210331'
select * from total_revenue where ts_code='002517.SZ'
select ts_code,end_date,max(扣非) 扣非  from profit_dedt where end_date>='20210331' group by ts_code,end_date order by 1,2 desc
select * from sheet
where "wc除lt dedt"::numeric>=0.75 and "流动比例"::numeric>1.5 and "应收账款风险"::numeric<0.5 and "商誉减值风险小"::numeric<0.25
and "wc除lt dedt"<>'——' and "流动比例"<>'——' and "应收账款风险"<>'——' and "商誉减值风险小"<> '——'
and "预告扣除非经常性损益后的净利润上限(亿元)
202"<>'——' and "预告扣除非经常性损益后的净利润上限(亿元)
202"::numeric>0
update sheet set "wc除lt dedt"=null where "wc除lt dedt"='——' ;
update sheet set "流动比例"=null where "流动比例"='——';
update sheet set "应收账款风险"=null where "应收账款风险"='——';
update sheet set "商誉减值风险小"=null where "商誉减值风险小"='——';
select * from (
select * from public.sheet where ("wc除lt dedt"::numeric>=0.75 or "wc除lt dedt" is null)
	                         and ("流动比例"::numeric>1.5 or "流动比例" is null) 
	                         and ("应收账款风险"::numeric<0.5 or "应收账款风险" is null) 
	                         and ("商誉减值风险小"::numeric<0.25 or "商誉减值风险小" is null)

 ) a left join (
SELECT *
FROM crosstab(
  'select ts_code,end_date,max(扣非) 扣非  from profit_dedt where end_date>=''20210331'' group by ts_code,end_date order by 1,2 desc',
	'select distinct end_date from profit_dedt where end_date>=''20210331'' order by 1 desc') as ("ts_code" text, -- 结果集中指定拼接字段类型为text
      "20220630" numeric,
	  "20220331" numeric,
	  "20211231" numeric,
	  "20210930" numeric,
	  "20210630" numeric,
	  "20210331" numeric) ) b on a."证券代码"=b.ts_code 
left join ( SELECT *
FROM crosstab(
  'select ts_code,end_date,max("净利润") 净利润  from total_revenue where end_date>=''20210331'' group by ts_code,end_date order by 1,2 desc',
	'select distinct end_date from total_revenue where end_date>=''20210331'' order by 1 desc') as ("ts_code" text, -- 结果集中指定拼接字段类型为text
      "20220630" numeric,
	  "20220331" numeric,
	  "20211231" numeric,
	  "20210930" numeric,
	  "20210630" numeric,
	  "20210331" numeric)
) c on a."证券代码"=c.ts_code 
left join (SELECT *
FROM crosstab(
  'select ts_code,end_date,max("总营收") 总营收  from total_revenue where end_date>=''20210331'' group by ts_code,end_date order by 1,2 desc',
	'select distinct end_date from total_revenue where end_date>=''20210331'' order by 1 desc') as ("ts_code" text, -- 结果集中指定拼接字段类型为text
      "20220630" numeric,
	  "20220331" numeric,
	  "20211231" numeric,
	  "20210930" numeric,
	  "20210630" numeric,
	  "20210331" numeric)) d on a."证券代码"=d.ts_code 
where "预告扣除非经常性损益后的净利润上限(亿元)
202"<> '——'

select distinct a.ts_code,b.name,industry,first_value("总营收") over (partition by a.ts_code order by end_date desc),
first_value("总营收") over (partition by a.ts_code order by end_date asc) ,
first_value("净利润") over (partition by a.ts_code order by end_date desc),
first_value("净利润") over (partition by a.ts_code order by end_date asc)
from total_revenue a left join stock_basic b on a.ts_code =b.ts_code where a.ts_code in (select name from sheet_name)
and left(end_date,4) in ('2021','2020','2019','2018','2017')
select distinct ts_code from total_revenue where ts_code in (select name from sheet_name)
and left(end_date,4) in ('2021','2020','2019','2018','2017')
select * from public.sheet a inner join (
SELECT *
FROM crosstab(
  'select ts_code,end_date,max(扣非) 扣非  from profit_dedt where end_date>=''20210331'' group by ts_code,end_date order by 1,2 desc',
	'select distinct end_date from profit_dedt where end_date>=''20210331'' order by 1 desc') as ("ts_code" text, -- 结果集中指定拼接字段类型为text
      "20220630" numeric,
	  "20220331" numeric,
	  "20211231" numeric,
	  "20210930" numeric,
	  "20210630" numeric,
	  "20210331" numeric) ) b on a."证券代码"=b.ts_code where "证券代码"='002517.SZ' and ("wc除lt dedt"::numeric>=0.75 or "wc除lt dedt" is null)
	                         and ("流动比例"::numeric>1.5 or "流动比例" is null) 
	                         and ("应收账款风险"::numeric<0.5 or "应收账款风险" is null) 
	                         and ("商誉减值风险小"::numeric<0.25 or "商誉减值风险小" is null)
select a.ts_code,a. from (
SELECT *
FROM crosstab(
  'select ts_code,end_date,max(扣非) 扣非  from profit_dedt where end_date>=''20210331'' group by ts_code,end_date order by 1,2 desc',
	'select distinct end_date from profit_dedt where end_date>=''20210331'' order by 1 desc') as ("ts_code" text, -- 结果集中指定拼接字段类型为text
      "20220630" numeric,
	  "20220331" numeric,
	  "20211231" numeric,
	  "20210930" numeric,
	  "20210630" numeric,
	  "20210331" numeric))  a 
left join ( SELECT *
FROM crosstab(
  'select ts_code,end_date,max("净利润") 净利润  from total_revenue where end_date>=''20210331'' group by ts_code,end_date order by 1,2 desc',
	'select distinct end_date from total_revenue where end_date>=''20210331'' order by 1 desc') as ("ts_code" text, -- 结果集中指定拼接字段类型为text
      "20220630" numeric,
	  "20220331" numeric,
	  "20211231" numeric,
	  "20210930" numeric,
	  "20210630" numeric,
	  "20210331" numeric)
) b on a.ts_code=b.ts_code 
left join (SELECT *
FROM crosstab(
  'select ts_code,end_date,max("总营收") 总营收  from total_revenue where end_date>=''20210331'' group by ts_code,end_date order by 1,2 desc',
	'select distinct end_date from total_revenue where end_date>=''20210331'' order by 1 desc') as ("ts_code" text, -- 结果集中指定拼接字段类型为text
      "20220630" numeric,
	  "20220331" numeric,
	  "20211231" numeric,
	  "20210930" numeric,
	  "20210630" numeric,
	  "20210331" numeric)) c on a.ts_code=c.ts_code 



CREATE OR REPLACE FUNCTION iif(
    boolean,
    anyelement,
    anyelement)
  RETURNS anyelement AS
' SELECT CASE $1 WHEN TRUE THEN $2 ELSE $3 END '
  LANGUAGE sql IMMUTABLE;
  
  
  truncate table index_daily;
  select min(trade_date) from index_daily where ts_code = '000001.SH'
  --周一涨，周五涨跌的概率 涨54%，跌45% 基本保持50%左右
  ;with cte as 
  (select cal_date,pct_chg from public.trade_cal a inner join index_daily b on a.cal_date = b.trade_date where week='星期一' and pct_chg>0 and ts_code='000001.SH')
   select count(*),iif(b.pct_chg>0,'涨'::text,'跌'::text) as "chg" from public.trade_cal a inner join index_daily b on a.cal_date = b.trade_date where week='星期五' and ts_code='000001.SH'
   and exists (select * from cte c where to_date(c.cal_date,'yyyymmdd')+interval '4day'=to_date(b.trade_date,'yyyymmdd'))
   group by iif(b.pct_chg>0,'涨'::text,'跌'::text)
   --周五涨，周一涨跌的概率 涨59% 
     ;with cte as 
  (select cal_date,pct_chg from public.trade_cal a inner join index_daily b on a.cal_date = b.trade_date where week='星期五' and pct_chg>0 and ts_code='000001.SH')
   select count(*),iif(b.pct_chg>0,'涨'::text,'跌'::text) as "chg" from public.trade_cal a inner join index_daily b on a.cal_date = b.trade_date where week='星期一' and ts_code='000001.SH'
   and exists (select * from cte c where to_date(c.cal_date,'yyyymmdd')+interval '3day'=to_date(b.trade_date,'yyyymmdd'))
   group by iif(b.pct_chg>0,'涨'::text,'跌'::text)
   
   --11月 总指数涨的概率68%,上证 65%,上证50 63% 创业板 61%
   select * from (
   select sum(pct_chg),left(trade_date,6),ts_code from index_daily where SUBSTRING(trade_date,5,2)='11'  group by left(trade_date,6),ts_code
   ) a 
   select count(*) from public.trade_cal a inner join index_daily b on a.cal_date = b.trade_date where week='星期五' and ts_code='000001.SH'
   
   order by cal_date
   select * from public.trade_cal a inner join index_daily b on a.cal_date = b.trade_date where week='星期一' and pct_chg>0
   and cal_date='20080721'
  select a.cal_date+4,a.week,b.pct_chg from public.trade_cal a 
  inner join index_daily b on a.cal_date = b.trade_date
  where week in ('星期一','星期五') and pct_chg>0
  order by a.cal_date asc
  
  select to_date(cal_date,'yyyymmdd')  +  interval  '1day' nextday,cal_date from public.trade_cal 
  select distinct index_code,con_code from index_weight where con_code in (select distinct ts_code from stock_basic)
  select * from public.index_basic where name like '%创业板%'
  select weight,a.ts_code,name, from (
  select *,row_number() over (partition by index_code,con_code order by trade_date desc) cnt
  from index_weight a inner join stock_basic b on a.con_code = b.ts_code ) a
  left join public.daily_basic c on a.ts_code = c.ts_code
  where cnt=1
  order by weight desc
  with cte as 
  (
	  select pct_chg,trade_date from public.index_daily where ts_code ='399006.SZ' order by trade_date limit 1 
  )
  select * from cte a inner join public.index_daily
  select pct_chg,trade_date from public.index_daily where ts_code ='399006.SZ' order by trade_date limit 100
  
  select *,lag(sign,1) over (partition by ts_code order by trade_date) from (
  select to_date(trade_date,'yyyyMMDD'),ts_code,pct_chg,case when pct_chg<0 then 1 else 0 end sign
  from public.index_daily where ts_code ='399006.SZ' order by trade_date  limit 100
  ) a
  
  --排序表
  create temp table index_daily (pct_chg numeric,trade_date date ,row_num int)
  --临时结果表
  create temp table index_daily_temp (pct_chg numeric,trade_date date ,sgin int,row_num int)
  --取后一天涨跌标记在excel里筛选
  SELECT *,lead (sgin,1) over (order by row_num) FROM index_daily_temp
  --插入排序表
  INSERT INTO index_daily 
  SELECT pct_chg,to_date(trade_date,'yyyyMMDD'),ROW_NUMBER() OVER (ORDER BY TRADE_DATE) FROM  public.index_daily where ts_code ='399006.SZ'
  select * from index_daily
  --递归取后一天涨跌，大于0则标记0 小于0则标记1，连续的则相加，不连续的则置零
  WITH RECURSIVE t AS (
  select pct_chg,trade_date,case when pct_chg<0 then 1 else 0 end as sign,row_num from index_daily where trade_date='20100601' 
  UNION ALL
  select b.pct_chg,b.trade_date,case when b.pct_chg >0 then 0 else t.sign+case when b.pct_chg<0 then 1 else 0 end end as sign,b.row_num from t  inner join index_daily b on t.row_num+1=b.row_num   
)
insert into index_daily_temp
SELECT * FROM t;
  
  
  --月末效应，指季末年末或者长假前，因回收资金的需求导致大盘走低，到长假前一天开始上涨
  --年末
  with cte as (
  select cal_date,cnt from (
  select *,row_number() over (partition by EXTRACT (year FROM to_date(cal_date,'yyyyMMDD')) order by cal_date desc) cnt
  from public.trade_cal where EXTRACT (month FROM to_date(cal_date,'yyyyMMDD')) =12 and  EXTRACT (year FROM to_date(cal_date,'yyyyMMDD'))>=2000) a where cnt <=3
  )
  select trade_date,pct_chg,b.cnt from  public.index_daily a inner join cte b on a.trade_date = b.cal_date
  where a.ts_code = '000001.SH'
  
  
  
  and ts_code in ('399006.SZ','000001.SH')
  
  SELECT * FROM public.index_basic where  ts_code ='000001.SH'
	select * from public.index_basic  
	  select * from public.trade_cal where date_trunc('MONTH',to_date(cal_date,'yyyyMMDD'))=12
 select * from  public.index_daily where ts_code ='399006.SZ'
 truncate table public.index_daily
  
  --元旦后一周涨幅
  select trade_date,pct_chg from public.index_daily a where exists (
	  select * from (
  select min(cal_date) cal_date 
  from public.trade_cal where EXTRACT (month FROM to_date(cal_date,'yyyyMMDD'))=1 
  group by EXTRACT (year FROM to_date(cal_date,'yyyyMMDD')) ) b where a.trade_date=b.cal_date
  ) and ts_code ='000001.SH'
  
  select trade_date,pct_chg from public.index_daily a where exists (
	  select * from (
  select max(cal_date) cal_date 
  from public.trade_cal where EXTRACT (month FROM to_date(cal_date,'yyyyMMDD'))=12 
  group by EXTRACT (year FROM to_date(cal_date,'yyyyMMDD')) ) b where a.trade_date=b.cal_date
  ) and ts_code ='000001.SH'
  
  select to_date('20220101','yyyyMMDD')+1
  select * from public.trade_cal where SUBSTRING(cal_china,4,2)='01' order by cal_china desc
  select * from public.trade_cal where right(cal_china,4)='0101'
  select year,ts_code,name,sum(pct_chg::numeric) from (
  select left(a.trade_date,4) as "year",a.ts_code,c.name,pct_chg 
  from public.index_daily a inner join public.index_basic c 
  on a.ts_code=c.ts_code
  where a.ts_code in (select ts_code from public.index_basic where index_type='深证行业指数')
  and exists (select * from public.trade_cal b where right(cal_china,4)='0101'
			  			and  to_date(a.trade_date,'yyyyMMDD')>to_date(b.cal_date,'yyyyMMDD') 
			  			and to_date(a.trade_date,'yyyyMMDD')<to_date(b.cal_date,'yyyyMMDD')+18)
	  ) a
  group by year,ts_code,name
  select * from public.index_basic where index_type='中证主题指数'
select  from public.index_basic where ts_code in ()
and 
  select  a.ts_code,a.name,max(b.trade_date),min(b.trade_date)
  from public.index_basic a inner join public.index_daily b on a.ts_code=b.ts_code
  group by a.ts_code,a.name
  SELECT * FROM public.index_basic  where ts_code ='399006.SZ'
  select max(trade_date)from public.index_daily where ts_code ='399006.SZ'
  select concat((((open::numeric)/(close_1::numeric)-1)*100)::numeric(10,2),'%') "开盘涨幅",
  concat((((close::numeric)/(close_1::numeric)-1)*100)::numeric(10,2),'%') "收盘涨幅",trade_date,ts_code from (
select *,lag(close,1) over (order by trade_date ) close_1 from public.index_daily where ts_code  in ('399006.SZ')
) a where trade_date in (

select cal_date_1 from (
select *,lead(cal_date,1) over (order by cal_date ) cal_date_1
	from public.trade_cal where is_open=1
	) a where cal_date in ('20111130','20120217','20120511','20150213','20150417','20150825','20151023','20160229','20180417',
						   '20180622','20181007','20190104','20190906','20191231','20210709','20211206','20220415','20221125')
) order by trade_date


--利润表
with cte as (
select * from (
select symbol,report_date,coalesce(total_operate_income,operate_income) as total_operate_income, netprofit ,deduct_parent_netprofit ,
ROW_NUMBER() OVER (PARTITION BY symbol order by report_date desc)cnt from public.profit_sheet 
) a where cnt <=7
) 
select symbol,report_date,total_operate_income , CASE WHEN SUBSTRING(report_date, 6, 5)<>'03-31' THEN Lag(total_operate_income,1)  OVER (PARTITION BY symbol ORDER BY report_date ) ELSE 0 END,
round((total_operate_income-CASE WHEN SUBSTRING(report_date, 6, 5)<>'03-31' THEN Lag(total_operate_income,1)  OVER (PARTITION BY symbol ORDER BY report_date ) ELSE 0 END)/10000,2) as "总营收/亿",
round((netprofit-CASE WHEN SUBSTRING(report_date, 6, 5)<>'03-31' THEN Lag(netprofit,1)  OVER (PARTITION BY symbol ORDER BY report_date) ELSE 0 END)/10000,2) as "净利润/亿",
round((deduct_parent_netprofit-CASE WHEN SUBSTRING(report_date, 6, 5)<>'03-31' THEN Lag(deduct_parent_netprofit,1)  OVER (PARTITION BY symbol ORDER BY report_date) ELSE 0 END)/10000,2) as "扣非净利润/亿" from cte
order by symbol,report_date


SELECT symbol,report_period,营业收入,归属于上市公司股东的净利润,扣除非经常性损益后的净利润 into TEMPORARY forecast
FROM crosstab(
    -- 第一个参数：查询原始数据，按 symbol 和 report_period 分组
    $$
    SELECT
        symbol,
        report_period,
        forecast_indicator,
        forecast_value
    FROM
        financial_forecast
	
    ORDER BY
        1, 2, 3
    $$,
    -- 第二个参数：动态生成列名（forecast_indicator 的唯一值）
    $$
    SELECT DISTINCT forecast_indicator
    FROM financial_forecast
    ORDER BY 1
    $$
) AS ct (
    symbol TEXT,
    report_period TEXT,
   "主营业务收入" NUMERIC,
	"净利润" NUMERIC,
	"归属于上市公司股东的净利润" NUMERIC,
	"扣除后营业收入" NUMERIC,
	"扣除非经常性损益后的净利润" NUMERIC,
	"每股收益" NUMERIC,
	"营业收入" NUMERIC
);

with cte as (
select * from (
select symbol,report_date,round(coalesce(total_operate_income,operate_income)/10000,2) as total_operate_income, round(parent_netprofit/10000,2) as parent_netprofit,round(deduct_parent_netprofit/10000,2) as  deduct_parent_netprofit,
ROW_NUMBER() OVER (PARTITION BY symbol order by report_date desc)cnt from public.profit_sheet
) a where cnt <=7
)  ,cte1 as (
select symbol,report_date,total_operate_income,parent_netprofit,deduct_parent_netprofit
from cte a  where left(symbol,6) in (select symbol from financial_express where report_period = '20241231') 
or left(symbol,6) in (select symbol from financial_forecast where report_period = '20241231')
union all
select symbol || '.S' || CASE WHEN LEFT(symbol, 3) IN ('000', '300') THEN 'Z' ELSE 'H' END AS symbol,
CAST(report_period as timestamp)::VARCHAR ,NULLIF("营业收入", 0),round(归属于上市公司股东的净利润/100000000,2),round(扣除非经常性损益后的净利润/100000000,2) from forecast where report_period='20241231'
union all
select symbol || '.S' || CASE WHEN LEFT(symbol, 3) IN ('000', '300') THEN 'Z' ELSE 'H' END AS symbol,CAST(report_period as timestamp)::VARCHAR ,round(revenue/100000000,2),round(net_profit/100000000,2),0 
from financial_express where report_period = '20241231'
), cte2 as (
select symbol,report_date,
CASE WHEN total_operate_income =0 THEN 0 ELSE
round(total_operate_income-CASE WHEN SUBSTRING(report_date, 6, 5)<>'03-31' 
THEN Lag(total_operate_income,1)  OVER (PARTITION BY symbol ORDER BY report_date ) ELSE 0 END, 2) END as "总营收/亿",
CASE WHEN parent_netprofit =0 THEN 0 ELSE 
round((parent_netprofit-CASE WHEN SUBSTRING(report_date, 6, 5)<>'03-31' THEN Lag(parent_netprofit, 1)  OVER (PARTITION BY symbol ORDER BY report_date) ELSE 0 END),2) END as "净利润/亿",
CASE WHEN deduct_parent_netprofit =0 THEN 0 ELSE 
round((deduct_parent_netprofit-CASE WHEN SUBSTRING(report_date, 6, 5)<>'03-31' THEN Lag(deduct_parent_netprofit,1)  OVER (PARTITION BY symbol ORDER BY report_date) ELSE 0 END),2) END as "扣非净利润/亿" from cte1
order by symbol,report_date
)
select * into TEMPORARY cte2 from cte2
--删除新股数据，会有超过2年的日期
delete from cte2 where report_date < '2023-01-01'
delete from cte2 where "总营收/亿" is null and "净利润/亿" is null and "扣非净利润/亿" is null
-- 总营收
SELECT *
FROM crosstab(
  'SELECT symbol, report_date, "总营收/亿" FROM cte2 ORDER BY 1,2',
  'SELECT DISTINCT report_date FROM cte2 ORDER BY 1'
) AS pivot_table (
  symbol varchar,
  "2023-03-31" numeric,
  "2023-06-30" numeric,
  "2023-09-30" numeric,
  "2023-12-31" numeric,
  "2024-03-31" numeric,
  "2024-06-30" numeric,
  "2024-09-30" numeric,
  "2024-12-31" numeric
);

-- 对净利润进行行转列
SELECT *
FROM crosstab(
  'SELECT symbol, report_date, "净利润/亿" FROM cte2 ORDER BY 1,2',
  'SELECT DISTINCT report_date FROM cte2 ORDER BY 1'
) AS pivot_table (
  symbol varchar,
  "2023-03-31" numeric,
  "2023-06-30" numeric,
  "2023-09-30" numeric,
  "2023-12-31" numeric,
  "2024-03-31" numeric,
  "2024-06-30" numeric,
  "2024-09-30" numeric,
  "2024-12-31" numeric
);

-- 对扣非净利润进行行转列
SELECT *
FROM crosstab(
  'SELECT symbol, report_date, "扣非净利润/亿" FROM cte2 ORDER BY 1,2',
  'SELECT DISTINCT report_date FROM cte2 ORDER BY 1'
) AS pivot_table (
  symbol varchar,
  "2023-03-31" numeric,
  "2023-06-30" numeric,
  "2023-09-30" numeric,
  "2023-12-31" numeric,
  "2024-03-31" numeric,
  "2024-06-30" numeric,
  "2024-09-30" numeric,
  "2024-12-31" numeric
);
DROP TABLE CTE2