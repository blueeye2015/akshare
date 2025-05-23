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
) a where cnt <=8 and symbol in (select symbol from public.profit_sheet where report_date='2024-12-31 00:00:00')
) 
select symbol,report_date,total_operate_income , CASE WHEN SUBSTRING(report_date, 6, 5)<>'03-31' THEN Lag(total_operate_income,1)  OVER (PARTITION BY symbol ORDER BY report_date ) ELSE 0 END,
round((total_operate_income-CASE WHEN SUBSTRING(report_date, 6, 5)<>'03-31' THEN Lag(total_operate_income,1)  OVER (PARTITION BY symbol ORDER BY report_date ) ELSE 0 END)/10000,2) as "总营收/亿",
round((netprofit-CASE WHEN SUBSTRING(report_date, 6, 5)<>'03-31' THEN Lag(netprofit,1)  OVER (PARTITION BY symbol ORDER BY report_date) ELSE 0 END)/10000,2) as "净利润/亿",
round((deduct_parent_netprofit-CASE WHEN SUBSTRING(report_date, 6, 5)<>'03-31' THEN Lag(deduct_parent_netprofit,1)  OVER (PARTITION BY symbol ORDER BY report_date) ELSE 0 END)/10000,2) as "扣非净利润/亿" 
into TEMPORARY cte1
from cte
order by symbol,report_date
delete from cte1 where report_date < '2023-03-31'


SELECT split_part(row_id, '_', 1) as symbol,
  split_part(row_id, '_', 2) as report_period,
  营业收入,归属于上市公司股东的净利润,扣除非经常性损益后的净利润 into TEMPORARY forecast
FROM crosstab(
    -- 第一个参数：查询原始数据，按 symbol 和 report_period 分组
    $$
    SELECT
        symbol || '_' || report_period as row_id,
        forecast_indicator,
        forecast_value
    FROM
        (SELECT *,
		 		ROW_NUMBER() OVER (PARTITION BY symbol,report_period,forecast_indicator 
				ORDER BY announce_date desc) CNT 
		 FROM public.performance_forecast) a
	WHERE CNT = 1	
    ORDER BY
        1, 2, 3
    $$,
    -- 第二个参数：动态生成列名（forecast_indicator 的唯一值）
    $$
	SELECT forecast_indicator FROM (
    SELECT DISTINCT forecast_indicator,
			 CASE WHEN forecast_indicator = '营业收入' THEN 1 
				  WHEN forecast_indicator = '归属于上市公司股东的净利润' THEN 2
				  ELSE 3 END as sortorder
    FROM performance_forecast 
	WHERE forecast_indicator IN ('归属于上市公司股东的净利润','扣除非经常性损益后的净利润','营业收入')
    ) a ORDER BY sortorder
    $$
) AS ct (
    row_id TEXT,
    "营业收入" NUMERIC,
	"归属于上市公司股东的净利润" NUMERIC,
	"扣除非经常性损益后的净利润" NUMERIC
);

with cte as (
select * from (
select symbol,report_date,round(coalesce(total_operate_income,operate_income)/10000,2) as total_operate_income, round(parent_netprofit/10000,2) as parent_netprofit,round(deduct_parent_netprofit/10000,2) as  deduct_parent_netprofit,
ROW_NUMBER() OVER (PARTITION BY symbol order by report_date desc)cnt,'profit' as 'tabletype' from public.profit_sheet
) a where cnt <=8
)  ,cte1 as (
select symbol,report_date,total_operate_income,parent_netprofit,deduct_parent_netprofit,tabletype
from cte a
union all
select 	symbol || '.S' || CASE WHEN LEFT(symbol, 2) IN ('00', '30') THEN 'Z' ELSE 'H' END AS symbol,
		CAST(report_period as timestamp)::VARCHAR ,NULLIF(round("营业收入"/100000000,2), 0),
		round(归属于上市公司股东的净利润/100000000,2),round(扣除非经常性损益后的净利润/100000000,2),'forecast' 
from forecast 
union all
select 	symbol || '.S' || CASE WHEN LEFT(symbol, 2) IN ('00', '30') THEN 'Z' ELSE 'H' END AS symbol,
		CAST(report_period as timestamp)::VARCHAR ,round(revenue/100000000,2),round(net_profit/100000000,2),0,'express' 
from 	performance_express 
), cte2 as (
select symbol,report_date,
NULLIF(CASE WHEN total_operate_income =0 THEN 0 ELSE
round(total_operate_income-CASE WHEN SUBSTRING(report_date, 6, 5)<>'03-31' 
THEN Lag(total_operate_income,1)  OVER (PARTITION BY symbol ORDER BY report_date ) ELSE 0 END, 2) END,0) as "总营收/亿",
NULLIF(CASE WHEN parent_netprofit =0 THEN 0 ELSE 
round((parent_netprofit-CASE WHEN SUBSTRING(report_date, 6, 5)<>'03-31' THEN Lag(parent_netprofit, 1)  OVER (PARTITION BY symbol ORDER BY report_date) ELSE 0 END),2) END,0) as "净利润/亿",
NULLIF(CASE WHEN deduct_parent_netprofit =0 THEN 0 ELSE 
round((deduct_parent_netprofit-CASE WHEN SUBSTRING(report_date, 6, 5)<>'03-31' THEN Lag(deduct_parent_netprofit,1)  OVER (PARTITION BY symbol ORDER BY report_date) ELSE 0 END),2) END,0) as "扣非净利润/亿" from cte1
order by symbol,report_date
)
select * into TEMPORARY cte2 from cte2
--删除新股数据，会有超过2年的日期
delete from cte2 where report_date < '2023-01-01'
delete from cte2 where "总营收/亿" is null and "净利润/亿" is null and "扣非净利润/亿" is null
delete from cte2 where left(symbol,1) in ('8','9')
-- 总营收
SELECT *
FROM crosstab(
  'SELECT symbol, report_date, "总营收/亿" FROM cte2 where  report_date::timestamp>=''2023-06-30'' and  report_date::timestamp<=''2025-03-31'' ORDER BY 1,2',
  'SELECT DISTINCT report_date FROM cte2 where  report_date::timestamp>=''2023-06-30'' and  report_date::timestamp<=''2025-03-31'' ORDER BY 1'
) AS pivot_table (
  symbol varchar,
  "2023-06-30" numeric,
  "2023-09-30" numeric,
  "2023-12-31" numeric,
  "2024-03-31" numeric,
  "2024-06-30" numeric,
  "2024-09-30" numeric,
  "2024-12-31" numeric,
  "2025-03-31" numeric
);

-- 对净利润进行行转列
SELECT *
FROM crosstab(
  'SELECT symbol, report_date, "净利润/亿" FROM cte2 where  report_date::timestamp>=''2023-06-30'' and  report_date::timestamp<=''2025-03-31'' ORDER BY 1,2',
  'SELECT DISTINCT report_date FROM cte2 where  report_date::timestamp>=''2023-06-30'' and  report_date::timestamp<=''2025-03-31'' ORDER BY 1'
) AS pivot_table (
  symbol varchar,
  "2023-06-30" numeric,
  "2023-09-30" numeric,
  "2023-12-31" numeric,
  "2024-03-31" numeric,
  "2024-06-30" numeric,
  "2024-09-30" numeric,
  "2024-12-31" numeric,
  "2025-03-31" numeric
);

-- 对扣非净利润进行行转列
SELECT *
FROM crosstab(
  'SELECT symbol, report_date, "扣非净利润/亿" FROM cte2 where  report_date::timestamp>=''2023-06-30'' and  report_date::timestamp<=''2025-03-31'' ORDER BY 1,2',
  'SELECT DISTINCT report_date FROM cte2 where  report_date::timestamp>=''2023-06-30'' and  report_date::timestamp<=''2025-03-31'' ORDER BY 1'
) AS pivot_table (
  symbol varchar,
  "2023-06-30" numeric,
  "2023-09-30" numeric,
  "2023-12-31" numeric,
  "2024-03-31" numeric,
  "2024-06-30" numeric,
  "2024-09-30" numeric,
  "2024-12-31" numeric,
  "2025-03-31" numeric
);
DROP TABLE CTE2
drop table forecast


select COALESCE(is_nan_or_null(total_current_assets)/nullif(is_nan_or_null(total_current_liab),0),0) as "流动比例",
COALESCE((is_nan_or_null(accounts_rece)+is_nan_or_null(note_rece)+is_nan_or_null(other_rece))/10000/is_nan_or_null(total_operate_income),0) as "应收账款风险",
COALESCE(( is_nan_or_null(goodwill)+is_nan_or_null(intangible_assets)+is_nan_or_null(develop_expense) )/nullif((is_nan_or_null(total_parent_equity)-is_nan_or_null(preferred_stock)-is_nan_or_null(perpetual_bond)),0),0) as "商誉减值风险小"
,COALESCE((is_nan_or_null(total_current_assets)-is_nan_or_null(total_current_liab))/nullif((is_nan_or_null(long_loan)+is_nan_or_null(bonds_payable)),0),0) as "wc除lt dedt",
* from (
select symbol,report_date,total_current_assets,total_current_liab,goodwill,intangible_assets,long_loan,bonds_payable,long_payable,
develop_expense,long_rece,preferred_stock,perpetual_bond,accounts_rece,total_parent_equity,note_rece,other_rece,ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY report_date desc)cnt 
from balance_sheet 
) a CROSS JOIN LATERAL (select total_operate_income from public.profit_sheet where symbol = a.symbol ORDER BY report_date desc limit 1) c where cnt =1 and a.symbol = '300502.SZ'

select symbol || '.S' || CASE WHEN LEFT(symbol, 3) IN ('000', '300') THEN 'Z' ELSE 'H' END AS symbol,round(total_mv/10000,2) from public.stock_indicator where trade_date =(select max(trade_date) from stock_indicator)


select symbol || '.S' || CASE WHEN LEFT(symbol, 3) IN ('000', '300') THEN 'Z' ELSE 'H' END AS symbol,round(total_mv/10000,2) from public.stock_indicator where trade_date =(select max(trade_date) from stock_indicator)

SELECT 
  split_part(row_id, '_', 1) as symbol,
  split_part(row_id, '_', 2) as report_period,
  "营业收入",
  "归属于上市公司股东的净利润",
  "扣除非经常性损益后的净利润",
  "每股收益"

FROM crosstab(
  $$
  SELECT
    symbol || '_' || report_period as row_id,
    forecast_indicator,
    forecast_value
  FROM
    performance_forecast
  WHERE 
    symbol='002204'
  ORDER BY
    1, 2
  $$,
  $$
  SELECT unnest(ARRAY['营业收入', '归属于上市公司股东的净利润', '扣除非经常性损益后的净利润', '每股收益'])
  $$
) AS ct (
  row_id TEXT,
  "营业收入" NUMERIC,
  "归属于上市公司股东的净利润" NUMERIC,
  "扣除非经常性损益后的净利润" NUMERIC,
  "每股收益" NUMERIC
);
WITH quarterly_periods AS (
    -- 定义季度期间，用于后续计算
    SELECT 
        2025 as year, 1 as quarter, '2025-03-31' as end_date UNION ALL
        SELECT 2024, 4, '2024-12-31' UNION ALL
        SELECT 2024, 3, '2024-09-30' UNION ALL
        SELECT 2024, 2, '2024-06-30' UNION ALL
        SELECT 2024, 1, '2024-03-31' UNION ALL
        SELECT 2023, 4, '2023-12-31'
),

latest_balance AS (
    -- 获取资产负债表最新季度数据
    SELECT fs.*,b.code as symbol, b.name as security_name_abbr
    FROM (
        SELECT 
            *,
            ROW_NUMBER() OVER (PARTITION BY ts_code ORDER BY end_date DESC,update_flag DESC) as rn
        FROM balancesheet 
     
    ) fs join stock_info_a_code_name b on fs.ts_code = b.code
    WHERE fs.rn = 1
),

profit_with_period AS (
    -- 为每个利润表数据标记其所属季度，先转换日期格式
    SELECT 
        p.*,
        -- 尝试转换日期，如果失败则使用备选方法
        CASE 
            WHEN EXTRACT(MONTH FROM CAST(report_date AS DATE)) = 3 THEN 1
            WHEN EXTRACT(MONTH FROM CAST(report_date AS DATE)) = 6 THEN 2
            WHEN EXTRACT(MONTH FROM CAST(report_date AS DATE)) = 9 THEN 3
            WHEN EXTRACT(MONTH FROM CAST(report_date AS DATE)) = 12 THEN 4
        END as quarter,
        EXTRACT(YEAR FROM CAST(report_date AS DATE)) as year
    FROM profit_sheet p
),

profit_with_quarterly_data AS (
    -- 使用窗口函数计算单季度数据
    SELECT 
        p.*,
        -- 计算单季度营收：当前累计值减去上一季度累计值（如果是第一季度则直接使用累计值）
        CASE 
            WHEN quarter = 1 THEN COALESCE(total_operate_income, 0)
            ELSE COALESCE(total_operate_income, 0) - COALESCE(LAG(total_operate_income, 1, 0) OVER (
                PARTITION BY symbol, year 
                ORDER BY quarter
            ), 0)
        END as quarterly_revenue,
        
        -- 计算单季度净利润：当前累计值减去上一季度累计值（如果是第一季度则直接使用累计值）
        CASE 
            WHEN quarter = 1 THEN COALESCE(parent_netprofit, 0)
            ELSE COALESCE(parent_netprofit, 0) - COALESCE(LAG(parent_netprofit, 1, 0) OVER (
                PARTITION BY symbol, year 
                ORDER BY quarter
            ), 0)
        END as quarterly_parent_netprofit
    FROM profit_with_period p
    WHERE quarter IS NOT NULL AND year IS NOT NULL
),

quarterly_profit AS (
    -- 筛选需要的季度数据
    SELECT *
    FROM profit_with_quarterly_data
    WHERE (year = 2025 AND quarter = 1) OR
          (year = 2024 AND quarter BETWEEN 1 AND 4) OR
          (year = 2023 AND quarter = 4)
),

latest_quarterly_profit AS (
    -- 获取最新季度的单季利润数据
    SELECT qp.*
    FROM (
        SELECT 
            *,
            ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY year DESC, quarter DESC) as rn
        FROM quarterly_profit
        WHERE (year = 2025 AND quarter = 1)
    ) qp
    WHERE qp.rn = 1
),

ltm_profit AS (
    -- 计算最近12个月利润 (LTM)
    SELECT 
        symbol,
        SUM(quarterly_parent_netprofit) as ltm_parent_netprofit,
        SUM(quarterly_revenue) as ltm_revenue
    FROM quarterly_profit
    WHERE (year = 2024 AND quarter BETWEEN 2 AND 4) OR
          (year = 2025 AND quarter = 1)
    GROUP BY symbol
),

prev_year_quarter AS (
    -- 获取去年同期单季数据
    SELECT qp.*
    FROM (
        SELECT 
            *,
            ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY report_date DESC) as rn
        FROM quarterly_profit
        WHERE (year = 2024 AND quarter = 1)
    ) qp
    WHERE qp.rn = 1
),

financial_health AS (
    SELECT 
        lb.symbol,
        lb.security_name_abbr as security_name,
        
        -- 1. 财务指标：(应收账款、票据+其它)/营收(LTM) < 50%
        -- 根据新表结构调整字段名
        CASE 
            WHEN is_nan_or_null(ltp.ltm_revenue) = 0 THEN NULL  -- 避免除以零
            ELSE (is_nan_or_null(lb.accounts_receiv) + is_nan_or_null(lb.notes_receiv) + is_nan_or_null(lb.oth_receiv))/10000 / 
                 is_nan_or_null(ltp.ltm_revenue) * 100  -- 如果ltm_revenue为NULL，使用1避免除以零
        END as receivables_to_revenue,
            
        -- 2. 财务指标：流动资产/流动负债 > 1.50
        CASE 
            WHEN is_nan_or_null(lb.total_cur_liab) = 0 THEN NULL  -- 避免除以零
            ELSE is_nan_or_null(lb.total_cur_assets) / is_nan_or_null(lb.total_cur_liab)
        END as current_ratio,
        
        -- 3. 稳健指标：(商誉+无形资产)/(净资产) < 25%
        CASE 
            WHEN is_nan_or_null(lb.total_hldr_eqy_exc_min_int) = 0 THEN NULL  -- 避免除以零
            ELSE (is_nan_or_null(lb.goodwill) + is_nan_or_null(lb.intan_assets)) / 
                 is_nan_or_null(lb.total_hldr_eqy_exc_min_int) * 100
        END as intangible_to_equity,
            
        -- 4. 健康指标：(流动资产-流动负债)/(长期借款+应付债券) > 75%
        CASE 
            WHEN is_nan_or_null(lb.lt_borr) + is_nan_or_null(lb.bond_payable) = 0 THEN NULL  -- 避免除以零
            ELSE (is_nan_or_null(lb.total_cur_assets) - is_nan_or_null(lb.total_cur_liab)) / 
                 is_nan_or_null(NULLIF(is_nan_or_null(lb.lt_borr) + is_nan_or_null(lb.bond_payable), 0)) * 100
        END as working_capital_to_long_debt,
            
        -- 5. 业绩指标：归属母公司股东净利润(LTM) > 0
        is_nan_or_null(ltp.ltm_parent_netprofit) as ltm_parent_netprofit,
        
        -- 6. 季度归属母公司股东净利润 > 0
        is_nan_or_null(lqp.quarterly_parent_netprofit) as quarterly_parent_netprofit,
        
        -- 7. 营收同比增长
        is_nan_or_null(lqp.quarterly_revenue) as current_revenue,
        is_nan_or_null(pyq.quarterly_revenue) as prev_year_revenue,
        
        -- 8. 净利润同比增长
        is_nan_or_null(lqp.quarterly_parent_netprofit) as current_netprofit,
        is_nan_or_null(pyq.quarterly_parent_netprofit) as prev_year_netprofit,
        
        -- 添加报告日期信息，便于调试
        lb.end_date as balance_report_date,
        lqp.report_date as current_profit_report_date,
        pyq.report_date as prev_year_report_date
        
    FROM latest_balance lb
    JOIN latest_quarterly_profit lqp ON lb.symbol = lqp.symbol
    LEFT JOIN ltm_profit ltp ON lb.symbol = ltp.symbol
    LEFT JOIN prev_year_quarter pyq ON lb.symbol = pyq.symbol
)
insert into financial_health
SELECT 
    symbol,
    security_name,
    receivables_to_revenue,
    current_ratio,
    intangible_to_equity,
    working_capital_to_long_debt,
    ltm_parent_netprofit/100000000 as ltm_parent_netprofit_billion,
    quarterly_parent_netprofit/100000000 as quarterly_parent_netprofit_billion,
    current_revenue/100000000 as current_revenue_billion,
    prev_year_revenue/100000000 as prev_year_revenue_billion,
    current_netprofit/100000000 as current_netprofit_billion,
    prev_year_netprofit/100000000 as prev_year_netprofit_billion,
    balance_report_date,
    current_profit_report_date,
    prev_year_report_date 
FROM financial_health
WHERE 
    -- 符合骑A指数的筛选条件，添加NULL值检查
    (receivables_to_revenue < 50 OR receivables_to_revenue IS NULL) AND
    (current_ratio > 1.50 OR current_ratio IS NULL) AND
    (intangible_to_equity < 25 OR intangible_to_equity IS NULL) AND
    (working_capital_to_long_debt > 75 OR working_capital_to_long_debt IS NULL) AND
    ltm_parent_netprofit > 0 AND
    quarterly_parent_netprofit > 0 AND
    current_revenue > prev_year_revenue AND
    current_netprofit > prev_year_netprofit
ORDER BY ltm_parent_netprofit DESC;



-- 计算特定交易日的统A指数PE/PB/PS值
WITH valid_stocks AS (
    -- 筛选出有效的股票数据（排除负值）
    SELECT 
        trade_date,
        -- PE值：剔除负值和异常值
        CASE WHEN pe > 0 AND pe < 1000 THEN pe ELSE NULL END AS valid_pe,
        CASE WHEN pe_ttm > 0 AND pe_ttm < 1000 THEN pe_ttm ELSE NULL END AS valid_pe_ttm,
        -- PB值：剔除负值和异常值
        CASE WHEN pb > 0 AND pb < 100 THEN pb ELSE NULL END AS valid_pb,
        -- PS值：剔除负值和异常值
        CASE WHEN ps > 0 AND ps < 100 THEN ps ELSE NULL END AS valid_ps,
        CASE WHEN ps_ttm > 0 AND ps_ttm < 100 THEN ps_ttm ELSE NULL END AS valid_ps_ttm
    FROM 
        public.daily_basic
	where ts_code in (select symbol from financial_health)
)
insert into all_stock_index_basic
SELECT 
    trade_date,
    -- 计算PE中位数
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY valid_pe) AS pe_median,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY valid_pe_ttm) AS pe_ttm_median,
    -- 计算PB中位数
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY valid_pb) AS pb_median,
    -- 计算PS中位数
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY valid_ps) AS ps_median,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY valid_ps_ttm) AS ps_ttm_median,
    -- 计算有效样本数量
    COUNT(valid_pe) AS valid_pe_count,
    COUNT(valid_pe_ttm) AS valid_pe_ttm_count,
    COUNT(valid_pb) AS valid_pb_count,
    COUNT(valid_ps) AS valid_ps_count,
    COUNT(valid_ps_ttm) AS valid_ps_ttm_count,
    -- 计算总样本数量
    COUNT(*) AS total_stocks 
	
FROM 
    valid_stocks
GROUP BY 
    trade_date;