
import tushare as ts
TUSHARE_TOKEN = '0c7918f3c7233d240c1e0271c00e6bf8d0cf947863b01d63b2c8ecf3'
pro = ts.pro_api(TUSHARE_TOKEN)
df = pro.namechange(ts_code='002052.SZ', fields='ts_code,name,start_date,end_date,change_reason')
print (df)