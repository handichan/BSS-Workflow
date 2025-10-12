-- county annual end use totals

SELECT "in.county",turnover,"in.state","year",sector,end_use,sum(county_hourly_cal_kwh) as county_ann_kwh 
FROM long_county_hourly_{turnover}_{weather} 
WHERE county_hourly_cal_kwh >= 0
GROUP BY "in.county",turnover,"in.state","year",sector,end_use
;