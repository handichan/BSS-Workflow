-- county annual end use totals

SELECT "in.county",turnover,"in.state","year",sector,end_use,sum(county_ann_kwh) as county_ann_kwh 
FROM long_county_annual_{turnover}_amy -- variable?
WHERE county_ann_kwh >= 0
GROUP BY "in.county",turnover,"in.state","year",sector,end_use
;