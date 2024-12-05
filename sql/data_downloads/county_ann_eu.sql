-- county annual end use totals

SELECT "in.county",turnover,"in.state","year",sector,end_use,sum(county_ann_kwh) as county_ann_kwh 
FROM county_annual_breakthrough_amy -- variable?
WHERE county_ann_kwh >= 0
GROUP BY "in.county",turnover,"in.state","year",sector,end_use

UNION all
SELECT "in.county",turnover,"in.state","year",sector,end_use,sum(county_ann_kwh) as county_ann_kwh 
FROM county_annual_ineff_amy -- variable?
WHERE turnover!='baseline' -- don't need the baseline from all scenarios
AND county_ann_kwh >= 0
GROUP BY "in.county",turnover,"in.state","year",sector,end_use
;