-- county annual end use totals

SELECT "in.county",
turnover,
"in.state",
"year",
sector,
end_use,
fuel, 
sum(county_hourly_cal_kwh) as county_ann_kwh 
FROM long_county_hourly_{turnover}_{disag_id} 
WHERE county_hourly_cal_kwh >= 0
GROUP BY "in.county",
turnover,
"in.state",
"year",
sector,
end_use,
fuel
;