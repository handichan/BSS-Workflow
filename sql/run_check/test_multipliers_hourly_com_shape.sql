with n_count as(
SELECT shape_ts,COUNT(DISTINCT("in.county")) as n_counties 
FROM com_hourly_disaggregation_multipliers_20250616_amy 
WHERE multiplier_hourly=multiplier_hourly 
AND "in.state" != 'AK' 
AND "in.state" != 'HI' 
GROUP BY shape_ts)
SELECT * from n_count 
WHERE n_counties!=3086 
ORDER BY shape_ts;