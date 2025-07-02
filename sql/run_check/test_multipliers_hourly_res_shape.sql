with n_weath as(
SELECT shape_ts,COUNT(DISTINCT("in.weather_file_city","in.state")) as n_weather 
FROM res_hourly_disaggregation_multipliers_20250616_amy 
WHERE multiplier_hourly=multiplier_hourly 
GROUP BY shape_ts)
SELECT * from n_weath 
WHERE n_weather!=1215 
ORDER BY shape_ts;