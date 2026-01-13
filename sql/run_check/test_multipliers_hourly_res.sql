SELECT "in.weather_file_longitude", "in.weather_file_city", shape_ts, end_use, fuel, SUM(multiplier_hourly) as multiplier_sum
FROM res_hourly_disaggregation_multipliers_{version}
WHERE multiplier_hourly=multiplier_hourly 
GROUP BY "in.weather_file_longitude", "in.weather_file_city", shape_ts, end_use, fuel;