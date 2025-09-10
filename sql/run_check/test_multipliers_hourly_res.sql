SELECT end_use,COUNT(DISTINCT("in.weather_file_city","in.state")) as n_weatherfile_city 
FROM {sector}_hourly_disaggregation_multipliers_{version}
WHERE multiplier_hourly=multiplier_hourly GROUP BY end_use;