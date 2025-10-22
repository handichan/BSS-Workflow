with n_eu as(
SELECT "in.state", "in.weather_file_city",COUNT(DISTINCT(end_use)) as n_uses 
FROM res_hourly_disaggregation_multipliers_{version}
FROM res_hourly_disaggregation_multipliers_{version}
WHERE multiplier_hourly=multiplier_hourly 
GROUP BY "in.state","in.weather_file_city")
SELECT * from n_eu 
WHERE n_uses!=8;