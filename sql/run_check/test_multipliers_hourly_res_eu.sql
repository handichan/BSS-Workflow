with n_eu as(
SELECT "in.state", "in.weather_file_city",COUNT(DISTINCT(end_use)) as n_uses 
FROM res_hourly_disaggregation_multipliers_20250616_amy 
WHERE multiplier_hourly=multiplier_hourly 
GROUP BY "in.state","in.weather_file_city"),
insuff as(
SELECT * from n_eu 
WHERE n_uses!=8)
SELECT insuff.*, geo_map.county_name, geo_map.population
FROM insuff
LEFT JOIN geo_map
ON insuff."in.county"=geo_map."stock.county"
ORDER BY geo_map.population;