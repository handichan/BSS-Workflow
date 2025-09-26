with n_eu as(
SELECT "in.state", "in.county",COUNT(DISTINCT(end_use)) as n_uses 
FROM com_hourly_disaggregation_multipliers_{version}
WHERE multiplier_hourly=multiplier_hourly 
AND "in.state" != 'AK' 
AND "in.state" !='HI' 
GROUP BY "in.state","in.county"),
insuff as(
SELECT * from n_eu 
WHERE n_uses!=9 )
SELECT insuff.*, geo_map.county_name, geo_map.population
FROM insuff
LEFT JOIN geo_map
ON insuff."in.county"=geo_map."stock.county"
ORDER BY geo_map.population;