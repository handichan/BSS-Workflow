SELECT end_use,COUNT(DISTINCT("in.weather_file_city")) as n_weatherfile_city 
FROM SECTORID_hourly_disaggregation_multipliers_VERSIONID
WHERE multiplier_hourly=multiplier_hourly GROUP BY end_use;