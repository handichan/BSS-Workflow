SELECT end_use,COUNT(DISTINCT("in.county")) as n_counties 
FROM SECTORID_hourly_disaggregation_multipliers_20240923 
WHERE multiplier_hourly=multiplier_hourly GROUP BY end_use;