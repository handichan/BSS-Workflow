SELECT end_use,COUNT(DISTINCT("in.county")) as n_counties 
FROM {sector}_hourly_{suffix}
WHERE multiplier_hourly=multiplier_hourly GROUP BY end_use;