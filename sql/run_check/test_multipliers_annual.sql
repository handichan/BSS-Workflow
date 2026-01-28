SELECT end_use,COUNT(DISTINCT("in.county")) as n_counties 
FROM {sector}_annual_{suffix}
WHERE multiplier_annual=multiplier_annual GROUP BY end_use;