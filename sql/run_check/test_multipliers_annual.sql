SELECT end_use,COUNT(DISTINCT("in.county")) as n_counties 
FROM {sector}_annual_disaggregation_multipliers_{version}
WHERE multiplier_annual=multiplier_annual GROUP BY end_use;