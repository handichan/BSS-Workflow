SELECT end_use,fuel,COUNT(DISTINCT("in.county")) as n_counties 
FROM {sector}_hourly_disaggregation_multipliers_{version}
WHERE multiplier_hourly=multiplier_hourly GROUP BY end_use, fuel;