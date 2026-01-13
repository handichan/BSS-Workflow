SELECT "in.state", group_ann, end_use, fuel, SUM(multiplier_annual) as multiplier_sum, COUNT(DISTINCT("in.county")) as n_counties 
FROM {sector}_annual_disaggregation_multipliers_{version}
WHERE multiplier_annual=multiplier_annual 
GROUP BY "in.state", group_ann, end_use, fuel;