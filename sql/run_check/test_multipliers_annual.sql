SELECT "in.state", sector, group_ann, end_use, fuel, SUM(multiplier_annual) as multiplier_sum, COUNT(DISTINCT("in.county")) as n_counties 
FROM {mult_com_annual}
WHERE multiplier_annual=multiplier_annual 
GROUP BY "in.state", sector, group_ann, end_use, fuel

UNION ALL

SELECT "in.state", sector, group_ann, end_use, fuel, SUM(multiplier_annual) as multiplier_sum, COUNT(DISTINCT("in.county")) as n_counties 
FROM {mult_res_annual}
WHERE multiplier_annual=multiplier_annual 
GROUP BY "in.state", sector, group_ann, end_use, fuel;

;
