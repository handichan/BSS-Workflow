SELECT end_use,COUNT(DISTINCT("in.county")) as n_counties 
FROM SECTORID_annual_disaggregation_multipliers_VERSIONID
WHERE multiplier_annual=multiplier_annual GROUP BY end_use;