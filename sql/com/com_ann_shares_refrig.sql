INSERT INTO {mult_com_annual}

WITH meta_filtered AS (
	SELECT meta."in.nhgis_county_gisjoin",
	    meta."in.state",
	    'com_refrig_ann_1' AS group_ann,
		sum(meta."calc.weighted.electricity.refrigeration.energy_consumption..tbtu") as refrigeration
	FROM "{meta_com}" as meta
	WHERE meta.upgrade = 0
	GROUP BY 
		meta."in.nhgis_county_gisjoin",
		meta."in.state"
)
    SELECT 
    "in.nhgis_county_gisjoin" as "in.county",
    group_ann,
    refrigeration / sum(refrigeration) OVER (PARTITION BY "in.state", group_ann) as multiplier_annual,
    'com' AS sector,
    "in.state",
    'Refrigeration' AS end_use
FROM meta_filtered
;