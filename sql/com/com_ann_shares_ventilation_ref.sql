INSERT INTO {mult_com_annual}

WITH meta_filtered AS (
	SELECT meta."in.nhgis_county_gisjoin",
	    meta."in.state",
	    'com_hvac_ann_41' AS group_ann,
		sum(meta."calc.weighted.electricity.fans.energy_consumption..tbtu") as ventilation
	FROM "{meta_com}" as meta
	WHERE meta.upgrade = 0
	GROUP BY 
		meta."in.nhgis_county_gisjoin",
		meta."in.state"
)
    SELECT 
    "in.nhgis_county_gisjoin" as "in.county",
    group_ann,
    ventilation / sum(ventilation) OVER (PARTITION BY "in.state", group_ann) as multiplier_annual,
    'com' AS sector,
    "in.state",
    'Ventilation' AS end_use
FROM meta_filtered
;