INSERT INTO {mult_com_annual}

WITH meta_filtered AS (
	SELECT meta."in.nhgis_county_gisjoin",
	    meta."in.state",
	    'com_hvac_ann_42' AS group_ann,
		sum(meta."calc.weighted.electricity.heating.energy_consumption..tbtu" + meta."calc.weighted.electricity.heat_recovery.energy_consumption..tbtu") as heating
	FROM "{meta_com}" as meta
	WHERE meta.upgrade = 0
	GROUP BY 
		meta."in.nhgis_county_gisjoin",
		meta."in.state"
)
    SELECT 
    "in.nhgis_county_gisjoin" as "in.county",
    group_ann,
    heating / sum(heating) OVER (PARTITION BY "in.state", group_ann) as multiplier_annual,
    'com' AS sector,
    "in.state",
    'Heating (Equip.)' AS end_use
FROM meta_filtered
;