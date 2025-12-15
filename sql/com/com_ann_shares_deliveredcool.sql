INSERT INTO com_annual_disaggregation_multipliers_{version}

WITH meta_filtered AS (
	SELECT meta."in.nhgis_county_gisjoin",
	    meta."in.state",
		sum(meta.weight * (meta."out.params.dx_cooling_load..j" + meta."out.params.heat_pump_cooling_total_load..j" + meta."out.params.vrf_total_cooling_load..j" + meta."out.params.wa_hp_cooling_load..j")) as cooling
	FROM "comstock_2025.1_parquet" as meta
	WHERE meta.upgrade = 0
	GROUP BY 
		meta."in.nhgis_county_gisjoin",
		meta."in.state"
)
    SELECT 
    "in.nhgis_county_gisjoin" as "in.county",
	'com_hvac_ann_43' AS group_ann,
    cooling / sum(cooling) OVER (PARTITION BY "in.state") as multiplier_annual,
    'com' AS sector,
    "in.state",
    'Cooling (Equip.)' AS end_use,
	'Electric' AS fuel
FROM meta_filtered

UNION ALL 
    SELECT 
    "in.nhgis_county_gisjoin" as "in.county",
	'com_hvac_ann_43' AS group_ann,
    cooling / sum(cooling) OVER (PARTITION BY "in.state") as multiplier_annual,
    'com' AS sector,
    "in.state",
    'Cooling (Equip.)' AS end_use,
	'Natural Gas' AS fuel
FROM meta_filtered

;