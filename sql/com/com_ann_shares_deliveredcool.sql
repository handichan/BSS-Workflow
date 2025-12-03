INSERT INTO {mult_com_annual}

WITH meta_filtered AS (
	SELECT meta."in.nhgis_county_gisjoin",
	    meta."in.state",
	    'com_hvac_ann_43' AS group_ann,
		sum(meta."out.params.dx_cooling_load..j" + meta."out.params.heat_pump_cooling_total_load..j" + meta."out.params.vrf_total_cooling_load..j" + meta."out.params.wa_hp_cooling_load..j") as cooling
	FROM "{meta_com}" as meta
	WHERE meta.upgrade = 0
	GROUP BY 
		meta."in.nhgis_county_gisjoin",
		meta."in.state"
)
    SELECT 
    "in.nhgis_county_gisjoin" as "in.county",
    group_ann,
    cooling / sum(cooling) OVER (PARTITION BY "in.state", group_ann) as multiplier_annual,
    'com' AS sector,
    "in.state",
    'Cooling (Equip.)' AS end_use
FROM meta_filtered
;