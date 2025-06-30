INSERT INTO com_annual_disaggregation_multipliers_VERSIONID

WITH meta_filtered AS (
	SELECT meta."in.nhgis_county_gisjoin",
	    meta."in.state",
	    'com_misc_ann_1' AS group_ann,
		sum(meta."calc.weighted.electricity.interior_equipment.energy_consumption..tbtu") as misc
	FROM "comstock_amy2018_release_2024.2_parquet" as meta
	WHERE meta.upgrade = 0
	GROUP BY 
		meta."in.nhgis_county_gisjoin",
		meta."in.state"
)
    SELECT 
    "in.nhgis_county_gisjoin" as "in.county",
    group_ann,
    misc / sum(misc) OVER (PARTITION BY "in.state", group_ann) as multiplier_annual,
    'com' AS sector,
    "in.state",
    'Other' AS end_use
FROM meta_filtered

UNION ALL

    SELECT 
    "in.nhgis_county_gisjoin" as "in.county",
    group_ann,
    misc / sum(misc) OVER (PARTITION BY "in.state", group_ann) as multiplier_annual,
    'com' AS sector,
    "in.state",
    'Computers and Electronics' AS end_use
FROM meta_filtered
;