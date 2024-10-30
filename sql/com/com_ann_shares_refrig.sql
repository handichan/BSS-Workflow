INSERT INTO com_annual_disaggregation_multipliers_VERSIONID

WITH meta_filtered AS (
	SELECT meta."in.nhgis_county_gisjoin",
	    meta."in.state",
	    'com_refrig_ann_1' AS group_ann,
		sum(meta."calc.weighted.electricity.refrigeration.energy_consumption..tbtu") as refrigeration
	FROM "comstock_amy2018_release_2024.1_metadata" as meta
	WHERE meta.upgrade = 0
	GROUP BY 
		meta."in.nhgis_county_gisjoin",
		meta."in.state"
)
    SELECT 
    "in.nhgis_county_gisjoin" as "in.county",
    group_ann,
    refrigeration / sum(refrigeration) OVER (PARTITION BY "in.state", group_ann) as multiplier_annual,
	'2024-07-19' AS group_version,
    'com' AS sector,
    "in.state",
    'Refrigeration' AS end_use
FROM meta_filtered
;