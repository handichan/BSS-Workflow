INSERT INTO com_annual_disaggregation_multipliers_{version}

WITH meta_filtered AS (
	SELECT meta."in.nhgis_county_gisjoin",
	    meta."in.state",
	    'com_light_ann_1' AS group_ann,
		sum(meta."calc.weighted.electricity.interior_lighting.energy_consumption..tbtu" + meta."calc.weighted.electricity.exterior_lighting.energy_consumption..tbtu") as lights
	FROM "comstock_amy2018_release_2024.2_parquet" as meta
	WHERE meta.upgrade = 0
	GROUP BY 
		meta."in.nhgis_county_gisjoin",
		meta."in.state"
)
    SELECT 
    "in.nhgis_county_gisjoin" as "in.county",
    group_ann,
    lights / sum(lights) OVER (PARTITION BY "in.state", group_ann) as multiplier_annual,
    'com' AS sector,
    "in.state",
    'Lighting' AS end_use
FROM meta_filtered
;