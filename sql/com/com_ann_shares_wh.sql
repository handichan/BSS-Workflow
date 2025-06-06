INSERT INTO com_annual_disaggregation_multipliers_VERSIONID
WITH meta_filtered AS (
	SELECT meta."in.nhgis_county_gisjoin",
	    meta."in.state",
		chars.group_ann,
        chars."version",
		sum(meta."calc.weighted.electricity.water_systems.energy_consumption..tbtu" + meta."calc.weighted.natural_gas.water_systems.energy_consumption..tbtu" + meta."calc.weighted.other_fuel.water_systems.energy_consumption..tbtu" + meta."calc.weighted.district_heating.water_systems.energy_consumption..tbtu") as wh
	FROM "comstock_amy2018_release_2024.2_parquet" as meta
		RIGHT JOIN com_ann_wh as chars ON meta."in.service_water_heating_fuel" = chars."in.service_water_heating_fuel"
		AND cast(meta.upgrade as varchar) = chars.upgrade
	WHERE cast(meta.upgrade as varchar) IN (SELECT DISTINCT upgrade FROM com_ann_wh)
	GROUP BY 
		meta."in.nhgis_county_gisjoin",
		meta."in.state",
		chars.group_ann,
        chars."version"
)
SELECT  "in.nhgis_county_gisjoin" as "in.county",
    group_ann,
    wh / sum(wh) OVER (PARTITION BY "in.state", group_ann, "version") as multiplier_annual,
    'com' AS sector,
    "in.state",
    'Water Heating' AS end_use
FROM meta_filtered;
