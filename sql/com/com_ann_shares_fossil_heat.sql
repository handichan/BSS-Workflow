-- rerun if there have been updates to com_ann_hvac
-- com_ann_hvac defines the grouping characteristics for hvac
    
INSERT INTO com_annual_disaggregation_multipliers_{version}
WITH meta_filtered AS (
	SELECT meta."in.nhgis_county_gisjoin",
	    meta."in.state",
		chars.group_ann,
		sum(meta."calc.weighted.natural_gas.heating.energy_consumption..tbtu" + meta."calc.weighted.other_fuel.heating.energy_consumption..tbtu" + meta."calc.weighted.electricity.heat_recovery.energy_consumption..tbtu") as heating
	FROM "comstock_2025.1_parquet" as meta
		RIGHT JOIN (SELECT * FROM com_ann_hvac WHERE group_ann in ('com_hvac_ann_12','com_hvac_ann_24')) as chars ON meta."in.heating_fuel" = chars."in.heating_fuel"
		AND meta."in.hvac_combined_type" = chars."in.hvac_combined_type"
		AND cast(meta.upgrade as varchar) = chars.upgrade
	WHERE cast(meta.upgrade as varchar) IN (SELECT DISTINCT upgrade FROM com_ann_hvac)
	GROUP BY 
		meta."in.nhgis_county_gisjoin",
		meta."in.state",
		chars.group_ann
),
geo_shares AS (
    SELECT "in.nhgis_county_gisjoin",
    "in.state",
    "version",
    group_ann,
    heating,
    heating / sum(heating) OVER (PARTITION BY "in.state", group_ann, "version") as heating_mult
FROM meta_filtered
)
    SELECT 
        "in.nhgis_county_gisjoin" as "in.county",
        group_ann,
        heating_mult AS multiplier_annual,
        'com' AS sector,
        "in.state",
        'Heating (Equip.)' AS end_use

    FROM geo_shares
;