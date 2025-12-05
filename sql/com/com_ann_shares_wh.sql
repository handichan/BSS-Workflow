INSERT INTO com_annual_disaggregation_multipliers_{version}
WITH meta_filtered AS (
	SELECT meta."in.nhgis_county_gisjoin",
	    meta."in.state",
		chars.group_ann,
		sum(meta."calc.weighted.electricity.water_systems.energy_consumption..tbtu") as wh_elec,
		sum(meta."calc.weighted.natural_gas.water_systems.energy_consumption..tbtu" + meta."calc.weighted.district_heating.water_systems.energy_consumption..tbtu") as wh_ng,
		sum(meta."calc.weighted.other_fuel.water_systems.energy_consumption..tbtu") as wh_fo
	FROM "comstock_2025.1_parquet" as meta
		RIGHT JOIN com_ann_wh as chars ON meta."in.service_water_heating_fuel" = chars."in.service_water_heating_fuel"
		AND cast(meta.upgrade as varchar) = chars.upgrade
	WHERE cast(meta.upgrade as varchar) IN (SELECT DISTINCT upgrade FROM com_ann_wh)
	GROUP BY 
		meta."in.nhgis_county_gisjoin",
		meta."in.state",
		chars.group_ann
),
geo_totals AS (
    SELECT "in.nhgis_county_gisjoin",
    "in.state",
    group_ann,
    wh_elec,
    sum(wh_elec) OVER (PARTITION BY "in.state", group_ann) as wh_elec_total,
    wh_ng,
    sum(wh_ng) OVER (PARTITION BY "in.state", group_ann) as wh_ng_total,
    wh_fo,
    sum(wh_fo) OVER (PARTITION BY "in.state", group_ann) as wh_fo_total
FROM meta_filtered
)
    SELECT 
        "in.nhgis_county_gisjoin" as "in.county",
        group_ann,
        wh_elec / wh_elec_total AS multiplier_annual,
        'com' AS sector,
        "in.state",
        'Water Heating' AS end_use,
	    'Electric' AS fuel
    FROM geo_totals
    WHERE wh_elec_total > 0

UNION ALL

    SELECT 
        "in.nhgis_county_gisjoin" as "in.county",
        group_ann,
        wh_ng / wh_ng_total AS multiplier_annual,
        'com' AS sector,
        "in.state",
        'Water Heating' AS end_use,
	    'Natural Gas' AS fuel
    FROM geo_totals
    WHERE wh_ng_total > 0

UNION ALL

    SELECT 
        "in.nhgis_county_gisjoin" as "in.county",
        group_ann,
        wh_fo / wh_fo_total AS multiplier_annual,
        'com' AS sector,
        "in.state",
        'Water Heating' AS end_use,
	    'Distillate/Other' AS fuel
    FROM geo_totals
    WHERE wh_fo_total > 0
	;
