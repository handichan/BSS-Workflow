-- rerun if there have been updates to com_ann_hvac
-- com_ann_hvac defines the grouping characteristics for hvac
    
INSERT INTO com_annual_disaggregation_multipliers_{version}
WITH meta_filtered AS (
    SELECT meta."in.nhgis_county_gisjoin",
        meta."in.state",
        chars.group_ann,
        sum(meta."calc.weighted.electricity.heating.energy_consumption..tbtu" + meta."calc.weighted.electricity.heat_recovery.energy_consumption..tbtu") as heating_elec,
		sum(meta."calc.weighted.natural_gas.heating.energy_consumption..tbtu") as heating_ng,
        sum(meta."calc.weighted.other_fuel.heating.energy_consumption..tbtu") as heating_fo,
        sum(meta."calc.weighted.electricity.cooling.energy_consumption..tbtu" + meta."calc.weighted.electricity.heat_rejection.energy_consumption..tbtu" + meta."calc.weighted.district_cooling.cooling.energy_consumption..tbtu" + meta."calc.weighted.electricity.pumps.energy_consumption..tbtu") as cooling,
        sum(meta."calc.weighted.electricity.fans.energy_consumption..tbtu") as ventilation
    FROM "comstock_2025.1_parquet" as meta
        RIGHT JOIN com_ann_hvac2 as chars ON meta."in.heating_fuel" = chars."in.heating_fuel"
        AND meta."in.hvac_combined_type" = chars."in.hvac_combined_type"
        AND cast(meta.upgrade as varchar) = chars.upgrade
    WHERE cast(meta.upgrade as varchar) IN (SELECT DISTINCT upgrade FROM com_ann_hvac2)
    GROUP BY 
        meta."in.nhgis_county_gisjoin",
        meta."in.state",
        chars.group_ann
),
geo_totals AS (
    SELECT "in.nhgis_county_gisjoin",
    "in.state",
    group_ann,
    heating_elec,
    sum(heating_elec) OVER (PARTITION BY "in.state", group_ann) as heating_elec_total,
    heating_ng,
    sum(heating_ng) OVER (PARTITION BY "in.state", group_ann) as heating_ng_total,
    heating_fo,
    sum(heating_fo) OVER (PARTITION BY "in.state", group_ann) as heating_fo_total,
    cooling,
    sum(cooling) OVER (PARTITION BY "in.state", group_ann) as cooling_total,
    ventilation,
    sum(ventilation) OVER (PARTITION BY "in.state", group_ann) as ventilation_total
FROM meta_filtered
)
    SELECT 
        "in.nhgis_county_gisjoin" as "in.county",
        group_ann,
        heating_elec / heating_elec_total AS multiplier_annual,
        'com' AS sector,
        "in.state",
        'Heating (Equip.)' AS end_use,
	    'Electric' AS fuel
    FROM geo_totals
    WHERE heating_elec_total > 0

UNION ALL

SELECT 
    "in.nhgis_county_gisjoin" as "in.county",
    group_ann,
    heating_ng / heating_ng_total AS multiplier_annual,
    'com' AS sector,
    "in.state",
    'Heating (Equip.)' AS end_use,
    'Natural Gas' AS fuel
FROM geo_totals
WHERE heating_ng_total > 0

UNION ALL

SELECT 
    "in.nhgis_county_gisjoin" as "in.county",
    group_ann,
    heating_fo / heating_fo_total AS multiplier_annual,
    'com' AS sector,
    "in.state",
    'Heating (Equip.)' AS end_use,
    'Distillate/Other' AS fuel
FROM geo_totals
WHERE heating_fo_total > 0

    UNION ALL

    SELECT 
        "in.nhgis_county_gisjoin" as "in.county",
        group_ann,
        cooling / cooling_total AS multiplier_annual,
        'com' AS sector,
        "in.state",
        'Cooling (Equip.)' AS end_use,
	    'Electric' AS fuel
FROM geo_totals

    UNION ALL

    SELECT 
        "in.nhgis_county_gisjoin" as "in.county",
        group_ann,
        cooling / cooling_total AS multiplier_annual,
        'com' AS sector,
        "in.state",
        'Cooling (Equip.)' AS end_use,
	    'Natural Gas' AS fuel
FROM geo_totals
    
    UNION ALL

    SELECT 
        "in.nhgis_county_gisjoin" as "in.county",
        group_ann,
        ventilation / ventilation_total AS multiplier_annual,
        'com' AS sector,
        "in.state",
        'Ventilation' AS end_use,
	    'Electric' AS fuel
FROM geo_totals
;