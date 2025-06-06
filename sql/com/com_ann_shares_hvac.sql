-- rerun if there have been updates to com_ann_hvac
-- com_ann_hvac defines the grouping characteristics for hvac
    
INSERT INTO com_annual_disaggregation_multipliers_VERSIONID
WITH meta_filtered AS (
    SELECT meta."in.nhgis_county_gisjoin",
        meta."in.state",
        chars.group_ann,
        chars."version",
        sum(meta."calc.weighted.electricity.heating.energy_consumption..tbtu" + meta."calc.weighted.electricity.heat_recovery.energy_consumption..tbtu") as heating,
        sum(meta."calc.weighted.electricity.cooling.energy_consumption..tbtu" + meta."calc.weighted.electricity.heat_rejection.energy_consumption..tbtu" + meta."calc.weighted.district_cooling.cooling.energy_consumption..tbtu" + meta."calc.weighted.electricity.pumps.energy_consumption..tbtu") as cooling,
        sum(meta."calc.weighted.electricity.fans.energy_consumption..tbtu") as ventilation
    FROM "comstock_amy2018_release_2024.2_parquet" as meta
        RIGHT JOIN com_ann_hvac as chars ON meta."in.heating_fuel" = chars."in.heating_fuel"
        AND meta."in.hvac_combined_type" = chars."in.hvac_combined_type"
        AND cast(meta.upgrade as varchar) = chars.upgrade
    WHERE cast(meta.upgrade as varchar) IN (SELECT DISTINCT upgrade FROM com_ann_hvac)
    AND group_ann NOT IN ('com_hvac_ann_42','com_hvac_ann_43')
    GROUP BY 
        meta."in.nhgis_county_gisjoin",
        meta."in.state",
        chars.group_ann,
        chars."version"
),
geo_shares AS (
    SELECT "in.nhgis_county_gisjoin",
    "in.state",
    "version",
    group_ann,
    heating,
    heating / sum(heating) OVER (PARTITION BY "in.state", group_ann, "version") as heating_mult,
    cooling,
    cooling / sum(cooling) OVER (PARTITION BY "in.state", group_ann, "version") as cooling_mult,
    ventilation,
    ventilation / sum(ventilation) OVER (PARTITION BY "in.state", group_ann, "version") as ventilation_mult
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

    UNION ALL

    SELECT 
        "in.nhgis_county_gisjoin" as "in.county",
        group_ann,
        cooling_mult AS multiplier_annual,
        'com' AS sector,
        "in.state",
        'Cooling (Equip.)' AS end_use

    FROM geo_shares
    
    
    UNION ALL

    SELECT 
        "in.nhgis_county_gisjoin" as "in.county",
        group_ann,
        ventilation_mult AS multiplier_annual,
        'com' AS sector,
        "in.state",
        'Ventilation' AS end_use

    FROM geo_shares
;