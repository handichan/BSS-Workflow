-- heating by fuel; technology agnostic
-- ComStock doesn't report delivered heat for all technologies
    
INSERT INTO com_annual_disaggregation_multipliers_{version}
WITH meta_filtered AS (
	SELECT meta."in.nhgis_county_gisjoin",
	    meta."in.state",
		sum(meta."calc.weighted.natural_gas.heating.energy_consumption..tbtu") as heating_ng,
        sum(meta."calc.weighted.other_fuel.heating.energy_consumption..tbtu") as heating_fo,
        sum(meta."calc.weighted.electricity.heat_recovery.energy_consumption..tbtu") as heating_elec
	FROM "comstock_2025.1_parquet" as meta
	WHERE meta.upgrade = 0
	GROUP BY 
		meta."in.nhgis_county_gisjoin",
		meta."in.state"
),
geo_totals AS (
    SELECT "in.nhgis_county_gisjoin",
    "in.state",
    'com_hvac_ann_42' as group_ann,
    heating_elec,
    sum(heating_elec) OVER (PARTITION BY "in.state") as heating_elec_total,
    heating_ng,
    sum(heating_ng) OVER (PARTITION BY "in.state") as heating_ng_total,
    heating_fo,
    sum(heating_fo) OVER (PARTITION BY "in.state") as heating_fo_total
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
;