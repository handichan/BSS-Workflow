INSERT INTO com_annual_disaggregation_multipliers_{version}

WITH meta_filtered AS (
	SELECT meta."in.nhgis_county_gisjoin",
	    meta."in.state",
	    'com_misc_ann_1' AS group_ann,
		sum(meta."calc.weighted.electricity.interior_equipment.energy_consumption..tbtu") as misc_elec,
        sum(meta."calc.weighted.natural_gas.interior_equipment.energy_consumption..tbtu") as misc_ng
	FROM "comstock_2025.1_parquet" as meta
	WHERE meta.upgrade = 0
	GROUP BY 
		meta."in.nhgis_county_gisjoin",
		meta."in.state"
)
    SELECT 
    "in.nhgis_county_gisjoin" as "in.county",
    group_ann,
    misc_elec / sum(misc_elec) OVER (PARTITION BY "in.state", group_ann) as multiplier_annual,
    'com' AS sector,
    "in.state",
    'Other' AS end_use,
    'Electric' AS fuel
FROM meta_filtered

UNION ALL

    SELECT 
    "in.nhgis_county_gisjoin" as "in.county",
    group_ann,
    misc_elec / sum(misc_elec) OVER (PARTITION BY "in.state", group_ann) as multiplier_annual,
    'com' AS sector,
    "in.state",
    'Computers and Electronics' AS end_use,
    'Electric' AS fuel
FROM meta_filtered

UNION ALL

    SELECT 
    "in.nhgis_county_gisjoin" as "in.county",
    group_ann,
    misc_ng / sum(misc_ng) OVER (PARTITION BY "in.state", group_ann) as multiplier_annual,
    'com' AS sector,
    "in.state",
    'Other' AS end_use,
    'Fossil' AS fuel
FROM meta_filtered
;