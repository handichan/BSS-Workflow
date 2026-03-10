INSERT INTO {mult_com_annual}

WITH meta_filtered AS (
	SELECT meta."in.nhgis_county_gisjoin",
	    meta."in.state",
		sum(meta."calc.weighted.electricity.water_systems.energy_consumption..tbtu" + 
        meta."calc.weighted.natural_gas.water_systems.energy_consumption..tbtu" + 
        meta."calc.weighted.district_heating.water_systems.energy_consumption..tbtu" +
        meta."calc.weighted.other_fuel.water_systems.energy_consumption..tbtu") as wh
	FROM "{meta_com}" as meta
	WHERE meta.upgrade = 0
	GROUP BY 
		meta."in.nhgis_county_gisjoin",
		meta."in.state"
),

normalized as (
    SELECT 
    "in.nhgis_county_gisjoin" as "in.county",
    wh / sum(wh) OVER (PARTITION BY "in.state") as multiplier_annual,
    "in.state"
FROM meta_filtered)

SELECT 
    "in.county",
	'com_wh_ann_3' AS group_ann,
    multiplier_annual,
    'com' AS sector,
    "in.state",
    'Water Heating' AS end_use,
	'Electric' AS fuel
FROM normalized

UNION ALL 

SELECT 
    "in.county",
	'com_wh_ann_3' AS group_ann,
    multiplier_annual,
    'com' AS sector,
    "in.state",
    'Water Heating' AS end_use,
	'Natural Gas' AS fuel
FROM normalized

UNION ALL 

SELECT 
    "in.county",
	'com_wh_ann_3' AS group_ann,
    multiplier_annual,
    'com' AS sector,
    "in.state",
    'Water Heating' AS end_use,
	'Distillate/Other' AS fuel
FROM normalized

;