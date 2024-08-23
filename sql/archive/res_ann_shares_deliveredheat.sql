-- we're disaggregating secondary heating based on delivered heat, not energy consumption for heating
-- rerun if another group_ann is added that's based on delivered heat

INSERT INTO annual_disaggregation_multipliers

WITH meta_filtered AS (
	SELECT "in.county",
	    "in.state",
	    sum("out.load.heating.energy_delivered.kbtu") as delivered_heat,
	    upgrade,
	    'res_hvac_ann_52' AS group_ann
	FROM "resstock_amy2018_release_2024.2_metadata"
	WHERE upgrade = 0
	GROUP BY 
		"in.county",
		"in.state",
		upgrade
		
	UNION ALL 
	
	SELECT "in.county",
	    "in.state",
	    sum("out.load.heating.energy_delivered.kbtu") as delivered_heat,
	    upgrade,
	    'res_hvac_ann_84' AS group_ann
	FROM "resstock_amy2018_release_2024.2_metadata"
	WHERE upgrade = 0
	AND "in.heating_fuel" != 'Electricity'
	GROUP BY 
		"in.county",
		"in.state",
		upgrade
),
geo_shares AS (
    SELECT "in.county",
    "in.state",
    upgrade,
    group_ann,
    delivered_heat,
    delivered_heat / sum(delivered_heat) OVER (PARTITION BY "in.state", upgrade) as heating_mult
FROM meta_filtered
ORDER BY "in.county"
) 
SELECT 
        "in.county",
        group_ann,
        heating_mult AS multiplier_annual,
	'2024-07-19' AS group_version,
    'res' AS sector,
    "in.state",
        'Heating (Equip.)' AS end_use
    FROM geo_shares;