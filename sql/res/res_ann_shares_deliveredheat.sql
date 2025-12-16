-- we're disaggregating secondary heating based on delivered heat, not energy consumption for heating

INSERT INTO res_annual_disaggregation_multipliers_{version}

WITH meta_filtered AS (
	SELECT "in.county",
		"in.weather_file_city",
		"in.weather_file_longitude",
	    "in.state",
	    sum("out.load.heating.energy_delivered.kbtu") as delivered_heat,
	    'res_hvac_ann_52' AS group_ann
	FROM "resstock_amy2018_release_2024.2_metadata" meta
	WHERE upgrade = 0
	GROUP BY 
		"in.county",
		"in.weather_file_city",
		"in.weather_file_longitude",
		"in.state"
		
	UNION ALL 
	
	SELECT "in.county",
		"in.weather_file_city",
		"in.weather_file_longitude",
	    "in.state",
	    sum("out.load.heating.energy_delivered.kbtu") as delivered_heat,
	    'res_hvac_ann_84' AS group_ann
	FROM "resstock_amy2018_release_2024.2_metadata"
	WHERE upgrade = 0
	AND "in.heating_fuel" != 'Electricity'
	GROUP BY 
		"in.county",
		"in.weather_file_city",
		"in.weather_file_longitude",
		"in.state"

	UNION ALL 
	
	SELECT "in.county",
		"in.weather_file_city",
		"in.weather_file_longitude",
	    "in.state",
	    sum("out.load.heating.energy_delivered.kbtu") as delivered_heat,
	    'res_hvac_ann_86' AS group_ann
	FROM "resstock_amy2018_release_2024.2_metadata"
	WHERE upgrade = 16
	AND "in.heating_fuel" != 'Electricity'
	GROUP BY 
		"in.county",
		"in.weather_file_city",
		"in.weather_file_longitude",
		"in.state"
)

SELECT 
	"in.county",
	"in.weather_file_city",
	"in.weather_file_longitude",
	group_ann,
	delivered_heat / sum(delivered_heat) OVER (PARTITION BY "in.state", group_ann) AS multiplier_annual,
	'res' AS sector,
	"in.state",
	'Heating (Equip.)' AS end_use,
	'All' AS fuel
FROM meta_filtered;