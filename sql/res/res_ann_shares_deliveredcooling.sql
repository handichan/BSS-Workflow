
INSERT INTO res_annual_disaggregation_multipliers_{version}

WITH meta_filtered AS (
	
	SELECT "in.county",
		"in.weather_file_city",
		"in.weather_file_longitude",
	    "in.state",
	    sum("out.load.cooling.energy_delivered.kbtu") as delivered_cool,
	    'res_hvac_ann_87' AS group_ann
	FROM "resstock_amy2018_release_2024.2_metadata" meta
	WHERE upgrade = 16
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
    delivered_cool / sum(delivered_cool) OVER (PARTITION BY "in.state", group_ann) as multiplier_annual,
    'res' AS sector,
    "in.state",
    'Cooling (Equip.)' AS end_use,
	'Electric' AS fuel
FROM meta_filtered;