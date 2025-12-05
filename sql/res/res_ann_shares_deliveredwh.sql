
INSERT INTO res_annual_disaggregation_multipliers_{version}

WITH meta_filtered AS (
	
	SELECT "in.county",
		"in.weather_file_city",
	    "in.state",
	    sum("out.load.hot_water.energy_delivered.kbtu") as delivered_wh,
	    'res_wh_ann_6' AS group_ann
	FROM "resstock_amy2018_release_2024.2_metadata" meta
	WHERE upgrade = 0
	GROUP BY 
		"in.county",
		"in.weather_file_city",
		"in.state"
)

SELECT 
    "in.county",
    "in.weather_file_city",
    group_ann,
    delivered_wh / sum(delivered_wh) OVER (PARTITION BY "in.state", group_ann) AS multiplier_annual,
    'res' AS sector,
    "in.state",
    'Water Heating' AS end_use,
    'All' AS fuel
FROM meta_filtered;