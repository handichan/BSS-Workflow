
INSERT INTO res_annual_disaggregation_multipliers_VERSIONID

WITH meta_filtered AS (
	
	SELECT "in.county",
		"in.weather_file_city",
	    "in.state",
	    sum("out.load.hot_water.energy_delivered.kbtu") as delivered_wh,
	    'res_wh_ann_6' AS group_ann
	FROM "resstock_amy2018_release_2024.2_metadata"
	WHERE upgrade = 0
	GROUP BY 
		"in.county",
		"in.weather_file_city",
		"in.state"
),
geo_shares AS (
    SELECT "in.county",
		"in.weather_file_city",
        "in.state",
        group_ann,
        delivered_wh,
        delivered_wh / sum(delivered_wh) OVER (PARTITION BY "in.state", group_ann) as wh_mult
    FROM meta_filtered
    ORDER BY "in.county"
) 
SELECT 
    "in.county",
    "in.weather_file_city",
    group_ann,
    wh_mult AS multiplier_annual,
    'res' AS sector,
    "in.state",
    'Water Heating' AS end_use
FROM geo_shares;