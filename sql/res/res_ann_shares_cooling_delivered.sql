
INSERT INTO {mult_res_annual}

WITH meta_filtered AS (
	
	SELECT "in.county",
		"in.weather_file_city",
		"in.weather_file_longitude",
	    "in.state",
	    sum("out.load.cooling.energy_delivered.kbtu") as delivered_cool
	FROM "{meta_res}" meta
	WHERE upgrade = 16
	GROUP BY 
		"in.county",
		"in.weather_file_city",
		"in.weather_file_longitude",
		"in.state"
),

normalized as(
	SELECT 
	"in.county",
    "in.weather_file_city",
	"in.weather_file_longitude",
    delivered_cool / sum(delivered_cool) OVER (PARTITION BY "in.state", group_ann) as multiplier_annual,
    "in.state"
FROM meta_filtered
)

SELECT 
    "in.county",
    "in.weather_file_city",
	"in.weather_file_longitude",
	'res_hvac_ann_87' AS group_ann,
    multiplier_annual,
    'res' AS sector,
    "in.state",
    'Cooling (Equip.)' AS end_use,
	'Electric' AS fuel
FROM normalized

UNION ALL 

SELECT
    "in.county",
    "in.weather_file_city",
	"in.weather_file_longitude",
	'res_hvac_ann_87' AS group_ann,
    multiplier_annual,
    'res' AS sector,
    "in.state",
    'Cooling (Equip.)' AS end_use,
	'Natural Gas' AS fuel
FROM normalized
;