INSERT INTO {mult_res_annual}

WITH meta_filtered AS (
	SELECT meta."in.county",
		meta."in.weather_file_city",
		meta."in.weather_file_longitude",
	    meta."in.state",
	    'res_light_ann_1' AS group_ann,
		sum(meta."out.electricity.lighting_exterior.energy_consumption" + meta."out.electricity.lighting_interior.energy_consumption" + meta."out.electricity.lighting_garage.energy_consumption") as lighting
	FROM "{meta_res}" as meta
	WHERE meta.upgrade = 0
	GROUP BY 
		meta."in.county",
		meta."in.weather_file_city",
		meta."in.weather_file_longitude",
		meta."in.state"
)
SELECT "in.county",
	"in.weather_file_city",
	"in.weather_file_longitude",
	group_ann,
	lighting / sum(lighting) OVER (PARTITION BY "in.state", group_ann) as multiplier_annual,
	'res' AS sector,
	"in.state",
	'Lighting' AS end_use,
	'Electric' AS fuel
FROM meta_filtered
;