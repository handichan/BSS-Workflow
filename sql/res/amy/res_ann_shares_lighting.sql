INSERT INTO res_annual_disaggregation_multipliers_VERSIONID

WITH meta_filtered AS (
	SELECT meta."in.county",
		meta."in.weather_file_city",
	    meta."in.state",
	    'res_light_ann_1' AS group_ann,
		sum(meta."out.electricity.lighting_exterior.energy_consumption" + meta."out.electricity.lighting_interior.energy_consumption" + meta."out.electricity.lighting_garage.energy_consumption") as lighting
	FROM "resstock_amy2018_release_2024.2_metadata" as meta
	WHERE meta.upgrade = 0
	GROUP BY 
		meta."in.county",
		meta."in.weather_file_city",
		meta."in.state"
)
SELECT "in.county",
	"in.weather_file_city",
	group_ann,
	lighting / sum(lighting) OVER (PARTITION BY "in.state", group_ann) as multiplier_annual,
	'res' AS sector,
	"in.state",
	'Lighting' AS end_use
FROM meta_filtered
;