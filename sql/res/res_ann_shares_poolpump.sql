INSERT INTO res_annual_disaggregation_multipliers_{version}

WITH meta_filtered AS (
	SELECT meta."in.county",
		meta."in.weather_file_city",
		meta."in.weather_file_longitude",
	    meta."in.state",
	    'res_pp_ann_1' AS group_ann,
		sum(meta."out.electricity.pool_pump.energy_consumption") as poolpump
	FROM "resstock_amy2018_release_2024.2_metadata" as meta
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
	poolpump / sum(poolpump) OVER (PARTITION BY "in.state", group_ann) as multiplier_annual,
	'res' AS sector,
	"in.state",
	'Other' AS end_use,
	'Electric' AS fuel
FROM meta_filtered
;
