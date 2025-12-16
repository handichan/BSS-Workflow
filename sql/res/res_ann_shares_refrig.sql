INSERT INTO res_annual_disaggregation_multipliers_{version}

WITH meta_filtered AS (
	SELECT meta."in.county",
		meta."in.weather_file_city",
		meta."in.weather_file_longitude",
	    meta."in.state",
	    'res_refrig_ann_1' AS group_ann,
		sum(meta."out.electricity.freezer.energy_consumption" + meta."out.electricity.refrigerator.energy_consumption") as refrig
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
	refrig / sum(refrig) OVER (PARTITION BY "in.state", group_ann) as multiplier_annual,
	'res' AS sector,
	"in.state",
	'Refrigeration' AS end_use,
	'Electric' AS fuel
FROM meta_filtered
;