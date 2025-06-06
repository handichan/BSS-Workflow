INSERT INTO res_annual_disaggregation_multipliers_VERSIONID

WITH meta_filtered AS (
	SELECT meta."in.county",
		meta."in.weather_file_city",
	    meta."in.state",
	    'res_cw_ann_1' AS group_ann,
		sum(meta."out.electricity.clothes_washer.energy_consumption") as cw
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
	cw / sum(cw) OVER (PARTITION BY "in.state", group_ann) as multiplier_annual,
	'res' AS sector,
	"in.state",
	'Other' AS end_use
FROM meta_filtered
;