INSERT INTO res_annual_disaggregation_multipliers_VERSIONID

WITH meta_filtered AS (
	SELECT meta."in.county",
		meta."in.weather_file_city",
	    meta."in.state",
	    'res_dw_ann_1' AS group_ann,
		sum(meta."out.electricity.dishwasher.energy_consumption") as dw
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
	dw / sum(dw) OVER (PARTITION BY "in.state", group_ann) as multiplier_annual,
	'res' AS sector,
	"in.state",
	'Other' AS end_use
FROM meta_filtered
;