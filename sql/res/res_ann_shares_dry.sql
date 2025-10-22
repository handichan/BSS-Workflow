-- rerun if there have been updates to res_ann_dry
-- res_ann_dry defines the grouping characteristics for clothes drying
    
INSERT INTO res_annual_disaggregation_multipliers_{version}

WITH meta_filtered AS (
	SELECT meta."in.county",
		meta."in.weather_file_city",
		meta."in.state",
		chars.group_ann,
		sum(meta."out.electricity.clothes_dryer.energy_consumption") as drying
	FROM "resstock_amy2018_release_2024.2_metadata" as meta
	RIGHT JOIN res_ann_dry as chars ON meta."in.clothes_dryer" = chars."in.clothes_dryer"
	AND cast(meta.upgrade as varchar) = chars.upgrade
	WHERE cast(meta.upgrade as varchar) IN (SELECT DISTINCT upgrade FROM res_ann_dry)
	GROUP BY 
		meta."in.county",
		meta."in.weather_file_city",
		meta."in.state",
		chars.group_ann
)
SELECT "in.county",
	"in.weather_file_city",
	group_ann,
	drying / sum(drying) OVER (PARTITION BY "in.state", group_ann) as multiplier_annual,
	'res' AS sector,
	"in.state",
	'Other' AS end_use
FROM meta_filtered
;