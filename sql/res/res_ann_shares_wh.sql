-- rerun if there have been updates to res_ann_wh
-- res_ann_wh defines the grouping characteristics for water heating (e.g. ER water heating HPWH, fossil water heating HPWH)
    
INSERT INTO res_annual_disaggregation_multipliers_VERSIONID

WITH meta_filtered AS (
	SELECT meta."in.county",
		meta."in.weather_file_city",
	    meta."in.state",
		chars.group_ann,
		sum(meta."out.electricity.hot_water.energy_consumption") as wh
	FROM "resstock_amy2018_release_2024.2_metadata" as meta
		RIGHT JOIN res_ann_wh as chars ON meta."in.water_heater_efficiency" = chars."in.water_heater_efficiency"
		AND cast(meta.upgrade as varchar) = chars.upgrade
	WHERE cast(meta.upgrade as varchar) IN (SELECT DISTINCT upgrade FROM res_ann_wh)
	AND group_ann NOT IN ('res_wh_ann_6')
	GROUP BY 
		meta."in.county",
		meta."in.weather_file_city",
		meta."in.state",
		chars.group_ann
)
SELECT "in.county",
	"in.weather_file_city",
	group_ann,
	wh / sum(wh) OVER (PARTITION BY "in.state", group_ann) as multiplier_annual,
	'res' AS sector,
	"in.state",
	'Water Heating' AS end_use
FROM meta_filtered
;