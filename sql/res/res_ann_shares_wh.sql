    
INSERT INTO res_annual_disaggregation_multipliers_{version}

WITH meta_filtered AS (
	SELECT meta."in.county",
		meta."in.weather_file_city",
		meta."in.weather_file_longitude",
	    meta."in.state",
		chars.group_ann,
		sum(meta."out.electricity.hot_water.energy_consumption") as wh_elec,
		sum(meta."out.natural_gas.hot_water.energy_consumption") as wh_ng,
		sum(meta."out.fuel_oil.hot_water.energy_consumption") as wh_fo,
		sum(meta."out.propane.hot_water.energy_consumption") as wh_prop
	FROM "resstock_amy2018_release_2024.2_metadata" as meta
		RIGHT JOIN res_ann_wh as chars ON meta."in.water_heater_efficiency" = chars."in.water_heater_efficiency"
		AND cast(meta.upgrade as varchar) = chars.upgrade
	WHERE cast(meta.upgrade as varchar) IN (SELECT DISTINCT upgrade FROM res_ann_wh)
	AND group_ann NOT IN ('res_wh_ann_6')
	GROUP BY 
		meta."in.county",
		meta."in.weather_file_city",
		meta."in.weather_file_longitude",
		meta."in.state",
		chars.group_ann
),
geo_totals AS (
    SELECT "in.county",
    	"in.weather_file_city",
		"in.weather_file_longitude",
        "in.state",
        group_ann,
        wh_elec,
        sum(wh_elec) OVER (PARTITION BY "in.state", group_ann) as wh_elec_total,
        wh_ng,
        sum(wh_ng) OVER (PARTITION BY "in.state", group_ann) as wh_ng_total,
        wh_fo,
        sum(wh_fo) OVER (PARTITION BY "in.state", group_ann) as wh_fo_total,
        wh_prop,
        sum(wh_prop) OVER (PARTITION BY "in.state", group_ann) as wh_prop_total
FROM meta_filtered
)

SELECT "in.county",
	"in.weather_file_city",
	"in.weather_file_longitude",
	group_ann,
	wh_elec / wh_elec_total as multiplier_annual,
	'res' AS sector,
	"in.state",
	'Water Heating' AS end_use,
	'Electric' AS fuel
FROM geo_totals
WHERE wh_elec_total > 0

UNION ALL

SELECT "in.county",
	"in.weather_file_city",
	"in.weather_file_longitude",
	group_ann,
	wh_ng / wh_ng_total as multiplier_annual,
	'res' AS sector,
	"in.state",
	'Water Heating' AS end_use,
	'Natural Gas' AS fuel
FROM geo_totals
WHERE wh_ng_total > 0

UNION ALL

SELECT "in.county",
	"in.weather_file_city",
	"in.weather_file_longitude",
	group_ann,
	wh_fo / wh_fo_total as multiplier_annual,
	'res' AS sector,
	"in.state",
	'Water Heating' AS end_use,
	'Distillate/Other' AS fuel
FROM geo_totals
WHERE wh_fo_total > 0

UNION ALL

SELECT "in.county",
	"in.weather_file_city",
	"in.weather_file_longitude",
	group_ann,
	wh_prop / wh_prop_total as multiplier_annual,
	'res' AS sector,
	"in.state",
	'Water Heating' AS end_use,
	'Propane' AS fuel
FROM geo_totals
WHERE wh_prop_total > 0
;