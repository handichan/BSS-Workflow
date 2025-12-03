    
INSERT INTO res_annual_disaggregation_multipliers_{version}

WITH meta_filtered AS (
	SELECT meta."in.county",
		meta."in.weather_file_city",
		meta."in.state",
		chars.group_ann,
		sum(meta."out.electricity.clothes_dryer.energy_consumption") as drying_elec,
		sum(meta."out.natural_gas.clothes_dryer.energy_consumption") as drying_ng,
		sum(meta."out.propane.clothes_dryer.energy_consumption") as drying_prop
	FROM "resstock_amy2018_release_2024.2_metadata" as meta
	RIGHT JOIN res_ann_dry as chars ON meta."in.clothes_dryer" = chars."in.clothes_dryer"
	AND cast(meta.upgrade as varchar) = chars.upgrade
	WHERE cast(meta.upgrade as varchar) IN (SELECT DISTINCT upgrade FROM res_ann_dry)
	GROUP BY 
		meta."in.county",
		meta."in.weather_file_city",
		meta."in.state",
		chars.group_ann
),
geo_totals AS (
    SELECT "in.county",
    	"in.weather_file_city",
        "in.state",
        group_ann,
        drying_elec,
        sum(drying_elec) OVER (PARTITION BY "in.state", group_ann) as drying_elec_total,
        drying_ng,
        sum(drying_ng) OVER (PARTITION BY "in.state", group_ann) as drying_ng_total,
        drying_prop,
        sum(drying_prop) OVER (PARTITION BY "in.state", group_ann) as drying_prop_total
FROM meta_filtered
)

SELECT "in.county",
	"in.weather_file_city",
	group_ann,
	drying_elec / drying_elec_total as multiplier_annual,
	'res' AS sector,
	"in.state",
	'Other' AS end_use,
	'Electric' AS fuel
FROM geo_totals
WHERE drying_elec_total > 0

UNION ALL

SELECT "in.county",
	"in.weather_file_city",
	group_ann,
	drying_ng / drying_ng_total as multiplier_annual,
	'res' AS sector,
	"in.state",
	'Other' AS end_use,
	'Natural Gas' AS fuel
FROM geo_totals
WHERE drying_ng_total > 0

UNION ALL

SELECT "in.county",
	"in.weather_file_city",
	group_ann,
	drying_prop / drying_prop_total as multiplier_annual,
	'res' AS sector,
	"in.state",
	'Other' AS end_use,
	'Propane' AS fuel
FROM geo_totals
WHERE drying_prop_total > 0
;