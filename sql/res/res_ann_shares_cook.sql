-- rerun if there have been updates to res_ann_cook
-- res_ann_cook defines the grouping characteristics for cooking
    

INSERT INTO res_annual_disaggregation_multipliers_{version}

WITH meta_filtered AS (
	SELECT meta."in.county",
		meta."in.weather_file_city",
		meta."in.state",
		chars.group_ann,
		sum(meta."out.electricity.range_oven.energy_consumption") as cooking_elec,
		sum(meta."out.natural_gas.range_oven.energy_consumption") as cooking_ng,
		sum(meta."out.propane.range_oven.energy_consumption") as cooking_prop
	FROM "resstock_amy2018_release_2024.2_metadata" as meta
	RIGHT JOIN res_ann_cook as chars ON meta."in.cooking_range" = chars."in.cooking_range"
	AND cast(meta.upgrade as varchar) = chars.upgrade
	WHERE cast(meta.upgrade as varchar) IN (SELECT DISTINCT upgrade FROM res_ann_cook)
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
        cooking_elec,
        sum(cooking_elec) OVER (PARTITION BY "in.state", group_ann) as cooking_elec_total,
        cooking_ng,
        sum(cooking_ng) OVER (PARTITION BY "in.state", group_ann) as cooking_ng_total,
        cooking_prop,
        sum(cooking_prop) OVER (PARTITION BY "in.state", group_ann) as cooking_prop_total
FROM meta_filtered
)

SELECT "in.county",
	"in.weather_file_city",
	group_ann,
	cooking_elec / cooking_elec_total as multiplier_annual,
	'res' AS sector,
	"in.state",
	'Cooking' AS end_use,
	'Electric' AS fuel
FROM geo_totals
WHERE cooking_elec_total > 0

UNION ALL

SELECT "in.county",
	"in.weather_file_city",
	group_ann,
	cooking_ng / cooking_ng_total as multiplier_annual,
	'res' AS sector,
	"in.state",
	'Cooking' AS end_use,
	'Natural Gas' AS fuel
FROM geo_totals
WHERE cooking_ng_total > 0

UNION ALL

SELECT "in.county",
	"in.weather_file_city",
	group_ann,
	cooking_prop / cooking_prop_total as multiplier_annual,
	'res' AS sector,
	"in.state",
	'Cooking' AS end_use,
	'Propane' AS fuel
FROM geo_totals
WHERE cooking_prop_total > 0
;