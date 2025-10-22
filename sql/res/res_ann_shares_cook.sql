-- rerun if there have been updates to res_ann_cook
-- res_ann_cook defines the grouping characteristics for cooking
    

INSERT INTO res_annual_disaggregation_multipliers_{version}

WITH meta_filtered AS (
	SELECT meta."in.county",
		meta."in.weather_file_city",
		meta."in.state",
		chars.group_ann,
		sum(meta."out.electricity.range_oven.energy_consumption") as cooking
	FROM "resstock_amy2018_release_2024.2_metadata" as meta
	RIGHT JOIN res_ann_cook as chars ON meta."in.cooking_range" = chars."in.cooking_range"
	AND cast(meta.upgrade as varchar) = chars.upgrade
	WHERE cast(meta.upgrade as varchar) IN (SELECT DISTINCT upgrade FROM res_ann_cook)
	GROUP BY 
		meta."in.county",
		meta."in.weather_file_city",
		meta."in.state",
		chars.group_ann
)
SELECT "in.county",
	"in.weather_file_city",
	group_ann,
	cooking / sum(cooking) OVER (PARTITION BY "in.state", group_ann) as multiplier_annual,
	'res' AS sector,
	"in.state",
	'Cooking' AS end_use
FROM meta_filtered
;