INSERT INTO res_hourly_disaggregation_multipliers_{version}

WITH meta_shapes AS (
	SELECT meta.bldg_id,
		meta."in.weather_file_city",
		meta."in.state",
		chars.shape_ts,
		chars.upgrade
	FROM "resstock_amy2018_release_2024.2_metadata" as meta
	RIGHT JOIN res_ts_cook as chars 
		ON meta."in.cooking_range" = chars."in.cooking_range"
		AND cast(meta.upgrade as varchar) = chars.upgrade
),

ts_not_agg AS (
	SELECT meta_shapes."in.weather_file_city",
	meta_shapes."in.state",
		meta_shapes.shape_ts,
		CASE
		WHEN extract(YEAR FROM DATE_TRUNC('hour', from_unixtime(ts."timestamp" / 1000000000)) + INTERVAL '1' HOUR) = 2019 THEN DATE_TRUNC('hour', from_unixtime(ts."timestamp" / 1000000000)) - INTERVAL '1' YEAR + INTERVAL '1' HOUR
		ELSE DATE_TRUNC('hour', from_unixtime(ts."timestamp" / 1000000000)) + INTERVAL '1' HOUR END as timestamp_hour,
		ts."out.electricity.range_oven.energy_consumption" as cooking_elec,
		ts."out.natural_gas.range_oven.energy_consumption" + ts."out.propane.range_oven.energy_consumption" as cooking_fossil
	FROM "resstock_amy2018_release_2024.2_by_state" as ts
		RIGHT JOIN meta_shapes ON ts.bldg_id = meta_shapes.bldg_id
		AND ts.upgrade = meta_shapes.upgrade
	WHERE ts.upgrade IN (SELECT DISTINCT upgrade FROM res_ts_cook)
	AND ts.state='{state}'
),

ts_agg AS(
	SELECT "in.weather_file_city",
	"in.state",
		shape_ts,
		timestamp_hour,
		sum(cooking_elec) as cooking_elec,
		sum(cooking_fossil) as cooking_fossil
	FROM ts_not_agg
	GROUP BY timestamp_hour,
	"in.state",
        "in.weather_file_city",
		shape_ts
),

ts_totals AS(
	SELECT "in.weather_file_city",
	shape_ts,
	timestamp_hour,
	cooking_elec as cooking_elec,
	sum(cooking_elec) OVER (PARTITION BY "in.state", "in.weather_file_city", shape_ts) as cooking_elec_total,
	cooking_fossil as cooking_fossil,
	sum(cooking_fossil) OVER (PARTITION BY "in.state", "in.weather_file_city", shape_ts) as cooking_fossil_total,
    'res' AS sector,
    "in.state"
FROM ts_agg
)

SELECT "in.weather_file_city",
	shape_ts,
	timestamp_hour,
	cooking_elec as kwh,
	cooking_elec / cooking_elec_total as multiplier_hourly,
    'res' AS sector,
    "in.state",
	'Cooking' as end_use,
	'Electric' as fuel
FROM ts_totals
WHERE cooking_elec_total > 0

UNION ALL

SELECT "in.weather_file_city",
	shape_ts,
	timestamp_hour,
	cooking_fossil as kwh,
	cooking_fossil / cooking_fossil_total as multiplier_hourly,
    'res' AS sector,
    "in.state",
	'Cooking' as end_use,
	'Fossil' as fuel
FROM ts_totals
WHERE cooking_fossil > 0
;