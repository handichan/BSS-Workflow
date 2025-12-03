
INSERT INTO res_hourly_disaggregation_multipliers_{version}
WITH meta_shapes AS (

	SELECT meta.bldg_id,
		meta."in.weather_file_city",
		meta."in.state",
		chars.shape_ts,
		chars.upgrade
	FROM "resstock_amy2018_release_2024.2_metadata" as meta
	RIGHT JOIN res_ts_dry as chars 
		ON meta."in.clothes_dryer" = chars."in.clothes_dryer"
		AND cast(meta.upgrade as varchar) = chars.upgrade
),

ts_not_agg AS (
	SELECT meta_shapes."in.weather_file_city",
	meta_shapes."in.state",
		meta_shapes.shape_ts,
		CASE
		WHEN extract(YEAR FROM DATE_TRUNC('hour', from_unixtime(ts."timestamp" / 1000000000)) + INTERVAL '1' HOUR) = 2019 THEN DATE_TRUNC('hour', from_unixtime(ts."timestamp" / 1000000000)) - INTERVAL '1' YEAR + INTERVAL '1' HOUR
		ELSE DATE_TRUNC('hour', from_unixtime(ts."timestamp" / 1000000000)) + INTERVAL '1' HOUR END as timestamp_hour,
		ts."out.electricity.clothes_dryer.energy_consumption" as drying_elec,
		ts."out.natural_gas.clothes_dryer.energy_consumption" + ts."out.propane.clothes_dryer.energy_consumption" as drying_fossil
	FROM "resstock_amy2018_release_2024.2_by_state" as ts
		RIGHT JOIN meta_shapes ON ts.bldg_id = meta_shapes.bldg_id
		AND ts.upgrade = meta_shapes.upgrade
	WHERE ts.upgrade IN (SELECT DISTINCT upgrade FROM res_ts_dry)
),

ts_agg AS(
	SELECT "in.weather_file_city",
	"in.state",
		shape_ts,
		timestamp_hour,
		sum(drying_elec) as drying_elec,
		sum(drying_fossil) as drying_fossil

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
	drying_elec as drying_elec,
	sum(drying_elec) OVER (PARTITION BY "in.state", "in.weather_file_city", shape_ts) as drying_elec_total,
	drying_fossil as drying_fossil,
	sum(drying_fossil) OVER (PARTITION BY "in.state", "in.weather_file_city", shape_ts) as drying_fossil_total,
    'res' AS sector,
    "in.state"
FROM ts_agg
)

SELECT "in.weather_file_city",
	shape_ts,
	timestamp_hour,
	drying_elec as kwh,
	drying_elec / drying_elec_total as multiplier_hourly,
    'res' AS sector,
    "in.state",
	'Other' as end_use,
	'Electric' as fuel
FROM ts_totals
WHERE drying_elec_total > 0

UNION ALL

SELECT "in.weather_file_city",
	shape_ts,
	timestamp_hour,
	drying_fossil as kwh,
	drying_fossil / drying_fossil_total as multiplier_hourly,
    'res' AS sector,
    "in.state",
	'Other' as end_use,
	'Fossil' as fuel
FROM ts_totals
WHERE drying_fossil > 0
;