INSERT INTO res_hourly_disaggregation_multipliers_{version}
WITH 
-- get the timeseries data for the building ids
-- calculate simplified end uses
-- filter to the appropriate partitions
ts_not_agg AS (
	SELECT meta."in.weather_file_city",
	meta."in.state",
		'res_pp_ts_1' AS shape_ts,
		CASE
		WHEN extract(YEAR FROM DATE_TRUNC('hour', from_unixtime(ts."timestamp" / 1000000000)) + INTERVAL '1' HOUR) = 2019 THEN DATE_TRUNC('hour', from_unixtime(ts."timestamp" / 1000000000)) - INTERVAL '1' YEAR + INTERVAL '1' HOUR
		ELSE DATE_TRUNC('hour', from_unixtime(ts."timestamp" / 1000000000)) + INTERVAL '1' HOUR END as timestamp_hour,
		ts."out.electricity.pool_pump.energy_consumption" as poolpump
	FROM "resstock_amy2018_release_2024.2_by_state" as ts
		RIGHT JOIN "resstock_amy2018_release_2024.2_metadata" as meta 
		ON ts.bldg_id = meta.bldg_id
		AND ts.upgrade = cast(meta.upgrade as varchar)
	WHERE ts.upgrade = '0'
),
-- aggregate to hourly by weather file, and shape
ts_agg AS(
	SELECT "in.weather_file_city",
	"in.state",
		shape_ts,
		timestamp_hour,
		sum(poolpump) as poolpump
	FROM ts_not_agg
	GROUP BY timestamp_hour,
	"in.state",
        "in.weather_file_city",
		shape_ts
)
-- normalize the shapes
SELECT "in.weather_file_city",
	shape_ts,
	timestamp_hour,
	poolpump as kwh,
	poolpump / sum(poolpump) OVER (PARTITION BY "in.state", "in.weather_file_city", shape_ts) as multiplier_hourly,
    'res' AS sector,
    "in.state",
	'Other' as end_use
FROM ts_agg
;