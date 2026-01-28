-- put into a temp table because weather files cross states, but it times out to do all the states at the same time
-- res_hourly_hvac_norm combines the states

INSERT INTO {mult_res_hourly}_temp
WITH meta_shapes AS (
-- assign each building id and upgrade combo to the appropriate shape based on the characteristics
	SELECT meta.bldg_id,
		meta."in.weather_file_city",
		meta."in.weather_file_longitude",
		chars.shape_ts,
		chars.upgrade
	FROM "{meta_res}" as meta
		INNER JOIN res_ts_cooling as chars ON meta."in.hvac_cooling_type" = chars."in.hvac_cooling_type"
		AND cast(meta.upgrade as varchar) = chars.upgrade
),

ts_not_agg AS (
	SELECT meta_shapes."in.weather_file_city",
		meta_shapes."in.weather_file_longitude",
		meta_shapes.shape_ts,
		CASE
		WHEN extract(YEAR FROM DATE_TRUNC('hour', from_unixtime(ts."timestamp" / 1000000000)) + INTERVAL '1' HOUR) = 2019
		THEN DATE_TRUNC('hour', from_unixtime(ts."timestamp" / 1000000000)) - INTERVAL '1' YEAR + INTERVAL '1' HOUR
		ELSE DATE_TRUNC('hour', from_unixtime(ts."timestamp" / 1000000000)) + INTERVAL '1' HOUR END as timestamp_hour,
		ts."out.electricity.cooling.energy_consumption" as cooling
	FROM "{ts_res}" as ts
		INNER JOIN meta_shapes ON ts.bldg_id = meta_shapes.bldg_id
		AND ts.upgrade = meta_shapes.upgrade
	WHERE ts.upgrade IN (SELECT DISTINCT upgrade FROM res_ts_cooling)
	AND ts.state='{state}'
),

ts_agg AS (
    SELECT
        "in.weather_file_city",
        "in.weather_file_longitude",
        shape_ts,
        timestamp_hour,
        SUM(cooling) AS kwh
    FROM ts_not_agg
    GROUP BY "in.weather_file_city", "in.weather_file_longitude", shape_ts, timestamp_hour
)

SELECT
    a."in.weather_file_city",
    a."in.weather_file_longitude",
    a.shape_ts,
    a.timestamp_hour,
    a.kwh,
    'res'              AS sector,
    'Cooling (Equip.)' AS end_use,
    u.fuel
FROM ts_agg a
CROSS JOIN UNNEST(
    ARRAY['Electric', 'Natural Gas']
) AS u(fuel);
