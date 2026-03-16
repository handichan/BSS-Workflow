
INSERT INTO {mult_res_hourly}_temp
WITH meta_shapes AS (

	SELECT meta.bldg_id,
		meta."in.weather_file_city",
		meta."in.weather_file_longitude",
		chars.shape_ts,
		chars.upgrade
	FROM "{meta_res}" as meta
	INNER JOIN res_ts_dry as chars 
		ON meta."in.clothes_dryer" = chars."in.clothes_dryer"
		AND cast(meta.upgrade as varchar) = chars.upgrade
),

ts_not_agg AS (
	SELECT meta_shapes."in.weather_file_city",
		meta_shapes."in.weather_file_longitude",
		meta_shapes.shape_ts,
		CASE
		WHEN extract(YEAR FROM DATE_TRUNC('hour', from_unixtime(ts."timestamp" / 1000000000)) + INTERVAL '1' HOUR) = 2019 THEN DATE_TRUNC('hour', from_unixtime(ts."timestamp" / 1000000000)) - INTERVAL '1' YEAR + INTERVAL '1' HOUR
		ELSE DATE_TRUNC('hour', from_unixtime(ts."timestamp" / 1000000000)) + INTERVAL '1' HOUR END as timestamp_hour,
		ts."out.electricity.clothes_dryer.energy_consumption" as drying_elec,
		ts."out.natural_gas.clothes_dryer.energy_consumption" + ts."out.propane.clothes_dryer.energy_consumption" as drying_fossil
	FROM "{ts_res}" as ts
		INNER JOIN meta_shapes ON ts.bldg_id = meta_shapes.bldg_id
		AND ts.upgrade = meta_shapes.upgrade
	WHERE ts.upgrade IN (SELECT DISTINCT upgrade FROM res_ts_dry)
	AND ts.state='{state}'
),

ts_agg AS (
    SELECT
        "in.weather_file_city",
        "in.weather_file_longitude",
        shape_ts,
        timestamp_hour,
        SUM(drying_elec) AS drying_elec,
		sum(drying_fossil) AS drying_fossil
    FROM ts_not_agg
    GROUP BY "in.weather_file_city", "in.weather_file_longitude", shape_ts, timestamp_hour
)

SELECT
    a."in.weather_file_city",
    a."in.weather_file_longitude",
    a.shape_ts,
    a.timestamp_hour,
    u.kwh,
    'res'              AS sector,
    'Other' AS end_use,
    u.fuel
FROM ts_agg a
CROSS JOIN UNNEST(
    ARRAY['Electric', 'Natural Gas'],
	ARRAY[a.drying_elec, a.drying_fossil]
) AS u(fuel, kwh);
