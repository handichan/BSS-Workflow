INSERT INTO {mult_res_hourly}_temp
WITH 
-- get the timeseries data for the building ids
-- calculate simplified end uses
-- filter to the appropriate partitions
ts_not_agg AS (
	SELECT meta."in.weather_file_city",
	meta."in.weather_file_longitude",
		'res_refrig_ts_1' AS shape_ts,
		CASE
		WHEN extract(YEAR FROM DATE_TRUNC('hour', from_unixtime(ts."timestamp" / 1000000000)) + INTERVAL '1' HOUR) = 2019 THEN DATE_TRUNC('hour', from_unixtime(ts."timestamp" / 1000000000)) - INTERVAL '1' YEAR + INTERVAL '1' HOUR
		ELSE DATE_TRUNC('hour', from_unixtime(ts."timestamp" / 1000000000)) + INTERVAL '1' HOUR END as timestamp_hour,
		ts."out.electricity.freezer.energy_consumption" + ts."out.electricity.refrigerator.energy_consumption" as refrig
	FROM "{ts_res}" as ts
		RIGHT JOIN "{meta_res}" as meta 
		ON ts.bldg_id = meta.bldg_id
		AND ts.upgrade = cast(meta.upgrade as varchar)
	WHERE ts.upgrade = '0'
	AND ts.state='{state}'
)

SELECT "in.weather_file_city",
	"in.weather_file_longitude",
	shape_ts,
	timestamp_hour,
	sum(refrig) as kwh,
	'res' AS sector,
	'Refrigeration' as end_use,
	'Electric' as fuel
FROM ts_not_agg
GROUP BY timestamp_hour,
	"in.weather_file_longitude",
	"in.weather_file_city",
	shape_ts
;