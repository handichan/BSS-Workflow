INSERT INTO {mult_res_hourly}_temp
WITH 
-- get the timeseries data for the building ids
-- calculate simplified end uses
-- filter to the appropriate partitions
ts_not_agg AS (
	SELECT meta."in.weather_file_city",
	meta."in.weather_file_longitude",
		'res_misc_ts_1' AS shape_ts,
		CASE
		WHEN extract(YEAR FROM DATE_TRUNC('hour', from_unixtime(ts."timestamp" / 1000000000)) + INTERVAL '1' HOUR) = 2019 THEN DATE_TRUNC('hour', from_unixtime(ts."timestamp" / 1000000000)) - INTERVAL '1' YEAR + INTERVAL '1' HOUR
		ELSE DATE_TRUNC('hour', from_unixtime(ts."timestamp" / 1000000000)) + INTERVAL '1' HOUR END as timestamp_hour,
		ts."out.electricity.plug_loads.energy_consumption" + ts."out.electricity.permanent_spa_heat.energy_consumption" + ts."out.electricity.permanent_spa_pump.energy_consumption" + ts."out.electricity.pool_heater.energy_consumption" + ts."out.electricity.well_pump.energy_consumption" as misc,
		ts."out.natural_gas.fireplace.energy_consumption" + ts."out.natural_gas.grill.energy_consumption" + ts."out.natural_gas.lighting.energy_consumption" + ts."out.natural_gas.permanent_spa_heat.energy_consumption" + ts."out.natural_gas.pool_heater.energy_consumption"as misc_ng
	FROM "{ts_res}" as ts
		RIGHT JOIN "{meta_res}" as meta 
		ON ts.bldg_id = meta.bldg_id
		AND ts.upgrade = cast(meta.upgrade as varchar)
	WHERE ts.upgrade = '0'
	AND ts.state='{state}'
),
-- aggregate to hourly by weather file, and shape
ts_agg AS(
	SELECT "in.weather_file_city",
	"in.weather_file_longitude",
		shape_ts,
		timestamp_hour,
		sum(misc) as misc,
		sum(misc_ng) as misc_ng
	FROM ts_not_agg
	GROUP BY timestamp_hour,
	"in.weather_file_longitude",
        "in.weather_file_city",
		shape_ts
)

SELECT
    a."in.weather_file_city",
    a."in.weather_file_longitude",
    a.shape_ts,
    a.timestamp_hour,
    u.kwh,
    'res' AS sector,
    u.end_use,
    u.fuel
FROM ts_agg a
CROSS JOIN UNNEST(
    ARRAY['Electric', 'Electric', 'Natural Gas', 'Propane', 'Distillate/Other'],
	ARRAY[a.misc, a.misc, a.misc_ng, a.misc_ng, a.misc_ng],
	ARRAY['Other', 'Computers and Electronics', 'Other', 'Other', 'Other']
) AS u(fuel, kwh, end_use);
