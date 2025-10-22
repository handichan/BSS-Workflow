INSERT INTO res_hourly_disaggregation_multipliers_{version}
WITH 
-- get the timeseries data for the building ids
-- calculate simplified end uses
-- filter to the appropriate partitions
ts_not_agg AS (
	SELECT meta."in.weather_file_city",
	meta."in.state",
    'res_misc_ts_2' as shape_ts,
		CASE
		WHEN extract(YEAR FROM DATE_TRUNC('hour', from_unixtime(ts."timestamp" / 1000000000)) + INTERVAL '1' HOUR) = 2019 THEN DATE_TRUNC('hour', from_unixtime(ts."timestamp" / 1000000000)) - INTERVAL '1' YEAR + INTERVAL '1' HOUR
		ELSE DATE_TRUNC('hour', from_unixtime(ts."timestamp" / 1000000000)) + INTERVAL '1' HOUR END as timestamp_hour,
        month(from_unixtime(ts."timestamp" / 1000000000)) as "month",
		ts."out.electricity.plug_loads.energy_consumption" + ts."out.electricity.permanent_spa_heat.energy_consumption" + ts."out.electricity.permanent_spa_pump.energy_consumption" + ts."out.electricity.pool_heater.energy_consumption" + ts."out.electricity.well_pump.energy_consumption" as misc
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
        "month",
		sum(misc) as misc
	FROM ts_not_agg
	GROUP BY timestamp_hour,
	"in.state",
        "in.weather_file_city",
        "month",
        shape_ts
),
-- aggregate to monthly by state
ts_month AS(
	SELECT "in.state",
        "month",
		sum(misc) as misc_month
	FROM ts_agg
	GROUP BY "month",
	"in.state"
),
-- aggregate to annual by state
ts_year AS(
	SELECT "in.state",
		sum(misc_month)/12 as misc_flat_month
	FROM ts_month
	GROUP BY "in.state"
),
-- calculate the state-level multiplier needed to get flat monthly misc
ts_flattening_mult AS(
    SELECT 
	ts_month."in.state",
    ts_month."month",
    misc_flat_month/misc_month as flattening_mult
        FROM ts_month 
        LEFT JOIN ts_year
        ON ts_month."in.state" = ts_year."in.state"
),
-- apply the monthly state-level multipliers so that each month will have the same misc consumption
ts_agg_flat AS(
	SELECT "in.weather_file_city",
	ts_agg."in.state",
		shape_ts,
		timestamp_hour,
		misc * flattening_mult as misc
        FROM ts_agg
        LEFT JOIN ts_flattening_mult
        ON ts_agg."in.state" = ts_flattening_mult."in.state"
        AND ts_agg."month" = ts_flattening_mult."month"
)
-- normalize the shapes
SELECT "in.weather_file_city",
	shape_ts,
	timestamp_hour,
	misc as kwh,
	misc / sum(misc) OVER (PARTITION BY "in.state", "in.weather_file_city", shape_ts) as multiplier_hourly,
    'res' AS sector,
    "in.state",
	'Other' as end_use
FROM ts_agg_flat
;