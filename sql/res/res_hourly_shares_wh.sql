
INSERT INTO {mult_res_hourly}_temp
WITH meta_shapes AS (

	SELECT meta.bldg_id,
		meta."in.weather_file_city",
		meta."in.weather_file_longitude",
		chars.shape_ts,
		chars.upgrade	FROM "{meta_res}" as meta
		INNER JOIN res_ts_wh as chars 
		ON meta."in.water_heater_efficiency" = chars."in.water_heater_efficiency"
		AND cast(meta.upgrade as varchar) = chars.upgrade
),

ts_not_agg AS (
	SELECT meta_shapes."in.weather_file_city",
	meta_shapes."in.weather_file_longitude",
		meta_shapes.shape_ts,
		CASE
		WHEN extract(YEAR FROM DATE_TRUNC('hour', from_unixtime(ts."timestamp" / 1000000000)) + INTERVAL '1' HOUR) = 2019 THEN DATE_TRUNC('hour', from_unixtime(ts."timestamp" / 1000000000)) - INTERVAL '1' YEAR + INTERVAL '1' HOUR
		ELSE DATE_TRUNC('hour', from_unixtime(ts."timestamp" / 1000000000)) + INTERVAL '1' HOUR END as timestamp_hour,
		ts."out.electricity.hot_water.energy_consumption" as wh_elec,
		ts."out.fuel_oil.hot_water.energy_consumption" + ts."out.natural_gas.hot_water.energy_consumption" + ts."out.propane.hot_water.energy_consumption" as wh_fossil
	FROM "{ts_res}" as ts
		INNER JOIN meta_shapes ON ts.bldg_id = meta_shapes.bldg_id
		AND ts.upgrade = meta_shapes.upgrade
	WHERE ts.state='{state}'
),

ts_agg AS(
	SELECT "in.weather_file_city",
	"in.weather_file_longitude",
		shape_ts,
		timestamp_hour,
		sum(wh_elec) as wh_elec,
		sum(wh_fossil) as wh_fossil
	FROM ts_not_agg
	GROUP BY timestamp_hour,
	"in.weather_file_longitude",
        "in.weather_file_city",
		shape_ts
),

-- Compute annual electric total per (city, shape_ts) to detect shapes with
-- structurally zero electric output -- e.g. res_wh_ts_1 ("fossil WH") only maps to
-- fossil in.water_heater_efficiency types, so wh_elec is zero for every building on
-- that shape everywhere, not just in some cities. res_ann_shares_wh.sql still
-- assigns these groups an annual Electric multiplier (using the fossil county
-- distribution as proxy) when BuildStock has no electric WH sample for the group,
-- so an Electric hourly shape is needed here too; use the fossil shape as the same
-- proxy at the hourly level.
ts_agg_totals AS (
    SELECT *,
        SUM(wh_elec) OVER (
            PARTITION BY "in.weather_file_longitude", "in.weather_file_city", shape_ts
        ) AS annual_elec_total
    FROM ts_agg
)

SELECT
    a."in.weather_file_city",
    a."in.weather_file_longitude",
    a.shape_ts,
    a.timestamp_hour,
    u.kwh,
    'res'              AS sector,
    'Water Heating' AS end_use,
    u.fuel
FROM ts_agg_totals a
CROSS JOIN UNNEST(
    ARRAY['Electric', 'Natural Gas', 'Distillate/Other', 'Propane'],
	ARRAY[
	    CASE WHEN a.annual_elec_total > 0 THEN a.wh_elec ELSE a.wh_fossil END,
	    a.wh_fossil,
	    a.wh_fossil,
	    a.wh_fossil
	]
) AS u(fuel, kwh);
