-- put into a temp table because weather files cross states, but it times out to do all the states at the same time
-- res_hourly_hvac_norm combines the states

INSERT INTO {mult_res_hourly}_temp
WITH meta_shapes AS (
	SELECT meta.bldg_id,
		meta."in.weather_file_city",
		meta."in.weather_file_longitude",
		chars.shape_ts,
		chars.upgrade
	FROM "{meta_res}" as meta
	INNER JOIN res_ts_heating as chars 
		ON meta."in.hvac_heating_type_and_fuel" = chars."in.hvac_heating_type_and_fuel"
		AND cast(meta.upgrade as varchar) = chars.upgrade
),

ts_not_agg AS (
	SELECT meta_shapes."in.weather_file_city",
		meta_shapes."in.weather_file_longitude",
		meta_shapes.shape_ts,
		CASE
		WHEN extract(YEAR FROM DATE_TRUNC('hour', from_unixtime(ts."timestamp" / 1000000000)) + INTERVAL '1' HOUR) = 2019 THEN DATE_TRUNC('hour', from_unixtime(ts."timestamp" / 1000000000)) - INTERVAL '1' YEAR + INTERVAL '1' HOUR
		ELSE DATE_TRUNC('hour', from_unixtime(ts."timestamp" / 1000000000)) + INTERVAL '1' HOUR END as timestamp_hour,
		ts."out.electricity.heating.energy_consumption" + ts."out.electricity.heating_hp_bkup.energy_consumption" as heating_elec,
		ts."out.fuel_oil.heating.energy_consumption" + ts."out.natural_gas.heating.energy_consumption" + ts."out.propane.heating.energy_consumption" as heating_fossil	FROM "{ts_res}" as ts
	INNER JOIN meta_shapes ON ts.bldg_id = meta_shapes.bldg_id
		AND ts.upgrade = meta_shapes.upgrade
	WHERE ts.state='{state}'
),

ts_agg AS(
	SELECT "in.weather_file_city",
		"in.weather_file_longitude",
		shape_ts,
		timestamp_hour,
		sum(heating_elec) as heating_elec,
		sum(heating_fossil) as heating_fossil
	FROM ts_not_agg
	GROUP BY timestamp_hour,
	"in.weather_file_longitude",
        "in.weather_file_city",
		shape_ts
),

-- Compute annual totals per (city, shape_ts) to detect zero-fossil cities
ts_agg_totals AS (
    SELECT *,
        SUM(heating_fossil) OVER (
            PARTITION BY "in.weather_file_longitude", "in.weather_file_city", shape_ts
        ) AS annual_fossil_total
    FROM ts_agg
)

SELECT
    a."in.weather_file_city",
    a."in.weather_file_longitude",
    a.shape_ts,
    a.timestamp_hour,
    u.kwh,
    'res'              AS sector,
    'Heating (Equip.)' AS end_use,
    u.fuel
FROM ts_agg_totals a
CROSS JOIN UNNEST(
    ARRAY['Electric', 'Natural Gas', 'Propane', 'Distillate/Other', 'Biomass'],
    ARRAY[
        a.heating_elec,
        -- Fallback: if no fossil heat in time series for this city+shape, use electric shape as proxy
        CASE WHEN a.annual_fossil_total > 0 THEN a.heating_fossil ELSE a.heating_elec END,
        CASE WHEN a.annual_fossil_total > 0 THEN a.heating_fossil ELSE a.heating_elec END,
        CASE WHEN a.annual_fossil_total > 0 THEN a.heating_fossil ELSE a.heating_elec END,
        CASE WHEN a.annual_fossil_total > 0 THEN a.heating_fossil ELSE a.heating_elec END
    ]
) AS u(fuel, kwh);
