-- put into a temp table because weather files cross states, but it times out to do all the states at the same time
-- res_hourly_hvac_norm combines the states

INSERT INTO res_hourly_hvac_temp_{version}
WITH meta_shapes AS (
	SELECT meta.bldg_id,
		meta."in.weather_file_city",
		meta."in.weather_file_longitude",
		chars.shape_ts,
		chars.upgrade
	FROM "resstock_amy2018_release_2024.2_metadata" as meta
	RIGHT JOIN res_ts_heating2 as chars 
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
		ts."out.fuel_oil.heating.energy_consumption" + ts."out.natural_gas.heating.energy_consumption" + ts."out.propane.heating.energy_consumption" as heating_fossil
	FROM "resstock_amy2018_release_2024.2_by_state" as ts
	RIGHT JOIN meta_shapes ON ts.bldg_id = meta_shapes.bldg_id
		AND ts.upgrade = meta_shapes.upgrade
	WHERE ts.upgrade IN (SELECT DISTINCT upgrade FROM res_ts_heating2)
	AND ts.state='{state}'
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

ts_totals AS(
	SELECT "in.weather_file_city",
	shape_ts,
	timestamp_hour,
	heating_elec as heating_elec,
	sum(heating_elec) OVER (PARTITION BY "in.weather_file_longitude", "in.weather_file_city", shape_ts) as heating_elec_total,
	heating_fossil as heating_fossil,
	sum(heating_fossil) OVER (PARTITION BY "in.weather_file_longitude", "in.weather_file_city", shape_ts) as heating_fossil_total,
    'res' AS sector,
    "in.weather_file_longitude"
FROM ts_agg
)

SELECT "in.weather_file_city",
    "in.weather_file_longitude",
	shape_ts,
	timestamp_hour,
	heating_elec as kwh,
    'res' AS sector,
	'Heating (Equip.)' as end_use,
	'Electric' as fuel
FROM ts_totals
WHERE heating_elec_total > 0

UNION ALL

SELECT "in.weather_file_city",
    "in.weather_file_longitude",
	shape_ts,
	timestamp_hour,
	heating_fossil as kwh,
    'res' AS sector,
	'Heating (Equip.)' as end_use,
	'Fossil' as fuel
FROM ts_totals
WHERE heating_fossil > 0
;