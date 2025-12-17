
INSERT INTO com_hourly_hvac_temp_{version}
WITH meta_shapes AS (
-- assign each building id and upgrade combo to the appropriate shape based on the characteristics
	SELECT 
        meta.bldg_id,
		meta."in.nhgis_county_gisjoin" as "in.county",
		meta."in.state",
		chars.shape_ts,
		chars.upgrade,
        meta.weight
    	FROM "comstock_2025.1_parquet" as meta
		RIGHT JOIN com_ts_heating2 as chars ON meta."in.heating_fuel" = chars."in.heating_fuel"
		AND meta."in.hvac_heat_type" = chars."in.hvac_heat_type"
        AND meta.applicability = chars.applicability
		AND cast(meta.upgrade as varchar) = chars.upgrade
        ),

ts_not_agg AS (
	SELECT meta_shapes."in.county",
	meta_shapes."in.state",
		meta_shapes.shape_ts,
		-- make sure all the hours are 2018
		CASE
		WHEN extract(YEAR FROM DATE_TRUNC('hour', ts."timestamp") + INTERVAL '1' HOUR) = 2019 THEN DATE_TRUNC('hour', ts."timestamp") - INTERVAL '1' YEAR + INTERVAL '1' HOUR
		ELSE DATE_TRUNC('hour', ts."timestamp") + INTERVAL '1' HOUR END as timestamp_hour,
		(ts."out.electricity.heating.energy_consumption" + ts."out.electricity.heat_recovery.energy_consumption") * meta_shapes.weight as heating_elec,
		(ts."out.natural_gas.heating.energy_consumption" + ts."out.other_fuel.heating.energy_consumption" + ts."out.district_heating.heating.energy_consumption") * meta_shapes.weight as heating_fossil
	FROM "comstock_2025.1_by_state" as ts
		RIGHT JOIN meta_shapes ON ts.bldg_id = meta_shapes.bldg_id
		AND ts.upgrade = cast(meta_shapes.upgrade as varchar)
	WHERE ts.upgrade IN (SELECT DISTINCT upgrade FROM com_ts_heating2)
	AND ts.state='{state}'
),
-- aggregate to hourly by county, and shape
ts_agg AS(
	SELECT "in.county",
	"in.state",
		shape_ts,
		timestamp_hour,
		sum(heating_elec) as heating_elec,
		sum(heating_fossil) as heating_fossil
	FROM ts_not_agg
	GROUP BY timestamp_hour,
	"in.state",
        "in.county",
		shape_ts
)
-- don't normalize the shapes
SELECT "in.county",
	shape_ts,
	timestamp_hour,
	heating_elec as kwh,
    'com' AS sector,
    "in.state",
	'Heating (Equip.)' as end_use,
	'Electric' as fuel
FROM ts_agg

UNION ALL

SELECT "in.county",
	shape_ts,
	timestamp_hour,
	heating_fossil as kwh,
    'com' AS sector,
    "in.state",
	'Heating (Equip.)' as end_use,
	'Fossil' as fuel
FROM ts_agg
;