-- rerun if there have been updates to com_ts_heating
-- com_ts_heating defines the grouping characteristics for heating shapes (e.g. ER heating, ES HP, GSHP with light envelope)
-- potential reasons to update com_ts_heating
    -- new ComStock upgrades
    -- disaggregate by new characteristics (e.g. building type, LMI status)

INSERT INTO com_hourly_disaggregation_multipliers_VERSIONID
WITH meta_shapes AS (
-- assign each building id and upgrade combo to the appropriate shape based on the characteristics
	SELECT 
        meta.bldg_id,
		meta."in.nhgis_county_gisjoin" as "in.county",
		meta."in.state",
		chars.shape_ts,
		chars.upgrade,
        chars."version",
        meta.weight
    	FROM "comstock_amy2018_release_2024.1_metadata" as meta
		RIGHT JOIN com_ts_heating as chars ON meta."in.heating_fuel" = chars."in.heating_fuel"
		AND meta."in.hvac_heat_type" = chars."in.hvac_heat_type"
        AND meta.applicability = chars.applicability
		AND cast(meta.upgrade as varchar) = chars.upgrade
        ),
-- get the timeseries data for the building ids
-- mostly this step is to make aliases to make the next step nicer
-- calculate simplified end uses
-- filter to the appropriate partitions!!!! doing it here vastly reduces the data scanned and therefore runtime
ts_not_agg AS (
	SELECT meta_shapes."in.county",
	meta_shapes."in.state",
		meta_shapes.shape_ts,
		meta_shapes."version",
		-- make sure all the hours are 2018
		CASE
		WHEN extract(YEAR FROM DATE_TRUNC('hour', from_unixtime(ts."timestamp" / 1000000000)) + INTERVAL '1' HOUR) = 2019 THEN DATE_TRUNC('hour', from_unixtime(ts."timestamp" / 1000000000)) - INTERVAL '1' YEAR + INTERVAL '1' HOUR
		ELSE DATE_TRUNC('hour', from_unixtime(ts."timestamp" / 1000000000)) + INTERVAL '1' HOUR END as timestamp_hour,
		(ts."out.electricity.heating.energy_consumption" + ts."out.electricity.heat_recovery.energy_consumption") * meta_shapes.weight as heating
	FROM "comstock_amy2018_release_2024.1_by_state" as ts
		RIGHT JOIN meta_shapes ON ts.bldg_id = meta_shapes.bldg_id
		AND ts.upgrade = cast(meta_shapes.upgrade as varchar)
	WHERE ts.upgrade IN (SELECT DISTINCT upgrade FROM com_ts_heating)
	AND ts.state='STATEID'
),
-- aggregate to hourly by county, and shape
ts_agg AS(
	SELECT "in.county",
	"in.state",
		shape_ts,
		timestamp_hour,
		"version",
		sum(heating) as heating
	FROM ts_not_agg
	GROUP BY timestamp_hour,
	"in.state",
        "in.county",
		shape_ts,
		"version"
)
-- normalize the shapes
SELECT "in.county",
	shape_ts,
	timestamp_hour,
	heating as kwh,
	heating / sum(heating) OVER (PARTITION BY "in.county", shape_ts, "version") as multiplier_hourly,
    "version" AS group_version,
    'com' AS sector,
    "in.state",
	'Heating (Equip.)' as end_use
FROM ts_agg
;