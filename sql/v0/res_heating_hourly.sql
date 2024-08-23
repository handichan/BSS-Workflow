-- rerun if there have been updates to res_ts_heating
-- res_ts_heating defines the grouping characteristics for heating shapes (e.g. ER heating, ES HP, GSHP with light envelope)
-- potential reasons to update res_ts_heating
    -- new ResStock upgrades
    -- disaggregate by new characteristics (e.g. building type, LMI status)

CREATE TABLE IF NOT EXISTS res_heating_hourly_STATEID_2024 AS 
WITH meta_shapes AS (
-- assign each building id and upgrade combo to the appropriate shape based on the characteristics
	SELECT meta.bldg_id,
		meta."in.county",
		chars.shape_ts,
		chars.upgrade
	FROM "resstock_amy2018_release_2024.2_metadata" as meta
		RIGHT JOIN res_ts_heating as chars ON meta."in.heating_fuel" = chars."in.heating_fuel"
		AND meta."in.hvac_cooling_type" = chars."in.hvac_cooling_type"
		AND cast(meta.upgrade as varchar) = chars.upgrade
),
-- get the timeseries data for the building ids
-- mostly this step is to make aliases to make the next step nicer
-- calculate simplified end uses
-- filter to the appropriate partitions!!!! doing it here vastly reduces the data scanned and therefore runtime
ts_not_agg AS (
	SELECT meta_shapes."in.county",
		meta_shapes.shape_ts,
		meta_shapes.upgrade,
		from_unixtime(ts."timestamp"/ POWER(10, 9)) as timestamp_hour,
		ts."out.electricity.heating.energy_consumption" + ts."out.electricity.heating_hp_bkup.energy_consumption" as heating
	FROM "resstock_amy2018_release_2024.2_by_state" as ts
		RIGHT JOIN meta_shapes ON ts.bldg_id = meta_shapes.bldg_id
		AND ts.upgrade = meta_shapes.upgrade
	-- WHERE ts.upgrade IN ('0', '3')
	WHERE ts.upgrade IN (SELECT DISTINCT upgrade FROM res_ts_heating)
	AND ts.state='STATEID'
	--AND ts.state IN ('WA', 'OR', 'CA', 'ID', 'MT')
	-- AND ts.state='MT'
	-- AND meta_shapes."in.county" = 'G3000850'
	--AND meta_shapes."in.county" = 'G2102010'
	-- 'G2102010' county in KY with 5 samples
	-- 'G5300330' King county WA
	--AND meta_shapes.shape_ts = 'res_heating_ts_1'
),
-- aggregate to hourly by upgrade, county, and shape
ts_agg AS(
	SELECT "in.county",
		shape_ts,
		upgrade,
		timestamp_hour,
		sum(heating) as heating
	FROM ts_not_agg
	GROUP BY timestamp_hour,
        "in.county",
		shape_ts,
		upgrade
)
-- normalize the shapes
SELECT "in.county",
	shape_ts,
	timestamp_hour,
	heating as kwh,
	heating / sum(heating) OVER (PARTITION BY "in.county", shape_ts, upgrade) as multiplier_hourly,
	'Heating (Equip.)' as end_use
FROM ts_agg
-- can get rid of ORDER BY if it adds a lot of time (except it doesn't seem to run without it??!?!)
ORDER BY shape_ts, "in.county", timestamp_hour;