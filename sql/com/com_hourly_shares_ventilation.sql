-- rerun if there have been updates to com_ts_ventilation
-- com_ts_ventilation defines the grouping characteristics for ventilation shapes (e.g. ES HP, GSHP with light envelope)
-- potential reasons to update com_ts_ventilation
    -- new ComStock upgrades
    -- disaggregate by new characteristics (e.g. building type, LMI status)

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
		RIGHT JOIN com_ts_ventilation as chars ON meta."in.hvac_cool_type" = chars."in.hvac_cool_type"
		AND meta."in.hvac_category" = chars."in.hvac_category"
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
		-- make sure all the hours are 2018
		CASE
		WHEN extract(YEAR FROM DATE_TRUNC('hour', ts."timestamp") + INTERVAL '1' HOUR) = 2019 THEN DATE_TRUNC('hour', ts."timestamp") - INTERVAL '1' YEAR + INTERVAL '1' HOUR
		ELSE DATE_TRUNC('hour', ts."timestamp") + INTERVAL '1' HOUR END as timestamp_hour,
		ts."out.electricity.fans.energy_consumption" * meta_shapes.weight as ventilation
	FROM "comstock_2025.1_by_state" as ts
		RIGHT JOIN meta_shapes ON ts.bldg_id = meta_shapes.bldg_id
		AND ts.upgrade = meta_shapes.upgrade
	WHERE ts.upgrade IN (SELECT DISTINCT upgrade FROM com_ts_ventilation)
	AND ts.state='{state}'
),

-- aggregate to hourly by county, and shape
ts_agg AS(
	SELECT "in.county",
	"in.state",
		shape_ts,
		timestamp_hour,
		sum(ventilation) as ventilation
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
	ventilation as kwh,
    'com' AS sector,
    "in.state",
	'Ventilation' as end_use,
	'Electric' AS fuel
FROM ts_agg
;