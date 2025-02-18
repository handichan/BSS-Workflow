-- rerun if there have been updates to res_ts_cook
-- res_ts_cook defines the grouping characteristics for cooking shapes
-- potential reasons to update res_ts_cook
    -- new ResStock upgrades
    -- disaggregate by new characteristics (e.g. building type, LMI status)

INSERT INTO res_hourly_disaggregation_multipliers_VERSIONID
WITH meta_shapes AS (
-- assign each building id and upgrade combo to the appropriate shape based on the characteristics
	SELECT meta.bldg_id,
		meta."in.county",
		meta."in.state",
		chars.shape_ts,
		chars.upgrade
	FROM "resstock_tmy3_release_2024.2_metadata" as meta
		RIGHT JOIN res_ts_cook as chars ON meta."in.cooking_range" = chars."in.cooking_range"
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
		CASE
		WHEN extract(YEAR FROM DATE_TRUNC('hour', from_unixtime(ts."timestamp" / 1000000000)) + INTERVAL '1' HOUR) = 2019 THEN DATE_TRUNC('hour', from_unixtime(ts."timestamp" / 1000000000)) - INTERVAL '1' YEAR + INTERVAL '1' HOUR
		ELSE DATE_TRUNC('hour', from_unixtime(ts."timestamp" / 1000000000)) + INTERVAL '1' HOUR END as timestamp_hour,
		ts."out.electricity.range_oven.energy_consumption" as cooking
	FROM "resstock_tmy3_release_2024.2_by_state" as ts
		RIGHT JOIN meta_shapes ON ts.bldg_id = meta_shapes.bldg_id
		AND ts.upgrade = meta_shapes.upgrade
	WHERE ts.upgrade IN (SELECT DISTINCT upgrade FROM res_ts_cook)
),
-- aggregate to hourly by county, and shape
ts_agg AS(
	SELECT "in.county",
	"in.state",
		shape_ts,
		timestamp_hour,
		sum(cooking) as cooking
	FROM ts_not_agg
	GROUP BY timestamp_hour,
	"in.state",
        "in.county",
		shape_ts
)
-- normalize the shapes
SELECT "in.county",
	shape_ts,
	timestamp_hour,
	cooking as kwh,
	cooking / sum(cooking) OVER (PARTITION BY "in.county", shape_ts) as multiplier_hourly,
    '2024-07-19' AS group_version,
    'res' AS sector,
    "in.state",
	'Cooking' as end_use
FROM ts_agg
;