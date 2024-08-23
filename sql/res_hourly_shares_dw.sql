INSERT INTO hourly_disaggregation_multipliers
WITH 
-- get the timeseries data for the building ids
-- calculate simplified end uses
-- filter to the appropriate partitions
ts_not_agg AS (
	SELECT meta."in.county",
	meta."in.state",
		'res_dw_ts_1' AS shape_ts,
		meta.upgrade,
		from_unixtime(ts."timestamp"/ POWER(10, 9)) as timestamp_hour,
		ts."out.electricity.dishwasher.energy_consumption" as dw
	FROM "resstock_amy2018_release_2024.2_by_state" as ts
		RIGHT JOIN "resstock_amy2018_release_2024.2_metadata" as meta 
		ON ts.bldg_id = meta.bldg_id
		AND ts.upgrade = cast(meta.upgrade as varchar)
	WHERE ts.upgrade = '0'
	-- AND meta."in.state" = 'MT'
),
-- aggregate to hourly by upgrade, county, and shape
ts_agg AS(
	SELECT "in.county",
	"in.state",
		shape_ts,
		upgrade,
		timestamp_hour,
		sum(dw) as dw
	FROM ts_not_agg
	GROUP BY timestamp_hour,
	"in.state",
        "in.county",
		shape_ts,
		upgrade
)
-- normalize the shapes
SELECT "in.county",
	shape_ts,
	timestamp_hour,
	dw as kwh,
	dw / sum(dw) OVER (PARTITION BY "in.county", shape_ts, upgrade) as multiplier_hourly,
    '2024-07-19' AS group_version,
    'res' AS sector,
    "in.state",
	'Other' as end_use
FROM ts_agg
;