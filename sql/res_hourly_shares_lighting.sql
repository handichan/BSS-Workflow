INSERT INTO hourly_disaggregation_multipliers
WITH 
-- get the timeseries data for the building ids
-- calculate simplified end uses
-- filter to the appropriate partitions
ts_not_agg AS (
	SELECT meta."in.county",
	meta."in.state",
		'res_light_ts_1' AS shape_ts,
		meta.upgrade,
		from_unixtime(ts."timestamp"/ POWER(10, 9)) as timestamp_hour,
		ts."out.electricity.lighting_exterior.energy_consumption" + ts."out.electricity.lighting_interior.energy_consumption" + ts."out.electricity.lighting_garage.energy_consumption" as lighting
	FROM "resstock_amy2018_release_2024.2_by_state" as ts
		RIGHT JOIN "resstock_amy2018_release_2024.2_metadata" as meta 
		ON ts.bldg_id = meta.bldg_id
		AND ts.upgrade = cast(meta.upgrade as varchar)
	WHERE ts.upgrade = '0'
),
-- aggregate to hourly by upgrade, county, and shape
ts_agg AS(
	SELECT "in.county",
	"in.state",
		shape_ts,
		upgrade,
		timestamp_hour,
		sum(lighting) as lighting
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
	lighting as kwh,
	lighting / sum(lighting) OVER (PARTITION BY "in.county", shape_ts, upgrade) as multiplier_hourly,
    '2024-07-19' AS group_version,
    'res' AS sector,
    "in.state",
	'Lighting' as end_use
FROM ts_agg
;