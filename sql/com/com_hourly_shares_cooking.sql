INSERT INTO com_hourly_disaggregation_multipliers_VERSIONID
WITH meta_filtered AS (
-- assign each building id and upgrade combo to the appropriate shape based on the characteristics
	SELECT 
        bldg_id,
        meta."in.state",
        weight,
		tz
    	FROM "comstock_amy2018_release_2024.2_parquet" as meta
    	INNER JOIN county2tz2state ON meta."in.nhgis_county_gisjoin" = county2tz2state."in.county"
    	WHERE "in.comstock_building_type" IN ('FullServiceRestaurant','Hospital','LargeHotel','PrimarySchool','QuickServiceRestaurant','SecondarySchool')
		AND upgrade = 0
        ),
ts_not_agg_base AS (
	SELECT meta_filtered.bldg_id,
		meta_filtered.tz,
		-- make sure all the hours are 2018
		CASE
		WHEN extract(YEAR FROM DATE_TRUNC('hour', from_unixtime(ts."timestamp" / 1000000000)) + INTERVAL '1' HOUR) = 2019 THEN DATE_TRUNC('hour', from_unixtime(ts."timestamp" / 1000000000)) - INTERVAL '1' YEAR + INTERVAL '1' HOUR
		ELSE DATE_TRUNC('hour', from_unixtime(ts."timestamp" / 1000000000)) + INTERVAL '1' HOUR END as timestamp_hour,
		ts."out.electricity.interior_equipment.energy_consumption" * meta_filtered.weight as int_equip
	FROM "comstock_amy2018_release_2024.2_by_state" as ts
		RIGHT JOIN meta_filtered ON ts.bldg_id = meta_filtered.bldg_id
	WHERE ts.upgrade = '0'
	-- can't filter to state because time zones cover multiple states
),
-- aggregate to hourly by upgrade, county, and shape
ts_agg_base AS(
	SELECT tz,
		timestamp_hour,
		sum(int_equip) as int_equip_base
	FROM ts_not_agg_base
	GROUP BY timestamp_hour,
		tz
		),
ts_not_agg_kitchen AS (
	SELECT meta_filtered.bldg_id,
		meta_filtered.tz,
		-- make sure all the hours are 2018
		CASE
		WHEN extract(YEAR FROM DATE_TRUNC('hour', from_unixtime(ts."timestamp" / 1000000000)) + INTERVAL '1' HOUR) = 2019 THEN DATE_TRUNC('hour', from_unixtime(ts."timestamp" / 1000000000)) - INTERVAL '1' YEAR + INTERVAL '1' HOUR
		ELSE DATE_TRUNC('hour', from_unixtime(ts."timestamp" / 1000000000)) + INTERVAL '1' HOUR END as timestamp_hour,
		ts."out.electricity.interior_equipment.energy_consumption" * meta_filtered.weight as int_equip
	FROM "comstock_amy2018_release_2024.2_by_state" as ts
		RIGHT JOIN meta_filtered ON ts.bldg_id = meta_filtered.bldg_id
	WHERE ts.upgrade = '28'
),
-- aggregate to hourly by upgrade, county, and shape
ts_agg_kitchen AS(
	SELECT tz,
		timestamp_hour,
		sum(int_equip) as int_equip_kitchen
	FROM ts_not_agg_kitchen
	GROUP BY timestamp_hour,
		tz
		),
ts_diff_not_norm AS(
SELECT ts_agg_base.tz,
ts_agg_base.timestamp_hour,
(int_equip_kitchen - int_equip_base) as int_equip_diff
FROM ts_agg_base
FULL JOIN ts_agg_kitchen
ON ts_agg_base.tz = ts_agg_kitchen.tz
AND ts_agg_base.timestamp_hour = ts_agg_kitchen.timestamp_hour
),
ts_norm_tz AS(
SELECT
tz,
'com_cook_ts_1' as shape_ts,
timestamp_hour,
int_equip_diff as kwh,
int_equip_diff/sum(int_equip_diff) OVER (PARTITION BY tz) as multiplier_hourly,
'com' as sector,
'Cooking' as end_use
FROM ts_diff_not_norm)

SELECT 
"in.county",
shape_ts,
timestamp_hour,
kwh,
multiplier_hourly,
sector,
"in.state",
end_use
FROM ts_norm_tz 
LEFT JOIN county2tz2state
ON ts_norm_tz.tz = county2tz2state.tz
;