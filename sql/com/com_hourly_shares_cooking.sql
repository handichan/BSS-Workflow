INSERT INTO {mult_com_hourly}

WITH meta_filtered AS (
    SELECT 
        m.bldg_id,
        m.weight,
        c.tz
    FROM "{meta_com}" m
    JOIN county2tz2state c
      ON m."in.nhgis_county_gisjoin" = c."in.county"
    WHERE m."in.comstock_building_type" IN (
        'FullServiceRestaurant',
        'Hospital',
        'LargeHotel',
        'PrimarySchool',
        'QuickServiceRestaurant',
        'SecondarySchool'
    )
      AND m.upgrade IN (0, 40)
),

ts_joined AS (
    SELECT
        mf.tz,
        ts.upgrade,
        CASE
            WHEN extract(YEAR FROM DATE_TRUNC('hour', ts."timestamp") + INTERVAL '1' HOUR) = 2019
            THEN DATE_TRUNC('hour', ts."timestamp") - INTERVAL '1' YEAR + INTERVAL '1' HOUR
            ELSE DATE_TRUNC('hour', ts."timestamp") + INTERVAL '1' HOUR
        END AS timestamp_hour,
        ts."out.electricity.interior_equipment.energy_consumption"
            * mf.weight AS int_equip
    FROM "{ts_com}" ts
    JOIN meta_filtered mf
      ON ts.bldg_id = mf.bldg_id
    WHERE ts.upgrade IN ('0','40')
),

ts_agg AS (
    SELECT
        tz,
        timestamp_hour,
        SUM(CASE WHEN upgrade = '0'  THEN int_equip ELSE 0 END) AS base,
        SUM(CASE WHEN upgrade = '40' THEN int_equip ELSE 0 END) AS kitchen
    FROM ts_joined
    GROUP BY tz, timestamp_hour
),

ts_diff AS (
    SELECT
        tz,
        timestamp_hour,
        kitchen - base AS int_equip_diff
    FROM ts_agg
),

ts_norm_tz AS (
    SELECT
        tz,
        'com_cook_ts_1' AS shape_ts,
        timestamp_hour,
        int_equip_diff AS kwh,
        int_equip_diff
          / SUM(int_equip_diff) OVER (PARTITION BY tz) AS multiplier_hourly,
        'com' AS sector,
        'Cooking' AS end_use
    FROM ts_diff
),

joined AS (
    SELECT
        c."in.county",
        c."in.state",
        t.shape_ts,
        t.timestamp_hour,
        t.kwh,
        t.multiplier_hourly,
        t.sector,
        t.end_use
    FROM ts_norm_tz t
    JOIN county2tz2state c
      ON t.tz = c.tz
)
SELECT
    "in.county",
    shape_ts,
    timestamp_hour,
    kwh,
    multiplier_hourly,
    sector,
    fuel,
    end_use,
    "in.state"
FROM joined
CROSS JOIN UNNEST(
    ARRAY['Electric', 'Natural Gas']
) AS u(fuel);