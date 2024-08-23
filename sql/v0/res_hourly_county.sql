CREATE TABLE res_hourly_county_STATEID AS
    WITH filtered_annual AS (
        SELECT *
        FROM annual_county_0618_ex_hc
        WHERE "in.state" IN ('STATEID')
          AND "year" IN (2024, 2050)
          AND county_ann_kwh > 0
    ),
    measure_map_ts_long AS (
    SELECT 
        meas,
        Scout_end_use,
        'original_ann' AS tech_stage,
        original_ts AS shape_ts
    FROM measure_map

    UNION ALL

    SELECT 
        meas,
        Scout_end_use,
        'measure_ann' AS tech_stage,
        measure_ts AS shape_ts
    FROM measure_map
    ),
    to_disagg AS (
        SELECT 
            fa."in.state",
            fa."year",
            fa."in.county",
            fa.end_use,
            mmtsl.shape_ts,
            fa.turnover,
            fa.county_ann_kwh
        FROM filtered_annual AS fa
        JOIN measure_map_ts_long AS mmtsl
          ON fa.meas = mmtsl.meas
          AND fa.end_use = mmtsl.Scout_end_use
          AND fa.tech_stage = mmtsl.tech_stage
    ),

    grouped_disagg AS (
        SELECT 
            "in.state",
            "year",
            "in.county",
            end_use,
            shape_ts,
            turnover,
            SUM(county_ann_kwh) AS county_ann_kwh
        FROM to_disagg
        GROUP BY
            "in.state",
            "year",
            "in.county",
            end_use,
            shape_ts,
            turnover
    ),
    -- Handle heating data
    heating_data AS (
        SELECT 
            gd."in.state",
            gd."year",
            gd."in.county",
            gd.end_use,
            h.timestamp_hour,
            gd.turnover,
            gd.county_ann_kwh * h.multiplier_hourly AS county_hourly_kwh
        FROM grouped_disagg AS gd
        LEFT JOIN (SELECT "in.county", end_use, shape_ts, timestamp_hour, multiplier_hourly FROM res_heating_hourly_STATEID_2024 WHERE multiplier_hourly > 0) AS h
        ON gd."in.county" = h."in.county"
        AND gd.end_use = h.end_use
        AND gd.shape_ts = h.shape_ts
    ),

    group_heating_data AS (
        SELECT
            "in.state",
            "in.county",
            "year",
            end_use,
            timestamp_hour,
            turnover,
            SUM(county_hourly_kwh) AS county_hourly_kwh
        FROM heating_data
        GROUP BY
            "in.state",
            "in.county",
            "year",
            end_use,
            timestamp_hour,
            turnover
    ),

    -- Handle cooling data
    cooling_data AS (
        SELECT 
            gd."in.state",
            gd."year",
            gd."in.county",
            gd.end_use,
            c.timestamp_hour,
            gd.turnover,
            gd.county_ann_kwh * c.multiplier_hourly AS county_hourly_kwh
        FROM grouped_disagg AS gd
        LEFT JOIN (SELECT "in.county", end_use, shape_ts, timestamp_hour, multiplier_hourly FROM res_cooling_hourly_STATEID_2024 WHERE multiplier_hourly > 0) AS c
        ON gd."in.county" = c."in.county"
        AND gd.end_use = c.end_use
        AND gd.shape_ts = c.shape_ts
    ),

    group_cooling_data AS (
        SELECT
            "in.state",
            "in.county",
            "year",
            end_use,
            timestamp_hour,
            turnover,
            SUM(county_hourly_kwh) AS county_hourly_kwh
        FROM cooling_data
        GROUP BY
            "in.state",
            "in.county",
            "year",
            end_use,
            timestamp_hour,
            turnover
    ),

    -- Combine heating and cooling data
    combined_data AS (
        SELECT * FROM group_heating_data
        UNION ALL
        SELECT * FROM group_cooling_data
    )

    -- SELECT *
    -- FROM combined_data;
    SELECT 
        "in.state",
        "in.county",
        "year",
        end_use,
        timestamp_hour,
        turnover,
        county_hourly_kwh
    FROM combined_data
    WHERE timestamp_hour IS NOT NULL;
