INSERT INTO county_hourly_com_YEARID_TURNOVERID
WITH filtered_annual AS (
    SELECT "in.county",
    meas,
    tech_stage,
    turnover,
    county_ann_kwh,
    scout_run,
    sector,
    "in.state",
    "year",
    end_use
    FROM county_annual_com_YEARID_TURNOVERID
    -- convert to variable
    WHERE "year" = YEARID
      AND county_ann_kwh > 0
      -- convert to variable
      AND scout_run = 'SCOUTRUNDATE'
      -- convert to variable
      AND end_use = 'ENDUSEID'
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
        fa.county_ann_kwh,
        fa.scout_run
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
        SUM(county_ann_kwh) AS county_ann_kwh,
        scout_run
    FROM to_disagg
    GROUP BY
        "in.state",
        "year",
        "in.county",
        end_use,
        shape_ts,
        turnover,
        scout_run
),

hourly_ungrouped AS (
    SELECT 
        gd."in.state",
        gd."year",
        gd."in.county",
        gd.end_use,
        h.timestamp_hour,
        h.sector,
        gd.turnover,
        gd.county_ann_kwh * h.multiplier_hourly AS county_hourly_kwh,
        gd.scout_run
    FROM grouped_disagg AS gd
    LEFT JOIN (SELECT 
    "in.county", end_use, shape_ts, timestamp_hour, sector, multiplier_hourly 
    FROM com_hourly_disaggregation_multipliers_VERSIONID
    WHERE multiplier_hourly >= 0
    -- convert to variable
    AND end_use = 'ENDUSEID'
    -- convert to variable
    AND group_version = '2024-07-19') AS h
    ON gd."in.county" = h."in.county"
    AND gd.end_use = h.end_use
    AND gd.shape_ts = h.shape_ts
),

hourly_grouped AS (
    SELECT
        "in.state",
        "in.county",
        "year",
        end_use,
        timestamp_hour,
        turnover,
        sector,
        SUM(county_hourly_kwh) AS county_hourly_kwh,
        scout_run
    FROM hourly_ungrouped
    GROUP BY
        "in.state",
        "in.county",
        "year",
        end_use,
        timestamp_hour,
        turnover,
        scout_run,
        sector
)

SELECT 
    "in.county",
    timestamp_hour,
    turnover,
    county_hourly_kwh,
    scout_run,
    sector,
    "in.state",
    "year",
    end_use
FROM hourly_grouped
WHERE timestamp_hour IS NOT NULL
;
