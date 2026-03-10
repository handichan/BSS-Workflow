INSERT INTO county_hourly_com_{year}_{turnover}_{disag_id}

WITH filtered_annual AS (
    SELECT 
        "in.county",
        meas,
        tech_stage,
        turnover,
        county_ann_kwh,
        scout_run,
        "in.state",
        "year",
        end_use,
        fuel
    FROM county_annual_com_{year}_{turnover}_{disag_id}
    WHERE "year" = {year}
      AND scout_run = '{scout_version}'
      AND end_use = '{enduse}'
      AND "in.state" = '{state}'
      AND county_ann_kwh = county_ann_kwh
),

measure_map_ts_long AS (
    SELECT 
        meas,
        Scout_end_use,
        'original_ann' AS tech_stage,
        original_ts AS shape_ts
    FROM measure_map2

    UNION ALL

    SELECT 
        meas,
        Scout_end_use,
        'measure_ann' AS tech_stage,
        measure_ts AS shape_ts
    FROM measure_map2
),

to_disagg AS (
    SELECT 
        fa."in.state",
        fa."year",
        fa."in.county",
        fa.end_use,
        fa.fuel,
        mm.shape_ts,
        fa.turnover,
        fa.county_ann_kwh,
        fa.scout_run
    FROM filtered_annual fa
    JOIN measure_map_ts_long mm
      ON fa.meas = mm.meas
     AND fa.end_use = mm.Scout_end_use
     AND fa.tech_stage = mm.tech_stage
),

grouped_disagg AS (
    SELECT 
        "in.state",
        "year",
        "in.county",
        end_use,
        fuel,
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
        fuel,
        shape_ts,
        turnover,
        scout_run
),

shape_ts_used AS (
    SELECT DISTINCT shape_ts
    FROM grouped_disagg
),


mult_filtered AS (
    SELECT 
        h."in.county",
        h.end_use,
        h.fuel,
        h.shape_ts,
        h.timestamp_hour,
        h.sector,
        h.multiplier_hourly
    FROM {mult_com_hourly} h
    JOIN shape_ts_used s
      ON h.shape_ts = s.shape_ts
    WHERE h.multiplier_hourly >= 0
      AND h."in.state" = '{state}'
      AND h.end_use = '{enduse}'
),

hourly_ungrouped AS (
    SELECT 
        gd."in.state",
        gd."year",
        gd."in.county",
        gd.end_use,
        gd.fuel,
        mf.timestamp_hour,
        mf.sector,
        gd.turnover,
        gd.county_ann_kwh * mf.multiplier_hourly AS county_hourly_kwh,
        gd.scout_run
    FROM grouped_disagg gd
    LEFT JOIN mult_filtered mf
      ON gd."in.county" = mf."in.county"
     AND gd.end_use = mf.end_use
     AND gd.shape_ts = mf.shape_ts
     AND gd.fuel = mf.fuel
)

SELECT
    "in.county",
    timestamp_hour,
    turnover,
    SUM(county_hourly_kwh) AS county_hourly_uncal_kwh,
    scout_run,
    sector,
    "in.state",
    "year",
    end_use,
    fuel
FROM hourly_ungrouped
GROUP BY
    "in.state",
    "in.county",
    "year",
    end_use,
    fuel,
    timestamp_hour,
    turnover,
    scout_run,
    sector
;