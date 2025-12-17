INSERT INTO  county_hourly_res_{year}_{turnover}_{weather}
WITH filtered_annual AS (
    SELECT "in.county",
    "in.weather_file_city",
    "in.weather_file_longitude",
    meas,
    tech_stage,
    turnover,
    county_ann_kwh,
    scout_run,
    sector,
    "in.state",
    "year",
    end_use,
    fuel
    FROM county_annual_res_{year}_{turnover}_{weather}
    WHERE "year" = {year}
      AND scout_run = '{scout_version}'
      AND end_use = '{enduse}'
      AND county_ann_kwh = county_ann_kwh
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
        fa."in.weather_file_city",
        fa."in.weather_file_longitude",
        fa.end_use,
        fa.fuel,
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
        "in.weather_file_city",
        "in.weather_file_longitude",
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
        "in.weather_file_city",
        "in.weather_file_longitude",
        end_use,
        fuel,
        shape_ts,
        turnover,
        scout_run
),

grouped_fossil AS (
    SELECT 
        "in.state", "in.weather_file_city", "in.weather_file_longitude", "year","in.county",end_use,shape_ts,fuel,turnover,county_ann_kwh,scout_run
    FROM grouped_disagg
    WHERE fuel IN ('Natural Gas', 'Propane', 'Biomass', 'Distillate/Other')
),

hourly_ungrouped AS (
    SELECT 
        gf."in.state",
        gf."year",
        gf."in.county",
        gf.end_use,
        gf.fuel,
        h.timestamp_hour,
        h.sector,
        gf.turnover,
        gf.county_ann_kwh * h.multiplier_hourly AS county_hourly_kwh,
        gf.scout_run
        FROM grouped_fossil as gf
    LEFT JOIN (SELECT 
    "in.weather_file_city", "in.weather_file_longitude", end_use, fuel, shape_ts, timestamp_hour, sector, multiplier_hourly 
    FROM res_hourly_disaggregation_multipliers_{version}
    WHERE multiplier_hourly >= 0
    AND fuel = 'Fossil'
    AND end_use = '{enduse}') AS h
    ON gf."in.weather_file_city" = h."in.weather_file_city"
    AND gf."in.weather_file_longitude" = h."in.weather_file_longitude"
    AND gf.end_use = h.end_use
    AND gf.shape_ts = h.shape_ts

    UNION 
    SELECT 
        gd."in.state",
        gd."year",
        gd."in.county",
        gd.end_use,
        gd.fuel,
        h.timestamp_hour,
        h.sector,
        gd.turnover,
        gd.county_ann_kwh * h.multiplier_hourly AS county_hourly_kwh,
        gd.scout_run
    FROM grouped_disagg AS gd
    LEFT JOIN (SELECT 
    "in.weather_file_city", "in.weather_file_longitude", end_use, fuel, shape_ts, timestamp_hour, sector, multiplier_hourly 
    FROM res_hourly_disaggregation_multipliers_{version}
    WHERE multiplier_hourly >= 0
    AND fuel = 'All'
    AND end_use = '{enduse}') AS h
    ON gd."in.weather_file_city" = h."in.weather_file_city"
    AND gd."in.weather_file_longitude" = h."in.weather_file_longitude"
    AND gd.end_use = h.end_use
    AND gd.shape_ts = h.shape_ts

    UNION 
    SELECT 
        gd."in.state",
        gd."year",
        gd."in.county",
        gd.end_use,
        gd.fuel,
        h.timestamp_hour,
        h.sector,
        gd.turnover,
        gd.county_ann_kwh * h.multiplier_hourly AS county_hourly_kwh,
        gd.scout_run
    FROM grouped_disagg AS gd
    LEFT JOIN (SELECT 
        "in.weather_file_city", "in.weather_file_longitude", end_use, fuel, shape_ts, timestamp_hour, sector, multiplier_hourly 
        FROM res_hourly_disaggregation_multipliers_{version}
        WHERE multiplier_hourly >= 0
        AND fuel NOT IN ('Fossil', 'All')
        AND end_use = '{enduse}') AS h
    ON gd."in.weather_file_city" = h."in.weather_file_city"
    AND gd."in.weather_file_longitude" = h."in.weather_file_longitude"
    AND gd.end_use = h.end_use
    AND gd.shape_ts = h.shape_ts
    AND gd.fuel = h.fuel
),

hourly_grouped AS (
    SELECT
        "in.state",
        "in.county",
        "year",
        end_use,
        fuel,
        timestamp_hour,
        turnover,
        sector,
        SUM(county_hourly_kwh) AS county_hourly_uncal_kwh,
        scout_run
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
),

hourly_calibrated AS (
    SELECT
        hg."in.state",
        hg."in.county",
        hg."year",
        month(hg.timestamp_hour) AS "month",
        hg.end_use,
        hg.fuel,
        hg.timestamp_hour,
        hg.turnover,
        hg.sector,
        county_hourly_uncal_kwh,
        county_hourly_uncal_kwh * COALESCE(cm.calibration_multiplier, 1) AS county_hourly_cal_kwh,
        hg.scout_run
    FROM hourly_grouped AS hg
    LEFT JOIN calibration_multipliers AS cm
      ON cm."in.state" = hg."in.state"
     AND cm."month"    = CAST(month(hg.timestamp_hour) AS INTEGER)
     AND cm.sector     = hg.sector
     AND hg.fuel = 'Electric'
    WHERE hg.sector = 'res'
)

SELECT 
    "in.county",
    timestamp_hour,
    turnover,
    county_hourly_uncal_kwh,
    county_hourly_cal_kwh,
    scout_run,
    sector,
    "in.state",
    "year",
    end_use,
    fuel
FROM hourly_calibrated
WHERE timestamp_hour IS NOT NULL
;
