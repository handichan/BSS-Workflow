INSERT INTO com_hourly_temp_{year}_{turnover}_{disag_id}_{state}
WITH filtered_annual AS (
    SELECT *
    FROM county_annual_com_{year}_{turnover}_{disag_id}
    WHERE "year" = {year}
      AND scout_run = '{scout_version}'
      AND end_use = '{enduse}'
      AND "in.state" = '{state}'       -- <--- filter partition
),

measure_map_ts_long AS (
    SELECT meas, Scout_end_use,
           'original_ann' AS tech_stage,
           original_ts AS shape_ts
    FROM measure_map
    UNION ALL
    SELECT meas, Scout_end_use,
           'measure_ann' AS tech_stage,
           measure_ts AS shape_ts
    FROM measure_map
),

to_disagg AS (
    SELECT fa."in.state",
           fa."year",
           fa."in.county",
           fa.end_use,
           fa.fuel,
           mmtsl.shape_ts,
           fa.turnover,
           fa.county_ann_kwh,
           fa.scout_run
    FROM filtered_annual fa
    JOIN measure_map_ts_long mmtsl
      ON fa.meas = mmtsl.meas
     AND fa.end_use = mmtsl.Scout_end_use
     AND fa.tech_stage = mmtsl.tech_stage
),

grouped_disagg AS (
    SELECT "in.state","year","in.county",
           end_use,fuel,shape_ts,turnover,
           SUM(county_ann_kwh) AS county_ann_kwh,
           scout_run
    FROM to_disagg
    GROUP BY "in.state","year","in.county",
             end_use,fuel,shape_ts,turnover,scout_run
),

hourly_ungrouped AS (
    SELECT gd.*,
           h.timestamp_hour,
           h.sector,
           gd.county_ann_kwh * h.multiplier_hourly AS county_hourly_kwh
    FROM grouped_disagg gd
    LEFT JOIN (
        SELECT *
        FROM {mult_com_hourly}
        WHERE end_use = '{enduse}'           -- <--- filter partition
          AND "in.state" = '{state}'
          AND multiplier_hourly >= 0
    ) h
      ON gd."in.county" = h."in.county"
     AND gd.end_use = h.end_use
     AND gd.shape_ts = h.shape_ts
     AND gd.fuel = h.fuel
)
SELECT *
FROM hourly_ungrouped
;