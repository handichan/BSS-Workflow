-- use gap for everything ('always gap')
CREATE TABLE mm_current as SELECT * from measure_map;


-- use misc for electric other ('neither gap')
DROP TABLE mm_current;

CREATE TABLE mm_current as SELECT * from measure_map 
WHERE meas NOT IN ('Gap','(R) Ref. Case Electric Other');

INSERT INTO mm_current
VALUES
('Gap', 'Other', 'com_misc_ann_1', 'com_misc_ann_1', 'com_misc_ts_1', 'com_misc_ts_1', 'com', 'Other', 'Other', 'Other', 'Other'),
('(R) Ref. Case Electric Other', 'Other', 'res_misc_ann_1', 'res_misc_ann_1', 'res_misc_ts_1', 'res_misc_ts_1', 'res', 'Other', 'Other', 'Other', 'Other');


-- use misc for res electric other ('com gap')
DROP TABLE mm_current;

CREATE TABLE mm_current as SELECT * from measure_map 
WHERE meas != '(R) Ref. Case Electric Other';

INSERT INTO mm_current
VALUES
('(R) Ref. Case Electric Other', 'Other', 'res_misc_ann_1', 'res_misc_ann_1', 'res_misc_ts_1', 'res_misc_ts_1', 'res', 'Other', 'Other', 'Other', 'Other');


-- for storing the values
CREATE EXTERNAL TABLE county_hourly_other (
    `in.county` string,
    timestamp_hour timestamp,
    turnover string,
    county_hourly_kwh double,
    scout_run string,
    sector string,
    `in.state` string,
    year int,
    end_use string,
    kind string
)
STORED AS parquet
LOCATION 's3://margaretbucket/county_hourly_other/'


-- modified hourly_county res
INSERT INTO county_hourly_other
WITH filtered_annual AS (
    SELECT "in.county",
    "in.weather_file_city",
    meas,
    tech_stage,
    turnover,
    county_ann_kwh,
    scout_run,
    sector,
    "in.state",
    "year",
    end_use
    FROM county_annual_res_2020_aeo25_20to50_bytech_gap_indiv
    WHERE "year" = 2020
      AND scout_run = '2025-06-16'
      AND end_use = 'Other'
      AND county_ann_kwh = county_ann_kwh
),

measure_map_ts_long AS (
SELECT 
    meas,
    Scout_end_use,
    'original_ann' AS tech_stage,
    original_ts AS shape_ts
FROM mm_current

UNION ALL

SELECT 
    meas,
    Scout_end_use,
    'measure_ann' AS tech_stage,
    measure_ts AS shape_ts
FROM mm_current
),

to_disagg AS (
    SELECT 
        fa."in.state",
        fa."year",
        fa."in.county",
        fa."in.weather_file_city",
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
        "in.weather_file_city",
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
        "in.weather_file_city",
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
    "in.weather_file_city", "in.state", end_use, shape_ts, timestamp_hour, sector, multiplier_hourly 
    FROM res_hourly_disaggregation_multipliers_20250806_amy
    WHERE multiplier_hourly >= 0
    AND end_use = 'Other') AS h
    ON gd."in.weather_file_city" = h."in.weather_file_city"
    AND gd."in.state" = h."in.state"
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
    end_use,
    'com gap' as kind
FROM hourly_grouped
WHERE timestamp_hour IS NOT NULL
;


-- modified hourly_county com
INSERT INTO county_hourly_other
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
    FROM county_annual_com_2020_aeo25_20to50_bytech_gap_indiv
    WHERE "year" = 2020
      AND scout_run = '2025-06-16'
      AND end_use = 'Other'
      AND county_ann_kwh = county_ann_kwh
),

measure_map_ts_long AS (
SELECT 
    meas,
    Scout_end_use,
    'original_ann' AS tech_stage,
    original_ts AS shape_ts
FROM mm_current

UNION ALL

SELECT 
    meas,
    Scout_end_use,
    'measure_ann' AS tech_stage,
    measure_ts AS shape_ts
FROM mm_current
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
    FROM com_hourly_disaggregation_multipliers_20250806_amy
    WHERE multiplier_hourly >= 0
    AND end_use = 'Other') AS h
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
    end_use,
    'com gap' as kind
FROM hourly_grouped
WHERE timestamp_hour IS NOT NULL
;


-- download

SELECT timestamp_hour, turnover, "in.state", sector, "year", end_use, sum(county_hourly_kwh) as state_hourly_kwh, 'normal end uses' as kind 
FROM long_county_hourly_aeo25_20to50_bytech_gap_indiv_amy
WHERE "year" = 2020 AND turnover = 'baseline' AND end_use != 'Other'
GROUP BY timestamp_hour, turnover, "in.state", sector, "year", end_use, 8

UNION ALL

SELECT timestamp_hour, turnover, "in.state", sector, "year", end_use, sum(county_hourly_kwh) as state_hourly_kwh, kind 
FROM county_hourly_other
WHERE "year" = 2020 AND turnover = 'baseline' 
GROUP BY timestamp_hour, turnover, "in.state", sector, "year", end_use, kind;



-- OR split Other into normal other and unspecified

-- for storing the values
CREATE EXTERNAL TABLE county_hourly_other_2 (
    `in.county` string,
    timestamp_hour timestamp,
    turnover string,
    county_hourly_kwh double,
    scout_run string,
    sector string,
    `in.state` string,
    year int,
    end_use string,
    kind string
)
STORED AS parquet
LOCATION 's3://margaretbucket/county_hourly_other_2/'


-- modified hourly_county res
-- have to run for each year
INSERT INTO county_hourly_other_2
WITH filtered_annual AS (
    SELECT "in.county",
    "in.weather_file_city",
    meas,
    tech_stage,
    turnover,
    county_ann_kwh,
    scout_run,
    sector,
    "in.state",
    "year",
    end_use
    FROM county_annual_res_2020_aeo25_20to50_bytech_indiv
    WHERE "year" < 2025
    AND sector = 'res'
      AND scout_run = '2025-06-16'
      AND end_use = 'Other'
      AND meas != '(R) Ref. Case Electric Other'
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
        "in.weather_file_city",
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
        "in.weather_file_city",
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
    "in.weather_file_city", "in.state", end_use, shape_ts, timestamp_hour, sector, multiplier_hourly 
    FROM res_hourly_disaggregation_multipliers_20250806_amy
    WHERE multiplier_hourly >= 0
    AND end_use = 'Other') AS h
    ON gd."in.weather_file_city" = h."in.weather_file_city"
    AND gd."in.state" = h."in.state"
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
    end_use,
    'unchanging other' as kind
FROM hourly_grouped
WHERE timestamp_hour IS NOT NULL
;

-- modified hourly_county res for '(R) Ref. Case Electric Other'
-- have to run for each year
INSERT INTO county_hourly_other_2
WITH filtered_annual AS (
    SELECT "in.county",
    "in.weather_file_city",
    meas,
    tech_stage,
    turnover,
    county_ann_kwh,
    scout_run,
    sector,
    "in.state",
    "year",
    end_use
    FROM county_annual_res_2020_aeo25_20to50_bytech_indiv
    WHERE "year" < 2025
    AND sector = 'res'
      AND scout_run = '2025-06-16'
      AND end_use = 'Other'
      AND meas = '(R) Ref. Case Electric Other'
      AND county_ann_kwh = county_ann_kwh
),


mm (meas, Scout_end_use, original_ann, measure_ann, original_ts, measure_ts, kind) as(
VALUES
('(R) Ref. Case Electric Other', 'Other', 'res_misc_ann_1', 'res_misc_ann_1', 'res_misc_ts_1', 'res_misc_ts_1', 'res other misc'),
('(R) Ref. Case Electric Other', 'Other', 'res_misc_ann_1', 'res_misc_ann_1', 'res_gap_ts_1', 'res_gap_ts_1', 'res other gap')),

measure_map_ts_long AS (
SELECT 
    meas,
    Scout_end_use,
    'original_ann' AS tech_stage,
    original_ts AS shape_ts,
    kind
FROM mm

UNION ALL

SELECT 
    meas,
    Scout_end_use,
    'measure_ann' AS tech_stage,
    measure_ts AS shape_ts,
    kind
FROM mm
),

to_disagg AS (
    SELECT 
        fa."in.state",
        fa."year",
        fa."in.county",
        fa."in.weather_file_city",
        fa.end_use,
        mmtsl.shape_ts,
        mmtsl.kind,
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
        end_use,
        shape_ts,
        turnover,
        kind,
        SUM(county_ann_kwh) AS county_ann_kwh,
        scout_run
    FROM to_disagg
    GROUP BY
        "in.state",
        "year",
        "in.county",
        "in.weather_file_city",
        end_use,
        shape_ts,
        turnover,
        kind,
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
        gd.kind,
        gd.county_ann_kwh * h.multiplier_hourly AS county_hourly_kwh,
        gd.scout_run
    FROM grouped_disagg AS gd
    LEFT JOIN (SELECT 
    "in.weather_file_city", "in.state", end_use, shape_ts, timestamp_hour, sector, multiplier_hourly 
    FROM res_hourly_disaggregation_multipliers_20250806_amy
    WHERE multiplier_hourly >= 0
    AND end_use = 'Other') AS h
    ON gd."in.weather_file_city" = h."in.weather_file_city"
    AND gd."in.state" = h."in.state"
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
        kind,
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
        kind,
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
    end_use,
    kind
FROM hourly_grouped
WHERE timestamp_hour IS NOT NULL
;

-- modified hourly_county com 
INSERT INTO county_hourly_other_2
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
    FROM long_county_annual_aeo25_20to50_bytech_indiv_amy
    WHERE "year" < 2025
    AND sector = 'com'
      AND scout_run = '2025-06-16'
      AND end_use = 'Other'
      AND meas != '(C) Ref. Case Electric Other'
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
    FROM com_hourly_disaggregation_multipliers_20250806_amy
    WHERE multiplier_hourly >= 0
    AND end_use = 'Other') AS h
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
    end_use,
    'unchanging other' as kind
FROM hourly_grouped
WHERE timestamp_hour IS NOT NULL
;

-- modified hourly_county com for (C) Ref. Case Electric Other
INSERT INTO county_hourly_other_2
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
    FROM long_county_annual_aeo25_20to50_bytech_indiv_amy
    WHERE "year" < 2025
    AND sector = 'com'
      AND scout_run = '2025-06-16'
      AND end_use = 'Other'
      AND meas = '(C) Ref. Case Electric Other'
      AND county_ann_kwh = county_ann_kwh
),

mm (meas, Scout_end_use, original_ann, measure_ann, original_ts, measure_ts, kind) as(
VALUES
('(C) Ref. Case Electric Other', 'Other', 'com_misc_ann_1', 'com_misc_ann_1', 'com_misc_ts_1', 'com_misc_ts_1', 'com other misc'),
('(C) Ref. Case Electric Other', 'Other', 'com_gap_ann_1', 'com_gap_ann_1', 'com_gap_ts_1', 'com_gap_ts_1', 'com other gap')),

measure_map_ts_long AS (
SELECT 
    meas,
    Scout_end_use,
    'original_ann' AS tech_stage,
    original_ts AS shape_ts,
    kind
FROM mm

UNION ALL

SELECT 
    meas,
    Scout_end_use,
    'measure_ann' AS tech_stage,
    measure_ts AS shape_ts,
    kind
FROM mm
),

to_disagg AS (
    SELECT 
        fa."in.state",
        fa."year",
        fa."in.county",
        fa.end_use,
        mmtsl.shape_ts,
        mmtsl.kind,
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
        kind,
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
        kind,
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
        gd.kind,
        gd.county_ann_kwh * h.multiplier_hourly AS county_hourly_kwh,
        gd.scout_run
    FROM grouped_disagg AS gd
    LEFT JOIN (SELECT 
    "in.county", end_use, shape_ts, timestamp_hour, sector, multiplier_hourly 
    FROM com_hourly_disaggregation_multipliers_20250806_amy
    WHERE multiplier_hourly >= 0
    AND end_use = 'Other') AS h
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
        kind,
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
        kind,
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
    end_use,
    kind
FROM hourly_grouped
WHERE timestamp_hour IS NOT NULL
;


-- download
-- state, month by end use

with 
normal_eu_gap as (
SELECT turnover,"year",month(timestamp_hour) as "month","in.state",sector,end_use, sum(county_hourly_kwh) as state_monthly_kwh 
FROM long_county_hourly_aeo25_20to50_bytech_gap_indiv_amy
WHERE turnover = 'baseline' AND end_use != 'Other'
GROUP BY turnover,"year",3,"in.state",sector,end_use
),

normal_eu_no_gap as (
SELECT turnover,"year",month(timestamp_hour) as "month","in.state",sector,end_use, sum(county_hourly_kwh) as state_monthly_kwh 
FROM long_county_hourly_aeo25_20to50_bytech_indiv_amy
WHERE turnover = 'baseline' AND end_use != 'Other'
GROUP BY turnover,"year",3,"in.state",sector,end_use
),

other_com_gap as(
SELECT turnover,"year",month(timestamp_hour) as "month","in.state",sector,end_use,sum(county_hourly_kwh) as state_monthly_kwh
FROM county_hourly_other_2 
WHERE turnover = 'baseline' AND kind in('unchanging other','com other gap','res other misc') 
GROUP BY turnover,"year",3,"in.state",sector,end_use
),

other_no_gap as(
SELECT turnover,"year",month(timestamp_hour) as "month","in.state",sector,end_use,sum(county_hourly_kwh) as state_monthly_kwh
FROM county_hourly_other_2 
WHERE turnover = 'baseline' AND kind in('unchanging other','com other misc','res other misc') 
GROUP BY turnover,"year",3,"in.state",sector,end_use
)

SELECT *, 'gap; com other' as kind FROM normal_eu_gap
UNION ALL 
SELECT *, 'gap; com other' as kind FROM other_com_gap

UNION ALL 

SELECT *, 'gap' as kind FROM normal_eu_gap
UNION ALL 
SELECT *, 'gap' as kind FROM other_no_gap

UNION ALL 

SELECT *, 'no gap; com other' as kind FROM normal_eu_no_gap
UNION ALL 
SELECT *, 'no gap; com other' as kind FROM other_com_gap
UNION ALL 

SELECT *, 'no gap' as kind FROM normal_eu_no_gap
UNION ALL 
SELECT *, 'no gap' as kind FROM other_no_gap
;

-- state, hour totals

SELECT timestamp_hour, turnover, "in.state", sector, "year", sum(county_hourly_kwh) as state_hourly_kwh, 'normal end uses' as kind 
FROM long_county_hourly_aeo25_20to50_bytech_gap_indiv_amy
WHERE "year" < 2025 AND turnover = 'baseline' AND end_use != 'Other'
GROUP BY timestamp_hour, turnover, "in.state", sector, "year", 7

UNION ALL

SELECT timestamp_hour, turnover, "in.state", sector, "year", sum(county_hourly_kwh) as state_hourly_kwh, kind 
FROM county_hourly_other_2
WHERE turnover = 'baseline' 
GROUP BY timestamp_hour, turnover, "in.state", sector, "year", kind;


SELECT timestamp_hour, turnover, "in.state", sector, "year", end_use, sum(county_hourly_kwh) as state_hourly_kwh, 'normal end uses' as kind 
FROM long_county_hourly_aeo25_20to50_bytech_gap_indiv_amy
WHERE turnover = 'baseline' AND end_use != 'Other'
GROUP BY timestamp_hour, turnover, "in.state", sector, "year", end_use, 8

UNION ALL

SELECT timestamp_hour, turnover, "in.state", sector, "year", end_use, sum(county_hourly_kwh) as state_hourly_kwh, kind 
FROM county_hourly_other_2
WHERE turnover = 'baseline' AND kind != 'neither gap'
GROUP BY timestamp_hour, turnover, "in.state", sector, "year", end_use, kind;