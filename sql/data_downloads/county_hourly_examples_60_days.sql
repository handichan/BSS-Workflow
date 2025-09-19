-- county, hourly, end use consumption for 12 example counties
-- hard-coded ones are chosen because of climate and equipment stock; 
-- others are biggest increase and decrease in annual totals

-- daily profiles for the peak day (chosen by the non-baseline scenario), monthly mean, max, and min

WITH ns AS (
  SELECT "in.county"
  FROM "resstock_amy2018_release_2024.2_metadata"
  WHERE upgrade = 0
  GROUP BY "in.county"
  HAVING COUNT("in.state") >= 50
),
county_totals as(
SELECT lca."in.county",lca.turnover,lca."in.state",lca."year", sum(lca.county_ann_kwh) as county_total_ann_kwh 
FROM ns
LEFT JOIN long_county_annual_{turnover}_amy lca ON ns."in.county" = lca."in.county"
WHERE turnover!='baseline'
AND lca.county_ann_kwh = lca.county_ann_kwh
AND "year" IN (2024, 2050)
GROUP BY lca."in.county",turnover,"in.state","year"
),

county_differences AS (
    SELECT 
    "in.county",turnover,"in.state",
    (MAX(CASE WHEN year = 2050 THEN county_total_ann_kwh END) - 
     MAX(CASE WHEN year = 2024 THEN county_total_ann_kwh END)) 
    / NULLIF(MAX(CASE WHEN year = 2024 THEN county_total_ann_kwh END), 0) AS percent_difference
FROM 
    county_totals
GROUP BY 
    "in.county",turnover,"in.state"
),

example_counties AS(

SELECT "in.county" , CAST('Large decrease' AS varchar) as example_type
FROM (

-- Smallest percent difference
    SELECT "in.county"
    FROM county_differences
    ORDER BY percent_difference ASC
    LIMIT 2
)

UNION ALL

-- Largest percent difference
SELECT "in.county" , CAST('Large increase' AS varchar) as example_type
FROM (
    SELECT "in.county"
    FROM county_differences
    ORDER BY percent_difference DESC
    LIMIT 2
)

UNION ALL
SELECT 'G1200110' as "in.county", CAST('Hot' AS varchar) as example_type
UNION ALL
SELECT 'G0400130' as "in.county", CAST('Hot' AS varchar) as example_type
UNION ALL
SELECT 'G3800170' as "in.county", CAST('Cold' AS varchar) as example_type
UNION ALL
SELECT 'G2700530' as "in.county", CAST('Cold' AS varchar) as example_type
UNION ALL
SELECT 'G3600810' as "in.county", CAST('High fossil heat' AS varchar) as example_type
UNION ALL
SELECT 'G1700310' as "in.county", CAST('High fossil heat' AS varchar) as example_type
UNION ALL
SELECT 'G1200310' as "in.county", CAST('High electric heat' AS varchar) as example_type
UNION ALL
SELECT 'G4500510' as "in.county", CAST('High electric heat' AS varchar) as example_type
),


-- Aggregate to hourly totals
hourly_data AS (
    SELECT
        lch."in.county",
        ec.example_type,
        lch.turnover,
        lch.year,
        lch.timestamp_hour,
        sum(lch.county_hourly_kwh) as county_hourly_kwh
    FROM long_county_hourly_{turnover}_amy lch
    INNER JOIN example_counties ec
        ON lch."in.county" = ec."in.county"
    WHERE lch.year IN (2024, 2050)
    GROUP BY 
        lch."in.county",
        ec.example_type,
        lch.turnover,
        lch.year,
        lch.timestamp_hour
),

-- Find the days each month with the highest and lowest hourly peaks
monthly_peak_min_days AS (
    SELECT DISTINCT
        h."in.county",
        h.example_type,
        h.year,
        h.turnover as peak_source_turnover,
        month(h.timestamp_hour) AS month,
        FIRST_VALUE(date_trunc('day', h.timestamp_hour)) 
            OVER (PARTITION BY h."in.county", h.turnover, h.year, month(h.timestamp_hour)
                  ORDER BY h.county_hourly_kwh DESC) AS peak_day,
        FIRST_VALUE(date_trunc('day', h.timestamp_hour)) 
            OVER (PARTITION BY h."in.county", h.turnover, h.year, month(h.timestamp_hour)
                  ORDER BY h.county_hourly_kwh ASC) AS min_peak_day
    FROM hourly_data h
),

-- Monthly peak day hourly profiles
monthly_peak_profiles AS (
    SELECT
        h."in.county",
        h.example_type,
        h."year",
        h.turnover,
        md.peak_source_turnover,
        'monthly_peak_day' AS day_type,
        month(h.timestamp_hour) AS month,
        hour(h.timestamp_hour)+1 AS hour_of_day,
        h.county_hourly_kwh,
        md.peak_day AS "date"
    FROM hourly_data h
    INNER JOIN monthly_peak_min_days md
        ON h."in.county" = md."in.county"
       AND h.year = md.year
       AND month(h.timestamp_hour) = md.month
       AND date_trunc('day', h.timestamp_hour) = md.peak_day
),

-- Monthly min-peak day hourly profiles 
monthly_min_profiles AS (
    SELECT
        h."in.county",
        h.example_type,
        h.year,
        h.turnover,
        md.peak_source_turnover,
        'monthly_min_peak_day' AS day_type,
        month(h.timestamp_hour) AS month,
        hour(h.timestamp_hour)+1 AS hour_of_day,
        h.county_hourly_kwh,
        md.min_peak_day AS "date"
    FROM hourly_data h
    INNER JOIN monthly_peak_min_days md
        ON h."in.county" = md."in.county"
       AND h.year = md.year
       AND month(h.timestamp_hour) = md.month
       AND date_trunc('day', h.timestamp_hour) = md.min_peak_day
),

-- Monthly mean hourly profiles for all turnovers
monthly_mean_profiles AS (
    SELECT
        h."in.county",
        h.example_type,
        h.year,
        h.turnover,
        NULL AS peak_source_turnover,
        'monthly_mean' AS day_type,
        month(h.timestamp_hour) AS month,
        hour(h.timestamp_hour)+1 AS hour_of_day,
        AVG(h.county_hourly_kwh) AS county_hourly_kwh,
        NULL AS "date"
    FROM hourly_data h
    GROUP BY h."in.county", h.example_type, h.year, h.turnover, month(h.timestamp_hour), hour(h.timestamp_hour)
)

-- Combine
SELECT *
FROM monthly_mean_profiles
UNION ALL
SELECT *
FROM monthly_peak_profiles
UNION ALL
SELECT *
FROM monthly_min_profiles
ORDER BY "in.county", year, turnover, month, day_type, hour_of_day;