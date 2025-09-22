-- percent change in peak demand by turnover

WITH ranked AS (
    SELECT
        "in.county",
        turnover,
        "year",
        timestamp_hour,
        SUM(county_hourly_cal_kwh) AS county_total_hourly_kwh,
        ROW_NUMBER() OVER (
            PARTITION BY "in.county", turnover, "year"
            ORDER BY SUM(county_hourly_cal_kwh) DESC
        ) AS rank_num
    FROM long_county_hourly_{turnover}_{weather}
    GROUP BY "in.county", turnover, "year", timestamp_hour
),
max_hours AS (
    SELECT
        "in.county",
        turnover,
        "year",
        county_total_hourly_kwh
    FROM ranked
    WHERE rank_num = 1
)
SELECT
    "in.county",
    turnover,
    MAX(CASE WHEN "year" = 2024 THEN county_total_hourly_kwh END) AS "2024_kwh",
    MAX(CASE WHEN "year" = 2050 THEN county_total_hourly_kwh END) AS "2050_kwh",
    MAX(CASE WHEN "year" = 2050 THEN county_total_hourly_kwh END) /
    NULLIF(MAX(CASE WHEN "year" = 2024 THEN county_total_hourly_kwh END), 0) - 1 AS percent_change
FROM max_hours
GROUP BY "in.county", turnover;
