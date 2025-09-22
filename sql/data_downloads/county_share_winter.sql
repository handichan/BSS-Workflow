-- share of top 100 hours that are in the winter (Nov - Feb)

WITH ranked AS (
    SELECT
        "in.county",
        turnover,
        "year",
        timestamp_hour,
        RANK() OVER (
            PARTITION BY "in.county", turnover, "year"
            ORDER BY SUM(county_hourly_cal_kwh) DESC
        ) AS rank_num
    FROM long_county_hourly_{turnover}_{weather}
    GROUP BY "in.county", turnover, "year", timestamp_hour
),
top100 AS (
    SELECT
        "in.county",
        turnover,
        "year",
        timestamp_hour
    FROM ranked
    WHERE rank_num <= 100
),
seasoned AS (
    SELECT
        "in.county",
        turnover,
        "year",
        CASE
            WHEN month(timestamp_hour) BETWEEN 5 AND 9 THEN 'Summer'
            WHEN month(timestamp_hour) IN (11,12,1,2) THEN 'Winter'
            ELSE 'Shoulder'
        END AS season
    FROM top100
)
SELECT
    "in.county",
    turnover,
    "year",
    SUM(CASE WHEN season = 'Winter' THEN 1 ELSE 0 END) * 1.0 / COUNT(*) AS share_winter
FROM seasoned
GROUP BY "in.county", turnover, "year";
