-- county, hourly, end use consumption for 12 example counties
-- hard-coded ones are chosen because of climate and equipment stock; 
-- others are biggest increase and decrease in annual totals
-- hourly for the peak day each year

with
ns as (
SELECT "in.county",count("in.state") as n FROM "resstock_amy2018_release_2024.2_metadata"
GROUP BY "in.county"
),
county_totals as(
SELECT "in.county",turnover,"in.state","year",sum(county_hourly_kwh) as county_total_ann_kwh 
FROM long_county_hourly_high_amy -- variable?
WHERE turnover!='baseline'
AND "year" IN (2024,2050)
AND "in.county" IN (SELECT "in.county" FROM ns WHERE n>=50)
GROUP BY "in.county",turnover,"in.state","year"
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

county_hourly_totals AS(
SELECT long_county_hourly_high_amy."in.county", timestamp_hour,
turnover, "year", "in.state", example_type, sum(county_hourly_kwh) as county_total_hourly_kwh FROM long_county_hourly_high_amy
RIGHT JOIN example_counties ON long_county_hourly_high_amy."in.county" = example_counties."in.county"
GROUP BY long_county_hourly_high_amy."in.county", timestamp_hour, turnover, "year", "in.state", example_type),

day_with_max AS(
SELECT "in.county", turnover as turnover_max, "year", 
extract(month from timestamp_hour) as "month", extract(day from timestamp_hour) as "day", extract(hour from timestamp_hour) as "hour"
FROM (
    SELECT *,
           RANK() OVER (PARTITION BY "in.county",turnover,"in.state","year" ORDER BY county_total_hourly_kwh DESC) AS rank_num
    FROM county_hourly_totals
) subquery
WHERE rank_num = 1),

with_day_hour AS (
    SELECT *, 
    extract(month from timestamp_hour) as "month", extract(day from timestamp_hour) as "day", extract(hour from timestamp_hour) as "hour"
FROM county_hourly_totals
)

SELECT with_day_hour.*, turnover_max
FROM with_day_hour
RIGHT JOIN day_with_max ON with_day_hour."in.county" = day_with_max."in.county"  AND with_day_hour."year" = day_with_max."year" AND with_day_hour."month" = day_with_max."month" AND with_day_hour."day" = day_with_max."day" 
;