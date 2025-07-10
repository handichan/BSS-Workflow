-- find top hour per month in each county; sum of res and com

WITH sub as(SELECT "in.county",timestamp_hour,extract(month from timestamp_hour) as "month",turnover,"in.state","year",county_hourly_kwh 
FROM long_county_hourly_TURNOVERID_amy -- variable?
),

county_hourly_totals as(
SELECT "in.county",timestamp_hour,extract(month from timestamp_hour) as "month",turnover,"in.state","year", sum(county_hourly_kwh) as county_total_hourly_kwh
FROM sub
GROUP BY "in.county",timestamp_hour,"month",turnover,"in.state","year"
)

SELECT *
FROM (
    SELECT *,
           RANK() OVER (PARTITION BY "in.county",turnover,"in.state","year","month" ORDER BY county_total_hourly_kwh DESC) AS rank_num
    FROM county_hourly_totals
) subquery
WHERE rank_num = 1;