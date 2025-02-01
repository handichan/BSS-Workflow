-- find top 100 hours in each county; sum of res and com

with county_totals as(
SELECT "in.county",timestamp_hour,turnover,"in.state","year",sum(county_hourly_kwh) as county_total_hourly_kwh 
FROM long_county_hourly_breakthrough_amy 
GROUP BY "in.county",timestamp_hour,turnover,"in.state","year"

UNION all
SELECT "in.county",timestamp_hour,turnover,"in.state","year",sum(county_hourly_kwh) as county_total_hourly_kwh 
FROM long_county_hourly_high_amy 
WHERE turnover!='baseline' -- don't need the baseline from both scenarios
GROUP BY "in.county",timestamp_hour,turnover,"in.state","year"

UNION all
SELECT "in.county",timestamp_hour,turnover,"in.state","year",sum(county_hourly_kwh) as county_total_hourly_kwh 
FROM long_county_hourly_ineff_amy 
WHERE turnover!='baseline' -- don't need the baseline from both scenarios
GROUP BY "in.county",timestamp_hour,turnover,"in.state","year"

UNION all
SELECT "in.county",timestamp_hour,turnover,"in.state","year",sum(county_hourly_kwh) as county_total_hourly_kwh 
FROM long_county_hourly_mid_amy 
WHERE turnover!='baseline' -- don't need the baseline from both scenarios
GROUP BY "in.county",timestamp_hour,turnover,"in.state","year"

UNION all
SELECT "in.county",timestamp_hour,turnover,"in.state","year",sum(county_hourly_kwh) as county_total_hourly_kwh 
FROM long_county_hourly_stated_amy 
WHERE turnover!='baseline' -- don't need the baseline from both scenarios
GROUP BY "in.county",timestamp_hour,turnover,"in.state","year"
)

SELECT *
FROM (
    SELECT *,
           RANK() OVER (PARTITION BY "in.county",turnover,"in.state","year" ORDER BY county_total_hourly_kwh DESC) AS rank_num
    FROM county_totals
) subquery
WHERE rank_num <= 100;