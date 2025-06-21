-- state monthly by sector for 2024 baseline
-- for comparison with EIA 861

with with_month as 
(SELECT *, month(timestamp_hour) as "month" FROM long_county_hourly_TURNOVERID_amy 
WHERE "year" = 2024 AND turnover='baseline'
)

SELECT "month", turnover, sector, "in.state", "year", sum(county_hourly_kwh) as state_monthly_kwh
FROM with_month
GROUP BY "month", turnover, sector, "in.state", "year";