-- state monthly by sector for comparison with EIA 861

with with_month as 
(SELECT turnover, sector, "in.state", "year", county_hourly_kwh, month(timestamp_hour) as "month" FROM long_county_hourly_TURNOVERID_amy 
WHERE turnover='baseline'
)

SELECT "month", turnover, sector, "in.state", "year", sum(county_hourly_kwh) as state_monthly_kwh
FROM with_month
GROUP BY "month", turnover, sector, "in.state", "year";