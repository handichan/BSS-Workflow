-- state monthly by sector for comparison with EIA 861
-- only needs to be run for one turnover

with with_month as 
(SELECT turnover, sector, fuel, "in.state", "year", county_hourly_uncal_kwh, month(timestamp_hour) as "month" 
FROM long_county_hourly_{turnover}_{disag_id} 
WHERE turnover='baseline' AND "year" < 2025
)

SELECT "month", turnover, sector, fuel, "in.state", "year", sum(county_hourly_uncal_kwh) as state_monthly_uncal_kwh
FROM with_month
GROUP BY "month", turnover, sector, fuel, "in.state", "year";