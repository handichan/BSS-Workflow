
with with_month as 
(SELECT turnover, sector, fuel, "in.state", "year", county_hourly_uncal_kwh, county_hourly_cal_kwh, month(timestamp_hour) as "month" 
FROM long_county_hourly_{turnover}_{disag_id} 
)

SELECT "month", turnover, sector, fuel, "in.state", "year", 
sum(county_hourly_uncal_kwh) as state_monthly_uncal_kwh,
sum(county_hourly_cal_kwh) as state_monthly_cal_kwh
FROM with_month
GROUP BY "month", turnover, sector, fuel, "in.state", "year";