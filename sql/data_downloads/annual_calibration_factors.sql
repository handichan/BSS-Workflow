-- calibrated and uncalibrated electricity consumption

SELECT turnover, sector, "in.state", "year", sum(county_hourly_cal_kwh) as annual_calibrated_kwh, sum(county_hourly_uncal_kwh) as annual_uncalibrated_kwh
FROM long_county_hourly_{turnover}_{weather} 
GROUP BY turnover, sector, "in.state", "year";