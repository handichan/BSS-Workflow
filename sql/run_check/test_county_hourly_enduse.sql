SELECT turnover, end_use,
       SUM(CASE WHEN source = 'commercial' THEN kwh_uncal ELSE 0 END) AS commercial_uncal_sum,
       SUM(CASE WHEN source = 'residential' THEN kwh_uncal ELSE 0 END) AS residential_uncal_sum,
       SUM(CASE WHEN source = 'state_com' THEN kwh_uncal ELSE 0 END) AS scout_commercial_sum,
       SUM(CASE WHEN source = 'state_res' THEN kwh_uncal ELSE 0 END) AS scout_residential_sum,
       SUM(CASE WHEN source = 'commercial' THEN kwh_cal ELSE 0 END) AS commercial_cal_sum,
       SUM(CASE WHEN source = 'residential' THEN kwh_cal ELSE 0 END) AS residential_cal_sum

FROM (
    SELECT 'commercial' AS source, turnover, end_use, county_hourly_uncal_kwh AS kwh_uncal, county_hourly_cal_kwh AS kwh_cal
    FROM long_county_hourly_{turnover}_{weather}
    WHERE county_hourly_uncal_kwh = county_hourly_uncal_kwh AND "year" = {year} AND sector = 'com'

    UNION ALL
    
    SELECT 'residential' AS source, turnover, end_use, county_hourly_uncal_kwh AS kwh_uncal, county_hourly_cal_kwh AS kwh_cal
    FROM long_county_hourly_{turnover}_{weather}
    WHERE county_hourly_uncal_kwh = county_hourly_uncal_kwh AND "year" = {year} AND sector = 'res'
    
    UNION ALL
    
    SELECT 'state_com' AS source, turnover, end_use, state_ann_kwh AS kwh_uncal, NULL AS kwh_cal
    FROM scout_annual_state_{turnover}
    WHERE "year" = {year} AND fuel = 'Electric' AND sector = 'com'
    
    UNION ALL
    
    SELECT 'state_res' AS source, turnover, end_use, state_ann_kwh AS kwh_uncal, NULL AS kwh_cal
    FROM scout_annual_state_{turnover}
    WHERE "year" = {year} AND fuel = 'Electric' AND sector = 'res'
) combined_results
GROUP BY turnover, end_use;
