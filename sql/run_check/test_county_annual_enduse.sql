WITH comparison AS(
SELECT turnover, end_use,
       SUM(CASE WHEN source = 'commercial' THEN kwh ELSE 0 END) AS commercial_sum,
       SUM(CASE WHEN source = 'residential' THEN kwh ELSE 0 END) AS residential_sum,
       SUM(CASE WHEN source = 'state_com' THEN kwh ELSE 0 END) AS scout_commercial_sum,
       SUM(CASE WHEN source = 'state_res' THEN kwh ELSE 0 END) AS scout_residential_sum
FROM (
    SELECT 'commercial' AS source, turnover, end_use, county_ann_kwh AS kwh
    FROM long_county_annual_{turnover}_{weather}
    WHERE county_ann_kwh = county_ann_kwh AND "year" = {year} AND sector = 'com'

    UNION ALL
    
    SELECT 'residential' AS source, turnover, end_use, county_ann_kwh AS kwh
    FROM long_county_annual_{turnover}_{weather}
    WHERE county_ann_kwh = county_ann_kwh AND "year" = {year} AND sector = 'res'
    
    UNION ALL
    
    SELECT 'state_com' AS source, turnover, end_use, state_ann_kwh AS kwh
    FROM scout_annual_state_{turnover}
    WHERE "year" = {year} AND fuel = 'Electric' AND sector = 'com'
    
    UNION ALL
    
    SELECT 'state_res' AS source, turnover, end_use, state_ann_kwh AS kwh
    FROM scout_annual_state_{turnover}
    WHERE "year" = {year} AND fuel = 'Electric' AND sector = 'res'
) combined_results
GROUP BY turnover, end_use)

SELECT turnover, end_use, commercial_sum, residential_sum, scout_commercial_sum, scout_residential_sum,
1 - commercial_sum / scout_commercial_sum AS diff_commercial,
1 - residential_sum / scout_residential_sum AS diff_residential
FROM comparison
;

