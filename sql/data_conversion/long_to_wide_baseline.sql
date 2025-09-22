CREATE TABLE wide_county_hourly_baseline_amy
WITH (
    external_location = 's3://{dest_bucket}/20250411/wide/county_hourly_baseline_amy/',
    format = 'Parquet'
    -- partitioned_by = ARRAY['sector', 'year', 'state']
) AS
    WITH formatted_cols AS(
    SELECT 
        turnover as scenario,
        "year",
        CASE 
            WHEN sector = 'res' THEN 'Residential'
            WHEN sector = 'com' THEN 'Commercial'
            ELSE sector 
        END AS sector,
        "in.state" as state,
        "in.county" as county,
        -- update the year of the timestamp to match the projection year
        date_parse(
            CONCAT(
                CAST(year AS VARCHAR), '-', 
                date_format(timestamp_hour, '%m-%d %H:%i:%s.%f')
            ), 
            '%Y-%m-%d %H:%i:%s.%f'
        ) AS date_time,
        end_use,
        county_hourly_cal_kwh
        -- convert to variable
        FROM long_county_hourly_ineff_amy
        -- we need the baseline from one table not all of them
        WHERE turnover = 'baseline'
    )

    SELECT 
        scenario,
        county,
        date_time,
        -- not sure why you have to use an aggregate expression here, but since there's only one value min, max, avg will all give the same result
        MAX(CASE WHEN end_use = 'Computers and Electronics' THEN county_hourly_cal_kwh END) AS computers_electronics,
        MAX(CASE WHEN end_use = 'Cooking' THEN county_hourly_cal_kwh END) AS cooking,
        MAX(CASE WHEN end_use = 'Cooling (Equip.)' THEN county_hourly_cal_kwh END) AS cooling,
        MAX(CASE WHEN end_use = 'Heating (Equip.)' THEN county_hourly_cal_kwh END) AS heating,
        MAX(CASE WHEN end_use = 'Lighting' THEN county_hourly_cal_kwh END) AS lighting,
        MAX(CASE WHEN end_use = 'Other' THEN county_hourly_cal_kwh END) AS other,
        MAX(CASE WHEN end_use = 'Refrigeration' THEN county_hourly_cal_kwh END) AS refrigeration,
        MAX(CASE WHEN end_use = 'Ventilation' THEN county_hourly_cal_kwh END) AS ventilation,
        MAX(CASE WHEN end_use = 'Water Heating' THEN county_hourly_cal_kwh END) AS water_heating,
        sector,
        "year",
        state
    FROM 
     formatted_cols 
     GROUP BY scenario ,
        "year",
        sector,
        state,
        county,
        date_time;