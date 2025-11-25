CREATE TABLE wide_county_hourly_{turnover}_amy
WITH (
    external_location = 's3://{dest_bucket}/{version}/wide/county_hourly_{turnover}_{weather}/',
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
        date_parse(
            CONCAT(
                CAST(year AS VARCHAR), '-', 
                date_format(timestamp_hour, '%m-%d %H:%i:%s.%f')
            ), 
            '%Y-%m-%d %H:%i:%s.%f'
        ) AS date_time,
        end_use,
        county_hourly_uncal_kwh,
        county_hourly_cal_kwh
        FROM long_county_hourly_{turnover}_amy
        WHERE turnover != 'baseline'
    )

    SELECT 
        scenario,
        county,
        date_time,
        -- not sure why you have to use an aggregate expression here, but since there's only one value min, max, avg will all give the same result
        MAX(CASE WHEN end_use = 'Computers and Electronics' THEN county_hourly_uncal_kwh END) AS uncal_computers_electronics,
        MAX(CASE WHEN end_use = 'Cooking' THEN county_hourly_uncal_kwh END) AS uncal_cooking,
        MAX(CASE WHEN end_use = 'Cooling (Equip.)' THEN county_hourly_uncal_kwh END) AS uncal_cooling,
        MAX(CASE WHEN end_use = 'Heating (Equip.)' THEN county_hourly_uncal_kwh END) AS uncal_heating,
        MAX(CASE WHEN end_use = 'Lighting' THEN county_hourly_uncal_kwh END) AS uncal_lighting,
        MAX(CASE WHEN end_use = 'Other' THEN county_hourly_uncal_kwh END) AS uncal_other,
        MAX(CASE WHEN end_use = 'Refrigeration' THEN county_hourly_uncal_kwh END) AS uncal_refrigeration,
        MAX(CASE WHEN end_use = 'Ventilation' THEN county_hourly_uncal_kwh END) AS uncal_ventilation,
        MAX(CASE WHEN end_use = 'Water Heating' THEN county_hourly_uncal_kwh END) AS uncal_water_heating,
        MAX(CASE WHEN end_use = 'Computers and Electronics' THEN county_hourly_cal_kwh END) AS cal_computers_electronics,
        MAX(CASE WHEN end_use = 'Cooking' THEN county_hourly_cal_kwh END) AS cal_cooking,
        MAX(CASE WHEN end_use = 'Cooling (Equip.)' THEN county_hourly_cal_kwh END) AS cal_cooling,
        MAX(CASE WHEN end_use = 'Heating (Equip.)' THEN county_hourly_cal_kwh END) AS cal_heating,
        MAX(CASE WHEN end_use = 'Lighting' THEN county_hourly_cal_kwh END) AS cal_lighting,
        MAX(CASE WHEN end_use = 'Other' THEN county_hourly_cal_kwh END) AS cal_other,
        MAX(CASE WHEN end_use = 'Refrigeration' THEN county_hourly_cal_kwh END) AS cal_refrigeration,
        MAX(CASE WHEN end_use = 'Ventilation' THEN county_hourly_cal_kwh END) AS cal_ventilation,
        MAX(CASE WHEN end_use = 'Water Heating' THEN county_hourly_cal_kwh END) AS cal_water_heating,
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