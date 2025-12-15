-- to hold hourly consumption by county and shape calculated from BuildStock
-- will be used to normalize HVAC shapes
CREATE EXTERNAL TABLE res_hourly_hvac_temp_{version}(
    `in.weather_file_city` string,
    `in.weather_file_longitude` double,
    shape_ts string,
    timestamp_hour timestamp,
    kwh double,
    sector string,
    `in.state` string,
    end_use string,
    fuel string
)
STORED AS parquet
LOCATION 's3://{dest_bucket}/res_hourly_hvac_temp_{version}/'
;
