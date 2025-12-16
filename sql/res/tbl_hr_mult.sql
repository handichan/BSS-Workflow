-- to hold hourly disaggregation multipliers calculated from BuildStock
CREATE EXTERNAL TABLE res_hourly_disaggregation_multipliers_{version}(
    `in.weather_file_city` string,
    shape_ts string,
    timestamp_hour timestamp,
    kwh double,
    multiplier_hourly double,
    sector string,
    `in.weather_file_longitude` double,
    end_use string,
    fuel string
)
STORED AS parquet
LOCATION 's3://{dest_bucket}/res_hourly_multipliers_{version}/'
;
