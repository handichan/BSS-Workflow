-- to hold hourly disaggregation multipliers calculated from BuildStock
CREATE EXTERNAL TABLE {mult_res_hourly}(
    `in.weather_file_city` string,
    `in.weather_file_longitude` double,
    shape_ts string,
    timestamp_hour timestamp,
    kwh double,
    multiplier_hourly double,
    sector string,
    `in.state` string,
    end_use string
)
STORED AS parquet
LOCATION 's3://{dest_bucket}/{mult_res_hourly}/'
;
