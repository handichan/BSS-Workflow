-- to hold hourly disaggregation multipliers calculated from BuildStock
CREATE EXTERNAL TABLE {mult_com_hourly}(
    `in.county` string,
    shape_ts string,
    timestamp_hour timestamp,
    kwh double,
    multiplier_hourly double,
    sector string,
    `in.state` string,
    end_use string
)
STORED AS parquet
LOCATION 's3://{dest_bucket}/{mult_com_hourly}/'
;
