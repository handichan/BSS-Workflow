-- to hold hourly disaggregation multipliers calculated from BuildStock
CREATE EXTERNAL TABLE com_hourly_disaggregation_multipliers_{version}(
    `in.county` string,
    shape_ts string,
    timestamp_hour timestamp,
    kwh double,
    multiplier_hourly double,
    sector string,
    `in.state` string,
    end_use string,
    fuel string
)
STORED AS parquet
LOCATION 's3://{dest_bucket}/com_hourly_multipliers_{version}/'
;
