-- to hold hourly disaggregation multipliers calculated from BuildStock
CREATE EXTERNAL TABLE hourly_disaggregation_multipliers(
    `in.county` string,
    shape_ts string,
    timestamp_hour timestamp,
    kwh double,
    multiplier_hourly double
)
PARTITIONED BY(
    group_version string,
    sector string,
    `in.state` string,
    end_use string
)
STORED AS parquet
LOCATION 's3://handibucket/'
;
