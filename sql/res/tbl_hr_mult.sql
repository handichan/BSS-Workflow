-- to hold hourly disaggregation multipliers calculated from BuildStock
CREATE EXTERNAL TABLE res_hourly_disaggregation_multipliers_VERSIONID(
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
LOCATION 's3://BUCKETNAMEID/res_hourly_multipliers_VERSIONID/'
;
