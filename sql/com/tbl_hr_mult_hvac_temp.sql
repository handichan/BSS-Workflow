-- to hold hourly consumption by county and shape calculated from BuildStock
-- will be used to normalize HVAC shapes
CREATE EXTERNAL TABLE com_hourly_hvac_temp_VERSIONID(
    `in.county` string,
    shape_ts string,
    timestamp_hour timestamp,
    kwh double,
    sector string,
    `in.state` string,
    end_use string
)
STORED AS parquet
LOCATION 's3://BUCKETNAMEID/com_hourly_multipliers_VERSIONID/'
;
