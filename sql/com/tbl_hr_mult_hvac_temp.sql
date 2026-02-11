-- to hold hourly consumption by county and shape calculated from BuildStock
-- will be used to normalize HVAC shapes
CREATE EXTERNAL TABLE {mult_com_hourly}_hvac_temp(
    `in.county` string,
    shape_ts string,
    timestamp_hour timestamp,
    kwh double,
    sector string,
    `in.state` string,
    end_use string,
    fuel string
)
STORED AS parquet
LOCATION 's3://{dest_bucket}/{mult_com_hourly}_hvac_temp/'
;
