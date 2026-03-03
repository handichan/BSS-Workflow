-- to hold annual geographic disaggregation multipliers calculated from BuildStock
CREATE EXTERNAL TABLE {mult_res_annual}(
    `in.county` string,
    `in.weather_file_city` string,
    `in.weather_file_longitude` double,
    group_ann string,
    multiplier_annual double,
    sector string,
    `in.state` string,
    end_use string
)
STORED AS parquet
LOCATION 's3://{dest_bucket}/{mult_res_annual}/'
;