-- to hold annual geographic disaggregation multipliers calculated from BuildStock
CREATE EXTERNAL TABLE res_annual_disaggregation_multipliers_VERSIONID(
    `in.county` string,
    `in.weather_file_city` string,
    group_ann string,
    multiplier_annual double,
    sector string,
    `in.state` string,
    end_use string
)
STORED AS parquet
LOCATION 's3://BUCKETNAMEID/res_annual_multipliers_VERSIONID/'
;