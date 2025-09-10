-- to hold annual geographic disaggregation multipliers calculated from BuildStock
CREATE EXTERNAL TABLE com_annual_disaggregation_multipliers_{version}(
    `in.county` string,
    group_ann string,
    multiplier_annual double,
    sector string,
    `in.state` string,
    end_use string
)
STORED AS parquet
LOCATION 's3://{dest_bucket}/com_annual_multipliers_{version}/'
;