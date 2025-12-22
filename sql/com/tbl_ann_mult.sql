-- to hold annual geographic disaggregation multipliers calculated from BuildStock
CREATE EXTERNAL TABLE {mult_com_annual}(
    `in.county` string,
    group_ann string,
    multiplier_annual double,
    sector string,
    `in.state` string,
    end_use string
)
STORED AS parquet
LOCATION 's3://{dest_bucket}/{mult_com_annual}/'
;