-- to hold annual geographic disaggregation multipliers calculated from BuildStock
CREATE EXTERNAL TABLE res_annual_disaggregation_multipliers_VERSIONID(
    `in.county` string,
    group_ann string,
    multiplier_annual double,
    sector string,
    `in.state` string,
    end_use string
)
STORED AS parquet
LOCATION 's3://handibucket/res_annual_multipliers_VERSIONID/'
;