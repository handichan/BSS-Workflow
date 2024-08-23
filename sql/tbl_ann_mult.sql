CREATE EXTERNAL TABLE annual_disaggregation_multipliers(
    `in.county` string,
    group_ann string,
    multiplier_annual double
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