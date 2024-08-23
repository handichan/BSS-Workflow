-- to hold results from Scout runs that have been disaggregated to annual, county
CREATE EXTERNAL TABLE annual_county_hc (
    `in.county` string,
    fuel string,
    meas string,
    tech_stage string,
    multiplier_annual double,
    state_ann_kwh double,
    turnover string,
    county_ann_kwh double
    )
PARTITIONED BY(
    scout_run string,
    sector string,
    `in.state` string,
    year string,
    end_use string
)
STORED AS parquet
LOCATION 's3://handibucket/'
;