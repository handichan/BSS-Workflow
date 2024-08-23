-- to hold results from Scout runs that have been disaggregated to hourly, county
CREATE EXTERNAL TABLE hourly_county (
    `in.county` string,
    timestamp_hour timestamp,
    turnover string,
    county_hourly_kwh double
)
PARTITIONED BY(
    scout_run string,
    sector string,
    `in.state` string,
    year int,
    end_use string
)
STORED AS parquet
LOCATION 's3://handibucket/';
