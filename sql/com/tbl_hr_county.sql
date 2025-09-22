-- to hold results from Scout runs that have been disaggregated to hourly, county
CREATE EXTERNAL TABLE county_hourly_com_{year}_{turnover}_{weather} (
    `in.county` string,
    timestamp_hour timestamp,
    turnover string,
    county_hourly_kwh double,
    scout_run string,
    sector string,
    `in.state` string,
    year int,
    end_use string
)
STORED AS parquet
LOCATION 's3://{dest_bucket}/{version}/county_runs/county_hourly_com_{year}_{turnover}_{weather}/'
