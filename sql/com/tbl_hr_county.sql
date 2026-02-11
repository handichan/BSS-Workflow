-- to hold results from Scout runs that have been disaggregated to hourly, county
CREATE EXTERNAL TABLE county_hourly_com_{year}_{turnover}_{disag_id} (
    `in.county` string,
    timestamp_hour timestamp,
    turnover string,
    county_hourly_uncal_kwh double,
    county_hourly_cal_kwh double,
    scout_run string,
    sector string,
    `in.state` string,
    year int,
    end_use string,
    fuel string
)
STORED AS parquet
LOCATION 's3://{dest_bucket}/{disag_id}/county_runs/county_hourly_com_{year}_{turnover}_{disag_id}/'
