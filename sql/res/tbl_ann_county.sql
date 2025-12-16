-- to hold results from Scout runs that have been disaggregated to annual, county
CREATE EXTERNAL TABLE county_annual_res_{year}_{turnover}_{weather} (
    `in.county` string,
    `in.weather_file_city` string,
    `in.weather_file_longitude` double,
    fuel string,
    meas string,
    tech_stage string,
    multiplier_annual double,
    state_ann_kwh double,
    turnover string,
    county_ann_kwh double,
    scout_run string,
    sector string,
    `in.state` string,
    year int,
    end_use string
)
STORED AS parquet
LOCATION 's3://{dest_bucket}/{version}/county_runs/county_annual_res_{year}_{turnover}_{weather}/'
;