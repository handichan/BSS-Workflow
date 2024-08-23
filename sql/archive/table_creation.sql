-- only needs to be done once
CREATE EXTERNAL TABLE scout_results(
meas string,
reg string,
end_use string,
fuel string,
year int,
tech_stage string,
state_ann_kwh double
)
PARTITIONED BY (
scout_run string,
turnover string
)
STORED AS parquet
LOCATION 's3://scoutresults/';

-- when there are new Scout results, first upload them into their own table and then add them to scout_results
-- if this can be done in one step that'd be great too
DROP TABLE scout_temp;

CREATE EXTERNAL TABLE scout_temp(
meas string,
reg string,
end_use string,
fuel string,
year int,
tech_stage string,
state_ann_kwh double,
scout_run string,
turnover string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
-- update to point to the correct folder
LOCATION 's3://scoutresults/06-28/'
TBLPROPERTIES ('skip.header.line.count'='1');

INSERT INTO scout_results
SELECT meas,reg,end_use,fuel,year,tech_stage,state_ann_kwh, scout_run,turnover
FROM scout_temp;



-- only needs to be done once
CREATE EXTERNAL TABLE annual_county (
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
year int,
end_use string
)
STORED AS parquet
LOCATION 's3://margaretbucket/';


-- only needs to be done once
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
LOCATION 's3://hourlycounty/';



CREATE EXTERNAL TABLE measure_map_2(
meas string,
scout_end_use string,
original_ann string,
measure_ann string,
original_ts string,
measure_ts string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
-- update to point to the correct folder
LOCATION 's3://mappings-county/res/res_hvac_measures/'
TBLPROPERTIES ('skip.header.line.count'='1')
;



