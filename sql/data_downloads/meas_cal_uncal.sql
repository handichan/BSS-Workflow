
-- download calibrated and uncalibrated electricty by measure, state, year
with agg as (SELECT "in.state", "year", turnover, sector, end_use, sum(county_hourly_uncal_kwh) as uncal_kwh, sum(county_hourly_cal_kwh) as cal_kwh
FROM long_county_hourly_ref_amy
WHERE turnover!='baseline'
GROUP BY "in.state", "year", turnover, sector, end_use),

cal_mult as (
SELECT "in.state", "year", turnover, sector, end_use, cal_kwh/uncal_kwh as cal_over_uncal
FROM agg),

scout as(
SELECT meas, reg, "year", end_use, fuel, turnover, sector, sum(state_ann_kwh) as state_ann_kwh_uncal
FROM scout_annual_state_state
WHERE turnover!='baseline'
GROUP BY meas, reg, end_use, fuel, turnover, sector, "year")

SELECT meas, scout.reg as "in.state", scout."year", scout.end_use, scout.fuel, scout.turnover, scout.sector, state_ann_kwh_uncal, state_ann_kwh_uncal*cal_over_uncal as state_ann_kwh_cal
FROM scout LEFT JOIN cal_mult 
ON scout.reg = cal_mult."in.state" AND 
scout."year" = cal_mult."year" AND
scout.end_use = cal_mult.end_use AND
scout.turnover = cal_mult.turnover AND
scout.sector = cal_mult.sector;
