-- to understand which measures are the most impactful
WITH totals as (
SELECT meas, end_use, "year", turnover, sector, sum(state_ann_kwh) as ann_kwh FROM scout_annual_state_{turnover}
WHERE fuel = 'Electric' 
GROUP BY meas, end_use, "year", turnover, sector)

SELECT meas, end_use, "year", turnover, sector, ann_kwh,
ann_kwh / sum(ann_kwh) OVER (PARTITION BY end_use, "year", turnover, sector) as share_of_eu,
ann_kwh / sum(ann_kwh) OVER (PARTITION BY "year", turnover, sector) as share_of_sector,
ann_kwh / sum(ann_kwh) OVER (PARTITION BY "year", turnover) as share_of_total
FROM totals
;