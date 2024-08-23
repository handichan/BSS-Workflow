-- STEP 1: create the table including partitions

-- this is based off of res_ann_shares_dw.sql

CREATE TABLE annual_disaggregation_multipliers
WITH (
    partitioned_by = ARRAY['group_version', 'sector', 'in.state', 'end_use'],
    format = 'PARQUET') AS
WITH meta_filtered AS (
	SELECT meta."in.county",
	    meta."in.state",
	    meta.upgrade,
        -- make sure that group_ann is a varchar that is at least as long as the longest group_ann ('res_fanspump_ann_1' is 18 characters)
	    cast('res_dw_ann_1' as varchar(30)) AS group_ann,
		sum(meta."out.electricity.dishwasher.energy_consumption") as dw
	FROM "resstock_amy2018_release_2024.2_metadata" as meta
	WHERE meta.upgrade = 0
	GROUP BY 
		meta."in.county",
		meta."in.state",
		meta.upgrade
)
    SELECT "in.county",
    group_ann,
    dw / sum(dw) OVER (PARTITION BY "in.state", group_ann, upgrade) as multiplier_annual,
    -- the variables to partition by have to be the last ones and in the order specified in the partitioned_by clause
    '2024-07-19' AS group_version,
    'res' AS sector,
    "in.state",
    -- make sure that end_use is a varchar that is at least as long as the longest end use name ('Computers and Electronics' is 25 characters)
    cast('Other' as varchar(30)) AS end_use
FROM meta_filtered
;

