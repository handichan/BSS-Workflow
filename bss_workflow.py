# bss_workflow.py (refactored)

import os
import sys
import time
import json
import math
import boto3
import pandas as pd
from io import StringIO
from os import getcwd
from argparse import ArgumentParser

# Optional R support (platform-specific setup)
import platform
import sys
from contextlib import redirect_stderr

# Try to import rpy2, but fail gracefully if R is not available
# First, try to set R_HOME if using conda environment
robjects = None
if "CONDA_PREFIX" in os.environ:
    conda_env = os.environ["CONDA_PREFIX"]
    potential_r = os.path.join(conda_env, "lib", "R")
    if os.path.exists(potential_r):
        os.environ["R_HOME"] = potential_r

try:
    # Suppress stderr to hide rpy2 initialization errors
    with redirect_stderr(open(os.devnull, 'w')):
        import rpy2.robjects as robjects
except Exception:
    # Silently fail - R support is optional
    robjects = None

pd.set_option("display.max_columns", None)

# ----------------------------
# Configuration
# ----------------------------

class Config:
    # Files/dirs
    JSON_PATH = "json/input.json"
    SQL_DIR = "sql"
    MAP_EU_DIR = "map_eu"
    MAP_MEAS_DIR = "map_meas"
    ENVELOPE_MAP_PATH = os.path.join(MAP_MEAS_DIR, "envelope_map.tsv")
    MEAS_MAP_PATH = os.path.join(MAP_MEAS_DIR, "measure_map.tsv")
    CALIB_MULT_PATH = os.path.join(MAP_MEAS_DIR, "calibration_multipliers.tsv")
    SCOUT_OUT_TSV = "scout/scout_tsv"
    SCOUT_IN_JSON = "scout/scout_json"
    OUTPUT_DIR = "agg_results"

    # data version identifiers
    SCOUT_RUN_DATE = "2026-01-09"       # identifier for the Scout result vintage
    VERSION_ID = "20260109"
    DISAG_ID = "20260305"               # identifier for disaggregated energy data
    WEATHER = "amy"

    # S3 locations
    DATABASE_NAME = "euss_oedi"         # S3 database in which all tables are located
    BUCKET_NAME = "margaretbucket"      # bucket in DATABASE_NAME where intermediate and long results will be stored
    EXTERNAL_S3_DIR = "datasets"        # folder in BUCKET_NAME where the files in MAP_EU_DIR, MAP_MEAS_DIR, CALIB_MULT_PATH will be uploaded
    DEST_BUCKET = "bss-workflow"        # bucket in DATABASE_NAME where publication ready results (e.g. wide tables) will be stored

    # names of tables that contain disaggregation multipliers
    MULTIPLIERS_TABLES = [
        f"com_annual_disaggregation_multipliers_{WEATHER}",   # mult_com_annual
        f"res_annual_disaggregation_multipliers_{WEATHER}",   # mult_res_annual
        f"com_hourly_disaggregation_multipliers_{WEATHER}",   # mult_com_hourly
        f"res_hourly_disaggregation_multipliers_{WEATHER}",   # mult_res_hourly
    ]

    # names of tables that contain BuildStock data - required to calculate disaggregation multipliers
    BLDSTOCK_TABLES = [
        "comstock_2025.1_parquet",                  # meta_com Commercial metadata
        "comstock_2025.1_by_state",                 # ts_com Commercial hourly data
        "comstock_2025.1_upgrade_0",                # gap_com Gap model
        "resstock_amy2018_release_2024.2_metadata", # meta_res Residential metadata
        "resstock_amy2018_release_2024.2_by_state"  # ts_res Residential hourly data
    ]

    # Scenarios to process
    # TURNOVERS = ["breakthrough", "ineff", "mid", "high", "stated"]
    # TURNOVERS = ['brk','aeo25_20to50_bytech_indiv','aeo25_20to50_bytech_gap_indiv']
    # this is used in bss paper
    TURNOVERS = ["aeo", "ref", "brk", "accel", "fossil", "state","dual_switch", "high_switch", "min_switch"]
    # TURNOVERS = ["aeo_010926"]
    # TURNOVERS = ['brk_010926']
    # TURNOVERS = ['brk_010926','aeo_010926']

    # scenarios that do NOT have envelope measures
    no_env_meas = [ 
        "aeo",
        "test",
        "fossil",
        "aeo25_20to50_byeu_indiv",
        "aeo25_20to50_bytech_gap_indiv",
        "aeo25_20to50_bytech_indiv",
        "min_switch",
        "dual_switch",
        "aeo_010926"
    ]

    # years in Scout results to process
    # YEARS = ['2026','2030','2035','2040','2045','2050']
    YEARS = ['2026','2030','2040','2050']
    # YEARS = ['2026']
    # YEARS = ['2022']
    # YEARS = ['2050']
    # YEARS = ['2020','2021','2022','2023','2024']
    # YEARS = ['2023','2024']
    # YEARS = ['2026','2030','2040']
    
    BASE_YEAR = '2026'      # baseline year for calculating % differences

    # states in Scout results to process
    US_STATES = [
        'AL', 'AR', 'AZ', 'CA', 'CO', 'CT', 'DC', 'DE', 'FL', 'GA',
        'IA', 'ID', 'IL', 'IN', 'KS', 'KY', 'LA', 'MA', 'MD', 'ME',
        'MI', 'MN', 'MO', 'MS', 'MT', 'NC', 'ND', 'NE', 'NH', 'NJ', 
        'NM', 'NV', 'NY', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC', 'SD',
        'TN', 'TX', 'UT', 'VA', 'VT', 'WA', 'WI', 'WV', 'WY',
    ]
    # US_STATES = ['WY']
    # US_STATES = ['OR']


    # input file locations -- change only if you rearranged folders and files compared to the repo
    JSON_PATH = "json/input.json"
    SQL_DIR = "sql"                     # folder that contains the SQL files
    MAP_EU_DIR = "map_eu"               # folder that contains the mapping files to define the disaggregation multipliers
    MAP_MEAS_DIR = "map_meas"           # folder that contains the measure map, envelope map, and calibration multipliers
    ENVELOPE_MAP_PATH = os.path.join(MAP_MEAS_DIR, "envelope_map.tsv")
    MEAS_MAP_PATH = os.path.join(MAP_MEAS_DIR, "measure_map.tsv")
    CALIB_MULT_PATH = os.path.join(MAP_MEAS_DIR, "calibration_multipliers.tsv")
    EIA_GROSS_PATH = "map_meas/eia_gross_consumption_by_state_sector_year_month.csv"    # file with monthly EIA electricity and gas consumption
    SCOUT_OUT_TSV = "scout_tsv"         # location where transformed Scout files will be saved as TSV
    SCOUT_IN_JSON = "scout_results"     # location of raw JSON files from Scout


# ----------------------------
# Utilities
# ----------------------------

def get_end_uses(sectorid: str):
    if sectorid == "res":
        return ['Computers and Electronics', 'Cooking', 'Cooling (Equip.)', 'Heating (Equip.)',
                'Lighting', 'Other', 'Refrigeration', 'Water Heating'
                ]
    return ['Computers and Electronics', 'Cooking', 'Cooling (Equip.)', 'Heating (Equip.)',
            'Lighting', 'Other', 'Refrigeration', 'Ventilation', 'Water Heating'
            ]


def get_boto3_clients():
    session = boto3.Session()
    return session.client("s3"), session.client("athena")


def start_athena_query(athena_client, query: str, output_location: str, database: str):
    return athena_client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={"Database": database},
        ResultConfiguration={"OutputLocation": output_location},
    )["QueryExecutionId"]


def wait_for_query_completion(athena_client, query_execution_id: str):
    while True:
        resp = athena_client.get_query_execution(QueryExecutionId=query_execution_id)
        status = resp["QueryExecution"]["Status"]["State"]
        if status == "SUCCEEDED":
            print("Query succeeded.")
            return resp
        if status in ["FAILED", "CANCELLED"]:
            reason = resp["QueryExecution"]["Status"].get("StateChangeReason", "Unknown")
            raise RuntimeError(f"Athena query {status.lower()}: {reason}")
        time.sleep(2)


def fetch_athena_results_csv_to_df(s3_client, results_s3_uri: str) -> pd.DataFrame:
    """
    Download CSV from s3://bucket/prefix/key into a DataFrame via boto3.
    """
    if not results_s3_uri.startswith("s3://"):
        raise ValueError(f"Unexpected results location: {results_s3_uri}")

    _, rest = results_s3_uri.split("s3://", 1)
    bucket, key = rest.split("/", 1)

    obj = s3_client.get_object(Bucket=bucket, Key=key)
    body = obj["Body"].read().decode("utf-8")
    return pd.read_csv(StringIO(body))


def execute_athena_query(athena_client, query: str, cfg: Config, *, is_create: bool, wait=True):
    """
    Generic runner. If wait=True, returns (results_s3_uri, df_or_None).
    If is_create=True, df_or_None is None.
    """
    output_location = f"s3://{cfg.BUCKET_NAME}/configs/"
    qid = start_athena_query(athena_client, query, output_location, cfg.DATABASE_NAME)

    if not wait:
        return qid, None

    qexec = wait_for_query_completion(athena_client, qid)
    result_loc = qexec["QueryExecution"]["ResultConfiguration"]["OutputLocation"]
    print(f"Results at: {result_loc}")
    return result_loc, (None if is_create else result_loc)


def execute_athena_query_to_df(s3_client, athena_client, query: str, cfg: Config) -> pd.DataFrame:
    """
    Execute a SELECT and return a DataFrame.
    """
    output_location = f"s3://{cfg.BUCKET_NAME}/diagnosis_csv/"
    qid = start_athena_query(athena_client, query, output_location, cfg.DATABASE_NAME)
    qexec = wait_for_query_completion(athena_client, qid)
    results_uri = qexec["QueryExecution"]["ResultConfiguration"]["OutputLocation"]
    return fetch_athena_results_csv_to_df(s3_client, results_uri)

def execute_athena_query_to_df2(s3_client, athena_client, query: str, table_name, cfg: Config) -> pd.DataFrame:
    """
    Execute a SELECT and return a DataFrame.
    """

    s3_bucket_target = "bss-workflow"
    s3_folder = "v2/annual/"
    s3_output_prefix = "athena_results/"
    output_location = f"s3://{cfg.BUCKET_NAME}/{s3_output_prefix}/"
    qid = start_athena_query(athena_client, query, output_location, cfg.DATABASE_NAME)
    qexec = wait_for_query_completion(athena_client, qid)
    results_uri = qexec["QueryExecution"]["ResultConfiguration"]["OutputLocation"]
    df = fetch_athena_results_csv_to_df(s3_client, results_uri)

    local_parquet_file = f"{table_name}.parquet"
    df.to_csv(f"{table_name}.csv", index=False)
    df.to_parquet(local_parquet_file, engine="pyarrow")
    print(f"Results saved as {local_parquet_file}")
    
    s3_client.upload_file(local_parquet_file, s3_bucket_target, f"{s3_folder}{local_parquet_file}")
    print(f"{local_parquet_file} is uploaded to {s3_bucket_target}/{s3_folder}")   


def upload_file_to_s3(s3_client, local_path: str, bucket: str, s3_path: str):
    s3_client.upload_file(local_path, bucket, s3_path)
    print(f"UPLOADED {os.path.basename(local_path)} to s3://{bucket}/{s3_path}")


def list_all_objects(s3_client, bucket: str, prefix: str):
    paginator = s3_client.get_paginator("list_objects_v2")
    items = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        items.extend(page.get("Contents", []))
    return items


def infer_column_types(df: pd.DataFrame):
    dtypes_map = {
        "object": "string",
        "int64": "int",
        "int32": "int",
        "float64": "double",
        "float32": "double",
        "bool": "boolean",
        "datetime64[ns]": "timestamp",
    }
    cols = []
    for col in df.columns:
        dtype = str(df[col].dtype)
        athena_type = "string" if col == "upgrade" else dtypes_map.get(dtype, "string")
        cols.append(f"`{col}` {athena_type}")
    return ",\n    ".join(cols)


def sql_create_table(df: pd.DataFrame, table_name: str, file_format: str, cfg: Config):
    file_format = file_format.lower()
    if file_format not in ("csv", "tsv"):
        raise ValueError("Unsupported file format. Use 'csv' or 'tsv'.")
    delimiter = "," if file_format == "csv" else "\t"
    schema = infer_column_types(df)
    return f"""
    CREATE EXTERNAL TABLE IF NOT EXISTS {table_name} (
        {schema}
    )
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY '{delimiter}'
    LOCATION 's3://{cfg.BUCKET_NAME}/{cfg.EXTERNAL_S3_DIR}/{table_name}/'
    TBLPROPERTIES ('skip.header.line.count'='1');
    """.strip()


def read_sql_file(rel_path: str, cfg: Config):
    abs_path = os.path.join(cfg.SQL_DIR, rel_path) if not os.path.isabs(rel_path) else rel_path
    with open(abs_path, "r", encoding="utf-8") as f:
        return f.read()


def file_to_df(file_path: str) -> pd.DataFrame:
    if file_path.endswith(".tsv"):
        return pd.read_csv(file_path, sep="\t")
    if file_path.endswith(".csv"):
        return pd.read_csv(file_path)
    raise ValueError("Please provide a .tsv or .csv file.")


def reshape_json(data, path=None):
    if path is None:
        path = []
    rows = []
    if isinstance(data, dict):
        for key, value in data.items():
            rows.extend(reshape_json(value, path + [key]))
    else:
        rows.append(path + [data])
    return rows


# ----------------------------
# Scout Generation # CORRECT
# ----------------------------


def add_sector(row: pd.Series):
    meas = row.get("meas")
    if pd.isna(meas):
        return None
    sec = str(meas).split(" ")[0]
    if "(C)" in sec or "Gap" in sec:
        return "com"
    if "(R)" in sec:
        return "res"
    return None

def compute_no_package_energy(wide_df: pd.DataFrame) -> pd.DataFrame:
    """
    From efficient_* columns (MMBtu), compute annual kWh columns.
    """
    if "efficient_measure_env_mmbtu" not in wide_df.columns:
        df = wide_df.copy()
    else:
        df = wide_df[wide_df["efficient_measure_env_mmbtu"].isna()].copy()

    df["original_ann"] = (df["efficient_mmbtu"] - df["efficient_measure_mmbtu"]) / 3412 * 1e6
    df["measure_ann"] = df["efficient_measure_mmbtu"] / 3412 * 1e6
    return df


def compute_with_package_energy(wide_df: pd.DataFrame, include_bldg_type: bool, envelope_map: pd.DataFrame) -> pd.DataFrame:
    """
    For envelope packages; requires efficient_measure_env_mmbtu column.
    """
    if "efficient_measure_env_mmbtu" not in wide_df.columns:
        return pd.DataFrame(columns=wide_df.columns)

    df = wide_df[~pd.isna(wide_df["efficient_measure_env_mmbtu"])].copy()
    df = df.merge(envelope_map, on="meas", how="left")

    def calc_measure(row):
        comp = row.get("component")
        if comp == "equipment":
            return (row["efficient_measure_mmbtu"] - row["efficient_measure_env_mmbtu"]) / 3412 * 1e6
        if comp == "equipment + env":
            return row["efficient_measure_env_mmbtu"] / 3412 * 1e6
        return None

    def calc_original(row):
        comp = row.get("component")
        if comp == "equipment":
            return (row["efficient_mmbtu"] - row["efficient_measure_mmbtu"]) / 3412 * 1e6
        if comp == "equipment + env":
            return 0
        return None

    df["measure_ann"] = df.apply(calc_measure, axis=1)
    df["original_ann"] = df.apply(calc_original, axis=1)

    keep_cols = ["meas_separated", "reg", "end_use", "fuel", "year",
                 "efficient_mmbtu", "efficient_measure_mmbtu",
                 "efficient_measure_env_mmbtu", "original_ann", "measure_ann"]
    if include_bldg_type and "bldg_type" in df.columns:
        keep_cols.insert(2, "bldg_type")
    keep_cols = [c for c in keep_cols if c in df.columns]
    return df[keep_cols].rename(columns={"meas_separated": "meas"})


def _scout_json_to_df(filename: str, include_env: bool, cfg: Config) -> pd.DataFrame:
    new_columns = [
        "meas", "adoption_scn", "metric", "reg",
        "bldg_type", "end_use", "fuel", "year", "value"
    ]
    with open(filename, "r") as f:
        json_df = json.load(f)
    meas_keys = list(json_df.keys())[:-1]

    all_df = pd.DataFrame()
    weights_df_all = pd.DataFrame() 

    for mea in meas_keys:
        json_data = json_df[mea]["Markets and Savings (by Category)"]
        data_from_json = reshape_json(json_data)
        df_from_json = pd.DataFrame(data_from_json)
        df_from_json["meas"] = mea
        all_df = df_from_json if all_df.empty else pd.concat([all_df, df_from_json], ignore_index=True)
        cols = ["meas"] + [c for c in all_df.columns if c != "meas"]
        all_df = all_df[cols]

        # ---- ComStock Gap Weights ----
        if "ComStock Gap Weights" in json_df[mea]:
            w_data = reshape_json(json_df[mea]["ComStock Gap Weights"])
            wdf = pd.DataFrame(w_data)
            wdf["meas"] = mea
            wdf = wdf[["meas",0,1,2]]
            wdf.columns = ["meas", "bldg_type", "year", "gap_weight"]
            weights_df_all = wdf if weights_df_all.empty else pd.concat([weights_df_all, wdf], ignore_index=True)

    # Name Markets & Savings columns and keep only energy-use metrics there
    all_df.columns = new_columns

    metrics = [
        "Efficient Energy Use (MMBtu)",
        "Efficient Energy Use, Measure (MMBtu)",
        "Baseline Energy Use (MMBtu)"
    ]
    if include_env:
        metrics.append("Efficient Energy Use, Measure-Envelope (MMBtu)")
        
    all_df = all_df[all_df["metric"].isin(metrics)].copy()

    # Fix measures without a fuel key
    to_shift = all_df[pd.isna(all_df["value"])].copy()
    if not to_shift.empty:
        to_shift.loc[:, "value"] = pd.to_numeric(to_shift["year"], errors="coerce")
        to_shift.loc[:, "year"] = to_shift["fuel"]
        to_shift.loc[:, "fuel"] = "Electric"
        df = pd.concat([all_df[pd.notna(all_df["value"])], to_shift])
    else:
        df = all_df

    out_path = os.path.join(f"{cfg.SCOUT_OUT_TSV}",
        f"scout_annual_state_{os.path.basename(filename).split('/')[0]}_df.tsv")
    os.makedirs(cfg.SCOUT_OUT_TSV, exist_ok=True)
    df.to_csv(out_path, sep="\t", index=False)

    out_weight_path = os.path.join(f"{cfg.SCOUT_OUT_TSV}",
        f"scout_annual_state_{os.path.basename(filename).split('/')[0]}_weights_df.tsv")
    weights_df_all.to_csv(out_weight_path, sep="\t", index=False)

    return df, weights_df_all



def scout_to_df(filename: str, cfg: Config) -> pd.DataFrame:
    return _scout_json_to_df(filename, include_env=True, cfg=cfg)


def scout_to_df_noenv(filename: str, cfg: Config) -> pd.DataFrame:
    return _scout_json_to_df(filename, include_env=False, cfg=cfg)


def _calc_annual_common(df: pd.DataFrame, gap_weights: pd.DataFrame, include_baseline: bool, turnover: str, include_bldg_type: bool, cfg: Config, include_env: bool): 
    envelope_map = file_to_df(cfg.ENVELOPE_MAP_PATH)

    grouping_cols = ["meas", "metric", "reg", "end_use", "fuel", "year"]
    pivot_index = ["meas", "reg", "end_use", "fuel", "year"]


    if include_bldg_type:
        if "bldg_type" in df.columns:
            grouping_cols.insert(3, "bldg_type")
            pivot_index.insert(2, "bldg_type")

    efficient_metrics = [
        "Efficient Energy Use (MMBtu)",
        "Efficient Energy Use, Measure (MMBtu)",
    ]
    if include_env:
        efficient_metrics.append("Efficient Energy Use, Measure-Envelope (MMBtu)")

    df = df.copy()

    efficient = df[df["metric"].isin(efficient_metrics)].copy()
    grouped = efficient.groupby(grouping_cols, dropna=False)["value"].sum().reset_index()
    wide = grouped.pivot(index=pivot_index, columns="metric", values="value").reset_index()

    rename_map = {
        "Efficient Energy Use (MMBtu)": "efficient_mmbtu",
        "Efficient Energy Use, Measure (MMBtu)": "efficient_measure_mmbtu",
        "Efficient Energy Use, Measure-Envelope (MMBtu)": "efficient_measure_env_mmbtu",
    }
    wide = wide.rename(columns={k: v for k, v in rename_map.items() if k in wide.columns})

    keep_cols = pivot_index + ["original_ann", "measure_ann"]

    # No-package
    no_pkg = compute_no_package_energy(wide)
    no_pkg = no_pkg[keep_cols] if set(keep_cols).issubset(no_pkg.columns) else no_pkg

    # With env-package (optional)
    frames = [no_pkg]
    if include_env:
        with_pkg = compute_with_package_energy(wide, include_bldg_type, envelope_map)
        with_pkg = with_pkg[keep_cols] if set(keep_cols).issubset(with_pkg.columns) else with_pkg
        frames.append(with_pkg)

    dflong = pd.concat(frames, ignore_index=True).melt(
        id_vars=pivot_index,
        value_vars=["original_ann", "measure_ann"],
        var_name="tech_stage",
        value_name="state_ann_kwh",
    )
    dflong["turnover"] = turnover

    # Optional baseline
    if include_baseline:
        base = df[df["metric"] == "Baseline Energy Use (MMBtu)"].copy()
        grouped_base = base.groupby(grouping_cols, dropna=False)["value"].sum().reset_index()
        grouped_base["state_ann_kwh"] = grouped_base["value"] / 3412 * 1e6
        grouped_base["tech_stage"] = "original_ann"
        grouped_base["turnover"] = "baseline"

        if include_env:
            # Align with 'equipment' split only
            grouped_base = grouped_base.merge(
                envelope_map[envelope_map["component"] == "equipment"],
                on="meas",
                how="left",
            )
            grouped_base["meas"] = grouped_base.apply(
                lambda r: r["meas_separated"] if pd.notnull(r.get("meas_separated")) and isinstance(r.get("meas_separated"), str) else r["meas"],
                axis=1,
            )

        final_cols = pivot_index + ["tech_stage", "state_ann_kwh", "turnover"]
        final_cols = [c for c in final_cols if c in grouped_base.columns]
        dflong = pd.concat([dflong, grouped_base[final_cols]], ignore_index=True)

    dflong["sector"] = dflong.apply(add_sector, axis=1)
    dflong["scout_run"] = cfg.SCOUT_RUN_DATE

    dflong_before_split = dflong.copy()
    # --- GAP SPLIT: commercial-electric only, using mirrored gap_weights ---
    if not gap_weights.empty:
        subset_mask = (dflong.get("sector") == "com") & (dflong.get("fuel") == "Electric")
        long_subset = dflong.loc[subset_mask].copy()

        # Join keys mirror baseline key set (present in dflong)
        join_keys = ["meas", "bldg_type", "year"]  

        if not long_subset.empty and all(k in long_subset.columns for k in join_keys):
            merged = long_subset.merge(
                gap_weights, on=join_keys, how="left", validate="m:1"
            )
            merged["gap_weight"] = merged["gap_weight"].fillna(0.0).astype(float)

            cols = list(merged.columns)
            # portion modeled in ComStock
            part1 = merged.copy()
            part1["state_ann_kwh"] = (1.0 - part1["gap_weight"]) * part1["state_ann_kwh"]

            # gap portion
            part2 = merged.copy()
            part2["state_ann_kwh"] = part2["gap_weight"] * part2["state_ann_kwh"]
            part2["meas"] = "Gap"

            expanded = pd.concat([part1[cols], part2[cols]], ignore_index=True)
            dflong = pd.concat([dflong.loc[~subset_mask], expanded], ignore_index=True)


            # --- conservation check: state_ann_kwh before vs after applying gap split ---
            os.makedirs("diagnostics", exist_ok=True)

            # group-by keys that should remain identical across the split (exclude 'meas')
            group_keys = [k for k in [
                "reg", "bldg_type", "end_use", "fuel", "year",
                "tech_stage", "turnover", "sector", "scout_run"
            ] if k in dflong.columns]

            # OVERALL (all rows, excluding 'meas')
            pre_all = (
                dflong_before_split
                .groupby(group_keys, dropna=False)["state_ann_kwh"].sum()
                .reset_index()
                .rename(columns={"state_ann_kwh": "kwh_before"})
            )
            post_all = (
                dflong
                .groupby(group_keys, dropna=False)["state_ann_kwh"].sum()
                .reset_index()
                .rename(columns={"state_ann_kwh": "kwh_after"})
            )

            cmp_all = pre_all.merge(post_all, on=group_keys, how="outer")

            # Explicitly convert to numeric first
            cmp_all["kwh_before"] = pd.to_numeric(cmp_all["kwh_before"], errors="coerce")
            cmp_all["kwh_after"]  = pd.to_numeric(cmp_all["kwh_after"], errors="coerce")

            # Now fill NaNs safely
            cmp_all = cmp_all.fillna({"kwh_before": 0.0, "kwh_after": 0.0})

            cmp_all["delta"] = cmp_all["kwh_after"] - cmp_all["kwh_before"]
            cmp_all["pct_delta"] = cmp_all.apply(
                lambda r: (r["delta"] / r["kwh_before"]) if r["kwh_before"] else (0.0 if r["kwh_after"] == 0 else float("inf")),
                axis=1
            )
            cmp_all["scope"] = "ALL_ROWS"

            # COM + ELECTRIC subset (the only rows we modify)
            mask_pre = (dflong_before_split.get("sector") == "com") & (dflong_before_split.get("fuel") == "Electric")
            mask_post = (dflong.get("sector") == "com") & (dflong.get("fuel") == "Electric")

            pre_sub = (
                dflong_before_split.loc[mask_pre, group_keys + ["state_ann_kwh"]]
                .groupby(group_keys, dropna=False)["state_ann_kwh"].sum()
                .reset_index()
                .rename(columns={"state_ann_kwh": "kwh_before"})
            )
            post_sub = (
                dflong.loc[mask_post, group_keys + ["state_ann_kwh"]]
                .groupby(group_keys, dropna=False)["state_ann_kwh"].sum()
                .reset_index()
                .rename(columns={"state_ann_kwh": "kwh_after"})
            )
            cmp_sub = pre_sub.merge(post_sub, on=group_keys, how="outer")

            # Explicitly convert to numeric first
            cmp_sub["kwh_before"] = pd.to_numeric(cmp_sub["kwh_before"], errors="coerce")
            cmp_sub["kwh_after"]  = pd.to_numeric(cmp_sub["kwh_after"], errors="coerce")

            cmp_sub = cmp_sub.fillna({"kwh_before": 0.0, "kwh_after": 0.0})

            cmp_sub["delta"] = cmp_sub["kwh_after"] - cmp_sub["kwh_before"]
            cmp_sub["pct_delta"] = cmp_sub.apply(
                lambda r: (r["delta"] / r["kwh_before"]) if r["kwh_before"] else (0.0 if r["kwh_after"] == 0 else float("inf")),
                axis=1
            )
            cmp_sub["scope"] = "COM_ELECTRIC_ONLY"

            # Totals rows at the top for quick glance
            def _totals_row(scope, df):
                tot_before = df["kwh_before"].sum()
                tot_after = df["kwh_after"].sum()
                row = {k: "ALL" for k in group_keys}
                row.update({
                    "kwh_before": tot_before,
                    "kwh_after": tot_after,
                    "delta": tot_after - tot_before,
                    "pct_delta": ((tot_after - tot_before) / tot_before) if tot_before else (0.0 if tot_after == 0 else float("inf")),
                    "scope": scope + "_TOTAL"
                })
                return pd.DataFrame([row])

            cmp_all = pd.concat([_totals_row("ALL_ROWS", cmp_all), cmp_all], ignore_index=True)
            cmp_sub = pd.concat([_totals_row("COM_ELECTRIC_ONLY", cmp_sub), cmp_sub], ignore_index=True)

            diagnostics_df = pd.concat([cmp_all, cmp_sub], ignore_index=True)
            out_csv = os.path.join("diagnostics", f"gap_kwh_conservation_{turnover}.csv")
            diagnostics_df.to_csv(out_csv, index=False)
            print(f"[Gap conservation check] Wrote {out_csv}")


    os.makedirs(cfg.SCOUT_OUT_TSV, exist_ok=True)
    local_path = os.path.join(cfg.SCOUT_OUT_TSV, f"scout_annual_state_{turnover}.tsv")
    dflong.to_csv(local_path, sep="\t", index=False)
    return dflong, local_path


def calc_annual(df: pd.DataFrame, gap_weights: pd.DataFrame, include_baseline: bool, turnover: str, include_bldg_type: bool, cfg: Config):
    return _calc_annual_common(df, gap_weights, include_baseline, turnover, include_bldg_type, cfg, include_env=True)


def calc_annual_noenv(df: pd.DataFrame, gap_weights: pd.DataFrame, include_baseline: bool, turnover: str, include_bldg_type: bool, cfg: Config):
    return _calc_annual_common(df, gap_weights, include_baseline, turnover, include_bldg_type, cfg, include_env=False)


# ----------------------------
# CSV/JSON conversions for templates
# ----------------------------

def convert_json_to_csv_folder(json_path: str, out_dir: str):
    os.makedirs(out_dir, exist_ok=True)
    with open(json_path, "r") as jf:
        data = json.load(jf)
    for key, val in data.items():
        df = pd.DataFrame(val)
        out = os.path.join(out_dir, f"{key}.csv")
        df.to_csv(out, index=False)
        print(f"CSV saved: {out}")


def convert_csv_folder_to_json(folder_path: str, json_path: str):
    json_dat = {}
    for file_name in os.listdir(folder_path):
        full = os.path.join(folder_path, file_name)
        if os.path.isfile(full) and file_name.endswith(".csv"):
            df = pd.read_csv(full).fillna("None")
            json_dat[os.path.splitext(file_name)[0]] = {col: df[col].tolist() for col in df.columns}
    with open(json_path, "w") as jf:
        json.dump(json_dat, jf, indent=4)
    print(f"Combined JSON saved to {json_path}")


# ----------------------------
# S3 table creators
# ----------------------------

def s3_create_tables_from_csvdir(s3_client, athena_client, cfg: Config):
    for file_name in os.listdir(cfg.MAP_EU_DIR):
        local_path = os.path.join(cfg.MAP_EU_DIR, file_name)
        table_name = os.path.splitext(file_name)[0]
        file_ext = os.path.splitext(file_name)[-1][1:].lower()
        if not os.path.isfile(local_path) or file_ext not in ("csv", "tsv"):
            continue

        s3_path = f"{cfg.EXTERNAL_S3_DIR}/{table_name}/{file_name}"
        upload_file_to_s3(s3_client, local_path, cfg.BUCKET_NAME, s3_path)

        delimiter = "," if file_ext == "csv" else "\t"
        df = pd.read_csv(local_path, delimiter=delimiter)
        query = sql_create_table(df, table_name, file_ext, cfg)
        execute_athena_query(athena_client, query, cfg, is_create=True, wait=True)


def s3_create_table_from_tsv(s3_client, athena_client, local_path: str, cfg: Config):
    file_name = os.path.basename(local_path)
    table_name = os.path.splitext(file_name)[0]
    file_ext = os.path.splitext(file_name)[-1][1:].lower()
    if file_ext not in ("csv", "tsv"):
        raise ValueError("Provide a .csv or .tsv file.")

    s3_path = f"{cfg.EXTERNAL_S3_DIR}/{table_name}/{file_name}"
    upload_file_to_s3(s3_client, local_path, cfg.BUCKET_NAME, s3_path)

    delimiter = "," if file_ext == "csv" else "\t"
    df = pd.read_csv(local_path, delimiter=delimiter)
    query = sql_create_table(df, table_name, file_ext, cfg)
    execute_athena_query(athena_client, query, cfg, is_create=True, wait=True)


def s3_insert_to_table_from_tsv(s3_client, athena_client, local_path: str, dest_table_name: str, cfg: Config):
    table_name = os.path.splitext(os.path.basename(local_path))[0]
    s3_create_table_from_tsv(s3_client, athena_client, local_path, cfg)
    sql = f"INSERT INTO {dest_table_name} SELECT * FROM {table_name};"
    execute_athena_query(athena_client, sql, cfg, is_create=True, wait=True)


def sql_to_s3table(athena_client, cfg: Config, sql_file: str, sectorid: str, yearid: str, turnover: str):
    sql_rel = f"{sectorid}/{sql_file}"
    template_raw = read_sql_file(sql_rel, cfg)

    sectorlong = "Commercial" if sectorid == "com" else "Residential"
    base_kwargs = dict(
        turnover=turnover,
        # version=cfg.MULT_VERSION_ID,
        sector=sectorid,
        year=yearid,
        dest_bucket=cfg.BUCKET_NAME,
        scout_version=cfg.SCOUT_RUN_DATE,
        sectorlong=sectorlong,
        disag_id=cfg.DISAG_ID,
        baseyear=cfg.BASE_YEAR,
    
        mult_com_annual=cfg.MULTIPLIERS_TABLES[0],
        mult_res_annual=cfg.MULTIPLIERS_TABLES[1],
        mult_com_hourly=cfg.MULTIPLIERS_TABLES[2],
        mult_res_hourly=cfg.MULTIPLIERS_TABLES[3],

        meta_com=cfg.BLDSTOCK_TABLES[0],
        ts_com=cfg.BLDSTOCK_TABLES[1],
        gap_com=cfg.BLDSTOCK_TABLES[2],
        meta_res=cfg.BLDSTOCK_TABLES[3],
        ts_res=cfg.BLDSTOCK_TABLES[4]
    )

    contains_state = "{state}" in template_raw
    contains_enduse = "{enduse}" in template_raw

    def render(**kw):
        return template_raw.format(**kw)


    # Case 1: both {state} and {enduse}
    if contains_state and contains_enduse:
        for st in cfg.US_STATES:
            for eu in get_end_uses(sectorid):
                q = render(**base_kwargs, state=st, enduse=eu)
                print(
                    f"RUN {sql_rel} | sector={sectorid} turnover={turnover} "
                    f"year={yearid} state={st} enduse={eu}"
                )
                execute_athena_query(athena_client, q, cfg, is_create=False, wait=True)
        return
    # Case 2: only {state}
    if contains_state:
        for st in cfg.US_STATES:
            q = render(**base_kwargs, state=st)
            print(
                f"RUN {sql_rel} | sector={sectorid} turnover={turnover} "
                f"year={yearid} state={st}"
            )
            execute_athena_query(athena_client, q, cfg, is_create=False, wait=True)
        return
    # Case 3: only {enduse}
    if contains_enduse:
        for eu in get_end_uses(sectorid):
            q = render(**base_kwargs, enduse=eu)
            print(f"RUN {sql_rel} | sector={sectorid} turnover={turnover} year={yearid} enduse={eu}")
            execute_athena_query(athena_client, q, cfg, is_create=False, wait=True)
        return
    # Case 4: neither placeholder -> single render
    q = render(**base_kwargs)
    print(f"RUN {sql_rel} | sector={sectorid} turnover={turnover} year={yearid}")
    execute_athena_query(athena_client, q, cfg, is_create=False, wait=True)


# ----------------------------
# Specific pipelines
# ----------------------------

def gen_multipliers(s3_client, athena_client, cfg: Config):
    sectors = ["com", "res"]
    tbl_res = [
        "tbl_ann_mult.sql",
        "res_ann_shares_cook.sql",
        "res_ann_shares_cooling_delivered.sql",
        "res_ann_shares_cw.sql",
        "res_ann_shares_dry.sql",
        "res_ann_shares_dw.sql",
        "res_ann_shares_fanspumps.sql",
        "res_ann_shares_heat_delivered.sql",
        "res_ann_shares_hvac.sql",
        "res_ann_shares_lighting.sql",
        "res_ann_shares_misc.sql",
        "res_ann_shares_poolpump.sql",
        "res_ann_shares_refrig.sql",
        "res_ann_shares_wh.sql",
        "res_ann_shares_wh_delivered.sql",
        "tbl_hr_mult.sql",
        "res_hourly_shares_cook.sql",
        "res_hourly_shares_cw.sql",
        "res_hourly_shares_dry.sql",
        "res_hourly_shares_dw.sql",
        "res_hourly_shares_fanspumps.sql",
        # "res_hourly_shares_gap.sql",       
        "res_hourly_shares_lighting.sql",
        "res_hourly_shares_misc.sql",
        "res_hourly_shares_poolpump.sql",
        "res_hourly_shares_refrig.sql",
        "res_hourly_shares_wh.sql",
        "tbl_hr_mult_hvac_temp.sql",
        "res_hourly_shares_cooling.sql",
        "res_hourly_shares_heating.sql",
        "res_hourly_hvac_norm.sql",
    ]
    tbl_com = [
        "tbl_ann_mult.sql",
        "com_ann_shares_cook.sql",
        "com_ann_shares_cool_delivered.sql",
        "com_ann_shares_gap.sql",
        "com_ann_shares_hvac.sql",
        "com_ann_shares_heat_agnostic.sql",
        "com_ann_shares_lighting.sql",
        "com_ann_shares_misc.sql",
        "com_ann_shares_refrig.sql",
        "com_ann_shares_ventilation_ref.sql",
        "com_ann_shares_wh.sql",
        "com_ann_shares_wh_agnostic.sql",
        "tbl_hr_mult.sql",
        "com_hourly_shares_cooking.sql",
        "com_hourly_shares_gap.sql",
        "com_hourly_shares_lighting.sql",
        "com_hourly_shares_misc.sql",
        "com_hourly_shares_refrig.sql",
        "com_hourly_shares_wh.sql",
        "tbl_hr_mult_hvac_temp.sql",
        "com_hourly_shares_cooling.sql",
        "com_hourly_shares_heating.sql",
        "com_hourly_shares_ventilation.sql",
        "com_hourly_shares_ventilation_ref.sql",
        "com_hourly_hvac_norm.sql",
    ]

    for sectorid in sectors:
        tbls = tbl_res if sectorid == "res" else tbl_com
        for tbl_name in tbls:
            # year/turnover are not used by these create-table templates -> pass placeholders anyway
            sql_to_s3table(athena_client, cfg, tbl_name, sectorid, yearid="2024", turnover="brk")


def county_hourly_examples_60_days(s3_client, athena_client, cfg: Config, turnover: str):
    # Step 1: find counties with largest annual increase and decrease
    dynamic_sql = """
    WITH ns AS (
        SELECT "in.county"
        FROM "{meta_res}"
        WHERE upgrade = 0
        GROUP BY "in.county"
        HAVING COUNT("in.state") >= 50
    ),
    county_totals AS (
        SELECT lca."in.county", lca.turnover, lca."in.state", lca."year",
            SUM(lca.county_ann_kwh) AS county_total_ann_kwh 
        FROM ns
        LEFT JOIN long_county_annual_{turnover}_{disag_id} lca ON ns."in.county" = lca."in.county"
        WHERE turnover != 'baseline'
        AND lca.county_ann_kwh IS NOT NULL
        AND "year" IN ({baseyear}, 2050)
        AND fuel = 'Electric'
        GROUP BY lca."in.county", turnover, "in.state", "year"
    ),
    county_differences AS (
        SELECT "in.county", turnover, "in.state",
            (MAX(CASE WHEN year = 2050 THEN county_total_ann_kwh END) - 
            MAX(CASE WHEN year = {baseyear} THEN county_total_ann_kwh END)) 
            / NULLIF(MAX(CASE WHEN year = {baseyear} THEN county_total_ann_kwh END), 0) AS percent_difference
        FROM county_totals
        GROUP BY "in.county", turnover, "in.state"
    )
    SELECT "in.county", 'Large decrease' AS example_type FROM (
        SELECT "in.county" FROM county_differences ORDER BY percent_difference ASC LIMIT 2
    )
    UNION ALL
    SELECT "in.county", 'Large increase' AS example_type FROM (
        SELECT "in.county" FROM county_differences ORDER BY percent_difference DESC LIMIT 2
    )
    """.format(
        meta_res=cfg.BLDSTOCK_TABLES[3],
        turnover=turnover,
        disag_id=cfg.DISAG_ID,
        baseyear=cfg.BASE_YEAR
    )

    dynamic_results = execute_athena_query_to_df(s3_client, athena_client, dynamic_sql, cfg)

    # Step 2: combine with hardcoded counties
    hardcoded = [
        ('G1200110', 'Hot'),                # Boward County, FL
        ('G0400130', 'Hot'),                # Maricopa County, AZ
        ('G3800170', 'Cold'),               # Cass County, ND
        ('G2700530', 'Cold'),               # Hennepin County, MN
        ('G3600810', 'High fossil heat'),   # Queens, NY
        ('G1700310', 'High fossil heat'),   # Cook County, IL
        ('G1200310', 'High electric heat'), # Duval County, FL
        ('G4500510', 'High electric heat'), # Horry County, SC
    ]
    dynamic = [(row['in.county'], row['example_type']) for _, row in dynamic_results.iterrows()]
    values_clause = ",\n        ".join(f"('{c}', '{t}')" for c, t in hardcoded + dynamic)

    # Step 3: find hourly consumption for example counties
    main_sql = """
    WITH example_counties AS (
        SELECT "in.county", example_type FROM (VALUES
            {values_clause}
        ) AS t("in.county", example_type)
    ),
    hourly_data AS (
        SELECT
            lch."in.county",
            ec.example_type,
            lch.turnover,
            lch.year,
            lch.timestamp_hour,
            SUM(lch.county_hourly_cal_kwh) AS county_hourly_kwh
        FROM long_county_hourly_{turnover}_{disag_id} lch
        INNER JOIN example_counties ec ON lch."in.county" = ec."in.county"
        WHERE lch.year IN ({baseyear}, 2050)
        AND lch.fuel = 'Electric'
        GROUP BY lch."in.county", ec.example_type, lch.turnover, lch.year, lch.timestamp_hour
    ),
    monthly_peak_min_days AS (
        SELECT DISTINCT
            h."in.county",
            h.example_type,
            h.year,
            h.turnover AS peak_source_turnover,
            month(h.timestamp_hour) AS month,
            FIRST_VALUE(date_trunc('day', h.timestamp_hour)) 
                OVER (PARTITION BY h."in.county", h.turnover, h.year, month(h.timestamp_hour)
                    ORDER BY h.county_hourly_kwh DESC) AS peak_day,
            FIRST_VALUE(date_trunc('day', h.timestamp_hour)) 
                OVER (PARTITION BY h."in.county", h.turnover, h.year, month(h.timestamp_hour)
                    ORDER BY h.county_hourly_kwh ASC) AS min_peak_day
        FROM hourly_data h
    ),
    monthly_peak_profiles AS (
        SELECT
            h."in.county",
            h.example_type,
            h."year",
            h.turnover,
            md.peak_source_turnover,
            'monthly_peak_day' AS day_type,
            month(h.timestamp_hour) AS month,
            hour(h.timestamp_hour)+1 AS hour_of_day,
            h.county_hourly_kwh,
            md.peak_day AS "date"
        FROM hourly_data h
        INNER JOIN monthly_peak_min_days md
            ON h."in.county" = md."in.county"
           AND h.year = md.year
           AND month(h.timestamp_hour) = md.month
           AND date_trunc('day', h.timestamp_hour) = md.peak_day
    ),
    monthly_min_profiles AS (
        SELECT
            h."in.county",
            h.example_type,
            h.year,
            h.turnover,
            md.peak_source_turnover,
            'monthly_min_peak_day' AS day_type,
            month(h.timestamp_hour) AS month,
            hour(h.timestamp_hour)+1 AS hour_of_day,
            h.county_hourly_kwh,
            md.min_peak_day AS "date"
        FROM hourly_data h
        INNER JOIN monthly_peak_min_days md
            ON h."in.county" = md."in.county"
           AND h.year = md.year
           AND month(h.timestamp_hour) = md.month
           AND date_trunc('day', h.timestamp_hour) = md.min_peak_day
    ),
    monthly_mean_profiles AS (
        SELECT
            h."in.county",
            h.example_type,
            h.year,
            h.turnover,
            NULL AS peak_source_turnover,
            'monthly_mean' AS day_type,
            month(h.timestamp_hour) AS month,
            hour(h.timestamp_hour)+1 AS hour_of_day,
            AVG(h.county_hourly_kwh) AS county_hourly_kwh,
            NULL AS "date"
        FROM hourly_data h
        GROUP BY h."in.county", h.example_type, h.year, h.turnover, month(h.timestamp_hour), hour(h.timestamp_hour)
    )
    SELECT *
    FROM monthly_mean_profiles
    UNION ALL
    SELECT *
    FROM monthly_peak_profiles
    UNION ALL
    SELECT *
    FROM monthly_min_profiles
    ORDER BY "in.county", year, turnover, month, day_type, hour_of_day
    """.format(
        values_clause=values_clause,
        turnover=turnover,
        disag_id=cfg.DISAG_ID,
        baseyear=cfg.BASE_YEAR
    )

    return main_sql

def get_csvs_for_R(s3_client, athena_client, cfg: Config):
    turnovers = cfg.TURNOVERS
    sql_files = [
        "county_ann_eu.sql",
        "county_monthly_maxes.sql",
        "state_monthly.sql",
        "county_peak_hour.sql",
        "county_share_winter.sql"
    ]
    out_dir = "R/generated_csvs"
    os.makedirs(out_dir, exist_ok=True)

    for t in turnovers:
        for sql_file in sql_files:
            sql_path = f"data_downloads/{sql_file}"
            template = read_sql_file(sql_path, cfg)
            q = template.format(turnover=t, dest_bucket=cfg.BUCKET_NAME, disag_id=cfg.DISAG_ID, baseyear=cfg.BASE_YEAR)
            df = execute_athena_query_to_df(s3_client, athena_client, q, cfg)
            out = os.path.join(out_dir, f"{t}_{os.path.basename(sql_file).replace('.sql', '.csv')}")
            df.to_csv(out, index=False)
            print(f"Saved {out}")

    # find the example days
    for t in turnovers:
        q = county_hourly_examples_60_days(s3_client, athena_client, cfg, turnover=t)
        df = execute_athena_query_to_df(s3_client, athena_client, q, cfg)
        out = os.path.join(out_dir, f"{t}_county_hourly_examples_60_days.csv")
        df.to_csv(out, index=False)
        print(f"Saved {out}")

def get_csv_for_calibration(s3_client, athena_client, cfg: Config):
    t = cfg.TURNOVERS[0]
    sql_file = "state_monthly_for_cal.sql"
    out_dir = "diagnostics"
    os.makedirs(out_dir, exist_ok=True)

    sql_path = f"data_downloads/{sql_file}"
    template = read_sql_file(sql_path, cfg)
    q = template.format(turnover=t, dest_bucket=cfg.BUCKET_NAME, disag_id=cfg.DISAG_ID, baseyear=cfg.BASE_YEAR)
    df = execute_athena_query_to_df(s3_client, athena_client, q, cfg)
    out = os.path.join(out_dir, "state_monthly_for_cal.csv")
    df.to_csv(out, index=False)
    print(f"Saved {out}")

def calc_calibration_multipliers(cfg: Config):
    state_monthly = pd.read_csv("diagnostics/state_monthly_for_cal.csv")
    eia_gross = pd.read_csv(EIA_GROSS_PATH)

    monthly_ratios = (
    state_monthly
        .merge(
            eia_gross,
            on=["in.state", "month", "sector", "year", "fuel"],
            how="inner"
        )
        .assign(
            gross_over_bss=lambda df: df["gross.kWh"] / df["state_monthly_uncal_kwh"]
        )
        .groupby(["sector", "in.state", "month", "fuel"], as_index=False)
        .agg(calibration_multiplier=("gross_over_bss", "mean"))
        )

    cmpath = cfg.CALIB_MULT_PATH
    monthly_ratios.to_csv(f"{cmpath}", index=False, sep="\t")
    print(f"Saved {cmpath}")

def generate_state_monthly_for_cal(s3_client, athena_client, cfg: Config):
    """
    Generate state monthly for calibration from the long_county_hourly_ref_amy table
    and save to diagnostics/state_monthly_for_cal.csv
    """
    os.makedirs("diagnostics", exist_ok=True)
    out_path = "diagnostics/state_monthly_for_cal.csv"

    # Using the state_monthly.sql template, but specifically for 'ref' turnover and 'amy' weather
    sql_path = "data_downloads/state_monthly.sql"
    template = read_sql_file(sql_path, cfg)
    
    # Override the default SQL to use specific table and turnover
    q = template.format(
        turnover='aeo', 
        weather='amy', 
        dest_bucket=cfg.BUCKET_NAME, 
        baseyear=cfg.BASE_YEAR
    )

    # Execute the query and save to CSV
    df = execute_athena_query_to_df(s3_client, athena_client, q, cfg)
    df.to_csv(out_path, index=False)
    print(f"Saved {out_path}")

def county_partition_multipliers(athena_client, cfg: Config):
    """
    Partition multipliers per-county using UNLOAD.
    """
    sql_dir = cfg.SQL_DIR
    # The county list comes from CSVs under ./sql/
    input_files = ["counties_com_hourly_mults.csv", "counties_res_hourly_mults.csv"]

    # Queries parameterized by {county_fips}
    q_com = """
        UNLOAD (
            SELECT "in.county", shape_ts, "timestamp_hour", multiplier_hourly, "in.state"
            FROM {mult_com_hourly}
            WHERE "in.county" = '{county_fips}'
        )
        TO 's3://bss-ief-bucket/multipliers_partitioned/com/in_county={county_fips}/'
        WITH (format = 'PARQUET');
    """.strip()

    q_res = """
        UNLOAD (
            SELECT "in.county", shape_ts, "timestamp_hour", multiplier_hourly, "in.state"
            FROM {mult_res_hourly}
            WHERE "in.county" = '{county_fips}'
        )
        TO 's3://bss-ief-bucket/multipliers_partitioned/res/in_county={county_fips}/'
        WITH (format = 'PARQUET');
    """.strip()

    for fname in input_files:
        path = os.path.join(sql_dir, fname)
        if not os.path.exists(path):
            print(f"Skip missing {path}")
            continue

        df = pd.read_csv(path)
        if "in.county" not in df.columns:
            print(f"File {fname} missing 'in.county' column.")
            continue
        fips_list = df["in.county"].dropna().astype(str).tolist()

        for fips in fips_list:
            template = q_com if "com" in fname else q_res
            q = template.format(county_fips=fips, mult_com_hourly=cfg.MULTIPLIERS_TABLES[2], mult_res_hourly=cfg.MULTIPLIERS_TABLES[3])
            print(f"UNLOAD county={fips} ({'com' if 'com' in fname else 'res'})")
            execute_athena_query(athena_client, q, cfg, is_create=False, wait=True)

# process Scout json from SCOUT_IN_JSON, register to Athena, and save as TSV to SCOUT_OUT_TSV
def gen_scoutdata(s3_client, athena_client, cfg: Config):

    # Ensure measure_map exists in Athena
    s3_create_table_from_tsv(s3_client, athena_client, cfg.MEAS_MAP_PATH, cfg)

    for turnover in cfg.TURNOVERS:
        scout_file = f"{turnover}.json"
        print(f">>> SCOUT FILE: {scout_file}")
        fp = os.path.join(cfg.SCOUT_IN_JSON, scout_file)

        # choose conversion path
        # Scout scenario does NOT have envelope measures
        if scout_file in cfg.no_env_meas:
            sdf, gap_weights = scout_to_df_noenv(fp, cfg)
            use_gap_model = not gap_weights.empty 

            ann_df, out_path = calc_annual_noenv(
                sdf,
                gap_weights,
                include_baseline=True,
                turnover=turnover,
                include_bldg_type=use_gap_model,
                cfg=cfg,
            )

        # Scout scenario HAS have envelope measures
        else:
            sdf, gap_weights = scout_to_df(fp, cfg) 
            use_gap_model = not gap_weights.empty

            ann_df, out_path = calc_annual(
                sdf,
                gap_weights,
                include_baseline=True,
                turnover=turnover,
                include_bldg_type=use_gap_model,
                cfg=cfg,
            )
        # check coverage of: measure map, envelope packages
        check_missing_meas_path = sdf
        check_missing_meas(check_missing_meas_path, cfg)
        if scout_file not in cfg.no_env_meas:
            check_missing_packages(sdf, cfg)

        # register TSV to Athena
        s3_create_table_from_tsv(s3_client, athena_client, out_path, cfg)
        print(f"Finished adding scout data {scout_file}")

# disaggregate to county, hourly; one table per sector, year, and scenario combination
def gen_countydata(s3, athena_client, cfg: Config):
    sectors = ["res", "com"]
    # sectors = ["res"]
    years = cfg.YEARS
    turnovers = cfg.TURNOVERS

    # annual disaggregation
    test_missing_mults(s3, athena_client, cfg, "test_missing_group_ann.sql")
    for s in sectors:
        for y in years:
            for t in turnovers:
                for name in ["tbl_ann_county.sql", 
                "annual_county.sql"]:
                    sql_to_s3table(athena_client, cfg, name, s, y, t)

    # hourly disaggregation
    for s in sectors:
        test_missing_mults(s3, athena_client, cfg, f"test_missing_shape_ts_{s}.sql")
        for y in years:
            for t in turnovers:
                for name in [
                    "tbl_hr_county.sql", 
                    "hourly_county.sql"
                ]:
                    sql_to_s3table(athena_client, cfg, name, s, y, t)


# ----------------------------
# Athena data conversions (long<->wide, county aggregation, etc.)
# ----------------------------

def convert_countyhourly_long_to_wide(athena_client, cfg: Config):
    sql_dir = "data_conversion"
    turnovers = cfg.TURNOVERS

    # # baseline
    # template = read_sql_file(f"{sql_dir}/long_to_wide_baseline.sql", cfg)
    # q = template.format(dest_bucket=cfg.BUCKET_NAME, version=cfg.MULT_VERSION_ID, disag_id=cfg.DISAG_ID)
    # execute_athena_query(athena_client, q, cfg, is_create=False, wait=True)

    # scenarios
    template = read_sql_file(f"{sql_dir}/long_to_wide.sql", cfg)
    for t in turnovers:
        q = template.format(turnover=t, dest_bucket=cfg.BUCKET_NAME, disag_id=cfg.DISAG_ID)
        execute_athena_query(athena_client, q, cfg, is_create=False, wait=True)


def convert_scout_long_to_wide(athena_client, cfg: Config):
    # sql_dir = "data_conversion"
    turnovers = cfg.TURNOVERS
    
    # # baseline
    # template = read_sql_file(f"{sql_dir}/long_to_wide_ann_baseline.sql", cfg)
    # q = template.format(dest_bucket=cfg.BUCKET_NAME, version=cfg.MULT_VERSION_ID)
    # execute_athena_query(athena_client, q, cfg, is_create=False, wait=True)

    # scenarios
    scout_header = f"""CREATE TABLE wide_scout_annual_state
        WITH (
            external_location = 's3://{{dest_bucket}}/{{disag_id}}/wide/scout_annual_state/',
            format = 'Parquet'
        ) AS
        WITH scout_agg AS(
        """
    scout_select_tpl = (
        'SELECT "year", reg, turnover, fuel, sector, end_use,sum(state_ann_kwh) AS state_ann_kwh '
        "FROM scout_annual_state_{turnover} "
        "WHERE turnover != 'baseline' "
        'GROUP BY "year", reg, turnover, fuel, sector, end_use '
    )
    scout_footer = f"""
        scout_formatted AS (
            SELECT
                turnover AS scenario,
                "year",
                reg AS state,
                LOWER(REGEXP_REPLACE(end_use, '[^A-Za-z0-9]+', '_')) AS eu,
                CASE fuel
                    WHEN 'Electric' THEN 'uncal_elec'
                    WHEN 'Propane' THEN 'propane'
                    WHEN 'Natural Gas' THEN 'natural_gas'
                    WHEN 'Biomass' THEN 'biomass'
                    WHEN 'Distillate/Other'THEN 'other'
                    ELSE 'other'
                END AS fuel_alias,
                sector,
                state_ann_kwh
            FROM scout_agg
        ),
        """

    county_hourly_header = "long_hourly AS("

    county_hourly_select_tpl = (
        'SELECT "year", "in.state" AS state, turnover AS scenario, sector, '
        "LOWER(REGEXP_REPLACE(end_use, '[^A-Za-z0-9]+', '_')) AS eu, "
        'sum(county_hourly_cal_kwh) AS cal_elec, sum(county_hourly_uncal_kwh) AS uncal_elec1 '
        "FROM long_county_hourly_{turnover}_{disag_id} "
        "WHERE turnover != 'baseline' "
        'GROUP BY "year", "in.state", turnover, sector, end_use '
    )
    
    combined_footer = f"""
        combined AS (
            SELECT
                coalesce(sc.state, hr.state)    AS state,
                coalesce(sc.sector, hr.sector)  AS sector,
                coalesce(sc.scenario, hr.scenario) AS scenario,
                coalesce(sc."year", hr."year")  AS "year",
                coalesce(sc.eu, hr.eu)          AS eu,

                MAX(CASE WHEN sc.fuel_alias = 'propane'      THEN sc.state_ann_kwh END)     AS propane_val,
                MAX(CASE WHEN sc.fuel_alias = 'natural_gas'  THEN sc.state_ann_kwh END)     AS natural_gas_val,
                MAX(CASE WHEN sc.fuel_alias = 'biomass'      THEN sc.state_ann_kwh END)     AS biomass_val,
                MAX(CASE WHEN sc.fuel_alias = 'other'        THEN sc.state_ann_kwh END)     AS other_val,
                MAX(CASE WHEN sc.fuel_alias = 'uncal_elec'        THEN sc.state_ann_kwh END)     AS uncal_elec_val,
                -- MAX(CASE WHEN sc.fuel_alias = 'uncal_elec'   THEN sc.state_ann_kwh END)     AS uncal_elec_scout_val,
                MAX(hr.uncal_elec1) AS uncal_elec1_val,
                MAX(hr.cal_elec)   AS cal_elec_val
            FROM scout_formatted sc
            FULL OUTER JOIN long_hourly hr
              ON sc.state   = hr.state
             AND sc.sector  = hr.sector
             AND sc.scenario= hr.scenario
             AND sc."year"  = hr."year"
             AND sc.eu      = hr.eu
            GROUP BY
                coalesce(sc.state, hr.state),
                coalesce(sc.sector, hr.sector),
                coalesce(sc.scenario, hr.scenario),
                coalesce(sc."year", hr."year"),
                coalesce(sc.eu, hr.eu)
        ),

        -- Pivot to wide
        wide AS (
            SELECT
                state,
                sector,
                scenario,
                "year",


                -- === COOLING ===
                MAX(CASE WHEN eu = 'cooling_equip_' THEN natural_gas_val END) AS "natural_gas.cooling.kwh",
                MAX(CASE WHEN eu = 'cooling_equip_' THEN uncal_elec_val  END) AS "electricity_uncalibrated.cooling.kwh",
                MAX(CASE WHEN eu = 'cooling_equip_' THEN cal_elec_val    END) AS "electricity_calibrated.cooling.kwh",

                -- === HEATING ===
                MAX(CASE WHEN eu = 'heating_equip_' THEN propane_val     END) AS "propane.heating.kwh",
                MAX(CASE WHEN eu = 'heating_equip_' THEN natural_gas_val END) AS "natural_gas.heating.kwh",
                MAX(CASE WHEN eu = 'heating_equip_' THEN biomass_val     END) AS "biomass.heating.kwh",
                MAX(CASE WHEN eu = 'heating_equip_' THEN other_val       END) AS "other.heating.kwh",
                MAX(CASE WHEN eu = 'heating_equip_' THEN uncal_elec_val  END) AS "electricity_uncalibrated.heating.kwh",
                MAX(CASE WHEN eu = 'heating_equip_' THEN cal_elec_val    END) AS "electricity_calibrated.heating.kwh",

                -- === WATER HEATING ===
                MAX(CASE WHEN eu = 'water_heating' THEN propane_val     END) AS "propane.water_heating.kwh",
                MAX(CASE WHEN eu = 'water_heating' THEN natural_gas_val END) AS "natural_gas.water_heating.kwh",
                MAX(CASE WHEN eu = 'water_heating' THEN other_val       END) AS "other.water_heating.kwh",
                MAX(CASE WHEN eu = 'water_heating' THEN uncal_elec_val  END) AS "electricity_uncalibrated.water_heating.kwh",
                MAX(CASE WHEN eu = 'water_heating' THEN cal_elec_val    END) AS "electricity_calibrated.water_heating.kwh",

                -- === LIGHTING ===
                MAX(CASE WHEN eu = 'lighting' THEN uncal_elec_val  END) AS "electricity_uncalibrated.lighting.kwh",
                MAX(CASE WHEN eu = 'lighting' THEN cal_elec_val    END) AS "electricity_calibrated.lighting.kwh",

                -- === VENTILATION ===
                MAX(CASE WHEN eu = 'ventilation' THEN uncal_elec_val  END) AS "electricity_uncalibrated.ventilation.kwh",
                MAX(CASE WHEN eu = 'ventilation' THEN cal_elec_val    END) AS "electricity_calibrated.ventilation.kwh",

                -- === REFRIGERATION ===
                MAX(CASE WHEN eu = 'refrigeration' THEN uncal_elec_val  END) AS "electricity_uncalibrated.refrigeration.kwh",
                MAX(CASE WHEN eu = 'refrigeration' THEN cal_elec_val    END) AS "electricity_calibrated.refrigeration.kwh",

                -- === COOKING ===
                MAX(CASE WHEN eu = 'cooking' THEN propane_val     END) AS "propane.cooking.kwh",
                MAX(CASE WHEN eu = 'cooking' THEN natural_gas_val END) AS "natural_gas.cooking.kwh",
                MAX(CASE WHEN eu = 'cooking' THEN uncal_elec_val  END) AS "electricity_uncalibrated.cooking.kwh",
                MAX(CASE WHEN eu = 'cooking' THEN cal_elec_val    END) AS "electricity_calibrated.cooking.kwh",

                -- === OTHER ===
                MAX(CASE WHEN eu = 'computers_and_electronics' THEN uncal_elec_val  END) AS "electricity_uncalibrated.computers_and_electronics.kwh",
                MAX(CASE WHEN eu = 'computers_and_electronics' THEN cal_elec_val    END) AS "electricity_calibrated.computers_and_electronics.kwh",

                -- === OTHER ===
                MAX(CASE WHEN eu = 'other' THEN natural_gas_val END) AS "natural_gas.other.kwh",
                MAX(CASE WHEN eu = 'other' THEN other_val       END) AS "other.other.kwh",
                MAX(CASE WHEN eu = 'other' THEN uncal_elec_val  END) AS "electricity_uncalibrated.other.kwh",
                MAX(CASE WHEN eu = 'other' THEN cal_elec_val    END) AS "electricity_calibrated.other.kwh"

            FROM combined
            GROUP BY state, sector, scenario, "year"
        )

        SELECT *
        FROM wide
        WHERE "year" IN (2026,2030,2035,2040,2045,2050)
        ;
    """
    scout_parts = []
    county_hourly_parts = []
    for turnover in turnovers:
        scout_parts.append(
            scout_select_tpl.format(turnover=turnover)
        )
        county_hourly_parts.append(
            county_hourly_select_tpl.format(turnover=turnover)
        )
    all_sql = scout_header + "\nUNION ALL\n".join(scout_parts) + "),\n" + scout_footer + \
                county_hourly_header + "\nUNION ALL\n".join(county_hourly_parts) + "),\n" + combined_footer

    q = all_sql.format(turnover=turnover, dest_bucket=cfg.BUCKET_NAME, disag_id=cfg.DISAG_ID)

    # print(q)
    execute_athena_query(athena_client, q, cfg, is_create=False, wait=True)



def _combine_countydata(
    sectors,
    years
):

    # ---- HOURLY ----
    hourly_header = f"""CREATE TABLE long_county_hourly_{{turnover}}_{{disag_id}}
        WITH (
            external_location = 's3://{{dest_bucket}}/{{disag_id}}/long/county_hourly_{{turnover}}_{{disag_id}}/',
            format = 'Parquet',
            partitioned_by = ARRAY['sector', 'year', 'in.state']
        ) AS
        WITH
        """

    # # if the table already exists
    # hourly_header = f"""INSERT INTO long_county_hourly_{{turnover}}_{{disag_id}} WITH 
    # """
    
    
    hourly_cte_tpl = (
        "hourly_with_month_{sector}_{year} AS ("
        "SELECT *, month(timestamp_hour) AS month_num "
        "FROM county_hourly_{sector}_{year}_{{turnover}}_{{disag_id}})"
    )

    hourly_select_tpl = (
        'SELECT h."in.county", h.timestamp_hour, h.turnover, h.county_hourly_uncal_kwh, '
        'h.county_hourly_uncal_kwh * COALESCE(cm.calibration_multiplier, 1) AS county_hourly_cal_kwh, '
        'h.scout_run, h.end_use, h.fuel, h.sector, h.year, h."in.state"\n'
        'FROM hourly_with_month_{sector}_{year} AS h\n'
        'LEFT JOIN (SELECT * FROM calibration_multipliers WHERE sector = \'{sector}\') AS cm '
        'ON cm."in.state" = h."in.state" '
        'AND cm."month" = h.month_num '
        'AND cm.fuel = h.fuel '
        'WHERE h.timestamp_hour IS NOT NULL'
    )

    cte_parts = []
    select_parts = []
    for sector in sectors:
        for yr in years:
            cte_parts.append(hourly_cte_tpl.format(sector=sector, year=yr))
            select_parts.append(hourly_select_tpl.format(sector=sector, year=yr))

    hourly_sql = (
        hourly_header
        + ",\n".join(cte_parts)
        + "\n"
        + "\nUNION ALL\n".join(select_parts)
        + ";"
    )

    # ---- ANNUAL ----
    annual_header = f"""CREATE TABLE long_county_annual_{{turnover}}_{{disag_id}}
        WITH (
            external_location = 's3://{{dest_bucket}}/{{disag_id}}/long/county_annual_{{turnover}}_{{disag_id}}/',
            format = 'Parquet',
            partitioned_by = ARRAY['sector', 'year', 'in.state']
        ) AS
        """

    # # if the table already exists
    # annual_header = f"""INSERT INTO long_county_annual_{{turnover}}_{{disag_id}}
    #     """

    annual_select_tpl = (
        'SELECT "in.county", fuel, meas, tech_stage, multiplier_annual, '
        'state_ann_kwh, turnover, county_ann_kwh, scout_run, end_use, '
        'sector, year, "in.state"\n'
        "FROM county_annual_{sector}_{year}_{turnover}_{disag_id}"
    )
    annual_parts = []
    for sector in sectors:
        for yr in years:
            annual_parts.append(
                annual_select_tpl.format(sector=sector, year=yr, turnover="{turnover}", disag_id="{disag_id}")
            )
    annual_sql = annual_header + "\nUNION ALL\n".join(annual_parts) + ";"

    return {"hourly": hourly_sql, "annual": annual_sql}

# combine the tables for each year and sector combination (output of gen_countydata) into one per scenario
# apply electricity and gas calibration multipliers
def combine_countydata(s3_client, athena_client, cfg: Config):

    # Ensure the calibration multipliers exist in Athena
    s3_create_table_from_tsv(s3_client, athena_client, cfg.CALIB_MULT_PATH, cfg)

    # sql_dir = "data_conversion"
    turnovers = cfg.TURNOVERS
    years = cfg.YEARS
    q_combined = _combine_countydata(
        ["res","com"], 
        # ["res"],
        years)

    queries = [
        q_combined["annual"], 
        q_combined["hourly"]]
    for query in queries:
        for t in turnovers:
            q = query.format(turnover=t, dest_bucket=cfg.BUCKET_NAME, disag_id=cfg.DISAG_ID)
            execute_athena_query(athena_client, q, cfg, is_create=False, wait=True)


# ----------------------------
# R runner
# ----------------------------

def run_r_script(r_file: str):
    if robjects is None:
        print("rpy2 not available; skipping R execution.")
        return
    # Get the absolute path to the folder containing THIS Python file
    project_root = os.path.dirname(os.path.abspath(__file__))
    # Build the absolute path to the R script
    r_path = os.path.join(project_root, "R", r_file)
    with open(r_path, "r", encoding="utf-8") as f:
        r_code = f.read()
    try:
        robjects.r(r_code)
        print("R script executed successfully.")
    except Exception as e:
        print(f"Error executing R script: {e}")


# ----------------------------
# Data movers for bss buckets
# ----------------------------

def bssiefbucket_insert(athena_client, s3_bucket: str, cfg: Config):
    turnovers = cfg.TURNOVERS
    years = cfg.YEARS
    s3_bucket = "bss-ief-bucket"

    query_template = """
    CREATE TABLE bss_ief_buildings_total_{turnover}_{year}
    WITH (
        external_location = 's3://{dest_bucket}/{turnover}/year={year}/',
        format = 'PARQUET',
        write_compression = 'SNAPPY',
        partitioned_by = ARRAY['state']
    ) AS
    SELECT 
        timestamp_hour,
        turnover,
        "in.county" AS county,
        buildings_kwh,
        year,
        "in.state" AS state
    FROM buildings_total_{turnover}
    WHERE year = {year};
    """.strip()


    for t in turnovers:
        for y in years:
            q = query_template.format(turnover=t, year=y, dest_bucket=s3_bucket)
            print(f"bss_ief insert: turnover={t} year={y}")
            execute_athena_query(athena_client, q, cfg, is_create=False, wait=True)

def bssiefbucket_parquetmerge(s3_client, cfg: Config):
    turnovers = cfg.TURNOVERS
    years = cfg.YEARS
    s3_bucket = "bss-ief-bucket"
    for t in turnovers:
        for y in years:
            top = f"{t}/year={y}/"
            print(f"Merging IEF bucket for {t} {y}")
            _merge_parquet_folders(s3_client, top, s3_bucket)


# def _merge_parquet_folders(s3_client, top_level_prefix: str, s3_bucket: str):
#     files = list_all_objects(s3_client, s3_bucket, top_level_prefix)
#     if not files:
#         print(f"No files found under s3://{s3_bucket}/{top_level_prefix}")
#         return

#     state_folders = set()
#     for f in files:
#         key = f["Key"]
#         rel = key[len(top_level_prefix):]
#         if "/" in rel:
#             state_folders.add(rel.split("/")[0])

#     print(f"Found state folders: {state_folders}")

#     for state in state_folders:
#         print(f"Processing state: {state}")
#         state_prefix = f"{top_level_prefix}{state}/"
#         page = s3_client.list_objects_v2(Bucket=s3_bucket, Prefix=state_prefix)
#         state_files = page.get("Contents", [])
#         os.makedirs(os.path.join("temp_files", state), exist_ok=True)
#         local_files = []

#         for obj in state_files:
#             k = obj["Key"]
#             if k.endswith("/"):
#                 continue
#             local = os.path.join("temp_files", state, os.path.basename(k))
#             s3_client.download_file(s3_bucket, k, local)
#             local_files.append(local)

#         df_list = []
#         for lf in local_files:
#             try:
#                 df = pd.read_parquet(lf)
#             except Exception:
#                 try:
#                     df = pd.read_csv(lf)
#                 except Exception:
#                     print(f"Skipping unreadable file: {lf}")
#                     continue
#             df_list.append(df)

#         if df_list:
#             combined = pd.concat(df_list, ignore_index=True)
#             combined_path = f"{state}.parquet"
#             combined.to_parquet(combined_path, engine="pyarrow", index=False)
#             s3_client.upload_file(combined_path, s3_bucket, f"{top_level_prefix}US states/{combined_path}")
#             print(f"Uploaded combined {combined_path} to s3://{s3_bucket}/{top_level_prefix}US states/")

#         for lf in local_files:
#             try:
#                 os.remove(lf)
#             except Exception:
#                 pass
#         try:
#             os.rmdir(os.path.join("temp_files", state))
#         except Exception:
#             pass
#         try:
#             if df_list:
#                 os.remove(combined_path)
#         except Exception:
#             pass


def bssbucket_insert(athena_client, cfg: Config):
    turnovers = cfg.TURNOVERS
    years = cfg.YEARS
    dest_bucket = cfg.DEST_BUCKET

    sectors = ["res", "com"]
    query_template = """
        CREATE TABLE bss_county_hourly_{turnover}_amy_{sector}_{year}
        WITH (
            external_location = 's3://{dest_bucket}/v2/county_hourly/{turnover}/sector={sector}/year={year}/',
            format = 'PARQUET',
            write_compression = 'SNAPPY',
            partitioned_by = ARRAY['state']
        ) AS
        SELECT *
        FROM wide_county_hourly_{turnover}_amy
        WHERE sector = '{sectorlong}' AND year = {year};
    """.strip()

    for t in turnovers:
        for s in sectors:
            for y in years:
                sectorlong = "Commercial" if s == "com" else "Residential"
                q = query_template.format(
                    turnover=t, sector=s, year=y, dest_bucket=dest_bucket, sectorlong=sectorlong
                )
                print(f"bss insert: {t} {s} {y}")
                execute_athena_query(athena_client, q, cfg, is_create=False, wait=True)


#rename the files for publication
#unzip parquets
#merge into state folder
#zip back to parquet file
def bssbucket_parquetmerge(s3_client, cfg: Config):
    turnovers = cfg.TURNOVERS
    years = cfg.YEARS
    dest_bucket = cfg.DEST_BUCKET
    sectors = ["com", "res"]

    for t in turnovers:
        for s in sectors:
            for y in years:
                top = f"v2/county_hourly/{t}/sector={s}/year={y}/"
                print(f"Merging BSS bucket for {t} {s} {y}")
                merge_and_replace_folders(s3_client, dest_bucket, top)


def bssbucket_parquet_scout(s3_client, athena_client, cfg:Config):
    tables = ["wide_scout_annual_state", "wide_scout_annual_state_baseline"]
    for tname in tables:
        q = f"SELECT * FROM {tname};"
        execute_athena_query_to_df2(s3_client, athena_client, q, tname, cfg)


def merge_and_replace_folders(s3_client, bucket_name: str, prefix: str):
    """
    Merge folders into parquet files and replace the original folders.
    
    Args:
        s3_client: Boto3 S3 client
        bucket_name: S3 bucket name (e.g., 'handibucket')
        prefix: S3 prefix path (e.g., 'multipliers/')
    
    Example:
        merge_and_replace_folders(s3_client, 'handibucket', 'multipliers/')
        This will merge folders a/, b/, c/, d/ into a.parquet, b.parquet, c.parquet, d.parquet
        and delete the original folders.
    """
    # List all objects under the prefix
    files = list_all_objects(s3_client, bucket_name, prefix)
    if not files:
        print(f"No files found under s3://{bucket_name}/{prefix}")
        return

    # Find all folder names (first level subdirectories)
    folder_names = set()
    for f in files:
        key = f["Key"]
        rel = key[len(prefix):]
        if "/" in rel:
            folder_name = rel.split("/")[0]
            folder_names.add(folder_name)

    print(f"Found folders to merge: {folder_names}")

    for folder_name in folder_names:
        print(f"Processing folder: {folder_name}")
        folder_prefix = f"{prefix}{folder_name}/"
        
        # List all files in this folder
        page = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=folder_prefix)
        folder_files = page.get("Contents", [])
        
        if not folder_files:
            print(f"No files found in folder {folder_name}")
            continue
        
        # Create temporary directory for this folder
        temp_dir = os.path.join("temp_files", folder_name)
        os.makedirs(temp_dir, exist_ok=True)
        local_files = []

        # Download all files from the folder
        for obj in folder_files:
            k = obj["Key"]
            if k.endswith("/"):
                continue
            local_file = os.path.join(temp_dir, os.path.basename(k))
            s3_client.download_file(bucket_name, k, local_file)
            local_files.append(local_file)

        # Read and combine all files
        df_list = []
        for lf in local_files:
            try:
                df = pd.read_parquet(lf)
            except Exception:
                try:
                    df = pd.read_csv(lf)
                except Exception:
                    print(f"Skipping unreadable file: {lf}")
                    continue
            df_list.append(df)

        if df_list:
            # Combine all dataframes
            combined = pd.concat(df_list, ignore_index=True)
            
            # Create parquet file
            parquet_filename = f"{folder_name}.parquet"
            combined.to_parquet(parquet_filename, engine="pyarrow", index=False)
            
            # Upload the parquet file to the same location as the original folder
            parquet_key = f"{prefix}{parquet_filename}"
            s3_client.upload_file(parquet_filename, bucket_name, parquet_key)
            print(f"Uploaded {parquet_filename} to s3://{bucket_name}/{parquet_key}")
            
            # Delete the original folder
            delete_folder_from_s3(s3_client, bucket_name, folder_prefix)
            print(f"Deleted original folder: s3://{bucket_name}/{folder_prefix}")
            
            # Clean up local parquet file
            try:
                os.remove(parquet_filename)
            except Exception:
                pass

        # Clean up local temporary files
        for lf in local_files:
            try:
                os.remove(lf)
            except Exception:
                pass
        try:
            os.rmdir(temp_dir)
        except Exception:
            pass


def delete_folder_from_s3(s3_client, bucket_name: str, folder_prefix: str):
    """
    Delete all objects in a folder from S3.
    
    Args:
        s3_client: Boto3 S3 client
        bucket_name: S3 bucket name
        folder_prefix: S3 prefix for the folder to delete (should end with '/')
    """
    # List all objects with the folder prefix
    paginator = s3_client.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=bucket_name, Prefix=folder_prefix)
    
    objects_to_delete = []
    for page in pages:
        if 'Contents' in page:
            for obj in page['Contents']:
                objects_to_delete.append({'Key': obj['Key']})
    
    if objects_to_delete:
        # Delete all objects in batches
        for i in range(0, len(objects_to_delete), 1000):
            batch = objects_to_delete[i:i+1000]
            s3_client.delete_objects(
                Bucket=bucket_name,
                Delete={'Objects': batch}
            )
        print(f"Deleted {len(objects_to_delete)} objects from s3://{bucket_name}/{folder_prefix}")
    else:
        print(f"No objects found to delete in s3://{bucket_name}/{folder_prefix}")


# ----------------------------
# Diagnostics & checks
# ----------------------------

# check that all measures in Scout json are in measure_map.tsv
def check_missing_meas(annual_state_scout_df: pd.DataFrame, cfg: Config):
    meas_files = [cfg.MEAS_MAP_PATH, cfg.ENVELOPE_MAP_PATH]
    for mfile in meas_files:
        try:
            measures = file_to_df(mfile)
            
            # Create sets of measure-end_use combinations from both sources
            if "Scout_end_use" in measures.columns:
                # For measure_map.tsv, use meas and Scout_end_use columns
                meas_enduse_in_map = set(
                    zip(measures.get("meas", pd.Series(dtype=str)), 
                        measures.get("Scout_end_use", pd.Series(dtype=str)))
                )
            else:
                # For envelope_map.tsv or other files, fall back to just meas
                meas_enduse_in_map = set(measures.get("meas", pd.Series(dtype=str)))
            
            # Create combinations from Scout data
            if "end_use" in annual_state_scout_df.columns:
                meas_enduse_in_scout = set(
                    zip(annual_state_scout_df.get("meas", pd.Series(dtype=str)), 
                        annual_state_scout_df.get("end_use", pd.Series(dtype=str)))
                )
            else:
                # Fallback to just meas if end_use column doesn't exist
                meas_enduse_in_scout = set(annual_state_scout_df.get("meas", pd.Series(dtype=str)))
            
            # Find missing combinations
            missing = meas_enduse_in_scout - meas_enduse_in_map
            
            if missing:
                n_missing = len(missing.index)
                print(f"WARNING: {n_missing} measure-end_use combinations from scout are missing in {mfile}.")
                for combo in missing:
                    if isinstance(combo, tuple):
                        print(f"  Measure: '{combo[0]}', End Use: '{combo[1]}'")
                    else:
                        print(f"  Measure: '{combo}'")
            else:
                print(f"PASSED: All measure-end_use combinations in scout are present in {mfile}.")
                
        except Exception as e:
            print(f"Error processing {mfile}: {e}")

# check that all packages in Scout json are in envelope_map.tsv
def check_missing_packages(scout_df: pd.DataFrame, cfg: Config):
    """
    Ensure all envelope package measures present in Scout json are defined in envelope_map.tsv.

    Logic:
    - Detect envelope-capable measures in Scout by presence of the metric
      "Efficient Energy Use, Measure-Envelope (MMBtu)".
    - Validate that each such 'meas' exists in envelope_map.tsv's 'meas' column.
    - Report any missing measures.
    """
    try:
        # Measures in Scout that require envelope mapping
        if "metric" not in scout_df.columns or "meas" not in scout_df.columns:
            print("Scout dataframe missing required columns for envelope check; skipping.")
            return

        env_metric = "Efficient Energy Use, Measure-Envelope (MMBtu)"
        # Only require mapping for measures that actually include the envelope metric
        # with a defined positive value
        scout_env_meas = set(
            scout_df.loc[
                (scout_df["metric"] == env_metric)
                & (scout_df["value"].notna())
                & (pd.to_numeric(scout_df["value"], errors="coerce").fillna(0) != 0),
                "meas"
            ].dropna().astype(str)
        )
        if not scout_env_meas:
            print("No envelope-package measures detected in Scout; nothing to validate.")
            return

        # Measures listed in envelope_map
        envelope_map_df = file_to_df(cfg.ENVELOPE_MAP_PATH)
        env_map_meas = set(envelope_map_df.get("meas", pd.Series(dtype=str)).dropna().astype(str))
        # Also accept matches against 'meas_separated' to avoid false positives
        if "meas_separated" in envelope_map_df.columns:
            env_map_meas |= set(envelope_map_df.get("meas_separated", pd.Series(dtype=str)).dropna().astype(str))

        missing = scout_env_meas - env_map_meas
        if missing:
            print("Missing envelope package measures in envelope_map.tsv:")
            for m in sorted(missing):
                print(f"  Measure: '{m}'")
            print("Warning: Some envelope package measures from Scout are missing in envelope_map.tsv.")
        else:
            print("All envelope package measures in Scout are present in envelope_map.tsv.")
    except Exception as e:
        print(f"Error during envelope packages check: {e}")


# check that all multipliers sum to 1
def test_multipliers(s3_client, athena_client, cfg: Config):
    sql_dir = "run_check"
    os.makedirs("diagnostics", exist_ok=True)

    # annual disaggregation multipliers sum to 1
    template = read_sql_file(f"{sql_dir}/test_multipliers_annual.sql", cfg)
    q = template.format(mult_com_annual=cfg.MULTIPLIERS_TABLES[0], mult_res_annual=cfg.MULTIPLIERS_TABLES[1])
    # print(q)
    df = execute_athena_query_to_df(s3_client, athena_client, q, cfg)
    out_csv = "./diagnostics/test_multipliers_annual.csv"
    if os.path.exists(out_csv):
        os.remove(out_csv)
    df.to_csv(out_csv, index=False)
    print(f"Saved {out_csv}")

    bad_group_ann = df.loc[((df['multiplier_sum'] > 1.01) | (df['multiplier_sum'] < 0.99) | (df['multiplier_sum'].isna()))]
    n_missing = len(bad_group_ann.index)
    if len(bad_group_ann.index) > 0:
        print(f"WARNING: {n_missing} multipliers in group_ann's do not sum to 1. Check {out_csv} for details.")
        print(bad_group_ann.head())
    else:
        print("PASSED: all group_ann's sum to 1.")

    # hourly disaggregation multipliers sum to 1
    hourly_test_files = ["test_multipliers_hourly_com.sql",
                        "test_multipliers_hourly_res.sql"]
    for sql_file in hourly_test_files:
        template = read_sql_file(f"{sql_dir}/{sql_file}", cfg)
        q = template.format(mult_com_hourly=cfg.MULTIPLIERS_TABLES[2], mult_res_hourly=cfg.MULTIPLIERS_TABLES[3])
        # print(q)
        df = execute_athena_query_to_df(s3_client, athena_client, q, cfg)
        out_csv = "./diagnostics/" + sql_file.split(".")[0] + ".csv"
        if os.path.exists(out_csv):
            os.remove(out_csv)
        df.to_csv(out_csv, index=False)

        bad_shape_ts = df.loc[((df['multiplier_sum'] > 1.01) | (df['multiplier_sum'] < 0.99) | (df['multiplier_sum'].isna()))]
        n_missing = len(bad_shape_ts.index)
        if len(bad_shape_ts.index) > 0:
            print(f"WARNING: {n_missing} shape_ts's do not sum to 1 in {sql_file}. Check {out_csv} for details.")
            print(bad_shape_ts.head())
        else:
            print(f"PASSED: all shape_ts's sum to 1 in {sql_file}.")

# check that all group_ann and shape_ts required for the scenarios are defined
def test_missing_mults(s3_client, athena_client, cfg: Config, sql_file):
    sql_dir = "run_check"
    years = cfg.YEARS
    turnovers = cfg.TURNOVERS
    disag_id = cfg.DISAG_ID

    os.makedirs("diagnostics", exist_ok=True)

    out_csv = f"./diagnostics/{sql_file.split('.')[0]}.csv"
    dfs = []
    template = read_sql_file(f"{sql_dir}/{sql_file}", cfg)

    for t in turnovers:
        for y in years:
            q = template.format(dest_bucket=cfg.BUCKET_NAME, turnover=t, year=y, disag_id=cfg.DISAG_ID, 
            mult_com_annual=cfg.MULTIPLIERS_TABLES[0], mult_res_annual=cfg.MULTIPLIERS_TABLES[1], 
            mult_com_hourly=cfg.MULTIPLIERS_TABLES[2], mult_res_hourly=cfg.MULTIPLIERS_TABLES[3]
            )
            df = execute_athena_query_to_df(s3_client, athena_client, q, cfg)

            dfs.append(df)

    final = pd.concat(dfs, ignore_index=True).drop_duplicates()
    if os.path.exists(out_csv):
        os.remove(out_csv)
    final.to_csv(out_csv, index=False)
    print(f"Saved {out_csv}")

    missing_mults = final[((final['mult_sum'] > 1.01) | (final['mult_sum'] < 0.99) | (final['mult_sum'].isna()))]
    n_missing = len(missing_mults.index)
    if n_missing > 0:
        print(f"WARNING: {n_missing} multipliers are missing or do not sum to 1 in {sql_file}.")
        print(missing_mults.head())
    else:
        print(f"PASSED: all multipliers are present and sum to 1 in {sql_file}.")

# check that county hourly results re-aggregate correctly
def test_county(s3_client, athena_client, cfg: Config):

    sql_dir = "run_check"
    years = cfg.YEARS
    turnovers = cfg.TURNOVERS
    disag_id = cfg.DISAG_ID
    years_sql = ", ".join(cfg.YEARS)

    os.makedirs("diagnostics", exist_ok=True)

    # test energy reaggregates to Scout results at end use and fuel level
    sql_files = ["test_scout_disagg_fuel.sql","test_scout_disagg_enduse.sql"]
    for sql_file in sql_files:
        out_csv = f"./diagnostics/{sql_file.split('.')[0]}.csv"
        dfs = []
        template = read_sql_file(f"{sql_dir}/{sql_file}", cfg)

        for t in turnovers:
            q = template.format(dest_bucket=cfg.BUCKET_NAME, turnover=t, disag_id=disag_id, years=years_sql)
            df = execute_athena_query_to_df(s3_client, athena_client, q, cfg)
            dfs.append(df)

        final = pd.concat(dfs, ignore_index=True).drop_duplicates()
        if os.path.exists(out_csv):
            os.remove(out_csv)
        final.to_csv(out_csv, index=False)
        print(f"Saved {out_csv}")

        bad_aggregation = final.loc[
            ((final['per_diff_ann'] > 0.001) | 
            (final['per_diff_ann'] < -0.001) | 
            (final['bss_ann_kwh'].notna() & (final['scout_kwh'].isna())) | 
            (final['bss_ann_kwh'].isna() & (final['scout_kwh'] != 0)) |
            (final['per_diff_hr'] > 0.001) | 
            (final['per_diff_hr'] < -0.001) |
            (final['bss_hr_kwh'].notna() & (final['scout_kwh'].isna())) | 
            (final['bss_hr_kwh'].isna() & (final['scout_kwh'] != 0)))]
        n_bad = len(bad_aggregation.index)
        if n_bad > 0:
            print(f"FAILED: {n_bad} re-aggregations are off by more than 0.1% in {sql_file}. Check {out_csv} for details.")
            print(bad_aggregation.head())
        else:
            print(f"PASSED: all re-aggregations are within 0.1% of Scout results in {sql_file}.")


    # test energy reaggregates to Scout results at the measure level
    sql_file = "test_scout_disagg_meas.sql"
    out_csv = f"./diagnostics/{sql_file.split('.')[0]}.csv"
    dfs = []
    template = read_sql_file(f"{sql_dir}/{sql_file}", cfg)

    for t in turnovers:
        q = template.format(dest_bucket=cfg.BUCKET_NAME, turnover=t, disag_id=disag_id, years=years_sql)
        df = execute_athena_query_to_df(s3_client, athena_client, q, cfg)
        dfs.append(df.sort_values(by=["turnover"], ascending=[True]))

    final = pd.concat(dfs, ignore_index=True).drop_duplicates()

    if os.path.exists(out_csv):
        os.remove(out_csv)
    final.to_csv(out_csv, index=False)
    print(f"Saved {out_csv}")

    bad_aggregation = final.loc[
        ((final['per_diff_ann'] > 0.001) | 
        (final['per_diff_ann'] < -0.001) | 
        (final['bss_ann_kwh'].notna() & (final['scout_kwh'].isna())) | 
        (final['bss_ann_kwh'].isna() & (final['scout_kwh'] != 0)))]
    n_bad = len(bad_aggregation.index)
    if n_bad > 0:
        print(f"WARNING: {n_bad} re-aggregations are off by more than 0.1% in {sql_file}. Check {out_csv} for details.")
        print(bad_aggregation.head())
    else:
        print(f"PASSED: all re-aggregations are within 0.1% of Scout results in {sql_file}.")

# ----------------------------
# CLI & main entry
# ----------------------------

def main(opts):
    cfg = Config()

    if opts.create_json:
        convert_csv_folder_to_json("csv_raw", cfg.JSON_PATH)

    # calculate disaggregation multipliers
    if opts.gen_mults:
        s3, athena = get_boto3_clients()
        s3_create_tables_from_csvdir(s3, athena, cfg)
        gen_multipliers(s3, athena, cfg)
        test_multipliers(s3, athena, cfg)

    # process and upload Scout results
    if opts.gen_scoutdata:
        s3, athena = get_boto3_clients()
        gen_scoutdata(s3, athena, cfg)
        # run_r_script("annual_graphs.R")

    # disaggregate to county hourly; one table per sector, year, scenario combination
    if opts.gen_county:
        s3, athena = get_boto3_clients()
        gen_countydata(s3, athena, cfg)

    # calculate calibration multipliers
    if opts.calibrate:
        s3, athena = get_boto3_clients()
        get_csv_for_calibration(s3, athena, cfg)
        calc_calibration_multipliers(cfg)

    # combine tables from gen_county into one long table per scenario
    if opts.combine_county:
        s3, athena = get_boto3_clients()
        combine_countydata(s3, athena, cfg)
        test_county(s3, athena, cfg)

    # download required csvs, create county and hourly graphs
    if opts.gen_hourlyviz:
        s3, athena = get_boto3_clients()
        get_csvs_for_R(s3, athena, cfg)
        # run_r_script("county and hourly graphs.R")

    if opts.combine_countydata:
        _, athena = get_boto3_clients()
        combine_countydata(athena, cfg)

    if opts.convert_wide:
        _, athena = get_boto3_clients()
        convert_countyhourly_long_to_wide(athena, cfg)
        convert_scout_long_to_wide(athena, cfg)

    # if opts.gen_countyall:
    #     s3, athena = get_boto3_clients()
    #     gen_scoutdata(s3, athena, cfg)
    #     run_r_script("annual_graphs.R")
    #     gen_countydata(s3, athena, cfg)
    #     get_csv_for_calibration(s3, athena, cfg)
    #     calc_calibration_multipliers(cfg)
    #     combine_countydata(s3, athena, cfg)
    #     test_county(s3, athena, cfg)
    #     get_csvs_for_R(s3, athena, cfg)
    #     run_r_script("county and hourly graphs.R")

    if opts.bssbucket_insert:
        _, athena = get_boto3_clients()
        bssbucket_insert(athena, cfg)

    if opts.bssbucket_parquetmerge:
        s3, athena = get_boto3_clients()
        s3_bucket = "bss-ief-bucket"
        # Insert and merge (BSS)
        # bssbucket_insert(athena, cfg)
        # bssbucket_parquetmerge(s3, cfg)
        merge_and_replace_folders(s3, 'bss-workflow', 'v2/annual_results/')
        # # Insert and merge (IEF)
        # bssiefbucket_insert(athena, cfg)
        # bssiefbucket_parquetmerge(s3, cfg)
        # bssbucket_parquet_scout(s3, athena, cfg)

    if opts.run_test:
        s3, athena = get_boto3_clients()
        test_county(s3, athena, cfg)
        test_multipliers(s3, athena, cfg)

    if opts.county_partition_mults:
        _, athena = get_boto3_clients()
        county_partition_multipliers(athena, cfg)

    if opts.gen_state_monthly_cal:
        s3, athena = get_boto3_clients()
        generate_state_monthly_for_cal(s3, athena, cfg)


if __name__ == "__main__":
    start_time = time.time()

    parser = ArgumentParser()
    parser.add_argument("--create_json", action="store_true", help="Create json/input.json from CSV files in csv_raw/")
    parser.add_argument("--gen_scoutdata", action="store_true", help="Process and upload Scout data")
    parser.add_argument("--gen_mults", action="store_true", help="Generate disaggregation multipliers from BuildStock")
    parser.add_argument("--gen_county", action="store_true", help="Generate county hourly data")
    parser.add_argument("--calibrate", action="store_true", help="Generate calibration multipliers and apply to existing county hourly tables")
    parser.add_argument("--combine_county", action="store_true", help="Combine county hourly tables")
    parser.add_argument("--gen_hourlyviz", action="store_true", help="Generate hourly visualizations")
    parser.add_argument("--convert_wide", action="store_true", help="Convert to wide format for publication")
    parser.add_argument("--gen_countyall", action="store_true", help="Process Scout data and disaggregate")
    parser.add_argument("--gen_state_monthly_cal", action="store_true", help="Generate State Monthly Calibration Data")
    parser.add_argument("--bssbucket_insert", action="store_true", help="Populate into bss-workflow")
    parser.add_argument("--bssbucket_parquetmerge", action="store_true", help="Populate + merge parquet under bucket")
    parser.add_argument("--run_test", action="store_true", help="Run diagnostics")
    parser.add_argument("--county_partition_mults", action="store_true", help="Partition multipliers by county")
    
    opts = parser.parse_args()
    main(opts)

    hours, rem = divmod(time.time() - start_time, 3600)
    minutes, seconds = divmod(rem, 60)
    print("--- Overall Runtime: %s (HH:MM:SS.mm) ---" %
          "{:0>2}:{:0>2}:{:05.2f}".format(int(hours), int(minutes), seconds))
