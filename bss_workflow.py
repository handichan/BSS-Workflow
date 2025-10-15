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

# Optional R support (kept as in your original)
os.environ["PATH"] += os.pathsep + "C:/Program Files/R/R-4.4.3"
os.environ["R_Home"] = "C:/Program Files/R/R-4.4.3"
try:
    import rpy2.robjects as robjects
except Exception:
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
    CALIB_MULT_PATH = os.path.join(MAP_MEAS_DIR, "calibration_multipliers.csv")
    SCOUT_OUT_TSV = "scout_tsv"
    SCOUT_IN_JSON = "scout_json"
    OUTPUT_DIR = "agg_results"
    EXTERNAL_S3_DIR = "datasets"
    DATABASE_NAME = "euss_oedi"

    # Runtime switches/identifiers
    DEST_BUCKET = "bss-workflow"
    BUCKET_NAME = "handibucket" 
    SCOUT_RUN_DATE = "2025-09-24"
    VERSION_ID = "20250924"
    WEATHER = "amy"

    # TURNOVERS = ["breakthrough", "ineff", "mid", "high", "stated"]
    # TURNOVERS = ['brk','aeo25_20to50_bytech_indiv','aeo25_20to50_bytech_gap_indiv']

    TURNOVERS = ["aeo", "ref", "brk", "accel", "fossil", "state","dual_switch", "high_switch", "min_switch"]
    # YEARS = ['2026','2030','2034','2035','2040','2044','2045','2050']
    YEARS = ['2026','2030','2040','2050','2060']
    BASE_YEAR = '2026'

    # Auxiliary constants
    US_STATES = [
        'AL', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'DC', 'FL', 'GA',
        'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MD', 'MA',
        'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NH', 'NJ', 'NM', 'NV',
        'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC', 'SD',
        'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY'
    ]

# ----------------------------
# Utilities
# ----------------------------

def get_end_uses(sectorid: str):
    if sectorid == "res":
        return ['Refrigeration', 'Cooling (Equip.)', 'Heating (Equip.)',
                'Other', 'Water Heating', 'Cooking', 'Lighting',
                'Computers and Electronics']
    return ['Refrigeration', 'Cooling (Equip.)', 'Heating (Equip.)',
            'Other', 'Water Heating', 'Cooking', 'Lighting',
            'Ventilation', 'Computers and Electronics']


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
        to_shift.loc[:, "value"] = to_shift["year"]
        to_shift.loc[:, "year"] = to_shift["fuel"]
        to_shift.loc[:, "fuel"] = "Electric"
        df = pd.concat([all_df[pd.notna(all_df["value"])], to_shift])
    else:
        df = all_df

    out_path = os.path.join(f"{cfg.SCOUT_OUT_TSV}_df",
        f"scout_annual_state_{os.path.basename(filename).split('/')[0]}_df.tsv")
    os.makedirs(cfg.SCOUT_OUT_TSV, exist_ok=True)
    df.to_csv(out_path, sep="\t", index=False)

    out_weight_path = os.path.join(f"{cfg.SCOUT_OUT_TSV}_df",
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
    # dflong_before_split = dflong.copy()
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
            cmp_all = pre_all.merge(post_all, on=group_keys, how="outer") \
                             .fillna({"kwh_before": 0.0, "kwh_after": 0.0})
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
            cmp_sub = pre_sub.merge(post_sub, on=group_keys, how="outer") \
                             .fillna({"kwh_before": 0.0, "kwh_after": 0.0})
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
        version=cfg.VERSION_ID,
        sector=sectorid,
        year=yearid,
        dest_bucket=cfg.BUCKET_NAME,
        scout_version=cfg.SCOUT_RUN_DATE,
        sectorlong=sectorlong,
        weather=cfg.WEATHER,
        baseyear=cfg.BASE_YEAR
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
            f"RUN {sql_rel} | sector={sectorid} turnover={turnover} "
            f"year={yearid} state={st} enduse={eu}"
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
    # lists as in original
    tbl_res = [
        "tbl_ann_mult.sql",
        "res_ann_shares_cook.sql",
        "res_ann_shares_lighting.sql",
        "res_ann_shares_refrig.sql",
        "res_ann_shares_wh.sql",
        "res_ann_shares_hvac.sql",
        "res_ann_shares_deliveredheat.sql",
        "res_ann_shares_deliveredcool.sql",
        "res_ann_shares_deliveredwh.sql",
        "res_ann_shares_cw.sql",
        "res_ann_shares_dry.sql",
        "res_ann_shares_dw.sql",
        "res_ann_shares_fanspumps.sql",
        "res_ann_shares_misc.sql",
        "res_ann_shares_poolpump.sql",
        "tbl_hr_mult.sql",
        "res_hourly_shares_cooling.sql",
        "res_hourly_shares_heating.sql",
        "res_hourly_shares_refrig.sql",
        "res_hourly_shares_lighting.sql",
        "res_hourly_shares_cook.sql",
        "res_hourly_shares_wh.sql",
        "res_hourly_shares_fanspumps.sql",
        "res_hourly_shares_dw.sql",
        "res_hourly_shares_dry.sql",
        "res_hourly_shares_cw.sql",
        "res_hourly_shares_poolpump.sql",
        "res_hourly_shares_misc.sql",
        "res_hourly_shares_misc_flat.sql",
        "res_hourly_shares_gap.sql",
    ]
    tbl_com = [
        "tbl_ann_mult.sql",
        "com_ann_shares_cook.sql",
        "com_ann_shares_deliveredcool.sql",
        "com_ann_shares_electric_heat.sql",
        "com_ann_shares_hvac.sql",
        "com_ann_shares_lighting.sql",
        "com_ann_shares_refrig.sql",
        "com_ann_shares_ventilation_ref.sql",
        "com_ann_shares_wh.sql",
        "com_ann_shares_misc.sql",
        "com_ann_shares_gap.sql",
        "com_ann_shares_fossil_heat.sql",
        "tbl_hr_mult.sql",
        "tbl_hr_mult_hvac_temp.sql",
        "com_hourly_shares_cooling.sql",
        "com_hourly_shares_heating.sql",
        "com_hourly_shares_lighting.sql",
        "com_hourly_shares_refrig.sql",
        "com_hourly_shares_ventilation.sql",
        "com_hourly_shares_ventilation_ref.sql",
        "com_hourly_shares_wh.sql",
        "com_hourly_shares_misc.sql",
        "com_hourly_shares_gap.sql",
        "com_hourly_shares_cooking.sql",
        "com_hourly_hvac_norm.sql",
    ]

    for sectorid in sectors:
        tbls = tbl_res if sectorid == "res" else tbl_com
        for tbl_name in tbls:
            # year/turnover are not used by these create-table templates -> pass placeholders anyway
            sql_to_s3table(athena_client, cfg, tbl_name, sectorid, yearid="2024", turnover="brk")


def get_csvs_for_R(s3_client, athena_client, cfg: Config):
    turnovers = cfg.TURNOVERS
    sql_files = [
        # "county_100_hrs.sql",
        "county_ann_eu.sql",
        "county_hourly_examples.sql",
        "county_monthly_maxes.sql",
        "state_monthly_baseline.sql",
        "state_monthly.sql",
        "county_hourly_examples_60_days.sql",
        "annual_calibration_factors.sql"
        "county_peak_hour.sql",
        "county_share_winter.sql"
    ]
    out_dir = "R/generated_csvs"
    os.makedirs(out_dir, exist_ok=True)

    for t in turnovers:
        for sql_file in sql_files:
            sql_path = f"data_downloads/{sql_file}"
            template = read_sql_file(sql_path, cfg)
            q = template.format(turnover=t, dest_bucket=cfg.BUCKET_NAME, weather=cfg.WEATHER, baseyear=cfg.BASE_YEAR)
            df = execute_athena_query_to_df(s3_client, athena_client, q, cfg)
            out = os.path.join(out_dir, f"{t}_{os.path.basename(sql_file).replace('.sql', '.csv')}")
            df.to_csv(out, index=False)
            print(f"Saved {out}")


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
            FROM com_hourly_disaggregation_multipliers_{versionid}
            WHERE "in.county" = '{county_fips}'
        )
        TO 's3://bss-ief-bucket/multipliers_partitioned/com/in_county={county_fips}/'
        WITH (format = 'PARQUET');
    """.strip()

    q_res = """
        UNLOAD (
            SELECT "in.county", shape_ts, "timestamp_hour", multiplier_hourly, "in.state"
            FROM res_hourly_disaggregation_multipliers_{versionid}_flat
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
            q = template.format(county_fips=fips, versionid=cfg.VERSION_ID)
            print(f"UNLOAD county={fips} ({'com' if 'com' in fname else 'res'})")
            execute_athena_query(athena_client, q, cfg, is_create=False, wait=True)


def gen_scoutdata(s3_client, athena_client, cfg: Config):

    # Ensure measure_map exists in Athena
    s3_create_table_from_tsv(s3_client, athena_client, cfg.MEAS_MAP_PATH, cfg)

    # Ensure the calibration multipliers exist in Athena
    s3_create_table_from_tsv(s3_client, athena_client, cfg.CALIB_MULT_PATH, cfg)


    for turnover in cfg.TURNOVERS:
        scout_file = f"{turnover}.json"
        print(f">>> SCOUT FILE: {scout_file}")
        fp = os.path.join(cfg.SCOUT_IN_JSON, scout_file)
        turnover = scout_file.split(".")[0]

        # choose conversion path
        if scout_file in {
            "aeo.json",
            "fossil.json",
            "aeo25_20to50_byeu_indiv.json",
            "aeo25_20to50_bytech_gap_indiv.json",
            "aeo25_20to50_bytech_indiv.json",
            "min_switch",
            "dual_switch"
        }:
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
        # check measures coverage
        check_missing_meas_path = sdf  # same as original intent—compare against maps
        check_missing_meas(check_missing_meas_path, cfg)

        # register TSV to Athena
        s3_create_table_from_tsv(s3_client, athena_client, out_path, cfg)
        print(f"Finished adding scout data {scout_file}")

def gen_countydata(athena_client, cfg: Config):
    sectors = ["res", "com"]
    years = cfg.YEARS
    turnovers = cfg.TURNOVERS

    for s in sectors:
        for y in years:
            for t in turnovers:
                for name in ["tbl_ann_county.sql", "annual_county.sql", 
                             "tbl_hr_county.sql", "hourly_county.sql"]:
                    sql_to_s3table(athena_client, cfg, name, s, y, t)



# ----------------------------
# Athena data conversions (long<->wide, county aggregation, etc.)
# ----------------------------

def convert_countyhourly_long_to_wide(athena_client, cfg: Config):
    sql_dir = "data_conversion"
    turnovers = cfg.TURNOVERS

    # # baseline
    # template = read_sql_file(f"{sql_dir}/long_to_wide_baseline.sql", cfg)
    # q = template.format(dest_bucket=cfg.BUCKET_NAME, version=cfg.VERSION_ID, weather=cfg.WEATHER)
    # execute_athena_query(athena_client, q, cfg, is_create=False, wait=True)

    # scenarios
    template = read_sql_file(f"{sql_dir}/long_to_wide.sql", cfg)
    for t in turnovers:
        q = template.format(turnover=t, dest_bucket=cfg.BUCKET_NAME, version=cfg.VERSION_ID, weather=cfg.WEATHER)
        execute_athena_query(athena_client, q, cfg, is_create=False, wait=True)


def convert_scout_long_to_wide(athena_client, cfg: Config):
    sql_dir = "data_conversion"
    turnovers = cfg.TURNOVERS
    
    # # baseline
    # template = read_sql_file(f"{sql_dir}/long_to_wide_ann_baseline.sql", cfg)
    # q = template.format(dest_bucket=cfg.BUCKET_NAME, version=cfg.VERSION_ID)
    # execute_athena_query(athena_client, q, cfg, is_create=False, wait=True)

    # scenarios
    scout_header = f"""CREATE TABLE wide_scout_annual_state
        WITH (
            external_location = 's3://{{dest_bucket}}/{{version}}/wide/scout_annual_state/',
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
        "FROM long_county_hourly_{turnover}_amy "
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

    q = all_sql.format(turnover=turnover, dest_bucket=cfg.BUCKET_NAME, version=cfg.VERSION_ID, weather=cfg.WEATHER)

    # print(q)
    execute_athena_query(athena_client, q, cfg, is_create=False, wait=True)



def _combine_countydata(
    sectors,
    years,
    return_combined: bool = True,
):

    # ---- HOURLY ----
    hourly_header = f"""CREATE TABLE long_county_hourly_{{turnover}}_{{weather}}
        WITH (
            external_location = 's3://{{dest_bucket}}/{{version}}/long/county_hourly_{{turnover}}_{{weather}}/',
            format = 'Parquet',
            partitioned_by = ARRAY['sector', 'year', 'in.state']
        ) AS
        """
    hourly_select_tpl = (
        'SELECT "in.county", timestamp_hour, turnover, county_hourly_uncal_kwh, county_hourly_cal_kwh, '
        'scout_run, end_use, sector, year, "in.state"\n'
        "FROM county_hourly_{sector}_{year}_{turnover}_{weather}"
    )
    hourly_parts = []
    for sector in sectors:
        for yr in years:
            hourly_parts.append(
                hourly_select_tpl.format(sector=sector, year=yr, turnover="{turnover}", weather="{weather}")
            )
    hourly_sql = hourly_header + "\nUNION ALL\n".join(hourly_parts) + ";"

    # ---- ANNUAL ----
    annual_header = f"""CREATE TABLE long_county_annual_{{turnover}}_{{weather}}
        WITH (
            external_location = 's3://{{dest_bucket}}/{{version}}/long/county_annual_{{turnover}}_{{weather}}/',
            format = 'Parquet',
            partitioned_by = ARRAY['sector', 'year', 'in.state']
        ) AS
        """
    annual_select_tpl = (
        'SELECT "in.county", fuel, meas, tech_stage, multiplier_annual, '
        'state_ann_kwh, turnover, county_ann_kwh, scout_run, end_use, '
        'sector, year, "in.state"\n'
        "FROM county_annual_{sector}_{year}_{turnover}_{weather}"
    )
    annual_parts = []
    for sector in sectors:
        for yr in years:
            annual_parts.append(
                annual_select_tpl.format(sector=sector, year=yr, turnover="{turnover}", weather="{weather}")
            )
    annual_sql = annual_header + "\nUNION ALL\n".join(annual_parts) + ";"

    if return_combined:
        return f"{hourly_sql}\n\n{annual_sql}"
    return {"hourly": hourly_sql, "annual": annual_sql}


def combine_countydata(athena_client, cfg: Config):
    sql_dir = "data_conversion"
    turnovers = cfg.TURNOVERS
    years = cfg.YEARS
    q_combined = _combine_countydata(
        ["res","com"], 
        years,
        False)

    queries = [q_combined["annual"], q_combined["hourly"]]
    for query in queries:
        for t in turnovers:
            q = query.format(turnover=t, dest_bucket=cfg.BUCKET_NAME, version=cfg.VERSION_ID, weather=cfg.WEATHER)
            execute_athena_query(athena_client, q, cfg, is_create=False, wait=True)


def test_county(s3_client, athena_client, cfg: Config):
    sql_dir = "run_check"
    sql_files = [
        # "test_county_annual_total.sql",
        # "test_county_annual_enduse.sql",
        # "test_county_annual_meas.sql",
        "test_county_hourly_total.sql",
        "test_county_hourly_enduse.sql",
        "test_county_hourly_state.sql",
    ]
    years = cfg.YEARS
    turnovers = cfg.TURNOVERS
    weather = cfg.WEATHER

    os.makedirs("diagnostics", exist_ok=True)

    for sql_file in sql_files:
        out_csv = f"./diagnostics/{sql_file.split('.')[0]}.csv"
        final = pd.DataFrame()
        template = read_sql_file(f"{sql_dir}/{sql_file}", cfg)

        for t in turnovers:
            for y in years:
                q = template.format(dest_bucket=cfg.BUCKET_NAME, turnover=t, year=y, weather=cfg.WEATHER)
                df = execute_athena_query_to_df(s3_client, athena_client, q, cfg)
                df["year"] = y
                if "enduse" in sql_file:
                    if set({"commercial_sum", "scout_commercial_sum"}).issubset(df.columns):
                        df["diff_commercial"] = (1 - df["commercial_sum"] / df["scout_commercial_sum"]).round(2)
                    if set({"residential_sum", "scout_residential_sum"}).issubset(df.columns):
                        df["diff_residential"] = (1 - df["residential_sum"] / df["scout_residential_sum"]).round(2)
                    df = df.sort_values(by=["end_use", "turnover"], ascending=[True, True])
                elif "total" in sql_file:
                    if set({"commercial_sum", "scout_commercial_sum"}).issubset(df.columns):
                        df["diff_commercial"] = (1 - df["commercial_sum"] / df["scout_commercial_sum"]).round(2)
                    if set({"residential_sum", "scout_residential_sum"}).issubset(df.columns):
                        df["diff_residential"] = (1 - df["residential_sum"] / df["scout_residential_sum"]).round(2)
                    df = df.sort_values(by=["turnover"], ascending=[True])

                final = pd.concat([final, df], ignore_index=True)

        if os.path.exists(out_csv):
            os.remove(out_csv)
        final.to_csv(out_csv, index=False)
        print(f"Saved {out_csv}")


def test_multipliers(s3_client, athena_client, cfg: Config):
    sql_dir = "run_check"
    test_files = ["test_multipliers_annual.sql", 
                  "test_multipliers_hourly_com.sql",
                  "test_multipliers_hourly_res.sql"]
    out_csv = "./diagnostics/test_multipliers.csv"
    os.makedirs("diagnostics", exist_ok=True)

    final = pd.DataFrame()
    for sql_file in test_files:
        result_col = sql_file.split(".")[0]
        sectors = ["res", "com"] if "annual" in sql_file else (["com"] if "com" in sql_file else ["res"])
        for s in sectors:
            template = read_sql_file(f"{sql_dir}/{sql_file}", cfg)
            q = template.format(version=cfg.VERSION_ID, sector=s)
            print(q)
            df = execute_athena_query_to_df(s3_client, athena_client, q, cfg)
            df["test_name"] = f"{result_col}_{s}"
            final = pd.concat([final, df], ignore_index=True)

    if os.path.exists(out_csv):
        os.remove(out_csv)
    final.to_csv(out_csv, index=False)
    print(f"Saved {out_csv}")

    # Sum-to-1 checks
    checks = [
        f"""
        with re_agg as(
            SELECT group_ann, sector, "in.state", end_use, sum(multiplier_annual) as added
            FROM res_annual_disaggregation_multipliers_{cfg.VERSION_ID}
            GROUP BY group_ann, sector, "in.state", end_use
        )
        SELECT * FROM re_agg WHERE added>1.001 OR added<.9999
        """,
        f"""
        with re_agg as(
            SELECT shape_ts, sector, "in.state", "in.weather_file_city", end_use, sum(multiplier_hourly) as added
            FROM res_hourly_disaggregation_multipliers_{cfg.VERSION_ID}
            GROUP BY shape_ts, sector, "in.state", "in.weather_file_city", end_use
        )
        SELECT * FROM re_agg WHERE added>1.001 OR added<.9999
        """,
        f"""
        with re_agg as(
            SELECT group_ann, sector, "in.state", end_use, sum(multiplier_annual) as added
            FROM com_annual_disaggregation_multipliers_{cfg.VERSION_ID}
            GROUP BY group_ann, sector, "in.state", end_use
        )
        SELECT * FROM re_agg WHERE added>1.001 OR added<.9999
        """,
        f"""
        with re_agg as(
            SELECT shape_ts, sector, "in.county", end_use, sum(multiplier_hourly) as added
            FROM com_hourly_disaggregation_multipliers_{cfg.VERSION_ID}
            GROUP BY shape_ts, sector, "in.county", end_use
        )
        SELECT * FROM re_agg WHERE added>1.001 OR added<.9999
        """,
    ]
    for idx, q in enumerate(checks, 1):
        df = execute_athena_query_to_df(s3_client, athena_client, q, cfg)
        if df.empty:
            print(f"Check {idx}: OK (sums ≈ 1)")


def test_compare_measures(s3_client, athena_client, cfg: Config):
    out_dir = "diagnostics"
    os.makedirs(out_dir, exist_ok=True)
    out_txt = os.path.join(out_dir, "test_measures-set.txt")

    years = cfg.YEARS
    turnovers = cfg.TURNOVERS

    query_county_annual = """
        SELECT DISTINCT meas FROM county_annual_com_{year}_{turnover}
        UNION ALL
        SELECT DISTINCT meas FROM county_annual_res_{year}_{turnover}
    """
    query_scout = "SELECT DISTINCT meas FROM scout_annual_state_{turnover} WHERE fuel = 'Electric'"
    query_measure_map = "SELECT DISTINCT meas FROM measure_map"

    with open(out_txt, "w") as fh:
        # Measure map baseline
        lst_measure_map = execute_athena_query_to_df(s3_client, athena_client, query_measure_map, cfg).dropna(how="all")["meas"].tolist()
        for t in turnovers:
            for y in years:
                q1 = query_county_annual.format(turnover=t, year=y)
                q2 = query_scout.format(turnover=t)

                county_annual_lst = execute_athena_query_to_df(s3_client, athena_client, q1, cfg).dropna(how="all")["meas"].tolist()
                scout_lst = execute_athena_query_to_df(s3_client, athena_client, q2, cfg).dropna(how="all")["meas"].tolist()

                print(f"Measure map has {len(lst_measure_map)} measures.", file=fh)
                print(f"County annual data ({y} {t}) has {len(county_annual_lst)} measures.", file=fh)
                print(f"Scout ({t}) has {len(scout_lst)} measures.", file=fh)

                if len(scout_lst) > len(lst_measure_map):
                    print("Measures in Scout BUT NOT in Measure map:", file=fh)
                    print(set(scout_lst) - set(lst_measure_map), file=fh)
                else:
                    if set(county_annual_lst) == set(scout_lst):
                        print(f"Both county annual data ({y} {t}) and Scout ({t}) have the same measures-set.", file=fh)
                    else:
                        print("Measures in Scout BUT NOT in County annual:", file=fh)
                        print(set(scout_lst) - set(county_annual_lst), file=fh)
                print("=" * 62, file=fh)

    print(f"Saved {out_txt}")


# ----------------------------
# R runner
# ----------------------------

def run_r_script(r_file: str):
    if robjects is None:
        print("rpy2 not available; skipping R execution.")
        return
    r_path = os.path.join("R", r_file)
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


def _merge_parquet_folders(s3_client, top_level_prefix: str, s3_bucket: str):
    files = list_all_objects(s3_client, s3_bucket, top_level_prefix)
    if not files:
        print(f"No files found under s3://{s3_bucket}/{top_level_prefix}")
        return

    state_folders = set()
    for f in files:
        key = f["Key"]
        rel = key[len(top_level_prefix):]
        if "/" in rel:
            state_folders.add(rel.split("/")[0])

    print(f"Found state folders: {state_folders}")

    for state in state_folders:
        print(f"Processing state: {state}")
        state_prefix = f"{top_level_prefix}{state}/"
        page = s3_client.list_objects_v2(Bucket=s3_bucket, Prefix=state_prefix)
        state_files = page.get("Contents", [])
        os.makedirs(os.path.join("temp_files", state), exist_ok=True)
        local_files = []

        for obj in state_files:
            k = obj["Key"]
            if k.endswith("/"):
                continue
            local = os.path.join("temp_files", state, os.path.basename(k))
            s3_client.download_file(s3_bucket, k, local)
            local_files.append(local)

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
            combined = pd.concat(df_list, ignore_index=True)
            combined_path = f"{state}.parquet"
            combined.to_parquet(combined_path, engine="pyarrow", index=False)
            s3_client.upload_file(combined_path, s3_bucket, f"{top_level_prefix}US states/{combined_path}")
            print(f"Uploaded combined {combined_path} to s3://{s3_bucket}/{top_level_prefix}US states/")

        for lf in local_files:
            try:
                os.remove(lf)
            except Exception:
                pass
        try:
            os.rmdir(os.path.join("temp_files", state))
        except Exception:
            pass
        try:
            if df_list:
                os.remove(combined_path)
        except Exception:
            pass


def bssbucket_insert(athena_client, cfg: Config):
    turnovers = cfg.TURNOVERS
    years = cfg.YEARS
    dest_bucket = 'bss-workflow'

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


def bssbucket_parquetmerge(s3_client, cfg: Config):
    turnovers = cfg.TURNOVERS
    years = cfg.YEARS
    dest_bucket = 'bss-workflow'
    sectors = ["com", "res"]

    for t in turnovers:
        for s in sectors:
            for y in years:
                top = f"v2/county_hourly/{t}/sector={s}/year={y}/"
                print(f"Merging BSS bucket for {t} {s} {y}")
                _merge_parquet_folders(s3_client, top, dest_bucket)


def bssbucket_parquet_scout(s3_client, athena_client, cfg:Config):
    tables = ["wide_scout_annual_state", "wide_scout_annual_state_baseline"]
    for tname in tables:
        q = f"SELECT * FROM {tname};"
        execute_athena_query_to_df2(s3_client, athena_client, q, tname, cfg)


# ----------------------------
# Diagnostics & checks
# ----------------------------

def check_missing_meas(annual_state_scout_df: pd.DataFrame, cfg: Config):
    meas_files = [cfg.MEAS_MAP_PATH, cfg.ENVELOPE_MAP_PATH]
    for mfile in meas_files:
        try:
            measures = file_to_df(mfile)
            meas_in_map = set(measures.get("meas", pd.Series(dtype=str)))
            meas_in_scout = set(annual_state_scout_df.get("meas", pd.Series(dtype=str)))
            missing = meas_in_scout - meas_in_map
            if missing:
                for m in missing:
                    print(f"Measure in scout but NOT in {mfile}: {m}")
                print(f"Warning: Some measures from scout are missing in {mfile}.")
            else:
                print(f"All measures in scout are present in {mfile}.")
        except Exception as e:
            print(f"Error processing {mfile}: {e}")


# ----------------------------
# CLI & main entry
# ----------------------------

def main(opts):
    cfg = Config()

    if opts.create_json:
        convert_csv_folder_to_json("csv_raw", cfg.JSON_PATH)

    if opts.gen_mults:
        s3, athena = get_boto3_clients()
        s3_create_tables_from_csvdir(s3, athena, cfg)
        gen_multipliers(s3, athena, cfg)
        test_multipliers(s3, athena, cfg)

    if opts.gen_scoutdata:
        s3, athena = get_boto3_clients()
        gen_scoutdata(s3, athena, cfg)
        run_r_script("annual_graphs.R")

    if opts.gen_county:
        _, athena = get_boto3_clients()
        gen_countydata(athena, cfg)

    if opts.convert_wide:
        _, athena = get_boto3_clients()
        # convert_countyhourly_long_to_wide(athena, cfg)
        convert_scout_long_to_wide(athena, cfg)

    if opts.gen_countyall:
        s3, athena = get_boto3_clients()
        gen_scoutdata(s3, athena, cfg)
        run_r_script("annual_graphs.R")
        gen_countydata(athena, cfg)
        combine_countydata(athena, cfg)
        test_county(s3, athena, cfg)
        get_csvs_for_R(s3, athena, cfg)
        run_r_script("county and hourly graphs.R")
        run_r_script("calibration.R")

    if opts.bssbucket_insert:
        _, athena = get_boto3_clients()
        bssbucket_insert(athena, cfg)

    if opts.bssbucket_parquetmerge:
        s3, athena = get_boto3_clients()
        s3_bucket = "bss-ief-bucket"
        # Insert and merge (BSS)
        # bssbucket_insert(athena, cfg)
        bssbucket_parquetmerge(s3, cfg)
        # # Insert and merge (IEF)
        # bssiefbucket_insert(athena, cfg)
        # bssiefbucket_parquetmerge(s3, cfg)
        # bssbucket_parquet_scout(s3, athena, cfg)

    if opts.run_test:
        s3, athena = get_boto3_clients()
        test_county(s3, athena, cfg)
        test_multipliers(s3, athena, cfg)
        test_compare_measures(s3, athena, cfg)

    if opts.county_partition_mults:
        _, athena = get_boto3_clients()
        county_partition_multipliers(athena, cfg)


if __name__ == "__main__":
    start_time = time.time()

    parser = ArgumentParser()
    parser.add_argument("--create_json", action="store_true", help="Create json/input.json from CSV files in csv_raw/")
    parser.add_argument("--gen_mults", action="store_true", help="Generate stock tables")
    parser.add_argument("--gen_county", action="store_true", help="Generate County Data")
    parser.add_argument("--gen_countyall", action="store_true", help="Generate County Data and post-process")
    parser.add_argument("--gen_scoutdata", action="store_true", help="Generate Scout Data")
    parser.add_argument("--convert_wide", action="store_true", help="Convert datasets as necessary")
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
