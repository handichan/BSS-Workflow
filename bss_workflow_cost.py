# this function 
# 1. translates JSON file to csv file
# 2. generate plots (as Margaret's R code)

import boto3
import time
import pandas as pd
import os
import json
from os import getcwd
from argparse import ArgumentParser
from io import StringIO
import math
import sys
import boto3
from botocore.exceptions import ClientError
# os.environ["PATH"] += os.pathsep + "C:/Program Files/R/R-4.4.2"
# os.environ["R_Home"] = "C:/Program Files/R/R-4.4.2"
# import rpy2.robjects as robjects

pd.set_option('display.max_columns', 30)
JSON_PATH = 'json/input.json'


CSV_DIR = "csv"
OUTPUT_DIR = "agg_results_cost"
EXTERNAL_S3_DIR = "datasets"
DATABASE_NAME = "euss_oedi"
BUCKET_NAME = 'xinweiz'

EUGROUP_DIR = f"map_eu"


SCOUT_RUN_DATE = "2025-01-28"
versionid = "20240923_amy"

ENVELOPE_MAP_FILE = os.path.join("map_meas", "envelope_map.tsv")
MEAS_MAP_FILE = os.path.join("map_meas", f"measure_map.tsv")
SQL_DIR = f"sql"


US_STATES = [
    'AL', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'DC', 'FL', 'GA',
    'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MD', 'MA',
    'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NH', 'NJ', 'NM', 'NV',
    'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC', 'SD',
    'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY']
def reshape_json(data, path=[]):
    rows = []
    if isinstance(data, dict):
        for key, value in data.items():
            new_path = path + [key]
            rows.extend(reshape_json(value, new_path))
    else:
        rows.append(path + [data])
    return rows


def scout_to_df(filename, myturnover):
    new_columns = [
            'meas', 'adoption_scn', 'metric',
            'reg', 'bldg_type', 'end_use',
            'fuel', 'year', 'value']
    with open(f'{filename}', 'r') as fname:
        json_df = json.load(fname)
    meas = list(json_df.keys())[:-1]

    all_df = pd.DataFrame()
    for mea in meas:
        json_data = json_df[mea]["Markets and Savings (by Category)"]

        data_from_json = reshape_json(json_data)
        
        df_from_json = pd.DataFrame(
                    data_from_json)
        df_from_json['meas'] = mea
        all_df = df_from_json if all_df.empty else pd.concat(
            [all_df, df_from_json], ignore_index=True)
        cols = ['meas'] + [col for col in all_df if col != 'meas']
        all_df = all_df[cols]

    all_df.columns = new_columns
    all_df = all_df[all_df['metric'].isin([
        'Efficient Energy Cost (USD)',
        'Baseline Energy Cost (USD)',
        'Energy Cost Savings (USD)'])]
    
    # fix measures that don't have a fuel key
    to_shift = all_df[pd.isna(all_df['value'])]
    to_shift.loc[:, 'value'] = to_shift['year']
    to_shift.loc[:, 'year'] = to_shift['fuel']
    to_shift.loc[:, 'fuel'] = 'Electric'

    df = pd.concat([all_df[pd.notna(all_df['value'])],to_shift])

    save_to_folder = 'cost_table_for_viz'
    os.makedirs(save_to_folder, exist_ok=True)
    df.to_csv(f'{save_to_folder}/{myturnover}.csv', index=False)
    print(f"Saved scout data to csv in {save_to_folder}/{myturnover}.csv") 
    return(df)

def file_to_df(file_path):
    # Check the file extension
    if file_path.endswith('.tsv'):
        df = pd.read_csv(file_path, sep='\t')
    elif file_path.endswith('.csv'):
        df = pd.read_csv(file_path)
    else:
        raise ValueError("Please provide a .tsv or .csv file.")
    return df

def gen_scoutdata_cost(subfolder=""):
    scout_files = [
        "aeo.json",
        "ref.json",
        "state.json",
        "fossil.json",
        "brk.json",
        "accel.json"
    ]
    
    for scout_file in scout_files:
        print(f">>>>>>>>>>>>>>>> FILE NAME = {scout_file}")
        # Join subfolder if provided
        scout_path = os.path.join("scout_results", subfolder, scout_file) if subfolder else os.path.join("scout_results", scout_file)
        myturnover = scout_file.split('_')[0]
        scout_df = scout_to_df(scout_path, myturnover)
        
def main(base_dir, subfolder):
    if opts.gen_scoutdata_cost:
        session = boto3.Session()
        s3_client = session.client('s3')
        athena_client = session.client('athena')
        gen_scoutdata_cost(subfolder)



if __name__ == "__main__":

    start_time = time.time()
    parser = ArgumentParser()
    parser.add_argument("--gen_scoutdata_cost", action="store_true",
                        help="Generate Scout Data for Cost")
    parser.add_argument("--folder", type=str, default="",
                        help="Subfolder under scout_results (e.g., 061425)")


    opts = parser.parse_args()
    base_dir = getcwd()
    main(base_dir, opts.folder)
    hours, rem = divmod(time.time() - start_time, 3600)
    minutes, seconds = divmod(rem, 60)
    print("--- Overall Runtime: %s (HH:MM:SS.mm) ---" %
          "{:0>2}:{:0>2}:{:05.2f}".format(int(hours), int(minutes), seconds))