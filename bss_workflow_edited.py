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
import rpy2.robjects as robjects


from botocore.exceptions import ClientError
# os.environ["PATH"] += os.pathsep + "C:/Program Files/R/R-4.4.2"
# os.environ["R_Home"] = "C:/Program Files/R/R-4.4.2"
# import rpy2.robjects as robjects

pd.set_option('display.max_columns', None)
JSON_PATH = 'json/input.json'


CSV_DIR = "csv"
OUTPUT_DIR = "agg_results"
EXTERNAL_S3_DIR = "datasets"
DATABASE_NAME = "euss_oedi"
BUCKET_NAME = 'xinweiz'

EUGROUP_DIR = f"map_eu"

# SCOUT_RUN_DATE = "2024-09-30"
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


def get_end_uses(sectorid):
    if sectorid == 'res':
        END_USES = ['Refrigeration', 'Cooling (Equip.)', 'Heating (Equip.)',
                    'Other', 'Water Heating', 'Cooking', 'Lighting',
                    'Computers and Electronics']
    else:
        END_USES = ['Refrigeration', 'Cooling (Equip.)', 'Heating (Equip.)',
                    'Other', 'Water Heating', 'Cooking', 'Lighting',
                    'Ventilation', 'Computers and Electronics']
    return END_USES


def get_var_char_values(data_dict):
    return [obj['VarCharValue'] for obj in data_dict['Data']]


def convert_json_to_csv_folder(mydir):
    if not os.path.exists(mydir):
        os.makedirs(mydir)
    with open(JSON_PATH, 'r') as json_file:
        data = json.load(json_file)
    for key in data:
        df = pd.DataFrame(data[key])
        csv_file_path = os.path.join(mydir, f"{key}.csv")
        df.to_csv(csv_file_path, index=False)
        print(f"CSV file saved: {csv_file_path}")


def convert_csv_folder_to_json(folder_path, json_path):
    json_dat = {}
    for file_name in os.listdir(folder_path):
        full_file_path = os.path.join(folder_path, file_name)
        if os.path.isfile(full_file_path) and file_name.endswith('.csv'):
            json_key = os.path.splitext(file_name)[0]
            data = pd.read_csv(full_file_path).fillna("None")
            json_dat[json_key] = {
                col: data[col].tolist() for col in data.columns}
    with open(json_path, 'w') as json_file:
        json.dump(json_dat, json_file, indent=4)
    print(f"Combined JSON data saved to {json_path}")


def split_into_sublists(data, num_sublists):
    n = len(data)
    sublist_length = n // num_sublists
    list_of_sublists = []
    for i in range(num_sublists):
        start_index = i * sublist_length
        end_index = (i + 1) * sublist_length if i != num_sublists - 1 else n
        list_of_sublists.append(data[start_index:end_index])
    return list_of_sublists


def fetch_query_results(client, query_execution_id):
    """Fetch the results of an executed Athena query."""
    query_result = client.get_query_results(
        QueryExecutionId=query_execution_id)
    result_data = query_result['ResultSet']

    if 'Rows' in result_data and len(result_data['Rows']) > 1:
        headers = get_var_char_values(result_data['Rows'][0])
        return [dict(zip(headers, get_var_char_values(row))) for
                row in result_data['Rows'][1:]]
    return None


def read_sql_file(sql_file):
    with open(os.path.join(SQL_DIR, sql_file), 'r', encoding='utf-8') as file:
        return file.read()


def reshape_json(data, path=[]):
    rows = []
    if isinstance(data, dict):
        for key, value in data.items():
            new_path = path + [key]
            rows.extend(reshape_json(value, new_path))
    else:
        rows.append(path + [data])
    return rows


def scout_to_df_noenv(filename):
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
    all_df = all_df[all_df['metric'].isin(['Efficient Energy Use (MMBtu)',
        'Efficient Energy Use, Measure (MMBtu)',
        'Baseline Energy Use (MMBtu)'])]
    print('Removing (R) Electric FS (Secondary Fossil Heating)')
    all_df = all_df[all_df['meas'] != '(R) Electric FS (Secondary Fossil Heating)']
    
    # fix measures that don't have a fuel key
    to_shift = all_df[pd.isna(all_df['value'])]
    to_shift['value'] = to_shift['year']
    to_shift['year'] = to_shift['fuel']
    to_shift['fuel'] = 'Electric'

    df = pd.concat([all_df[pd.notna(all_df['value'])],to_shift])

    return(df)


def calc_annual_noenv(df, include_baseline, turnover):
    
    efficient = df[df['metric'].isin(['Efficient Energy Use (MMBtu)',
        'Efficient Energy Use, Measure (MMBtu)'])]
    grouped = efficient.groupby(['meas','metric','reg',
        'end_use','fuel','year'])['value'].sum().reset_index()
    wide = grouped.pivot(index=['meas','reg','end_use','fuel','year'],
        columns='metric', values='value').reset_index()
    wide.columns = ['meas','reg','end_use','fuel','year','efficient_mmbtu',
                    'efficient_measure_mmbtu']
    wide = wide.assign(original_ann = lambda x: (
        x.efficient_mmbtu - x.efficient_measure_mmbtu) / 
        3412*10**6, measure_ann = lambda x: x.efficient_measure_mmbtu / 
        3412*10**6)
    long = wide.melt(id_vars = ['meas','reg','end_use','fuel','year'],
        value_vars = ['original_ann','measure_ann'], 
        var_name = 'tech_stage', value_name = 'state_ann_kwh')
    long['turnover'] = turnover
    to_return = long
    
    if include_baseline:
        base = df[df['metric']=='Baseline Energy Use (MMBtu)']
        grouped_base = base.groupby(['meas','metric','reg','end_use',
            'fuel','year'])['value'].sum().reset_index().assign(
            state_ann_kwh = lambda x: x.value / 3412*10**6)
        grouped_base['tech_stage'] = 'original_ann'
        grouped_base['turnover'] = 'baseline'
        to_return = pd.concat([to_return,grouped_base[['meas','reg',
            'end_use','fuel','year','tech_stage','state_ann_kwh','turnover']]])

    # local_path = os.path.join(OUTPUT_DIR, f"scout_annual_state_{turnover}_prior.tsv")
    # to_return.to_csv(local_path, sep='\t', index = False)

    to_return['sector'] = to_return.apply(add_sector, axis=1)
    to_return['scout_run'] = SCOUT_RUN_DATE

    local_path = os.path.join(OUTPUT_DIR, f"scout_annual_state_{turnover}.tsv")
    to_return.to_csv(local_path, sep='\t', index = False)
    return(to_return, local_path)


def scout_to_df(filename):
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
    all_df = all_df[all_df['metric'].isin(['Efficient Energy Use (MMBtu)',
        'Efficient Energy Use, Measure (MMBtu)',
        'Efficient Energy Use, Measure-Envelope (MMBtu)',
        'Baseline Energy Use (MMBtu)'])]
    
    # fix measures that don't have a fuel key
    to_shift = all_df[pd.isna(all_df['value'])]
    to_shift.loc[:, 'value'] = to_shift['year']
    to_shift.loc[:, 'year'] = to_shift['fuel']
    to_shift.loc[:, 'fuel'] = 'Electric'

    df = pd.concat([all_df[pd.notna(all_df['value'])],to_shift])


    return(df)


def add_sector(row):
    if pd.isna(row['meas']):
        # Uncomment the following lines if you need to print debug info
        # print(row['meas'])
        # print(row)
        # print(row.index)
        return None
    else:
        # Extract the first section before space
        sec = row['meas'].split(' ')[0]
        # Determine category based on content in 'sec'
        if '(C)' in sec:
            return 'com'
        elif '(R)' in sec:
            return 'res'
        else:
            return None


def file_to_df(file_path):
    # Check the file extension
    if file_path.endswith('.tsv'):
        df = pd.read_csv(file_path, sep='\t')
    elif file_path.endswith('.csv'):
        df = pd.read_csv(file_path)
    else:
        raise ValueError("Please provide a .tsv or .csv file.")
    return df


def check_missing_meas(measure_map_df, annual_state_scout_df):
    meas_in_measure_map = set(measure_map_df['meas'])
    meas_in_annual_state_scout = set(annual_state_scout_df['meas'])

    missing_meas = meas_in_annual_state_scout - meas_in_measure_map
    if missing_meas:
        for meas in missing_meas:
            print(f"Measures in 'annual_state_scout' but not in 'measure_map': {meas}")
        raise ValueError("Some measures from 'annual_state_scout' are missing in 'measure_map'.")
    else:
        print("All measures in 'annual_state_scout' are present in 'measure_map'.")


def infer_column_types(df):
    dtypes_map = {
        'object': 'string',
        'int64': 'int',
        'int32': 'int',
        'float64': 'double',
        'float32': 'double',
        'bool': 'boolean',
        'datetime64[ns]': 'timestamp'
    }
    
    columns = []
    for col in df.columns:
        dtype = str(df[col].dtype)
        athena_type = 'string' if col == 'upgrade' else dtypes_map.get(dtype, 'string')
        columns.append(f"`{col}` {athena_type}")
    
    return ",\n    ".join(columns)


def upload_file_to_s3(client, local_path, bucket, s3_path):
    client.upload_file(local_path, bucket, s3_path)
    print(f"""UPLOADED {os.path.basename(local_path)} 
          to s3://{bucket}/{s3_path}""")


def sql_create_table(df, table_name, file_format):
    # columns_sql = ',\n'.join([f"`{col}` STRING" for col in df.columns])
    escape_char = '\\'
    delimiter = None
    header = True

    if file_format.lower() == 'csv':
        default_delimiter = ','
    elif file_format.lower() == 'tsv':
        default_delimiter = '\t'
    else:
        logger.error("Unsupported file format. Use 'csv' or 'tsv'.")
    schema = infer_column_types(df)

    if delimiter is None:
        delimiter = default_delimiter

    sql_str = f"""
    CREATE EXTERNAL TABLE IF NOT EXISTS {table_name} (
        {schema}
    )
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY '{delimiter}'
    LOCATION 's3://{BUCKET_NAME}/{EXTERNAL_S3_DIR}/{table_name}/'
    TBLPROPERTIES ('skip.header.line.count'='1');
    """
    return sql_str



def start_athena_query(client, query, output_location):
    """Starts an Athena query execution and returns the query execution ID."""
    response = client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={'Database': DATABASE_NAME},
        ResultConfiguration={'OutputLocation': output_location}
    )
    return response['QueryExecutionId']

def wait_for_query_completion(client, query_execution_id):
    """Waits for the Athena query to complete and returns its final status."""
    while True:
        response = client.get_query_execution(QueryExecutionId=query_execution_id)
        status = response['QueryExecution']['Status']['State']
        
        if status == 'SUCCEEDED':
            print('Query succeeded!')
            return status, response
        elif status in ['FAILED', 'CANCELLED']:
            reason = response['QueryExecution']['Status'].get(
                'StateChangeReason', 'Unknown failure reason')
            raise Exception(f'Query {status.lower()}: {reason}')
        
        time.sleep(2)

def fetch_query_results(client, query_execution_id):
    """Fetches the query results from Athena."""
    return client.get_query_results(QueryExecutionId=query_execution_id)

def execute_athena_query(client, query, is_create, wait=True):
    """Executes an Athena query and optionally waits for completion."""
    output_location = f"s3://{BUCKET_NAME}/configs/"
    query_execution_id = start_athena_query(client, query, output_location)
    
    if not wait:
        return query_execution_id, None
    
    status, query_status = wait_for_query_completion(client, query_execution_id)
    
    if status == 'SUCCEEDED':
        result_loc = query_status['QueryExecution']['ResultConfiguration']['OutputLocation']
        print(f"SQL query succeeded and results are stored in {result_loc}")
        print('fetching query results')
        query_results = fetch_query_results(client, query_execution_id) if not is_create else None
        print(f'fetching query results finished for {query[:80]}')
        # return result_loc, query_results

def execute_athena_query_to_df(athena_client, query, wait=True):
    """Executes an Athena query and returns the result as a Pandas DataFrame."""
    output_location = f"s3://{BUCKET_NAME}/diagnosis_csv/"
    query_execution_id = start_athena_query(athena_client, query, output_location)
    
    if not wait:
        return query_execution_id, None
    
    wait_for_query_completion(athena_client, query_execution_id)
    results_output_location = f"{output_location}{query_execution_id}.csv"
    df = pd.read_csv(results_output_location)
    return df

def execute_athena_query_to_df2(s3_client, athena_client, query):
    """Executes an Athena query, downloads results, and saves as CSV and Parquet."""
    s3_bucket_target = "bss-workflow"
    s3_folder = "annual/"
    s3_output_prefix = "athena_results/"
    output_location = f"s3://{BUCKET_NAME}/{s3_output_prefix}"
    
    query_execution_id = start_athena_query(athena_client, query, output_location)
    wait_for_query_completion(athena_client, query_execution_id)
    
    output_file = f"{s3_output_prefix}{query_execution_id}.csv"
    local_parquet_file = "annual_results.parquet"
    s3_client.download_file(BUCKET_NAME, output_file, "query_results.csv")
    
    df = pd.read_csv("query_results.csv")
    df.to_csv("annual_results.csv", index=False)
    df.to_parquet(local_parquet_file, engine="pyarrow")
    print(f"Results saved as {local_parquet_file}")
    
    s3_client.upload_file(local_parquet_file, s3_bucket_target, f"{s3_folder}{local_parquet_file}")
    print(f"{local_parquet_file} is uploaded to {s3_bucket_target}/{s3_folder}")
    


def get_csvs_for_R(athena_client):

    turnovers = ['breakthrough','ineff','mid','high','stated']

    sql_files = [
        'county_100_hrs.sql',
        'county_ann_eu.sql',
        'county_hourly_examples.sql',
        'county_monthly_maxes.sql',
        'state_monthly_2024.sql'
        ]

    for my_turnover in turnovers:
        for sql_file in sql_files:
            sql_path = f"data_downloads/{sql_file}"
            csv_file = sql_file.replace(".sql", ".csv")
            query = read_sql_file(sql_path)
            if "TURNOVERID" in query:
                query = query.replace("TURNOVERID", f"{my_turnover}")
            df = execute_athena_query_to_df(athena_client, query, wait=True)
            
            if os.path.exists(csv_file):
                os.remove(csv_file)
            df.to_csv(f"R/generated_csvs/{my_turnover}_{csv_file}", index=False)
            print(f"{csv_file} is saved!")


def test_county(athena_client):
    sql_dir = "run_check"
    sql_files = [
        "test_county_annual_total.sql",
        "test_county_annual_enduse.sql",
        "test_county_hourly_total.sql",
        "test_county_hourly_enduse.sql"
    ]

    years = ['2024','2025','2030','2035','2040','2045','2050']
    turnovers = ['breakthrough','ineff','mid','high','stated']

    # years = [2018,2019,2020,2021,2022,2023]
    # turnovers = ['ineffuncal']

    for sql_file in sql_files:
        print(f"Querying for {sql_file}")
        csv_out = f"{sql_file.split('.')[0]}.csv"

        final_df = pd.DataFrame()

        for my_turnover in turnovers:
            for my_year in years:
                # print(f"Querying for {my_turnover} {my_year}")

                query = read_sql_file(f"{sql_dir}/{sql_file}")
                if "TURNOVERID" in query:
                    query = query.replace("TURNOVERID", f"{my_turnover}")
                if "YEARID" in query:
                    query = query.replace("YEARID", f"{my_year}")    

                df = execute_athena_query_to_df(athena_client, query)

                df['year'] = my_year

                if "enduse" in sql_file:
                    df['diff_commercial'] = (1 - df['commercial_sum'] / df['scout_commercial_sum']).round(2)
                    df['diff_residential'] = (1 - df['residential_sum'] / df['scout_residential_sum']).round(2)
                    df = df.sort_values(by=['end_use', 'turnover'], ascending=[True, True])
                elif "total" in sql_file:
                    df['diff'] = (1 - (df['commercial_sum'] + df['residential_sum']) / df['scout_sum']).round(2)
                    df = df.sort_values(by=['turnover'], ascending=[True])

                final_df = pd.concat([final_df, df], ignore_index=True)

        if os.path.exists(csv_out):
            os.remove(csv_out)
        final_df.to_csv(f"./diagnostics/{csv_out}", index=False)
        print(f"{csv_out} is saved!")


def test_multipliers(athena_client):
    ## Check for number of counties
    sql_dir = "run_check"
    sql_files = ["test_multipliers_annual.sql", "test_multipliers_hourly.sql" ]
    csv_out = "test_multipliers.csv"

    final_df = pd.DataFrame()

    for sql_file in sql_files:
        result_col = sql_file.split(".")[0]
        sectors = ['res','com']
        for my_sec in sectors:
            query = read_sql_file(f"{sql_dir}/{sql_file}")
            if "SECTORID" in query:
                query = query.replace("SECTORID", f"{my_sec}")

            print(f"\n{query}")
            df = execute_athena_query_to_df(athena_client, query)
            df['test_name'] = f"{result_col}_{my_sec}"

            final_df = pd.concat([final_df, df], ignore_index=True)
    if os.path.exists(csv_out):
        os.remove(csv_out)
    final_df.to_csv(f"./diagnostics/{csv_out}", index=False)
    print(f"{csv_out} is saved!")


    ## Check if all multipliers sum to 1
    queries = [
    f"""
        with re_agg as( SELECT group_ann,group_version,sector,"in.state",
        end_use,sum(multiplier_annual) as added 
        FROM res_annual_disaggregation_multipliers_20240923 GROUP BY group_ann,
        group_version,sector,"in.state",end_use) 
        SELECT * FROM re_agg WHERE added>1.001 OR added<.9999
    """,
    f"""
        with re_agg as( SELECT shape_ts,group_version,sector,"in.county",
        end_use,sum(multiplier_hourly) as added 
        FROM res_hourly_disaggregation_multipliers_20240923 GROUP BY shape_ts,
        group_version,sector,"in.county",end_use) 
        SELECT * FROM re_agg WHERE added>1.001 OR added<.9999
    """
    ]
    if (execute_athena_query_to_df(athena_client, queries[0])).empty:
        print("Annual mutipliers are all sum to 1.\n")
    if (execute_athena_query_to_df(athena_client, queries[1])).empty:
        print("Hourly mutipliers are all sum to 1.\n")


def test_compare_measures(athena_client):
    txt_out = 'test_measures-set.txt'
    out = f"./diagnostics/{txt_out}"

    # years = [2024,2030,2040,2050]
    turnovers = ['breakthrough','ineff','mid','high','stated']
    years = [2024]

    query_county_annual = f"""
        SELECT DISTINCT meas FROM county_annual_com_YEARID_TURNOVERID 
        UNION ALL SELECT DISTINCT meas FROM county_annual_res_YEARID_TURNOVERID
    """
    query_scout = f"""
        SELECT DISTINCT meas FROM scout_annual_state_TURNOVERID WHERE fuel = 'Electric'
    """
    query_measure_map = f"""
        SELECT DISTINCT meas FROM measure_map_20240927
    """
    with open(out, 'w') as file:
        sys.stdout = file
        
        lst_measure_map = (execute_athena_query_to_df(athena_client, query_measure_map).dropna(how='all'))['meas'].tolist()

        for my_turnover in turnovers:
            for my_year in years:
                query1 = query_county_annual.replace("TURNOVERID", f"{my_turnover}")
                query1 = query1.replace("YEARID", f"{my_year}")
                query2 = query_scout.replace("TURNOVERID", f"{my_turnover}")

                county_annual_lst = (execute_athena_query_to_df(athena_client, query1).dropna(how='all'))['meas'].tolist()
                scout_lst = (execute_athena_query_to_df(athena_client, query2).dropna(how='all'))['meas'].tolist()

                print(f"Measure map has {len(lst_measure_map)} measures.")
                print(f"County annual data ({my_year} {my_turnover}) has {len(county_annual_lst)} measures.")
                print(f"Scout ({my_turnover}) has {len(scout_lst)} measures.")

                if(len(scout_lst) > len(lst_measure_map)):
                    print("Measures in Scout BUT NOT in Measure map:")
                    print(set(scout_lst) - set(lst_measure_map))
                else:
                    if set(county_annual_lst) == set(scout_lst):
                        print(f"""Both county annual data ({my_year} {my_turnover}) and Scout ({my_turnover}) have the same measures-set.""")
                    else:
                        print("Measures in Scout BUT NOT in County annual:")
                        print(set(scout_lst) - set(county_annual_lst))
                        print(scout_lst)
                print("===============================================================\n")
    sys.stdout = sys.__stdout__
    print(f"{txt_out} is saved!")

def table_exists(glue_client, database_name, table_name):
    try:
        glue_client.get_table(DatabaseName=database_name, Name=table_name)
        return True
    except ClientError as e:
        if e.response['Error']['Code'] == 'EntityNotFoundException':
            return False
        else:
            raise

def sql_to_s3table(s3_client, athena_client, glue_client, database, sql_file, sectorid, yearid, turnover):
    sql_file = f"{sectorid}/{sql_file}"
    query = read_sql_file(sql_file)
    suff = f"{sectorid} {turnover} {yearid}"
    END_USES = get_end_uses(sectorid)

    # table_name = f"county_annual_{sectorid}_{yearid}_{turnover}"  # Adjust to match your naming pattern

    # if table_exists(glue_client, database, table_name):
    #     print(f"[SKIP] Table '{table_name}' already exists. Skipping creation.")
    #     return
    
    if "VERSIONID" in query:
        query = query.replace("VERSIONID", f"{versionid}")
    if "MEASVERSION" in query:
        query = query.replace("MEASVERSION", f"{measversion}")
    if "TURNOVERID" in query:
        query = query.replace("TURNOVERID", f"{turnover}")
    if "SCOUTRUNDATE" in query:
        query = query.replace("SCOUTRUNDATE", f"{SCOUT_RUN_DATE}")
    if "YEARID" in query:
        query = query.replace("YEARID", f"{yearid}")

    def execute_statements(query_block, context_info=""):
        # Check for multiple statements (semi-colons separating them)
        statements = [stmt.strip() for stmt in query_block.strip().split(';') if stmt.strip()]
        for stmt in statements:
            print(f"Executing ({context_info}):\n{stmt[:80]}...\n")  # Print first 80 chars for brevity
            execute_athena_query(athena_client, stmt, False)
            
    if "STATEID" in query:
        for uss in US_STATES:
            query1 = query.replace("STATEID", f"{uss}")
            print(f"START QUERYING FOR {sql_file} {suff} {uss}")
            # print(query1)
            execute_statements(query1, f"{sql_file} {suff} {uss}")
    elif "ENDUSEID" in query:
        for eu in END_USES:
            query1 = query.replace("ENDUSEID_name", f"{eu.split()[0]}")
            query1 = query1.replace("ENDUSEID", f"{eu}")
            print(f"START QUERYING FOR {sql_file} {suff} {eu}")
            # print(query1)
            execute_statements(query1, f"{sql_file} {suff} {eu}")
    print(f"START QUERYING FOR {sql_file} {suff}")
    execute_statements(query, f"{sql_file} {suff}")
    print(f"FINISHED QUERYING {sql_file} {suff}")


def s3_create_tables_from_csvdir(s3_client, athena_client):
    for file_name in os.listdir(EUGROUP_DIR):
        local_path = os.path.join(EUGROUP_DIR, file_name)
        table_name = os.path.splitext(file_name)[0]
        file_format = os.path.splitext(file_name)[-1][1:]
        if os.path.isfile(local_path):
            s3_path = f"{EXTERNAL_S3_DIR}/{table_name}/{file_name}"
            upload_file_to_s3(s3_client, local_path, BUCKET_NAME, s3_path)

            # Determine the delimiter based on the file extension
            if file_name.endswith('.csv'):
                delimiter = ','
            elif file_name.endswith('.tsv'):
                delimiter = '\t'
            else:
                raise ValueError("Unsupported file format. Please provide a .csv or .tsv file.")
            
            df = pd.read_csv(local_path, delimiter=delimiter)

            sql_query = sql_create_table(df, table_name, file_format)
            _, _ = execute_athena_query(athena_client, sql_query, True)


def s3_create_table_from_tsv(s3_client, athena_client, local_path):

    dir_name = os.path.dirname(local_path)
    file_name = os.path.basename(local_path)
    table_name = os.path.splitext(file_name)[0]
    file_format = os.path.splitext(file_name)[-1][1:]
    print(local_path)
    print(table_name)
    print(file_name)

    if os.path.isfile(local_path):
        s3_path = f"{EXTERNAL_S3_DIR}/{table_name}/{file_name}"
        upload_file_to_s3(s3_client, local_path, BUCKET_NAME, s3_path)

        # Determine the delimiter based on the file extension

        if file_name.endswith('.csv'):
            delimiter = ','
        elif file_name.endswith('.tsv'):
            delimiter = '\t'
        else:
            raise ValueError("Unsupported file format. Please provide a .csv or .tsv file.")

        df = pd.read_csv(local_path, delimiter=delimiter)

        sql_query = sql_create_table(df, table_name, file_format)
        _, _ = execute_athena_query(athena_client, sql_query, True)


def s3_insert_to_table_from_tsv(s3_client, athena_client, local_path, dest_table_name):
    table_name = os.path.splitext(os.path.basename(local_path))[0]
    s3_create_table_from_tsv(s3_client, athena_client, local_path)
    sql_query = f"""
        INSERT INTO {dest_table_name}
        SELECT *
        FROM {table_name};
    """
    _, _ = execute_athena_query(athena_client, sql_query, True)


def df_to_s3table2(s3_client, athena_client, df, table_name):
    file_format = "tsv"
    file_name = f"{table_name}.tsv"
    local_path = os.path.join(OUTPUT_DIR, file_name)
    df.to_csv(f"{OUTPUT_DIR}/{file_name}", index=False, delimiter=file_format)
    if os.path.isfile(local_path):
        s3_path = f"{EXTERNAL_S3_DIR}/{table_name}/{file_name}"
        upload_file_to_s3(s3_client, local_path, BUCKET_NAME, s3_path)

        sql_query = sql_create_table(df, table_name, file_format)
        _, _ = execute_athena_query(athena_client, sql_query, True)


def run_r_script(r_file):
    r_path = os.path.join('R', r_file)
    with open(r_path, 'r') as file:
        r_code = file.read()
    try:
        robjects.r(r_code)
        print("R script executed successfully.")
    except Exception as e:
        print(f"Error executing R script: {e}")


def calc_annual(df, include_baseline, turnover):
    envelope_map = file_to_df(ENVELOPE_MAP_FILE)

    efficient = df[df['metric'].isin(['Efficient Energy Use (MMBtu)',
        'Efficient Energy Use, Measure (MMBtu)',
        'Efficient Energy Use, Measure-Envelope (MMBtu)'])]

    grouped = efficient.groupby(['meas','metric','reg',
        'end_use','fuel','year'])['value'].sum().reset_index()

    # print(efficient.columns)
    print(f">>>>>>>>>>>>>>>> COLUMN NAMES= {grouped.columns}")
    wide = grouped.pivot(index=['meas','reg','end_use','fuel','year'],
        columns='metric', values='value').reset_index()

    wide.columns = ['meas','reg','end_use','fuel','year','efficient_mmbtu',
                    'efficient_measure_mmbtu','efficient_measure_env_mmbtu']
    no_packages = wide[pd.isna(wide['efficient_measure_env_mmbtu'])]
    no_packages = no_packages.assign(original_ann = lambda x: (
        x.efficient_mmbtu - x.efficient_measure_mmbtu) / 
        3412*10**6, measure_ann = lambda x: x.efficient_measure_mmbtu / 
        3412*10**6)

    with_packages = wide[~pd.isna(wide['efficient_measure_env_mmbtu'])]
    with_packages = with_packages.merge(envelope_map,on='meas',how='left')
    with_packages['measure_ann'] = with_packages.apply(
    lambda row: (row['efficient_measure_mmbtu'] - row['efficient_measure_env_mmbtu']) / 3412*10**6 
                if row['component'] == 'equipment' 
                else row['efficient_measure_env_mmbtu'] /3412*10**6
                if row['component'] == 'equipment + env' 
                else None, axis=1)


    with_packages['original_ann'] = with_packages.apply(
    lambda row: (row['efficient_mmbtu'] - row['efficient_measure_mmbtu']) / 3412*10**6 
                if row['component'] == 'equipment' 
                else 0
                if row['component'] == 'equipment + env' 
                else None, axis=1)
    
    # with_packages = with_packages.assign(original_ann = lambda x: (
    #     x.efficient_mmbtu - x.efficient_measure_mmbtu) / 
    #     3412*10**6)

    with_packages = with_packages[['meas_separated', 'reg', 'end_use', 'fuel', 'year', 'efficient_mmbtu',
           'efficient_measure_mmbtu', 'efficient_measure_env_mmbtu',
           'original_ann', 'measure_ann']]
    with_packages = with_packages.rename(columns={'meas_separated': 'meas'})

    long = pd.concat([no_packages,with_packages]).melt(id_vars = ['meas','reg','end_use','fuel','year'],
        value_vars = ['original_ann','measure_ann'], 
        var_name = 'tech_stage', value_name = 'state_ann_kwh')
    long['turnover'] = turnover

    to_return = long
    
    if include_baseline:
        base = df[df['metric']=='Baseline Energy Use (MMBtu)']
        grouped_base = base.groupby(['meas','metric','reg','end_use',
            'fuel','year'])['value'].sum().reset_index().assign(
            state_ann_kwh = lambda x: x.value / 3412*10**6)
        grouped_base['tech_stage'] = 'original_ann'
        grouped_base['turnover'] = 'baseline'
        grouped_base = grouped_base.merge(envelope_map[envelope_map['component']=='equipment'],on='meas',how='left')
        grouped_base['meas'] = grouped_base.apply(lambda row: row['meas_separated'] 
                      if pd.notnull(row['meas_separated']) and isinstance(row['meas_separated'], (str)) 
                      else row['meas'], axis=1)
        grouped_base = grouped_base[['meas','reg',
            'end_use','fuel','year','tech_stage','state_ann_kwh','turnover']]
        #grouped_base = grouped_base.rename(columns={'meas_separated': 'meas'})
        to_return = pd.concat([to_return,grouped_base])

    # local_path = os.path.join(OUTPUT_DIR, f"scout_annual_state_{turnover}_prior.tsv")
    # to_return.to_csv(local_path, sep='\t', index = False)

    to_return['sector'] = to_return.apply(add_sector, axis=1)
    to_return['scout_run'] = SCOUT_RUN_DATE

    local_path = os.path.join(OUTPUT_DIR, f"scout_annual_state_{turnover}.tsv")
    to_return.to_csv(local_path, sep='\t', index = False)
    return(to_return, local_path)


def list_all_objects(s3_client, bucket, prefix):
    """
    Lists all objects in an S3 bucket for a given prefix, handling pagination.
    """
    paginator = s3_client.get_paginator('list_objects_v2')
    operation_parameters = {'Bucket': bucket, 'Prefix': prefix}
    all_objects = []

    for page in paginator.paginate(**operation_parameters):
        all_objects.extend(page.get('Contents', []))
    
    return all_objects


def gen_multipliers(s3_client, athena_client):
    # s3_create_tables_from_csvdir(s3_client, athena_client)
    sectors = ['res']

    tbl_res = [
        "tbl_ann_mult.sql",
        "res_ann_shares_cook.sql",
        "res_ann_shares_lighting.sql",
        "res_ann_shares_refrig.sql",
        "res_ann_shares_wh.sql",
        "res_ann_shares_hvac.sql",
        "res_ann_shares_deliveredheat.sql",
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
        "res_hourly_shares_misc_flat.sql"
    ]

    tbl_com = [
        "tbl_ann_mult.sql",
        "com_ann_shares_cook.sql",
        "com_ann_shares_hvac.sql",
        "com_ann_shares_lighting.sql",
        "com_ann_shares_refrig.sql",
        "com_ann_shares_ventilation_ref.sql",
        "com_ann_shares_wh.sql",
        "com_ann_shares_misc.sql",
        "com_ann_shares_fossil_heat.sql",
        
        "tbl_hr_mult.sql",
        "com_hourly_shares_cooling.sql",
        "com_hourly_shares_heating.sql",
        "com_hourly_shares_lighting.sql",
        "com_hourly_shares_refrig.sql",
        "com_hourly_shares_ventilation.sql",
        "com_hourly_shares_ventilation_ref.sql",
        "com_hourly_shares_wh.sql",
        "com_hourly_shares_misc.sql",
        "com_hourly_shares_cooking.sql"
    ]
    for sectorid in sectors:
        if sectorid == 'res':
            tbl_names = tbl_res
        if sectorid == 'com':
            tbl_names = tbl_com
        
        for tbl_name in tbl_names:
            sql_to_s3table(s3_client, athena_client, tbl_name, sectorid, yearid=None, turnover=None)

    ## UNUSED
    # s3_create_table_from_tsv(s3_client, athena_client, os.path.join("addons", "county2tz_2.tsv"))
    # s3_create_table_from_tsv(s3_client, athena_client, os.path.join("eugroup", "res_ann_cook.tsv"))
    # s3_create_table_from_tsv(s3_client, athena_client, os.path.join("eugroup", "res_ts_cook.tsv"))
    # s3_insert_to_table_from_tsv(s3_client, athena_client, os.path.join("addons", "ann_dis_mult_cook.csv"), "annual_disaggregation_multipliers_0719")
def execute_statements(athena_client, query_block, context_info=""):
    # Check for multiple statements (semi-colons separating them)
    statements = [stmt.strip() for stmt in query_block.strip().split(';') if stmt.strip()]
    for stmt in statements:
        execute_athena_query(athena_client, stmt, False)

def combine_countydata(athena_client):
    sql_dir = "data_conversion"
    sql_files = [
        "combine_annual_2024_2050.sql",
        "combine_hourly_2024_2050.sql"
    ]
    turnovers = ['breakthrough','ineff','mid','high','stated']
    for sql_file in sql_files:
        print(f"Querying for {sql_file}")
        for my_turnover in turnovers:
            print(f'turnover id: {my_turnover}')
            query = read_sql_file(f"{sql_dir}/{sql_file}")
            if "TURNOVERID" in query:
                
                query = query.replace("TURNOVERID", f"{my_turnover}")
            # execute_athena_query(athena_client, query, False)
            execute_statements(athena_client, query)


def convert_long_to_wide(athena_client):
    sql_dir = "data_conversion"
    sql_files = [
        "long_to_wide.sql"
    ]
    turnovers = ['breakthrough','ineff','mid','high','stated']
    for sql_file in sql_files:
        print(f"Querying for {sql_file}")
        for my_turnover in turnovers:
            query = read_sql_file(f"{sql_dir}/{sql_file}")
            if "TURNOVERID" in query:
                query = query.replace("TURNOVERID", f"{my_turnover}")
            execute_athena_query(athena_client, query, False)

def convert_long_to_wide_scout(s3_client, athena_client):
    sql_dir = "data_conversion"
    sql_files = [
        "long_to_wide_ann.sql"
    ]
    for sql_file in sql_files:
        print(f"Querying for {sql_file}")
        query = read_sql_file(f"{sql_dir}/{sql_file}")
        execute_athena_query(athena_client, query, False)

    # download, parqueting, and upload to bss-workflow
    TABLE_NAME = "wide_scout_annual_state"
    query2 = f"SELECT * FROM {TABLE_NAME};"
    execute_athena_query_to_df2(s3_client, athena_client, query2)

def bssbucket_insert_scout(athena_client):
    query = f"""
    CREATE TABLE wide_scout_annual_state_TURNOVERID_YEARID
    WITH (
        external_location = 's3://bss-workflow/annual/scenario=TURNOVERID/year=YEARID/',
        format = 'PARQUET',
        write_compression = 'SNAPPY',
        partitioned_by = ARRAY['state']
    ) AS
    SELECT *
    FROM wide_scout_annual_state
    WHERE scenario = 'TURNOVERID' AND year = YEARID;
    """
    turnovers = ['breakthrough','ineff','mid','high','stated']
    years = [2024,2025,2030,2035,2040,2045,2050]
    sectors = ['res','com']
    for myturnover in turnovers:
        for myyear in years:
            query1 = query
            if "TURNOVERID" in query:
                query1 = query1.replace("TURNOVERID", f"{myturnover}")
            if "YEARID" in query:
                query1 = query1.replace("YEARID", f"{myyear}")
                print(f"Querying for {query1}")
                execute_athena_query(athena_client, query1, False) 


def gen_scoutdata(s3_client, athena_client):
    # scout_files = [
    #     "breakthrough_exog.json",
    #     "ineff_exog.json",
    #     "high_exog.json",
    #     "mid_exog.json",
    #     "stated_exog.json"
    #     ]
    scout_files = [
        "ineff.json",
        "stated.json"
    ]

    measure_map = file_to_df(MEAS_MAP_FILE)
    # s3_create_table_from_tsv(s3_client, athena_client, MEAS_MAP_FILE)
    for scout_file in scout_files:
        print(f">>>>>>>>>>>>>>>>FILE NAME= {scout_file}")
        SCOUT_RESULTS_FILEPATH = os.path.join("scout_results", scout_file)
        myturnover = scout_file.split('_')[0]

        if scout_file in ["ineff.json", "stated.json","ineff20182023_exog.json",
            "ineffrecal_exog.json", "ineffrecal_01092025.json", "ineffuncal_12102024.json"]:
            scout_df = scout_to_df_noenv(SCOUT_RESULTS_FILEPATH)
            scout_ann_df, scout_ann_local_path = calc_annual_noenv(scout_df,include_baseline = True, turnover = myturnover)
        else:
            scout_df = scout_to_df(SCOUT_RESULTS_FILEPATH)
            scout_ann_df, scout_ann_local_path = calc_annual(scout_df,include_baseline = True, turnover = myturnover)

        check_missing_meas(measure_map, scout_df)
        s3_create_table_from_tsv(s3_client, athena_client, scout_ann_local_path)
        print(f"Finished adding scout data {scout_file}")


    
def gen_countydata(s3_client, athena_client, glue_client, database):
    sectors = ['com','res']
    # sectors = ['res']
    years = ['2024','2025','2030','2035','2040','2045','2050']
    turnovers = ['breakthrough','ineff','mid','high','stated']

    # years = ['2018','2019','2020','2021','2022','2023']
    # turnovers = ['ineffuncal']

    for sectorid in sectors:
        for yearid in years:
            for myturnover in turnovers:
                sql_to_s3table(s3_client, athena_client, glue_client, database, "tbl_ann_county.sql", sectorid, yearid, myturnover)
                sql_to_s3table(s3_client, athena_client, glue_client, database, "annual_county.sql", sectorid, yearid, myturnover)
                sql_to_s3table(s3_client, athena_client, glue_client, database, "tbl_hr_county.sql", sectorid, yearid, myturnover)
                sql_to_s3table(s3_client, athena_client, glue_client, database, "hourly_county.sql", sectorid, yearid, myturnover)

def bssbucket_insert(athena_client):
    query = f"""
    CREATE TABLE county_hourly_TURNOVERID_amy_SECTORID_YEARID
    WITH (
        external_location = 's3://bss-workflow/county_hourly/TURNOVERID/sector=SECTORID/year=YEARID/',
        format = 'PARQUET',
        write_compression = 'SNAPPY',
        partitioned_by = ARRAY['state']
    ) AS
    SELECT *
    FROM wide_county_hourly_TURNOVERID_amy
    WHERE sector = 'SECTORLONGID' AND year = YEARID;
    """
    turnovers = ['breakthrough','ineff','mid','high','stated']
    years = [2024,2025,2030,2035,2040,2045,2050]
    sectors = ['res','com']
    for myturnover in turnovers:
        for mysector in sectors:
            for myyear in years:
                mysectorlong = "Commercial" if mysector == 'com' else "Residential"
                query1 = query
                if "TURNOVERID" in query:
                    query1 = query1.replace("TURNOVERID", f"{myturnover}")
                if "SECTORID" in query:
                    query1 = query1.replace("SECTORID", f"{mysector}")
                if "YEARID" in query:
                    query1 = query1.replace("YEARID", f"{myyear}")
                if "SECTORLONGID" in query:
                    query1 = query1.replace("SECTORLONGID", f"{mysectorlong}")
                    print(f"Querying for {query1}")
                    execute_athena_query(athena_client, query1, False) 


def bssbucket_parquetmerge(s3, s3_bucket):

    turnovers = ['breakthrough','ineff','mid','high','stated']
    years = [2024,2025,2030,2035,2040,2045,2050]
    sectors = ['com','res']

    # turnovers = ['ineff']
    # years = [2024]
    # sectors = ['com']

    for myturnover in turnovers:
        for mysector in sectors:
            for myyear in years:

                top_level_folder = f'county_hourly/{myturnover}/sector={mysector}/year={myyear}/'

                response = s3.list_objects_v2(Bucket=s3_bucket, Prefix=top_level_folder)
                # files = response.get('Contents', [])
                files = list_all_objects(s3, s3_bucket, top_level_folder)

                if not files:
                    print("No files found in the folder.")
                    exit()

                state_folders = set()
                for file in files:
                    file_key = file['Key']
                    # Skip files directly in the top-level folder
                    relative_path = file_key[len(top_level_folder):]
                    if '/' in relative_path:
                        state_folder = relative_path.split('/')[0]
                        state_folders.add(state_folder)
                print(f"{myturnover} {mysector} {myyear}\n{state_folders}")

                for state in state_folders:
                    print(f"Processing folder: {state}")
                    state_folder_prefix = f"{top_level_folder}{state}/"
                    
                    # List all files in the state folder
                    state_files_response = s3.list_objects_v2(Bucket=s3_bucket, Prefix=state_folder_prefix)
                    state_files = state_files_response.get('Contents', [])

                    os.makedirs(f'temp_files/{state}', exist_ok=True)
                    local_files = []

                    for file in state_files:
                        file_key = file['Key']
                        if not file_key.endswith('/'):  # Skip subdirectories
                            # local_file_path = f'temp_files/{state}/{os.path.basename(file_key)}'
                            local_file_path = os.path.join("temp_files", state, os.path.basename(file_key))
                            s3.download_file(s3_bucket, file_key, local_file_path)
                            local_files.append(local_file_path)

                    df_list = []
                    for file_path in local_files:
                        try:
                            df = pd.read_parquet(file_path)
                            # print(f"Loaded Parquet file: {file_path}")
                        except Exception as parquet_err:
                            try:
                                # Fallback to reading as CSV
                                df = pd.read_csv(file_path)
                                # print(f"Loaded CSV file: {file_path}")
                            except Exception as csv_err:
                                print(f"Skipping file (not readable): {file_path}")
                                continue
                        df_list.append(df)

                    if df_list:
                        combined_df = pd.concat(df_list, ignore_index=True)
                        combined_file_path = f'{state}.parquet'
                        combined_df.to_parquet(combined_file_path, engine='pyarrow', index=False)

                        # Upload the combined file to S3
                        s3.upload_file(combined_file_path, s3_bucket, f'{top_level_folder}US states/{combined_file_path}')
                        print(f"Uploaded combined file for state: {state}")
                    
                    # # Step 6: Delete state folder and its contents from S3
                    # print(f"Deleting state folder: {state_folder_prefix}")
                    # for file in state_files:
                    #     s3.delete_object(Bucket=s3_bucket, Key=file['Key'])

                    for file in local_files:
                        os.remove(file)
                    os.rmdir(f'temp_files/{state}')
                    if df_list:
                        os.remove(combined_file_path)

                print("All state folders processed.")


def main(base_dir):

    if opts.create_json is True:
        convert_csv_folder_to_json('csv_raw', 'json/input.json')
    if opts.gen_mults is True:
        session = boto3.Session()
        s3_client = session.client('s3')
        athena_client = session.client('athena')

        gen_multipliers(s3_client, athena_client)

    if opts.gen_scoutdata is True:
        session = boto3.Session()
        s3_client = session.client('s3')
        athena_client = session.client('athena')

        gen_scoutdata(s3_client, athena_client)

    if opts.gen_county is True:
        session = boto3.Session()
        s3_client = session.client('s3')
        athena_client = session.client('athena')

        gen_countydata(s3_client, athena_client)

    if opts.convert_long_to_wide is True:
        session = boto3.Session()
        s3_client = session.client('s3')
        athena_client = session.client('athena')

        convert_long_to_wide(athena_client)

    if opts.convert_scout is True:
        session = boto3.Session()
        s3_client = session.client('s3')
        athena_client = session.client('athena')

        convert_long_to_wide_scout(s3_client, athena_client)
        # bssbucket_insert_scout(athena_client)

    if opts.gen_countyall is True:
        # Xinwei's notes
        # run 
        # 1. gen_countydata(s3_client, athena_client, glue_client, database)
        # 2. combine_countydata(athena_client)
        # 3. run_r_script('annual_graphs.R')
        # 4. get_csvs_for_R(athena_client)
        # 5. run_r_script('county and hourly graphs.R')

        session = boto3.Session()
        s3_client = session.client('s3')
        athena_client = session.client('athena')
        glue_client = session.client('glue') 
        database = 'euss_oedi'

        # gen_scoutdata(s3_client, athena_client)
        # gen_countydata(s3_client, athena_client, glue_client, database)
        # combine_countydata(athena_client)
        # convert_long_to_wide(athena_client)
        # test_county(athena_client)
        # run_r_script('annual_graphs.R')

        # get_csvs_for_R(athena_client)
        run_r_script('county and hourly graphs.R')

    if opts.bssbucket_insert is True:
        session = boto3.Session()
        s3_client = session.client('s3')
        athena_client = session.client('athena')

        bssbucket_insert(athena_client)
    
    if opts.bssbucket_parquetmerge is True:
        from urllib.parse import urlparse
        session = boto3.Session()
        athena_client = session.client('athena')
        s3 = boto3.client('s3')
        s3_bucket = 'bss-workflow'

        bssbucket_insert(athena_client)
        bssbucket_parquetmerge(s3, s3_bucket)

    if opts.run_test is True:
        session = boto3.Session()
        s3_client = session.client('s3')
        athena_client = session.client('athena')
        # check if the data is consistant
        test_county(athena_client)
        test_multipliers(athena_client)
        test_compare_measures(athena_client)
        
        run_r_script('annual_graphs.R')

        get_csvs_for_R(athena_client)
        run_r_script('county and hourly graphs.R')

        # athena (shere you query the datasets through glue within squl ) --> AWS - Glue (where we store in tables in csv, table of repersentation in a squl format)--> S3 (actual the dataset stored)

        
if __name__ == "__main__":

    start_time = time.time()
    parser = ArgumentParser()
    parser.add_argument("--create_json", action="store_true",
                        help=""""Create json/input.json from csv files.
                        Store CSV files in csv_raw/""")
    parser.add_argument("--gen_mults", action="store_true",
                        help=""""Generate Stock tables""")
    parser.add_argument("--gen_county", action="store_true",
                        help=""""Generate County Data""")
    parser.add_argument("--gen_countyall", action="store_true",
                        help=""""Generate County Data and post process""")
    parser.add_argument("--gen_scoutdata", action="store_true",
                        help=""""Generate Scout Data""")
    parser.add_argument("--convert_long_to_wide", action="store_true",
                        help=""""Convert datasets as necessary""")
    parser.add_argument("--convert_scout", action="store_true",
                        help=""""Convert datasets as necessary""")
    parser.add_argument("--bssbucket_insert", action="store_true",
                        help=""""Populate into bss-workflow""")
    parser.add_argument("--bssbucket_parquetmerge", action="store_true",
                        help=""""Populate into bss-workflow""")
    parser.add_argument("--run_test", action="store_true",
                        help=""""Run Diagnosis""")

    opts = parser.parse_args()
    base_dir = getcwd()
    main(base_dir)
    hours, rem = divmod(time.time() - start_time, 3600)
    minutes, seconds = divmod(rem, 60)
    print("--- Overall Runtime: %s (HH:MM:SS.mm) ---" %
          "{:0>2}:{:0>2}:{:05.2f}".format(int(hours), int(minutes), seconds))

