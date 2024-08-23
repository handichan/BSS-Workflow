import boto3
import time
import pandas as pd
import os
import json
from os import getcwd
from argparse import ArgumentParser
from io import StringIO

JSON_PATH = 'json/input.json'
SQL_DIR = "sql"
CSV_DIR = "csv"
OUTPUT_DIR = "agg_results"
EXTERNAL_S3_DIR = "datasets"
DATABASE_NAME = "euss_oedi"
BUCKET_NAME = 'handibucket'


MEAS_MAP_FILE = f"{CSV_DIR}/meas/2024-06-21 res hvac measures.tsv"
SCOUT_RESULTS_FILE = f"scout_results/06-18/exogenous.json"

SCOUT_RUN_DATE = "2024-06-18"


US_STATES = [
    'AL', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'DC', 'FL', 'GA',
    'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MD', 'MA',
    'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NH', 'NJ', 'NM', 'NV',
    'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC', 'SD',
    'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY']

END_USES = ['Refrigeration', 'Cooling (Equip.)', 'Heating (Equip.)',
            'Other']

# # US_STATES = ['AL','AZ']
# US_STATES = [
#     'AR', 'CA', 'CO', 'CT', 'DE', 'DC', 'FL', 'GA',
#     'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MD', 'MA',
#     'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NH', 'NJ', 'NM', 'NV',
#     'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC', 'SD',
#     'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY']


def get_var_char_values(data_dict):
    return [obj['VarCharValue'] for obj in data_dict['Data']]


def wait_for_query_to_complete(client, query_execution_id):
    status = 'RUNNING'
    max_attempts = 360
    while max_attempts > 0:
        max_attempts -= 1
        query_status = client.get_query_execution(
            QueryExecutionId=query_execution_id)
        status = query_status['QueryExecution']['Status']['State']
        print(f"Query status: {status}, Attempts left: {max_attempts}")
        if status in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
            return status, query_status
        time.sleep(5)

def preprocess_file(file_path, delimiter='\t'):  ### NOT USED ###
    # clean up a TSV file
    processed_lines = []
    with open(file_path, 'r') as file:
        for i, line in enumerate(file, start=1):
            stripped_line = line.strip()
            fields = stripped_line.split(delimiter)
            if i == 1:
                num_columns = len(fields)
            if len(fields) == num_columns:
                processed_lines.append(line)
            else:
                print(f"Skipping line {i} due to incorrect number of fields: {line}")

    processed_file_path = file_path.replace('.tsv', '_processed.tsv').replace('.csv', '_processed.csv')
    with open(processed_file_path, 'w') as processed_file:
        processed_file.writelines(processed_lines)
    
    return processed_file_path

def s3_create_tables_from_csvdir(s3_client, athena_client):
    for file_name in os.listdir(CSV_DIR):
        local_path = os.path.join(CSV_DIR, file_name)
        file_no_ext = os.path.splitext(file_name)[0]
        if os.path.isfile(local_path):
            s3_path = f"{EXTERNAL_S3_DIR}/{file_no_ext}/{file_name}"
            upload_file_to_s3(s3_client, local_path, BUCKET_NAME, s3_path)

            # Determine the delimiter based on the file extension
            if file_name.endswith('.csv'):
                delimiter = ','
            elif file_name.endswith('.tsv'):
                delimiter = '\t'
            else:
                raise ValueError("Unsupported file format. Please provide a .csv or .tsv file.")
            
            # processed_file_path = preprocess_file(local_path, delimiter)
            # df = pd.read_csv(processed_file_path, delimiter=delimiter)
            df = pd.read_csv(local_path, delimiter=delimiter)

            sql_query = sql_create_table(df, file_no_ext)
            _, _ = execute_athena_query(athena_client, sql_query, True)


def df_to_s3table(s3_client, athena_client, df, table_name):
    file_name = f"{table_name}.csv"
    local_path = os.path.join(OUTPUT_DIR, file_name)
    df.to_csv(f"{OUTPUT_DIR}/{file_name}", index=False)
    if os.path.isfile(local_path):
        s3_path = f"{EXTERNAL_S3_DIR}/{table_name}/{file_name}"
        upload_file_to_s3(s3_client, local_path, BUCKET_NAME, s3_path)
        sql_query = sql_create_table(df, table_name)
        _, _ = execute_athena_query(athena_client, sql_query, True)    
    

def sql_create_table(df, table_name):
    columns_sql = ',\n'.join([f"`{col}` STRING" for col in df.columns])
    sql_str = f"""
    CREATE EXTERNAL TABLE IF NOT EXISTS {table_name} (
        {columns_sql}
    )
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ','
    LOCATION 's3://{BUCKET_NAME}/{EXTERNAL_S3_DIR}/{table_name}/'
    TBLPROPERTIES ('skip.header.line.count'='1');
    """
    return sql_str


def upload_file_to_s3(client, local_path, bucket, s3_path):
    client.upload_file(local_path, bucket, s3_path)
    print(f"""UPLOADED {os.path.basename(local_path)} 
          to s3://{bucket}/{s3_path}""")


def convert_json_to_csv_folder():
    if not os.path.exists(CSV_DIR):
        os.makedirs(CSV_DIR)
    with open(JSON_PATH, 'r') as json_file:
        data = json.load(json_file)
    for key in data:
        df = pd.DataFrame(data[key])
        csv_file_path = os.path.join(CSV_DIR, f"{key}.csv")
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


def execute_athena_query(client, query, is_create, wait=True):
    response = client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={'Database': DATABASE_NAME},
        ResultConfiguration={'OutputLocation': f"s3://{BUCKET_NAME}/configs/"}
    )
    query_execution_id = response['QueryExecutionId']

    if not wait:
        return query_execution_id, None

    status, query_status = wait_for_query_to_complete(
        client, query_execution_id)
    if status in ['FAILED', 'CANCELLED']:
        print(query_status['QueryExecution']['Status'].
              get('StateChangeReason', 'Unknown failure reason'))
        return False, None

    if status == "SUCCEEDED":
        result_loc = query_status['QueryExecution'][
            'ResultConfiguration']['OutputLocation']
        print(f"SQL query succeeded and results are stored in {result_loc}")
        return result_loc, fetch_query_results(
            client, query_execution_id) if not is_create else None


def sql_query_execute_csvout(s3_client, athena_client, query):
    s3_location, query_results = execute_athena_query(
        athena_client, query, False)
    if query_results:
        s3_path = s3_location.replace('s3://', '')
        s3_object_key = '/'.join(s3_path.split('/')[1:])
        s3_filename = s3_path.split('/')[-1]
        print(f"{s3_path}\n{s3_object_key}\n{s3_filename}")

        response = s3_client.get_object(Bucket=BUCKET_NAME, Key=s3_object_key)
        csv_string = response['Body'].read().decode('utf-8')
        df = pd.read_csv(StringIO(csv_string))
        print(f"Query results stored: {s3_location}")
        return df
    elif s3_location:
        print(f"""Query completed but no results.
              Results path: {s3_location}""")
    else:
        print("Query failed or was cancelled.")



def sql_to_csv(s3_client, athena_client, sql_file):
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)
    query = read_sql_file(sql_file)
    fname = os.path.splitext(sql_file)[0]
    if "STATEID" in query:
        for uss in US_STATES:
            query1 = query.replace("STATEID", f"{uss}")
            df = sql_query_execute_csvout(s3_client, athena_client, query1)
            if df is not None:
                df.to_csv(f"{OUTPUT_DIR}/{fname}_{uss}.csv", index=False)
    else:
        df = sql_query_execute_csvout(s3_client, athena_client, query)
        if df is not None:
            df.to_csv(f"{OUTPUT_DIR}/{fname}.csv", index=False)

def s3table_to_csv(s3_client, athena_client, s3table, stateid):
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)
    stateid = f"_{stateid}" if stateid == "STATEID" else ""
    query = f"""
    SELECT * FROM {s3table}{stateid};
    """
    fname = s3table
    if "STATEID" in query:
        for uss in US_STATES:
            query1 = query.replace("STATEID", f"{uss}")
            df = sql_query_execute_csvout(s3_client, athena_client, query1)
            if df is not None:
                df.to_csv(f"{OUTPUT_DIR}/{fname}_{uss}.csv", index=False)
                print(f"{fname}_{uss}.csv is successfully saved!")
    else:
        df = sql_query_execute_csvout(s3_client, athena_client, query)
        if df is not None:
            df.to_csv(f"{OUTPUT_DIR}/{fname}.csv", index=False)
            print(f"{fname}.csv is successfully saved!")

def sql_to_s3table(s3_client, athena_client, sql_file):
    query = read_sql_file(sql_file)
    if "STATEID" in query:
        for uss in US_STATES:
            query1 = query.replace("STATEID", f"{uss}")
            execute_athena_query(athena_client, query1, False)
    else:
        execute_athena_query(athena_client, query, False)
    print(f"Queried {sql_file}")


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


def scout_to_df(filename):
    new_columns = [
            'meas', 'adoption_scn', 'metric',
            'reg', 'bldg_type', 'end_use',
            'fuel', 'year', 'value',
            'sector']
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
        all_df['sector'] = 'res' if mea.find('(R)') >= 0 else 'com'
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


def calc_annual(df, include_baseline = False):
    turnover = 'ex'
    efficient = df[df['metric'].isin(['Efficient Energy Use (MMBtu)',
        'Efficient Energy Use, Measure (MMBtu)'])]
    grouped = efficient.groupby(['meas','metric','reg',
        'end_use','fuel','year','sector'])[
        'value'].sum().reset_index()
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
            'fuel','year','sector'])['value'].sum().reset_index().assign(
            state_ann_kwh = lambda x: x.value / 3412*10**6)
        grouped_base['tech_stage'] = 'original_ann'
        grouped_base['turnover'] = 'baseline'
        to_return = pd.concat([to_return,grouped_base[['meas','reg',
            'end_use','fuel','year','tech_stage','state_ann_kwh','turnover']]])
        to_return['scout_run'] = SCOUT_RUN_DATE
    to_return.to_csv(f"{OUTPUT_DIR}/annual_state.tsv", sep='\t', index = False)
    return(to_return)


def _todf(file):
    return pd.read_csv(file)


def file_to_df(file_path):
    # Check the file extension
    if file_path.endswith('.tsv'):
        df = pd.read_csv(file_path, sep='\t')
    elif file_path.endswith('.csv'):
        df = pd.read_csv(file_path)
    else:
        raise ValueError("Unsupported file format. Please provide a .tsv or .csv file.")
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

def main(base_dir):
    if opts.create_json is True:
        convert_csv_folder_to_json('csv_raw', 'json/input.json')
    if opts.gen_stocks is True:
        session = boto3.Session()
        s3_client = session.client('s3')
        athena_client = session.client('athena')

        s3_create_tables_from_csvdir(s3_client, athena_client)

        sql_to_s3table(s3_client, athena_client, "tbl_hr_mult.sql")
        sql_to_s3table(s3_client, athena_client, "res_hourly_shares_cooling.sql")
        sql_to_s3table(s3_client, athena_client, "res_hourly_shares_heating.sql")
        sql_to_s3table(s3_client, athena_client, "res_hourly_shares_refrig.sql")
        sql_to_s3table(s3_client, athena_client, "res_hourly_shares_poolpump.sql")
        sql_to_s3table(s3_client, athena_client, "res_hourly_shares_wh.sql")

        sql_to_s3table(s3_client, athena_client, "res_hourly_shares_lighting.sql")
        sql_to_s3table(s3_client, athena_client, "res_hourly_shares_dw.sql")
        sql_to_s3table(s3_client, athena_client, "res_hourly_shares_cw.sql")
        sql_to_s3table(s3_client, athena_client, "res_hourly_shares_cook.sql")
        sql_to_s3table(s3_client, athena_client, "res_hourly_shares_dry.sql")
        sql_to_s3table(s3_client, athena_client, "res_hourly_shares_fanspumps.sql")
        sql_to_s3table(s3_client, athena_client, "res_hourly_shares_misc.sql") # remove hot_tub_heater and hot_tub_pump'

    
        sql_to_s3table(s3_client, athena_client, "tbl_ann_mult.sql")
        sql_to_s3table(s3_client, athena_client, "res_ann_shares_cook.sql")
        sql_to_s3table(s3_client, athena_client, "res_ann_shares_cw.sql")
        sql_to_s3table(s3_client, athena_client, "res_ann_shares_deliveredheat.sql")
        sql_to_s3table(s3_client, athena_client, "res_ann_shares_dry.sql")
        sql_to_s3table(s3_client, athena_client, "res_ann_shares_dw.sql")
        sql_to_s3table(s3_client, athena_client, "res_ann_shares_fanspumps.sql")
        sql_to_s3table(s3_client, athena_client, "res_ann_shares_hvac.sql")
        sql_to_s3table(s3_client, athena_client, "res_ann_shares_lighting.sql")
        sql_to_s3table(s3_client, athena_client, "res_ann_shares_misc.sql") #remove hot_tub_heater and hot_tub_pump'      
        sql_to_s3table(s3_client, athena_client, "res_ann_shares_poolpump.sql")
        sql_to_s3table(s3_client, athena_client, "res_ann_shares_refrig.sql")
        sql_to_s3table(s3_client, athena_client, "res_ann_shares_wh.sql")

    if opts.gen_county is True:
        session = boto3.Session()
        s3_client = session.client('s3')
        athena_client = session.client('athena')

        # measure_map = file_to_df(MEAS_MAP_FILE)
        # scout_df = scout_to_df(SCOUT_RESULTS_FILE)

        # annual_state_scout = calc_annual(scout_df, include_baseline=True)
        # check_missing_meas(measure_map, annual_state_scout)
        
        # df_to_s3table(s3_client, athena_client, measure_map, "measure_map")
        # df_to_s3table(s3_client, athena_client, annual_state_scout, "annual_state_scout")

        # change table name to Scout's results description (date_exo)
        # sql_to_s3table(s3_client, athena_client, "tbl_ann_county.sql")
        sql_to_s3table(s3_client, athena_client, "annual_county.sql")
        
        # sql_to_s3table(s3_client, athena_client, "tbl_hr_county.sql")
        # sql_to_s3table(s3_client, athena_client, "hourly_county.sql") # need to be fixed
        
if __name__ == "__main__":
    start_time = time.time()
    parser = ArgumentParser()
    parser.add_argument("--create_json", action="store_true",
                        help=""""Create json/input.json from csv files.
                        Store CSV files in csv_raw/""")
    parser.add_argument("--gen_stocks", action="store_true",
                        help=""""Generate Stock tables""")
    parser.add_argument("--gen_county", action="store_true",
                        help=""""Generate County Hourly""")

    opts = parser.parse_args()
    base_dir = getcwd()
    main(base_dir)
    hours, rem = divmod(time.time() - start_time, 3600)
    minutes, seconds = divmod(rem, 60)
    print("--- Overall Runtime: %s (HH:MM:SS.mm) ---" %
          "{:0>2}:{:0>2}:{:05.2f}".format(int(hours), int(minutes), seconds))
