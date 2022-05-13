import argparse
from pathlib import Path
import time
import csv
import requests
import re
from datetime import datetime, timedelta
import os

# query timeout value in miliseconds
HEADERS = {'GSQL-TIMEOUT': '36000000'}

# ================ BEGIN: Variables and Functions from Cypher ========================
result_mapping = {
     1: ["INT32", "BOOL", "INT32", "INT32", "FLOAT32", "INT32", "FLOAT32"],
     2: ["STRING", "INT32", "INT32", "INT32"],
     3: ["ID", "STRING", "DATETIME", "ID", "INT32"],
     4: ["ID", "STRING", "STRING", "DATETIME", "INT32"],
     5: ["ID", "INT32", "INT32", "INT32", "INT32"],
     6: ["ID", "INT32"],
     7: ["STRING", "INT32"],
     8: ["ID", "INT32", "INT32"],
     9: ["ID", "STRING", "STRING", "INT32", "INT32"],
    10: ["ID", "STRING", "INT32"],
    11: ["INT64"],
    12: ["INT32", "INT32"],
    13: ["ID", "INT32", "INT32", "FLOAT32"],
    14: ["ID", "ID", "STRING", "INT32"],
    15: ["FLOAT32"],
    16: ["ID", "INT32", "INT32"],
    17: ["ID", "INT32"],
    18: ["ID", "ID", "INT32"],
    19: ["ID", "ID", "FLOAT32"],
    20: ["ID", "INT64"],
}

def convert_value_to_string(value, type):
    if type == "ID[]" or type == "INT[]" or type == "INT32[]" or type == "INT64[]":
        return "[" + ",".join([str(int(x)) for x in value]) + "]"
    elif type == "ID" or type == "INT" or type == "INT32" or type == "INT64":
        return str(int(value))
    elif type == "FLOAT" or type == "FLOAT32" or type == "FLOAT64":
        return str(float(value))
    elif type == "STRING[]":
        return "[" + ";".join([f'"{v}"' for v in value]) + "]"
    elif type in ["STRING"]:
        return f'"{value}"'
    elif type in [ "DATETIME", "DATE"]:
        return value.replace(" ", "T")
    elif type == "BOOL":
        return str(bool(value))
    else:
        raise ValueError(f"Result type {type} not found")

def cast_parameter_to_driver_input(value, type):
    if type == "ID[]" or type == "INT[]" or type == "INT32[]" or type == "INT64[]":
        return [int(x) for x in value.split(";")]
    elif type == "ID" or type == "INT" or type == "INT32" or type == "INT64":
        return int(value)
    elif type == "STRING[]":
        return value.split(";")
    elif type in ["STRING", "DATETIME", "DATE"]:
        return value
    else:
        raise ValueError(f"Parameter type {type} not found")
# ================ END: Variables and Functions from Cypher ========================

def run_query(endpoint, query_num, parameters):
    start = time.time()
    if query_num == 15:
        requests.get(f'{endpoint}/query/ldbc_snb/bi15precompute', headers=HEADERS, params=parameters)
    response = requests.get(f'{endpoint}/query/ldbc_snb/bi{query_num}', headers=HEADERS, params=parameters).json()
    if query_num == 15:
        requests.get(f'{endpoint}/query/ldbc_snb/bi15cleanup', headers=HEADERS)

    end = time.time()
    if response['error']:
        print(response['message'])
        return '<>', 0
    results, duration = response['results'][0]['result'], end - start
    # for BI-11 and BI-15, result is a single value
    if isinstance(results, int) or isinstance(results, float):
        return f"[<{results}>]", duration
    
    #convert results from [dict()] to [[]] 
    results = [[v for k,v in res.items()] for res in results]
    #convert results to string
    mapping = result_mapping[query_num]
    results = "[" + ";".join([
        f'<{",".join([convert_value_to_string(result[i], type) for i, type in enumerate(mapping)])}>'
        for result in results
    ]) + "]"
    return results, duration

def precompute(query_num, endpoint):
    print(f"Precomputing weights for Q{query_num}")
    start = time.time()
    response = requests.get(f'{endpoint}/query/ldbc_snb/bi{query_num}precompute', headers=HEADERS).json()
    return time.time() - start

def cleanup(query_num, endpoint):
    print(f"Cleaning weights for Q{query_num}")
    start = time.time()
    response = requests.get(f'{endpoint}/query/ldbc_snb/bi{query_num}cleanup', headers=HEADERS).json()
    return time.time() - start

def run_queries(query_variants, results_file, timings_file, args):
    sf = os.environ.get("SF")
    start = time.time()
    for query_variant in query_variants:
        print(f"========================= Q{query_variant} =========================")
        query_num = int(re.sub("[^0-9]", "", query_variant))
        parameters_csv = csv.DictReader(open(args.para / f'bi-{query_variant}.csv'), delimiter='|')
        parameters = [{"name": t[0], "type": t[1]} for t in [f.split(":") for f in parameters_csv.fieldnames]]

        for i,query_parameters in enumerate(parameters_csv):
            query_parameters_split = {k.split(":")[0]: v for k, v in query_parameters.items()}
            query_parameters_in_order = f'<{";".join([query_parameters_split[parameter["name"]] for parameter in parameters])}>'
            query_parameters = {k.split(":")[0]: cast_parameter_to_driver_input(v, k.split(":")[1]) for k, v in query_parameters.items()}
            # Q1 parameter name is conflict with TG data type keyword 'datetime' 
            if query_num == 1: query_parameters = {'date': query_parameters['datetime']}

            results, duration = run_query(args.endpoint, query_num, query_parameters)

            results_file.write(f"{query_num}|{query_variant}|{query_parameters_in_order}|{results}\n")
            results_file.flush()
            timings_file.write(f"TigerGraph|{sf}|{query_variant}|{query_parameters_in_order}|{duration:.6f}\n")
            timings_file.flush()
            # test run: 1 query, regular run: 10 queries
            if args.test or i == args.nruns-1:
                break
    return time.time() - start

# main functions
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='BI query driver')
    parser.add_argument('--para', type=Path, default=Path('../parameters'), help='parameter folder')
    parser.add_argument('--skip', action='store_true', help='skip precompute')
    parser.add_argument('--test', action='store_true', help='test mode only run one time')
    parser.add_argument('--nruns', '-n', type=int, default=10, help='number of runs')
    parser.add_argument('--endpoint', type=str, default='http://127.0.0.1:9000',help='tigergraph endpoints')
    args = parser.parse_args()
    
    output = Path('output')
    output.mkdir(exist_ok=True)
    results_file = open(output/'results.csv', 'w')
    timings_file = open(output/'timings.csv', 'w')
    timings_file.write(f"tool|sf|q|parameters|time\n")
    query_variants = ["1", "2a", "2b", "3", "4", "5", "6", "7", "8a", "8b", "9", "10a", "10b", "11", "12", "13", "14a", "14b", "15a", "15b", "16a", "16b", "17", "18", "19a", "19b", "20"]

    sf = os.environ.get("SF")
    # precomputation for BI 4, 6, 19, 20
    query_nums = [int(re.sub("[^0-9]", "", query_variant)) for query_variant in query_variants]
    for query_num in [4,6,19,20]:
        if not args.skip and query_num in query_nums:
            timings_file.write(f"TigerGraph|{sf}|bi{query_num}precompute|{precompute(query_num, args.endpoint):.6f}\n")
            timings_file.flush()
    
    run_queries(query_variants, results_file, timings_file, args)
    # cleanup for 19 and 20
    for query_num in [19,20]:
        if not args.skip and query_num in query_nums:
            timings_file.write(f"TigerGraph|{sf}|bi{query_num}cleanup|{cleanup(query_num, args.endpoint):.6f}\n")
            timings_file.flush()
    
    results_file.close()
    timings_file.close()
