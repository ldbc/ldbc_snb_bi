import argparse
from pathlib import Path
import time
import csv
import requests
import re
from datetime import datetime, timedelta

parser = argparse.ArgumentParser(description='BI query driver')
parser_validate = parser.add_argument('mode', type=str, choices=['benchmark', 'validate'], help='mode of the driver')
#parser_validate.add_argument('--nrun', type=int, help='number of runs')
args = parser.parse_args()

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
    15: ["ID[]", "FLOAT32"],
    16: ["ID", "INT32", "INT32"],
    17: ["ID", "INT32"],
    18: ["ID", "INT32"],
    19: ["ID", "ID", "FLOAT32"],
    20: ["ID", "INT64"],
}

def convert_value_to_string(value, type):
    if type == "ID[]" or type == "INT[]" or type == "INT32[]" or type == "INT64[]":
        return ";".join([str(int(x)) for x in value])
    elif type == "ID" or type == "INT" or type == "INT32" or type == "INT64":
        return str(int(value))
    elif type == "FLOAT" or type == "FLOAT32" or type == "FLOAT64":
        return str(float(value))
    elif type == "STRING[]":
        return "[" + ";".join([f'"{v}"' for v in value]) + "]"
    elif type in ["STRING", "DATETIME", "DATE"]:
        return f'"{value}"'
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

def run_query(name, parameters):
    ENDPOINT = 'http://127.0.0.1:9000/query/ldbc_snb/'
    HEADERS = {'GSQL-TIMEOUT': '36000000'}
    start = time.time()
    if name=='bi19': requests.get(ENDPOINT + f'bi19_add_weighted_edges', headers=HEADERS, params=parameters) 
    response = requests.get(ENDPOINT + name, headers=HEADERS, params=parameters).json()
    if name=='bi19': requests.get(ENDPOINT + f'bi19_delete_weighted_edges', headers=HEADERS) 
    end = time.time()
    duration = end - start
    return response['results'][0]['result'], duration
    
res = Path('results')
res.mkdir(exist_ok = True)
if args.mode == 'validate':
    res_file = res / 'validation_params.csv'
elif args.mode == 'benchmark':
    res_file = res / 'results.csv'
if res_file.exists(): res_file.unlink()
fout = open(res_file, 'a')

for query_variant in ["1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13", "14", "15", "16", "17", "18", "19", "20"]:
    print(f"========================= Q{query_variant} =========================")
    query_num = int(re.sub("[^0-9]", "", query_variant))
    parameters_csv = csv.DictReader(open(f'../parameters/bi-{query_variant}.csv'), delimiter='|')
    parameters = [{"name": t[0], "type": t[1]} for t in [f.split(":") for f in parameters_csv.fieldnames]]
    
    for query_parameters in parameters_csv:
        query_parameters = {k.split(":")[0]: cast_parameter_to_driver_input(v, k.split(":")[1]) for k, v in query_parameters.items()}
        query_parameters_in_order = f'<{";".join([convert_value_to_string(query_parameters[parameter["name"]], parameter["type"]) for parameter in parameters])}>'
        if query_num == 1: query_parameters = {'date': query_parameters['datetime']}
        results, duration = run_query(f'bi{query_num}', query_parameters)
        mapping = result_mapping[query_num]
        if query_num != 11:
            #convert results from [dict()] to [[]] 
            results = [[v for k,v in res.items()] for res in results]
            #convert results to string
            results = "[" + ";".join([
                f'<{",".join([convert_value_to_string(result[i], type) for i, type in enumerate(mapping)])}>'
                for result in results
            ]) + "]"
        print(f"{query_num}|{query_variant}|{query_parameters_in_order}|{results}")
        