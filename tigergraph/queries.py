import argparse
from pathlib import Path
import time
import csv
import requests
import re
from datetime import datetime, timedelta
import os

parser = argparse.ArgumentParser(description='BI query driver')
parser.add_argument('--test', action='store_true', help='test mode only run one time')
parser.add_argument('--endpoint', type=str, default='http://127.0.0.1:9000',help='tigergraph endpoints')
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

def run_query(query_num, parameters):
    start = time.time()
    response = requests.get(f'{args.endpoint}/query/ldbc_snb/bi{query_num}', 
        headers={'GSQL-TIMEOUT': '36000000'}, 
        params=parameters).json()
    end = time.time()
    if response['error']:
        print(response['message'])
        return '<>', 0    
    results, duration = response['results'][0]['result'], end - start
    if isinstance(results, int):
        return results, duration
    
    #convert results from [dict()] to [[]] 
    results = [[v for k,v in res.items()] for res in results]
    #convert results to string
    mapping = result_mapping[query_num]
    results = "[" + ";".join([
        f'<{",".join([convert_value_to_string(result[i], type) for i, type in enumerate(mapping)])}>'
        for result in results
    ]) + "]"
    return results, duration

sf = os.environ.get("SF")
res = Path('output')
res.mkdir(exist_ok = True)
results_file = open(res / 'results.csv', 'w')
timings_file = open(res / 'timings.csv', 'w')
timings_file.write(f"sf|q|parameters|time\n")
query_variants = ["1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13", "14", "15", "16", "17", "18", "19", "20"]
for query_variant in query_variants:
    print(f"========================= Q{query_variant} =========================")
    query_num = int(re.sub("[^0-9]", "", query_variant))
    parameters_csv = csv.DictReader(open(f'../parameters/bi-{query_variant}.csv'), delimiter='|')
    parameters = [{"name": t[0], "type": t[1]} for t in [f.split(":") for f in parameters_csv.fieldnames]]
    
    for i,query_parameters in enumerate(parameters_csv):
        query_parameters_split = {k.split(":")[0]: v for k, v in query_parameters.items()}
        query_parameters_in_order = f'<{";".join([query_parameters_split[parameter["name"]] for parameter in parameters])}>'

        query_parameters = {k.split(":")[0]: cast_parameter_to_driver_input(v, k.split(":")[1]) for k, v in query_parameters.items()}
        if query_num == 1: query_parameters = {'date': query_parameters['datetime']}
        results, duration = run_query(query_num, query_parameters)
        if query_num == 11: results = f"[<{results}>]"
        results_file.write(f"{query_num}|{query_variant}|{query_parameters_in_order}|{results}\n")
        results_file.flush()
        timings_file.write(f"{sf}|{query_variant}|{query_parameters_in_order}|{duration}\n")
        timings_file.flush()
        # test run: 1 query, regular run: 10 queries
        if args.test or i == 9:
            break
        