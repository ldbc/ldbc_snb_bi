import csv
import datetime
import os
import re
import psycopg2
import time
import sys

# Usage: bi.py [--test]

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

def convert_value_to_string(value, result_type, input):
    if result_type == "ID[]" or result_type == "INT[]" or result_type == "INT32[]" or result_type == "INT64[]":
        return value.replace("{", "[").replace("}", "]").replace(";", ",")
    elif result_type == "ID" or result_type == "INT" or result_type == "INT32" or result_type == "INT64":
        return str(int(value))
    elif result_type == "FLOAT" or result_type == "FLOAT32" or result_type == "FLOAT64":
        return str(float(value))
    elif result_type == "STRING[]":
        return "[" + ";".join([f'"{v}"' for v in value]) + "]"
    elif result_type == "STRING":
        return f'"{value}"'
    elif result_type == "DATETIME":
        return f"{datetime.datetime.strftime(value, '%Y-%m-%dT%H:%M:%S.%f')[:-3]}+00:00"
    elif result_type == "DATE":
        return datetime.datetime.strftime(value, '%Y-%m-%d')
    elif result_type == "BOOL":
        return str(bool(value))
    else:
        raise ValueError(f"Result type {result_type} not found")

def run_query(con, query_num, query_spec, query_parameters):
    start = time.time()
    cur = con.cursor()

    for key in query_parameters.keys():
        query_spec = query_spec.replace(f":{key}", query_parameters[key])

    cur.execute(query_spec)
    results = cur.fetchall()
    end = time.time()
    duration = end - start

    mapping = result_mapping[query_num]
    result_tuples = "[" + ";".join([
            f'<{",".join([convert_value_to_string(result[i], type, False) for i, type in enumerate(mapping)])}>'
            for result in results
        ]) + "]"
    print("-> " + result_tuples)

    if test:
        print(f"-> {duration:.4f} seconds")
        print(f"-> {results}")
    return (results, duration)

def convert_to_datetime(timestamp):
    dt = datetime.datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%S.%f+00:00")
    return f"'{dt}'::timestamp"

def convert_to_date(timestamp):
    dt = datetime.datetime.strptime(timestamp, '%Y-%m-%d')
    return f"'{dt}'::date"

sf = os.environ.get("SF")
test = False
if len(sys.argv) > 1:
    if sys.argv[1] == "--test":
        test = True

results_file = open(f'output/results.csv', 'w')
timings_file = open(f'output/timings.csv', 'w')
timings_file.write(f"sf|q|time\n")

con = psycopg2.connect(host="localhost", port=8000, user="postgres", password="mysecretpassword")

for query_variant in ["1", "2a", "2b", "3", "4", "5", "6", "7", "8a", "8b", "9", "10a", "10b", "11", "12", "13", "14a", "14b", "15a", "15b", "16a", "16b", "17", "18"]: #, "19a", "19b", "20"
    query_num = int(re.sub("[^0-9]", "", query_variant))
    query_subvariant = re.sub("[^ab]", "", query_variant)

    print(f"========================= Q {query_num:02d}{query_subvariant.rjust(1)} =========================")
    query_file = open(f'queries/bi-{query_num}.sql', 'r')
    query_spec = query_file.read()

    parameters_csv = csv.DictReader(open(f'../parameters/bi-{query_variant}.csv'), delimiter='|')

    i = 0
    for query_parameters in parameters_csv:
        i = i + 1
        # convert fields based on type designators
        query_parameters = {k: f"{v}::bigint"         if re.match('.*:(ID|LONG)', k)       else v for k, v in query_parameters.items()}
        query_parameters = {k: convert_to_date(v)     if re.match('.*:DATE$', k)           else v for k, v in query_parameters.items()}
        query_parameters = {k: convert_to_datetime(v) if re.match('.*:DATETIME', k)        else v for k, v in query_parameters.items()}
        query_parameters = {k: f"'{v}'"               if re.match('.*:STRING([^[]|$)', k)  else v for k, v in query_parameters.items()}
        query_parameters = {k:
            "("
            + ', '.join([f"'{e}'" for e in v.split(';') ])
            + ")"
            if re.findall('\[\]$', k) else v for k, v in query_parameters.items()}
        # drop type designators
        type_pattern = re.compile(':.*')
        query_parameters = {type_pattern.sub('', k): v for k, v in query_parameters.items()}
        (results, duration) = run_query(con, query_num, query_spec, query_parameters)

        timings_file.write(f"{sf}|{query_variant}|{duration}\n")
        timings_file.flush()
        #results_file.write(f"{query_num}|{query_variant}|{query_parameters_in_order}|{results}\n")
        results_file.flush()

        # test run: 1 query, regular run: 10 queries
        if test or i == 10:
            break

results_file.close()
timings_file.close()

con.close()
