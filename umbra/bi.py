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


def cast_parameter_to_driver_input(value, parameter_type):
    if parameter_type == "INT" or parameter_type == "INT32":
        return value
    elif parameter_type == "ID" or parameter_type == "INT64":
        return f"{value}::bigint"
    elif parameter_type == "STRING[]":
        return "(" + ', '.join([f"'{e}'" for e in value.split(';') ]) + ")"
    elif parameter_type == "STRING":
        return f"'{value}'"
    elif parameter_type == "DATETIME":
        return convert_to_datetime(value)
    elif parameter_type == "DATE":
        return convert_to_date(value)
    else:
        raise ValueError(f"Parameter type {parameter_type} not found")


def run_query(pg_con, query_num, query_spec, query_parameters):
    for key in query_parameters.keys():
        query_spec = query_spec.replace(f":{key}", query_parameters[key])

    cur = pg_con.cursor()
    start = time.time()
    cur.execute(query_spec)
    results = cur.fetchall()
    end = time.time()
    duration = end - start

    mapping = result_mapping[query_num]
    result_tuples = "[" + ";".join([
            f'<{",".join([convert_value_to_string(result[i], type, False) for i, type in enumerate(mapping)])}>'
            for result in results
        ]) + "]"

    if test:
        print(f"-> {duration:.4f} seconds")
        print(f"-> {result_tuples}")
    return (result_tuples, duration)


def convert_to_datetime(timestamp):
    dt = datetime.datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%S.%f+00:00")
    return f"'{dt}'::timestamp"


def convert_to_date(timestamp):
    dt = datetime.datetime.strptime(timestamp, '%Y-%m-%d')
    return f"'{dt}'::date"


sf = os.environ.get("SF")
test = False
pgtuning = False
if len(sys.argv) > 1:
    if sys.argv[1] == "--test":
        test = True
    if sys.argv[1] == "--pgtuning":
        pgtuning = True


results_file = open(f'output/results.csv', 'w')
timings_file = open(f'output/timings.csv', 'w')
timings_file.write(f"sf|q|parameters|time\n")

pg_con = psycopg2.connect(host="localhost", user="postgres", password="mysecretpassword", port=8000)

for query_variant in ["1", "2a", "2b", "3", "4", "5", "6", "7", "8a", "8b", "9", "10a", "10b", "11", "12", "13", "14a", "14b", "16a", "16b", "17", "18", "15b"]: #, "15a"
    query_num = int(re.sub("[^0-9]", "", query_variant))
    query_subvariant = re.sub("[^ab]", "", query_variant)

    print(f"========================= Q {query_num:02d}{query_subvariant.rjust(1)} =========================")
    query_file = open(f'queries/bi-{query_num}.sql', 'r')
    query_spec = query_file.read()

    parameters_csv = csv.DictReader(open(f'../parameters/bi-{query_variant}.csv'), delimiter='|')
    parameters = [{"name": t[0], "type": t[1]} for t in [f.split(":") for f in parameters_csv.fieldnames]]

    i = 0
    for query_parameters in parameters_csv:
        i = i + 1

        query_parameters_converted = {k.split(":")[0]: cast_parameter_to_driver_input(v, k.split(":")[1]) for k, v in query_parameters.items()}

        query_parameters_split = {k.split(":")[0]: v for k, v in query_parameters.items()}
        query_parameters_in_order = f'<{";".join([query_parameters_split[parameter["name"]] for parameter in parameters])}>'

        (results, duration) = run_query(pg_con, query_num, query_spec, query_parameters_converted)

        timings_file.write(f"{sf}|{query_variant}|{query_parameters_in_order}|{duration}\n")
        timings_file.flush()
        results_file.write(f"{query_num}|{query_variant}|{query_parameters_in_order}|{results}\n")
        results_file.flush()

        # - test run: 1 query
        # - regular run: 10 queries
        # - paramgen tuning: 50 queries
        if (test) or (not pgtuning and i == 10) or (pgtuning and i == 100):
            break

results_file.close()
timings_file.close()

pg_con.close()
