from neo4j import GraphDatabase
from neo4j.time import DateTime, Date
import datetime
import time
import csv
import re

# TODO: provide two modes of operation: validation and benchmark
# validation CSV header: Query ID|Query variant|Parameters|Results
# benchmark CSV header:  Query ID|Query variant|Response time

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
    elif type == "STRING":
        return f'"{value}"'
    elif type == "DATETIME":
        return f"{datetime.datetime.strftime(value, '%Y-%m-%dT%H:%M:%S.%f')[:-3]}+00:00"
    elif type == "DATE":
        return datetime.datetime.strftime(value, '%Y-%m-%d')
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
    elif type == "STRING":
        return value
    elif type == "DATETIME":
        dt = datetime.datetime.strptime(value, '%Y-%m-%dT%H:%M:%S.%f+00:00')
        return datetime.datetime(dt.year, dt.month, dt.day, dt.hour, dt.minute, dt.second, dt.microsecond*1000, tzinfo=datetime.timezone.utc)
    elif type == "DATE":
        dt = datetime.datetime.strptime(value, '%Y-%m-%d')
        return datetime.datetime(dt.year, dt.month, dt.day, tzinfo=datetime.timezone.utc)
    else:
        raise ValueError(f"Parameter type {type} not found")

def query_fun(tx, query_num, query_spec, query_parameters):
    results = tx.run(query_spec, query_parameters)
    mapping = result_mapping[query_num]
    result_tuples = "[" + ";".join([
            f'<{",".join([convert_value_to_string(result[i], type) for i, type in enumerate(mapping)])}>'
            for result in results
        ]) + "]"
    return result_tuples

def run_query(session, query_num, query_id, query_spec, query_parameters):
    print(f'Q{query_id}: {query_parameters}')
    start = time.time()
    results = session.read_transaction(query_fun, query_num, query_spec, query_parameters)
    end = time.time()
    duration = end - start
    #print("Q{}: {:.4f} seconds, {} tuples".format(query_id, duration, results[0]))
    return results


driver = GraphDatabase.driver("bolt://localhost:7687")

result_file = open(f'output/validation_params.csv', 'w')

session = driver.session()
for query_variant in ["1", "2a", "2b", "3", "4", "5", "6", "7", "8a", "8b", "9", "10a", "10b", "11", "12", "13", "14a", "14b", "15a", "15b", "16a", "16b", "17", "18", "19a", "19b", "20"]:
#for query_variant in ["14a"]:
    print(f"========================= Q{query_variant} =========================")
    query_num = int(re.sub("[^0-9]", "", query_variant))
    query_file = open(f'queries/bi-{query_num}.cypher', 'r')
    query_spec = query_file.read()

    parameters_csv = csv.DictReader(open(f'../parameters/bi-{query_variant}.csv'), delimiter='|')
    parameters = [{"name": t[0], "type": t[1]} for t in [f.split(":") for f in parameters_csv.fieldnames]]
    #parameter_names = [k.split(":")[0] for k in parameters_csv.fieldnames]

    for query_parameters in parameters_csv:
        query_parameters = {k.split(":")[0]: cast_parameter_to_driver_input(v, k.split(":")[1]) for k, v in query_parameters.items()}
        query_parameters_in_order = f'<{";".join([convert_value_to_string(query_parameters[parameter["name"]], parameter["type"]) for parameter in parameters])}>'

        results = run_query(session, query_num, query_variant, query_spec, query_parameters)
        print(f"{query_num}|{query_variant}|{query_parameters_in_order}|{results}")

    query_file.close()

session.close()
driver.close()
