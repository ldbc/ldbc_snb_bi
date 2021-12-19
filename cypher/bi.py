from neo4j import GraphDatabase, time
from datetime import datetime
from neo4j.time import DateTime, Date
import time
import pytz
import csv
import re

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

def convert_result(value, type):
    if type == "ID[]" or type == "INT[]" or type == "INT32[]" or type == "INT64[]":
        return value.split(";") # todo parse list
    elif type == "BOOL":
        return bool(value)
    elif type == "FLOAT" or type == "FLOAT32" or type == "FLOAT64":
        return float(value)
    elif type == "ID" or type == "INT" or type == "INT32" or type == "INT64":
        return int(value)
    elif type == "STRING[]":
        return value.split(";")
    elif type == "STRING":
        return value
    elif type == "DATETIME":
        return f"{datetime.strftime(value.to_native(), '%Y-%m-%dT%H:%M:%S.%f')[:-3]}+00:00"
    elif type == "DATE":
        return datetime.strftime(value.to_native(), '%Y-%m-%d')
    else:
        raise ValueError("type not found")

def convert_parameter(value, type):
    #print(f"converting {value} to type {type}")

    if type == "ID[]" or type == "INT[]" or type == "INT32[]" or type == "INT64[]":
        return [int(x) for x in value.split(";")]
    elif type == "ID" or type == "INT" or type == "INT32" or type == "INT64":
        return int(value)
    elif type == "STRING[]":
        return value.split(";")
    elif type == "STRING":
        return value
    elif type == "DATETIME":
        dt = datetime.strptime(value, '%Y-%m-%dT%H:%M:%S.%f+00:00')
        return DateTime(dt.year, dt.month, dt.day, dt.hour, dt.minute, dt.second, dt.microsecond*1000)
    elif type == "DATE":
        dt = datetime.strptime(value, '%Y-%m-%d')
        return Date(dt.year, dt.month, dt.day)
    else:
        raise ValueError("type not found")

#@unit_of_work(timeout=300)
def query_fun(tx, query_num, query_spec, query_parameters):
    results = tx.run(query_spec, query_parameters)
    mapping = result_mapping[query_num]
    result_tuples = [
        [convert_result(result[i], type) for i, type in enumerate(mapping)]
        for result in results
    ]
    print(f"{len(result_tuples)} results:")
    for result_tuple in result_tuples:
        print(f"- {result_tuple}")

def run_query(session, query_num, query_id, query_spec, query_parameters):
    print(f'Q{query_id}: {query_parameters}')
    start = time.time()
    results = session.read_transaction(query_fun, query_num, query_spec, query_parameters)
    end = time.time()
    duration = end - start
    #print("Q{}: {:.4f} seconds, {} tuples".format(query_id, duration, results[0]))


driver = GraphDatabase.driver("bolt://localhost:7687")

with driver.session() as session:
    for query_variant in ["1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13", "14a", "14b", "15", "16", "17", "18", "19", "20"]:
    #for query_variant in ["14a"]:
        print(f"========================= Q{query_variant} =========================")
        query_num = int(re.sub("[^0-9]", "", query_variant))
        query_file = open(f'queries/bi-{query_num}.cypher', 'r')
        query_spec = query_file.read()

        parameters_csv = csv.DictReader(open(f'../parameters/bi-{query_variant}.csv'), delimiter='|')
        parameter_types = {t[0]: t[1] for t in [f.split(":") for f in parameters_csv.fieldnames]}
        parameter_names = [k.split(":")[0] for k in parameters_csv.fieldnames]

        for query_parameters in parameters_csv:
            # convert parameter values based on type designators
            #print(f"raw parameters: {query_parameters}")
            query_parameters = {k: convert_parameter(v, k.split(":")[1]) for k, v in query_parameters.items()}

            query_parameters = {k.split(":")[0]: v for k, v in query_parameters.items()}
            #print(f"converted parameters: {query_parameters}")

            run_query(session, query_num, query_variant, query_spec, query_parameters)


driver.close()
