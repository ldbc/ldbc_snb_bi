import datetime
import time
import re
import json
import sys
sys.path.append('../common')
from result_mapping import result_mapping


def convert_value_to_string(value, result_type, input):
    if result_type == "ID[]" or result_type == "INT[]" or result_type == "INT32[]" or result_type == "INT64[]":
        return [int(x) for x in value]
    elif result_type == "ID" or result_type == "INT" or result_type == "INT32" or result_type == "INT64":
        return int(value)
    elif result_type == "FLOAT" or result_type == "FLOAT32" or result_type == "FLOAT64":
        return float(value)
    elif result_type == "STRING[]":
        return value
    elif result_type == "STRING":
        return value
    elif result_type == "DATETIME":
        if input:
            return f"{datetime.datetime.strftime(value, '%Y-%m-%dT%H:%M:%S.%f')[:-3]}+00:00"
        else:
            return f"{datetime.datetime.strftime(value.to_native(), '%Y-%m-%dT%H:%M:%S.%f')[:-3]}+00:00"
    elif result_type == "DATE":
        if input:
            return datetime.datetime.strftime(value, '%Y-%m-%d')
        else:
            return datetime.datetime.strftime(value.to_native(), '%Y-%m-%d')
    elif result_type == "BOOL":
        return bool(value)
    else:
        raise ValueError(f"Result type {result_type} not found")

def cast_parameter_to_driver_input(value, parameter_type):
    if parameter_type == "ID[]" or parameter_type == "INT[]" or parameter_type == "INT32[]" or parameter_type == "INT64[]":
        return [int(x) for x in value.split(";")]
    elif parameter_type == "ID" or parameter_type == "INT" or parameter_type == "INT32" or parameter_type == "INT64":
        return int(value)
    elif parameter_type == "STRING[]":
        return value.split(";")
    elif parameter_type == "STRING":
        return value
    elif parameter_type == "DATETIME":
        dt = datetime.datetime.strptime(value, '%Y-%m-%dT%H:%M:%S.%f+00:00')
        return datetime.datetime(dt.year, dt.month, dt.day, dt.hour, dt.minute, dt.second, dt.microsecond*1000, tzinfo=datetime.timezone.utc)
    elif parameter_type == "DATE":
        dt = datetime.datetime.strptime(value, '%Y-%m-%d')
        return datetime.datetime(dt.year, dt.month, dt.day, tzinfo=datetime.timezone.utc)
    else:
        raise ValueError(f"Parameter type {parameter_type} not found")

def read_query_fun(tx, query_num, query_spec, query_parameters):
    results = tx.run(query_spec, query_parameters)
    mapping = result_mapping[query_num]
    result_tuples = [
            {
                result_descriptor["name"]: convert_value_to_string(result[i], result_descriptor["type"], False)
                for i, result_descriptor in enumerate(mapping)
            }
            for result in results
        ]

    return json.dumps(result_tuples)


def write_query_fun(tx, query_spec):
    tx.run(query_spec, {})


def run_query(session, query_num, query_variant, query_spec, query_parameters, test):
    if test:
        print(f'Q{query_variant}: {query_parameters}')

    start = time.time()
    results = session.write_transaction(read_query_fun, query_num, query_spec, query_parameters)
    end = time.time()
    duration = end - start
    if test:
        print(f"-> {duration:.4f} seconds")
        print(f"-> {results}")
    return (results, duration)


def run_queries(query_variants, parameter_csvs, session, sf, batch_id, batch_type, test, pgtuning, timings_file, results_file):
    start = time.time()

    for query_variant in query_variants:
        query_num = int(re.sub("[^0-9]", "", query_variant))
        query_subvariant = re.sub("[^ab]", "", query_variant)

        print(f"========================= Q {query_num:02d}{query_subvariant.rjust(1)} =========================")
        query_file = open(f'queries/bi-{query_num}.cypher', 'r')
        query_spec = query_file.read()
        query_file.close()

        parameters_csv = parameter_csvs[query_variant]

        i = 0
        for query_parameters in parameters_csv:
            i = i + 1

            query_parameters_converted = {k.split(":")[0]: cast_parameter_to_driver_input(v, k.split(":")[1]) for k, v in query_parameters.items()}

            query_parameters_split = {k.split(":")[0]: v for k, v in query_parameters.items()}
            query_parameters_in_order = json.dumps(query_parameters_split)

            (results, duration) = run_query(session, query_num, query_variant, query_spec, query_parameters_converted, test)

            timings_file.write(f"Neo4j|{sf}|{batch_id}|{batch_type}|{query_variant}|{query_parameters_in_order}|{duration}\n")
            timings_file.flush()
            results_file.write(f"{query_num}|{query_variant}|{query_parameters_in_order}|{results}\n")
            results_file.flush()

            # - test run: 1 query
            # - regular run: 30 queries
            # - paramgen tuning: 100 queries
            if (test) or (not pgtuning and i == 30) or (pgtuning and i == 100):
                break

    return time.time() - start


def run_precomputations(sf, query_variants, session, batch_date, batch_type, timings_file):
    if "19a" in query_variants or "19b" in query_variants:
        start = time.time()
        print("Creating graph (precomputing weights) for Q19")
        session.write_transaction(write_query_fun, open(f'queries/bi-19-drop-graph.cypher', 'r').read())
        session.write_transaction(write_query_fun, open(f'queries/bi-19-create-graph.cypher', 'r').read())
        end = time.time()
        duration = end - start
        timings_file.write(f"Neo4j|{sf}|{batch_date}|{batch_type}|q19precomputation||{duration}\n")

    if "20a" in query_variants or "20b" in query_variants:
        start = time.time()
        print("Creating graph (precomputing weights) for Q20")
        session.write_transaction(write_query_fun, open(f'queries/bi-20-drop-graph.cypher', 'r').read())
        session.write_transaction(write_query_fun, open(f'queries/bi-20-create-graph.cypher', 'r').read())
        end = time.time()
        duration = end - start
        timings_file.write(f"Neo4j|{sf}|{batch_date}|{batch_type}|q20precomputation||{duration}\n")
