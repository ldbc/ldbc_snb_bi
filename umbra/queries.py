import csv
import datetime
import json
import os
import re
import psycopg2
import time
import sys
from pathlib import Path

# Usage: queries.py [--test]

result_mapping = {
     1: [{"name": "year", "type": "INT32"}, {"name": "isComment", "type": "BOOL"}, {"name": "lengthCategory", "type": "INT32"}, {"name": "messageCount", "type": "INT32"}, {"name": "averageMessageLength", "type": "FLOAT32"}, {"name": "sumMessageLength", "type": "INT32"}, {"name": "percentageOfMessages", "type": "FLOAT32"}],
     2: [{"name": "tag.name", "type": "STRING"}, {"name": "countWindow1", "type": "INT32"}, {"name": "countWindow2", "type": "INT32"}, {"name": "diff", "type": "INT32"}],
     3: [{"name": "forum.id", "type": "ID"}, {"name": "forum.title", "type": "STRING"}, {"name": "forum.creationDate", "type": "DATETIME"}, {"name": "person.id", "type": "ID"}, {"name": "messageCount", "type": "INT32"}],
     4: [{"name": "person.id", "type": "ID"}, {"name": "person.firstName", "type": "STRING"}, {"name": "person.lastName", "type": "STRING"}, {"name": "person.creationDate", "type": "DATETIME"}, {"name": "messageCount", "type": "INT32"}],
     5: [{"name": "person.id", "type": "ID"}, {"name": "replyCount", "type": "INT32"}, {"name": "likeCount", "type": "INT32"}, {"name": "messageCount", "type": "INT32"}, {"name": "score", "type": "INT32"}],
     6: [{"name": "person1.id", "type": "ID"}, {"name": "authorityScore", "type": "INT32"}],
     7: [{"name": "relatedTag.name", "type": "STRING"}, {"name": "count", "type": "INT32"}],
     8: [{"name": "person.id", "type": "ID"}, {"name": "score", "type": "INT32"}, {"name": "friendsScore", "type": "INT32"}],
     9: [{"name": "person.id", "type": "ID"}, {"name": "person.firstName", "type": "STRING"}, {"name": "person.lastName", "type": "STRING"}, {"name": "threadCount", "type": "INT32"}, {"name": "messageCount", "type": "INT32"}],
    10: [{"name": "expertCandidatePerson.id", "type": "ID"}, {"name": "tag.name", "type": "STRING"}, {"name": "messageCount", "type": "INT32"}],
    11: [{"name": "count", "type": "INT64"}],
    12: [{"name": "messageCount", "type": "INT32"}, {"name": "personCount", "type": "INT32"}],
    13: [{"name": "zombie.id", "type": "ID"}, {"name": "zombieLikeCount", "type": "INT32"}, {"name": "totalLikeCount", "type": "INT32"}, {"name": "zombieScore", "type": "FLOAT32"}],
    14: [{"name": "person1.id", "type": "ID"}, {"name": "person2.id", "type": "ID"}, {"name": "city1.name", "type": "STRING"}, {"name": "score", "type": "INT32"}],
    15: [{"name": "weight", "type": "FLOAT32"}],
    16: [{"name": "person.id", "type": "ID"}, {"name": "messageCountA", "type": "INT32"}, {"name": "messageCountB", "type": "INT32"}],
    17: [{"name": "person1.id", "type": "ID"}, {"name": "messageCount", "type": "INT32"}],
    18: [{"name": "person1.id", "type": "ID"}, {"name": "person2.id", "type": "ID"}, {"name": "mutualFriendCount", "type": "INT32"}],
    19: [{"name": "person1.id", "type": "ID"}, {"name": "person2.id", "type": "ID"}, {"name": "totalWeight", "type": "FLOAT32"}],
    20: [{"name": "person1.id", "type": "ID"}, {"name": "totalWeight", "type": "INT64"}],
}


def convert_value_to_string(value, result_type):
    if result_type == "ID[]" or result_type == "INT[]" or result_type == "INT32[]" or result_type == "INT64[]":
        return [int(x) for x in value.replace("{", "").replace("}", "").split(";")]
    elif result_type == "ID" or result_type == "INT" or result_type == "INT32" or result_type == "INT64":
        return int(value)
    elif result_type == "FLOAT" or result_type == "FLOAT32" or result_type == "FLOAT64":
        return float(value)
    elif result_type == "STRING[]":
        return [x.replace('"') for x in value.replace("{", "").replace("}", "").split(";")]
    elif result_type == "STRING":
        return value
    elif result_type == "DATETIME":
        return f"{datetime.datetime.strftime(value, '%Y-%m-%dT%H:%M:%S.%f')[:-3]}+00:00"
    elif result_type == "DATE":
        return datetime.datetime.strftime(value, '%Y-%m-%d')
    elif result_type == "BOOL":
        return bool(value)
    else:
        raise ValueError(f"Result type {result_type} not found")


def escape_apostrophes(s):
    return s.replace("'", "''")


def convert_to_datetime(timestamp):
    dt = datetime.datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%S.%f+00:00")
    return f"'{dt}'::timestamp"


def convert_to_date(timestamp):
    dt = datetime.datetime.strptime(timestamp, '%Y-%m-%d')
    return f"'{dt}'::date"


def cast_parameter_to_driver_input(value, parameter_type):
    if parameter_type == "INT" or parameter_type == "INT32":
        return value
    elif parameter_type == "ID" or parameter_type == "INT64":
        return f"{value}::bigint"
    elif parameter_type == "STRING[]":
        return "(" + ', '.join([f"'{e}'" for e in value.split(';') ]) + ")"
    elif parameter_type == "STRING":
        return f"'{escape_apostrophes(value)}'"
    elif parameter_type == "DATETIME":
        return convert_to_datetime(value)
    elif parameter_type == "DATE":
        return convert_to_date(value)
    else:
        raise ValueError(f"Parameter type {parameter_type} not found")


def run_script(pg_con, cur, filename):
    with open(filename, "r") as f:
        queries_file = f.read()
        # strip comments
        queries_file = re.sub(r"\n--.*", "", queries_file)
        queries = queries_file.split(";")
        for query in queries:
            if query.isspace():
                continue

            sql_statement = re.findall(r"^((CREATE|INSERT|DROP|DELETE|SELECT|COPY|UPDATE|ALTER) [A-Za-z0-9_ ]*)", query, re.MULTILINE)
            print(f"{sql_statement[0][0].strip()} ...")
            start = time.time()
            cur.execute(query)
            pg_con.commit()
            end = time.time()
            duration = end - start
            print(f"-> {duration:.4f} seconds")

def run_query(pg_con, query_num, query_variant, query_spec, query_parameters, test):
    if test:
        print(f'Q{query_variant}: {query_parameters}')

    for key in query_parameters.keys():
        query_spec = query_spec.replace(f":{key}", query_parameters[key])

    cur = pg_con.cursor()
    start = time.time()
    cur.execute(query_spec)
    results = cur.fetchall()
    end = time.time()
    duration = end - start

    mapping = result_mapping[query_num]
    result_tuples = [
            {
                result_descriptor["name"]: convert_value_to_string(result[i], result_descriptor["type"])
                for i, result_descriptor in enumerate(mapping)
            }
            for result in results
        ]

    if test:
        print(f"-> {duration:.4f} seconds")
        print(f"-> {result_tuples}")
    return (json.dumps(result_tuples), duration)

param_dir_env = os.environ.get("UMBRA_PARAM_DIR")

def run_precomputations(query_variants, pg_con, cur, batch_id, sf, timings_file):
    if "4" in query_variants:
        start = time.time()
        run_script(pg_con, cur, "dml/precomp/bi-4.sql")
        end = time.time()
        timings_file.write(f"Umbra|{sf}|{batch_id}|q4precomputation||{end-start}\n")
    if "6" in query_variants:
        start = time.time()
        run_script(pg_con, cur, "dml/precomp/bi-6.sql")
        end = time.time()
        timings_file.write(f"Umbra|{sf}|{batch_id}|q6precomputation||{end-start}\n")
    if "19a" in query_variants or "19b" in query_variants:
        start = time.time()
        run_script(pg_con, cur, "dml/precomp/bi-19.sql")
        end = time.time()
        timings_file.write(f"Umbra|{sf}|{batch_id}|q19precomputation||{end-start}\n")
    if "20a" in query_variants or "20b" in query_variants:
        start = time.time()
        run_script(pg_con, cur, "dml/precomp/bi-20.sql")
        end = time.time()
        timings_file.write(f"Umbra|{sf}|{batch_id}|q20precomputation||{end-start}\n")

def run_queries(query_variants, pg_con, sf, test, pgtuning, batch_id, timings_file, results_file):
    param_dir = param_dir_env
    if param_dir is None:
        param_dir = f"../parameters/parameters-sf{sf}"
    start = time.time()

    for query_variant in query_variants:
        query_num = int(re.sub("[^0-9]", "", query_variant))
        query_subvariant = re.sub("[^ab]", "", query_variant)

        print(f"========================= Q {query_num:02d}{query_subvariant.rjust(1)} =========================")
        query_file = open(f'queries/bi-{query_num}.sql', 'r')
        query_spec = query_file.read()
        query_file.close()

        parameters_csv = csv.DictReader(open(f'{param_dir}/bi-{query_variant}.csv'), delimiter='|')
        parameters = [{"name": t[0], "type": t[1]} for t in [f.split(":") for f in parameters_csv.fieldnames]]

        i = 0
        for query_parameters in parameters_csv:
            i = i + 1

            query_parameters_converted = {k.split(":")[0]: cast_parameter_to_driver_input(v, k.split(":")[1]) for k, v in query_parameters.items()}

            query_parameters_split = {k.split(":")[0]: v for k, v in query_parameters.items()}
            query_parameters_in_order = json.dumps(query_parameters_split)

            (results, duration) = run_query(pg_con, query_num, query_variant, query_spec, query_parameters_converted, test)

            timings_file.write(f"Umbra|{sf}|{batch_id}|{query_variant}|{query_parameters_in_order}|{duration}\n")
            timings_file.flush()
            results_file.write(f"{query_num}|{query_variant}|{query_parameters_in_order}|{results}\n")
            results_file.flush()

            # - test run: 1 query
            # - regular run: 10 queries
            # - paramgen tuning: 50 queries
            if (test) or (not pgtuning and i == 10) or (pgtuning and i == 100):
                break

    return time.time() - start


if __name__ == '__main__':
    sf = os.environ.get("SF")
    if sf is None:
        print("${SF} environment variable must be set")
        exit(1)
    test = False
    pgtuning = False
    if len(sys.argv) > 1:
        if sys.argv[1] == "--test":
            test = True
        if sys.argv[1] == "--pgtuning":
            pgtuning = True

    query_variants = ["1", "2a", "2b", "3", "4", "5", "6", "7", "8a", "8b", "9", "10a", "10b", "11", "12", "13", "14a", "14b", "15a", "15b", "16a", "16b", "17", "18", "19a", "19b", "20a", "20b"]

    output = Path(f'output/output-sf{sf}')
    output.mkdir(parents=True, exist_ok=True)
    open(f"output/output-sf{sf}/results.csv", "w").close()
    open(f"output/output-sf{sf}/timings.csv", "w").close()

    timings_file = open(f"output/output-sf{sf}/timings.csv", "a")
    timings_file.write(f"tool|sf|day|q|parameters|time\n")
    results_file = open(f"output/output-sf{sf}/results.csv", "a")

    pg_con = psycopg2.connect(host="localhost", user="postgres", password="mysecretpassword", port=8000)
    pg_con.autocommit = True
    
    reads_time = run_queries(query_variants, pg_con, sf, test, pgtuning, None, timings_file, results_file)
    timings_file.write(f"Umbra|{sf}||reads||{reads_time:.6f}\n")

    pg_con.close()
