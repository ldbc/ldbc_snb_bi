import datetime
import json
import os
import re
import time
import sys
sys.path.append('../common')
from result_mapping import result_mapping

def load_mht(cur, csvpath):
    cur.execute("BEGIN BULK WRITE;")
    cur.execute(f"INSERT INTO Message_hasTag_Tag SELECT a, b, c FROM UMBRA.CSVVIEW('{csvpath}', 'DELIMITER \"|\", HEADER, NULL \"\", FORMAT text', 'a timestamp with time zone NOT NULL, b bigint NOT NULL, c bigint NOT NULL')")
    cur.execute("COMMIT;")

def load_plm(cur, csvpath):
    cur.execute("BEGIN BULK WRITE;")
    cur.execute(f"INSERT INTO Person_likes_Message SELECT a, b, c FROM UMBRA.CSVVIEW('{csvpath}', 'DELIMITER \"|\", HEADER, NULL \"\", FORMAT text', 'a timestamp with time zone NOT NULL, b bigint NOT NULL, c bigint NOT NULL')")
    cur.execute("COMMIT;")

def load_post(cur, csvpath):
    cur.execute("BEGIN BULK WRITE;")
    cur.execute(f"""
INSERT INTO Message
SELECT
    creationDate,
    id AS MessageId,
    id AS RootPostId,
    language AS RootPostLanguage,
    content,
    imageFile,
    locationIP,
    browserUsed,
    length,
    CreatorPersonId,
    ContainerForumId,
    LocationCountryId,
    NULL::bigint AS ParentMessageId
FROM UMBRA.CSVVIEW(
    '{csvpath}',
    'DELIMITER "|", HEADER, NULL "", FORMAT text',
    'creationDate timestamp with time zone NOT NULL, id bigint NOT NULL, imageFile text, locationIP text NOT NULL, browserUsed text NOT NULL, language text, content text, length int NOT NULL, CreatorPersonId bigint NOT NULL, ContainerForumId bigint NOT NULL, LocationCountryId bigint NOT NULL'
    )
""")
    cur.execute("COMMIT;")

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
            if query == "" or query.isspace():
                continue

            sql_statement = re.findall(r"^((CREATE|INSERT|DROP|DELETE|SELECT|COPY|UPDATE|ALTER) [A-Za-z0-9_ ]*)", query, re.MULTILINE)
            is_update = True if re.match(r"^((CREATE|INSERT|DROP|DELETE|COPY|UPDATE|ALTER) [A-Za-z0-9_ ]*)", sql_statement[0][0], re.MULTILINE) else False

            print(f"{sql_statement[0][0].strip()} ...")
            start = time.time()
            if is_update:
                cur.execute("BEGIN BULK WRITE;")
            cur.execute(query)
            cur.execute("COMMIT;")
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
    cur.execute("COMMIT;")
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

def run_precomputations(query_variants, pg_con, cur, batch_id, batch_type, sf, timings_file):
    if "4" in query_variants:
        start = time.time()
        run_script(pg_con, cur, "dml/precomp/bi-4.sql")
        end = time.time()
        timings_file.write(f"Umbra|{sf}|{batch_id}|{batch_type}|q4precomputation||{end-start}\n")
    if "6" in query_variants:
        start = time.time()
        run_script(pg_con, cur, "dml/precomp/bi-6.sql")
        end = time.time()
        timings_file.write(f"Umbra|{sf}|{batch_id}|{batch_type}|q6precomputation||{end-start}\n")
    if "19a" in query_variants or "19b" in query_variants:
        start = time.time()
        run_script(pg_con, cur, "dml/precomp/bi-19.sql")
        end = time.time()
        timings_file.write(f"Umbra|{sf}|{batch_id}|{batch_type}|q19precomputation||{end-start}\n")
    if "20a" in query_variants or "20b" in query_variants:
        start = time.time()
        run_script(pg_con, cur, "dml/precomp/bi-20.sql")
        end = time.time()
        timings_file.write(f"Umbra|{sf}|{batch_id}|{batch_type}|q20precomputation||{end-start}\n")

def run_queries(query_variants, parameter_csvs, pg_con, sf, test, pgtuning, batch_id, batch_type, timings_file, results_file):
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

        parameters_csv = parameter_csvs[query_variant]

        i = 0
        for query_parameters in parameters_csv:
            i = i + 1

            query_parameters_converted = {k.split(":")[0]: cast_parameter_to_driver_input(v, k.split(":")[1]) for k, v in query_parameters.items()}

            query_parameters_split = {k.split(":")[0]: v for k, v in query_parameters.items()}
            query_parameters_in_order = json.dumps(query_parameters_split)

            (results, duration) = run_query(pg_con, query_num, query_variant, query_spec, query_parameters_converted, test)

            timings_file.write(f"Umbra|{sf}|{batch_id}|{batch_type}|{query_variant}|{query_parameters_in_order}|{duration}\n")
            timings_file.flush()
            results_file.write(f"{query_num}|{query_variant}|{query_parameters_in_order}|{results}\n")
            results_file.flush()

            # - test run: 1 query
            # - regular run: 30 queries
            # - paramgen tuning: 100 queries
            if (test) or (not pgtuning and i == 30) or (pgtuning and i == 100):
                break

    return time.time() - start
