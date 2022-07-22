import sys
import os
import duckdb
import getopt
import logging
import timeit
from datetime import date
import multiprocessing
import csv

lane_limits = [16, 32, 64, 128, 256, 512, 1024]


def sort_results(result, timing_dict, params, query, sf, subquery, workload):
    output_file = open(f'output/results-{sf}-{subquery}-{workload}.csv', 'w')
    timing_output = open(f'output/timings-{sf}-{subquery}-{workload}.csv', 'w')
    timing_output.write(f"sf|q|parameters|time\n")

    filtered_param_headers = extract_headers(params)
    for idx, param in enumerate(params[1:]):
        param = param.strip().split("|")
        r = [r for r in result if str(r[-2]) == param[0] and str(r[-1]) == param[-1]]
        formatted_parameters = f"<{';'.join(str(parameter) for parameter in param)}>"
        formatted_output = []
        for entry in r:
            formatted_entry = f"<{','.join(str(result) for result in entry[:len(entry) - len(filtered_param_headers)])}>"
            formatted_output.append(formatted_entry)
        formatted_output_string = f"[{';'.join(entry for entry in formatted_output)}]"
        output_file.write(
            f"{query}|{subquery}|{formatted_parameters}|{formatted_output_string}\n")
        full_timing = timing_dict['precompute_timing'] + timing_dict['csr_timing'] + timing_dict['parameter_timing'][
            idx] + timing_dict['result_timing'] + (timing_dict['path_timing'] / len(timing_dict['parameter_timing']))

        timing_output.write(f"DuckDB|{sf}|{subquery}|{formatted_parameters}|{full_timing}\n")
    output_file.close()
    timing_output.close()


def extract_headers(params):
    param_headers = params[0]
    param_headers_formatted = param_headers.split("|")
    filtered_param_headers = [param.split(":")[0].lower() for param in param_headers_formatted]
    return filtered_param_headers


def run_script(con, filename, params=None, sf=None, lane=1024, thread=8):
    queries = open(filename).read().split(";")
    csr_timing = 0
    precompute_timing = 0
    parameter_timing = []
    total_timing = 0
    timing = 0
    path_timing = 0
    other_timing = 0

    graph_stats = {'vertices': 0, 'edges': 0, 'paths': 0}
    for query in queries:
        logging.debug(query)
        if "-- PRECOMPUTE" in query:
            start = timeit.default_timer()
            con.execute(query)
            stop = timeit.default_timer()
            timing = stop - start
            precompute_timing += timing
        elif "-- CSR CREATION" in query:
            start = timeit.default_timer()
            con.execute(query)
            stop = timeit.default_timer()
            timing = stop - start
            csr_timing += timing
        elif "-- PARAMS" in query:
            original_query = query
            filtered_param_headers = extract_headers(params)
            for line in params[1:]:
                custom_query = original_query
                line = line.strip("\n")
                split_line = line.split("|")
                for i in range(len(filtered_param_headers)):
                    custom_query = custom_query.replace(f":{filtered_param_headers[i]}", f"{split_line[i]}")
                logging.debug(f"Starting {line}")
                start = timeit.default_timer()

                con.execute(custom_query)
                stop = timeit.default_timer()

                timing = stop - start
                total_timing += timing

                parameter_timing.append(timing)
            timing = 0
        elif "-- NUMPATHS" in query:
            start = timeit.default_timer()
            graph_stats['paths'] = con.execute(query).fetchone()[0]
            stop = timeit.default_timer()
            timing = (stop - start)
            other_timing += timing
        elif "-- NUMVERTICESEDGES" in query:
            start = timeit.default_timer()
            graph_stats['vertices'], graph_stats['edges'] = con.execute(query).fetchall()[0]
            stop = timeit.default_timer()
            timing = (stop - start)
            other_timing += timing
        elif "-- RESULTS" in query:
            start = timeit.default_timer()
            final_result = con.execute(query).fetchall()
            stop = timeit.default_timer()
            timing = (stop - start)
            result_timing = timing
            total_timing += timing
            timing_dict = {"precompute_timing": precompute_timing, "csr_timing": csr_timing,
                           "average_parameter_timing": sum(parameter_timing) / len(parameter_timing),
                           "result_timing": result_timing, "total_timing": total_timing,
                           "parameter_timing": parameter_timing, "path_timing": path_timing,
                           "other_timing": other_timing}
            return final_result, timing_dict, graph_stats
        elif "-- DEBUG" in query:
            result = con.execute(query).fetchdf()
            logging.debug(result)
        elif "-- PRAGMA" in query:
            if "set_lane_limit" in query:
                query = query.replace(":param", str(lane))
            elif "threads" in query:
                query = query.replace(":param", str(thread))
            logging.debug(query)
            start = timeit.default_timer()
            con.execute(query)
            stop = timeit.default_timer()
            timing = (stop - start)
            other_timing += timing
        elif "-- PATH" in query:
            start = timeit.default_timer()
            con.execute(query)
            stop = timeit.default_timer()
            timing = stop - start
            path_timing += timing
        else:
            start = timeit.default_timer()
            con.execute(query)
            stop = timeit.default_timer()
            timing = stop - start
            other_timing += timing
        if timing == 0:
            logging.critical(f"TIMING WAS 0 for query: {query}")
        total_timing += timing
        logging.debug(total_timing)


def process_arguments(argv):
    sf = ''
    query = ''
    only_load = False
    workload = ''
    threads = list(range(2, multiprocessing.cpu_count() + 1, 2))
    lanes = None
    experimental_mode = False
    file_format = 'parquet'
    try:
        opts, args = getopt.getopt(argv, "hs:q:l:w:a:t:e:f:",
                                   ["scalefactor=", "query=", "load=", "workload=", "lanes=", "threads=",
                                    "experiment=", "format="])
    except getopt.GetoptError:
        logging.info(
            'load.py -s <scalefactor> -q <query> -l <load only> -w <workload> -a <lanes> -t <threads>, -e <experimental> -f <file format>')
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            logging.info(
                'load.py -s <scalefactor> -q <query> -l <load only database (1 or 0)> '
                '-w <workload (bi or interactive)> -a <number of lanes> -t <number of threads>')
            sys.exit()
        elif opt in ('-l', "--load"):
            only_load = bool(int(arg))
            if only_load:
                logging.info("Loading only the database")
        elif opt in ('-w', '--workload'):
            if arg == 'bi' or arg == 'interactive':
                workload = arg
            else:
                logging.critical("Unrecognized workload detected. Options are (bi, interactive)")
                sys.exit()
        elif opt in ("-s", "--scalefactor"):
            sf = arg
        elif opt in ("-a", "--lanes"):
            lanes = int(arg)
        elif opt in ("-t", "--threads"):
            threads = int(arg)
        elif opt in ("-q", "--query"):
            query = arg
        elif opt in ("-e", "--experiment"):
            experimental_mode = bool(int(arg))
        elif opt in ("-f", "--format"):
            file_format = arg
    return sf, query, only_load, workload, lanes, threads, experimental_mode, file_format


def write_timing_dict(timing_dict, sf, query, workload, graph_stats, lane=1024, thread=8):
    filename = f'benchmark/timings.csv'
    fieldnames = ["sf", "query", "total_timing", "result_timing", "csr_timing", "precompute_timing",
                  "average_parameter_timing", "total_parameter_timing", "path_timing", "other_timing", "lanes",
                  "threads",
                  "vertices", "edges", "paths", "workload", "date"]

    with open(filename, 'a', newline='\n') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        if os.path.getsize(filename) == 0:
            writer.writeheader()
        today = date.today().strftime("%b-%d-%Y")
        writer.writerow(
            {"sf": sf, "query": query, "total_timing": timing_dict['total_timing'],
             "result_timing": timing_dict['result_timing'], "csr_timing": timing_dict['csr_timing'],
             "precompute_timing": timing_dict['precompute_timing'],
             "average_parameter_timing": timing_dict['average_parameter_timing'],
             "total_parameter_timing": sum(timing_dict['parameter_timing']), "path_timing": timing_dict['path_timing'],
             "other_timing": timing_dict['other_timing'], "lanes": lane,
             "threads": thread, "vertices": graph_stats['vertices'], "edges": graph_stats['edges'],
             "paths": graph_stats['paths'],
             "workload": workload, "date": today
             })


def main(argv):
    sf, query, only_load, workload, lanes, threads, experimental_mode, file_format = process_arguments(argv)
    file_location = validate_input(query, workload)
    if experimental_mode:
        if lanes is None:
            for lane in lane_limits:
                run_per_lane(file_location, lane, only_load, query, sf, threads, workload, file_format)
        else:
            run_per_lane(file_location, lanes, only_load, query, sf, threads, workload, file_format)

    else:
        if isinstance(threads, list):
            run_duckdb(file_location, lanes, only_load, query, sf, max(threads), workload, file_format)
        else:
            run_duckdb(file_location, lanes, only_load, query, sf, threads, workload, file_format)


def run_per_lane(file_location, lane, only_load, query, sf, threads, workload, file_format):
    if isinstance(threads, list):
        for thread in threads:
            timing_dict, subquery, graph_stats = run_duckdb(file_location, lane, only_load, query, sf,
                                                            thread,
                                                            workload, file_format)
            write_timing_dict(timing_dict, sf, subquery, workload, graph_stats, lane, thread)
    else:
        timing_dict, subquery, graph_stats = run_duckdb(file_location, lane, only_load, query, sf, threads,
                                                        workload, file_format)
        write_timing_dict(timing_dict, sf, subquery, workload, graph_stats, lane, threads)


def load_entities_parquet(con, data_dir, query):
    run_script(con, "ddl/drop-tables-parquet.sql")
    file_location = f"{data_dir}/parquet"
    schema = open(f"{file_location}/schema.sql").read().split(';')
    for query in schema:
        con.execute(query)
    load = open(f"{file_location}/load.sql").read().split(';')
    for query in load:
        con.execute(query)
        if "person_knows_person" in query:
            file_location = query.split("'")[1]
            con.execute(f"COPY person_knows_person (creationDate, Person2id, Person1id) FROM '{file_location}' (FORMAT 'parquet')")
            con.execute(f"ALTER table person_knows_person ADD COLUMN weight integer")
            con.execute(f"UPDATE person_knows_person SET weight=rowid % 10 + 1 where weight is NULL")

def run_duckdb(file_location, lanes, only_load, query, sf, threads, workload, file_format):
    con = duckdb.connect("snb_benchmark.duckdb", read_only=False)
    run_script(con, "ddl/drop-tables.sql")
    run_script(con, "ddl/schema-composite-merged-fk.sql")
    data_dir = f'../../ldbc_snb_datagen_spark/out-sf{sf}'
    params = open(f'../parameters/parameters-sf{sf}/{workload}-{query}.csv').readlines()  # parameters-sf{sf}/
    if file_format == 'csv':
        start = timeit.default_timer()
        run_script(con, "ddl/drop-tables.sql")
        run_script(con, "ddl/schema-composite-merged-fk.sql")
        load_entities_csv(con, data_dir, query)
        stop = timeit.default_timer()
        print(stop - start)

    else:
        start = timeit.default_timer()
        load_entities_parquet(con, data_dir, query)
        stop = timeit.default_timer()
        print(stop - start)
    if not only_load:
        subquery = query
        if query == '19a' or query == '19b':
            query = '19'

        result, timing_dict, graph_stats = run_script(con, file_location, params, sf, lanes, threads)
        sort_results(result, timing_dict, params, query, sf, subquery, workload)
        return timing_dict, subquery, graph_stats


def validate_input(query, workload):
    # TODO Update validating input based on new parameters
    try:
        if query.isnumeric():
            assert (1 <= int(query) <= 20), "Invalid query number, should be in range (1,20)."
        else:
            assert (query == '19a' or query == '19b')
    except AssertionError as msg:
        logging.critical(msg)
        sys.exit(1)
    except ValueError as msg:
        logging.critical(msg)
        sys.exit(1)
    file_location = None
    if query == '19a' or query == '19b':
        file_location = f"queries/{workload}/q19.sql"
    else:
        file_location = f"queries/{workload}/q{query}-modified.sql" # TODO REMOVE MODIFIED AFTER TESTING CHEAPEST
    try:
        open(file_location)
    except FileNotFoundError:
        logging.critical(f"File at {file_location} not found, possibly not implemented yet.")
        sys.exit(1)
    return file_location


def load_entities_csv(con, data_dir: str, query: str):
    static_path = f"{data_dir}/graphs/csv/bi/composite-merged-fk/initial_snapshot/static"
    dynamic_path = f"{data_dir}/graphs/csv/bi/composite-merged-fk/initial_snapshot/dynamic"
    static_entities = ["Organisation", "Place", "Tag", "TagClass"]
    dynamic_entities = ["Comment", "Comment_hasTag_Tag", "Forum", "Forum_hasMember_Person", "Forum_hasTag_Tag",
                        "Person", "Person_hasInterest_Tag", "Person_knows_Person", "Person_likes_Comment",
                        "Person_likes_Post", "Person_studyAt_University", "Person_workAt_Company", "Post",
                        "Post_hasTag_Tag"]
    if query == "20":
        # Query 20
        static_entities = ["Organisation"]
        dynamic_entities = ["Person", "Person_knows_Person", "Person_studyAt_University", "Person_workAt_Company"]
    elif query == "19a" or query == "19b":
        # Query 19
        static_entities = ["Place"]
        dynamic_entities = ["Comment", "Person", "Person_knows_Person", "Post"]
    elif query == '13':
        # Query 13 Interactive
        static_entities = []
        dynamic_entities = ["Person", "Person_knows_Person"]
    logging.info("## Static entities")
    for entity in static_entities:
        for csv_file in [f for f in os.listdir(f"{static_path}/{entity}") if
                         f.startswith("part-") and (f.endswith(".csv") or f.endswith(".csv.gz"))]:
            csv_path = f"{static_path}/{entity}/{csv_file}"
            logging.debug(f"- {csv_path}")
            con.execute(
                f"COPY {entity} FROM '{csv_path}' (DELIMITER '|', HEADER, FORMAT csv)")
            logging.info("Loaded static entities.")
    logging.info("## Dynamic entities")
    for entity in dynamic_entities:
        for csv_file in [f for f in os.listdir(f"{dynamic_path}/{entity}") if
                         f.startswith("part-") and (f.endswith(".csv") or f.endswith(".csv.gz"))]:
            csv_path = f"{dynamic_path}/{entity}/{csv_file}"
            logging.debug(f"- {csv_path}")
            con.execute(
                f"COPY {entity} FROM '{csv_path}' (DELIMITER '|', HEADER, FORMAT csv)")
            if entity == "Person_knows_Person":
                con.execute(
                    f"COPY {entity} (creationDate, Person2id, Person1id) FROM '{csv_path}' (DELIMITER '|', HEADER, FORMAT csv)")
                con.execute(f"ALTER table person_knows_person ADD COLUMN weight integer")
                con.execute(f"UPDATE person_knows_person SET weight=rowid % 10 + 1 where weight is NULL")
    logging.info("Loaded dynamic entities.")
    logging.info("Load initial snapshot")


if __name__ == "__main__":
    logging.basicConfig(format='%(process)d-%(levelname)s-%(message)s', level=logging.DEBUG)
    main(sys.argv[1:])
