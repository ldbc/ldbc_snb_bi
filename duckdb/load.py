import sys
import os
import re
import time
import duckdb
import getopt
import logging
import pandas as pd
import timeit

SORT_ORDER_DICT = {'20': ['person1id', 'weight']}


def sort_results(result, timing, params, query, sf):
    output_file = open('output/results.csv', 'w')
    timing_output = open('output/timings.csv', 'w')
    timing_output.write(f"sf|q|parameters|time\n")

    filtered_param_headers = extract_headers(params)

    for idx, row in result.iterrows():
        formatted_parameters = f"<{';'.join(str(parameter) for parameter in row[filtered_param_headers])}>"
        formatted_output = f"[<{','.join(str(result) for result in row.drop(labels=filtered_param_headers))}>]"
        output_file.write(
            f"{query}|{query}|{formatted_parameters}|{formatted_output}\n")
        timing_output.write(f"DuckDB|{sf}|{query}|{formatted_parameters}|{timing[idx]}\n")
    output_file.close()
    timing_output.close()


def extract_headers(params):
    param_headers = params[0]
    param_headers_formatted = param_headers.split("|")
    filtered_param_headers = [param.split(":")[0].lower() for param in param_headers_formatted]
    return filtered_param_headers


def run_script(con, filename, params=None, sf=None):
    queries = open(filename).read().split(";")
    final_result = []
    final_timing = []
    for query in queries:
        logging.debug(query)
        if "-- PARAMS" in query:
            original_query = query
            filtered_param_headers = extract_headers(params)
            for line in params[1:]:
                custom_query = original_query
                line = line.strip("\n")
                split_line = line.split("|")
                print(split_line[-1])
                if split_line[-1] == "28587302326999":
                    pass
                for i in range(len(filtered_param_headers)):
                    custom_query = custom_query.replace(f"{filtered_param_headers[i]}", f"{split_line[i]}")
                # print(custom_query)
                start = timeit.default_timer()

                result = con.execute(custom_query).fetchdf()

                stop = timeit.default_timer()

                timing = stop - start
                final_result.append(result)
                final_timing.append(timing)
        # elif "-- OUTPUT" in query:
        #
        #     start = timeit.default_timer()
        #
        #     result = con.execute(query).fetchdf()
        #
        #     stop = timeit.default_timer()
        #
        #     timing = stop - start
        #     return result, timing
        else:
            con.execute(query)
    if final_result and final_timing:
        final_result = pd.concat(final_result)
        return final_result, final_timing

#  -- PARAMS
# -- INSERT INTO src_dst (src, dst, cName) (SELECT p.rowid, p2.rowid, c.name
# -- FROM PersonUniversity p
# -- JOIN Person_workAt_Company pwc on p.id = pwc.PersonId
# -- JOIN Company c on (pwc.CompanyId = c.id AND c.name = 'company')
# -- JOIN PersonUniversity p2 on p2.id = person2id);
# --
# -- update src_dst s set v_size = (select count(*) from PersonUniversity p) where v_size is NULL;
# -- CREATE TABLE src_dst
# --     (id integer default 0,
# --     v_size bigint,
# --     src bigint,
# --     dst bigint,
# --     cName varchar);

def process_arguments(argv):
    sf = ''
    query = ''
    try:
        opts, args = getopt.getopt(argv, "hs:q:", ["scalefactor=", "query="])
    except getopt.GetoptError:
        logging.info('load.py -s <scalefactor> -q <query>')
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print('load.py -s <scalefactor> -q <query>')
            sys.exit()
        elif opt in ("-s", "--scalefactor"):
            sf = arg
        elif opt in ("-q", "--query"):
            query = arg
    return sf, query





def main(argv):
    sf, query = process_arguments(argv)
    file_location = validate_input(query)

    con = duckdb.connect("snb_benchmark.duckdb", read_only=False)
    run_script(con, "ddl/drop-tables.sql")
    run_script(con, "ddl/schema-composite-merged-fk.sql")

    data_dir = f'../../ldbc_snb_datagen_spark/out-sf{sf}/graphs/csv/bi/composite-merged-fk'

    load_entities(con, data_dir)
    params = open(f'../parameters/bi-20.csv').readlines() # parameters-sf{sf}/

    result, timing = run_script(con, file_location, params, sf)
    sort_results(result, timing, params, query, sf)


def validate_input(query):
    try:
        assert (1 <= int(query) <= 20), "Invalid query number, should be in range (1,20)."
    except AssertionError as msg:
        logging.critical(msg)
        sys.exit(1)
    except ValueError as msg:
        logging.critical(msg)
        sys.exit(1)
    file_location = f"queries/q{query}.sql"
    try:
        open(file_location)
    except FileNotFoundError:
        logging.critical(f"File at {file_location} not found, possibly not implemented yet.")
        sys.exit(1)
    return file_location


def load_entities(con, data_dir):
    static_path = f"{data_dir}/initial_snapshot/static"
    dynamic_path = f"{data_dir}/initial_snapshot/dynamic"
    static_entities = ["Organisation", "Place", "Tag", "TagClass"]
    dynamic_entities = ["Comment", "Comment_hasTag_Tag", "Forum", "Forum_hasMember_Person", "Forum_hasTag_Tag",
                        "Person", "Person_hasInterest_Tag", "Person_knows_Person", "Person_likes_Comment",
                        "Person_likes_Post", "Person_studyAt_University", "Person_workAt_Company", "Post",
                        "Post_hasTag_Tag"]
    static_entities = ["Organisation"]
    dynamic_entities = ["Person", "Person_knows_Person", "Person_studyAt_University", "Person_workAt_Company"]

    logging.info("## Static entities")
    for entity in static_entities:
        for csv_file in [f for f in os.listdir(f"{static_path}/{entity}") if
                         f.startswith("part-") and f.endswith(".csv")]:
            csv_path = f"{entity}/{csv_file}"
            logging.debug(f"- {csv_path}")
            con.execute(
                f"COPY {entity} FROM '{data_dir}/initial_snapshot/static/{entity}/{csv_file}' (DELIMITER '|', HEADER, FORMAT csv)")
            logging.info("Loaded static entities.")
    logging.info("## Dynamic entities")
    for entity in dynamic_entities:
        for csv_file in [f for f in os.listdir(f"{dynamic_path}/{entity}") if
                         f.startswith("part-") and f.endswith(".csv")]:
            csv_path = f"{entity}/{csv_file}"
            logging.debug(f"- {csv_path}")
            con.execute(
                f"COPY {entity} FROM '{data_dir}/initial_snapshot/dynamic/{entity}/{csv_file}' (DELIMITER '|', HEADER, FORMAT csv)")
            if entity == "Person_knows_Person":
                con.execute(
                    f"COPY {entity} (creationDate, Person2id, Person1id) FROM '{data_dir}/initial_snapshot/dynamic/{entity}/{csv_file}' (DELIMITER '|', HEADER, FORMAT csv)")
    logging.info("Loaded dynamic entities.")
    logging.info("Load initial snapshot")


if __name__ == "__main__":
    logging.basicConfig(format='%(process)d-%(levelname)s-%(message)s', level=logging.DEBUG)
    main(sys.argv[1:])
