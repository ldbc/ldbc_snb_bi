#!/usr/bin/python3
import argparse
from pathlib import Path
import time
import csv
import requests
import re
import os
import subprocess
import datetime
import json
# query timeout value in miliseconds
HEADERS = {'GSQL-TIMEOUT': '36000000'}

# ================ BEGIN: Variables and Functions from Cypher ========================

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
        return [int(x) for x in value]
    elif result_type == "ID" or result_type == "INT" or result_type == "INT32" or result_type == "INT64":
        return int(value)
    elif result_type == "FLOAT" or result_type == "FLOAT32" or result_type == "FLOAT64":
        return float(value)
    elif result_type == "STRING[]":
        return value
    elif result_type == "STRING":
        return value
    elif result_type in ["DATETIME", "DATE"]:
        return value.replace(" ", "T")
    elif result_type == "BOOL":
        return bool(value)
    else:
        raise ValueError(f"Result type {result_type} not found")


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

def run_query(endpoint, query_num, parameters):
    start = time.time()
    response = requests.get(f'{endpoint}/query/ldbc_snb/bi{query_num}', headers=HEADERS, params=parameters).json()
    end = time.time()
    duration = end - start
    if response['error']:
        if query_num == 11:
            return f"""[{{"count": 0}}]""", duration
        elif query_num == 15:
            return f"""[{{"weight": -1.0}}]""", duration
        else:
            print(response['message'])
            return '[]', 0
    results = response['results'][0]['result']
    # for BI-11 and BI-15, result is a single value
    if query_num == 11:
        return f"""[{{"count": {results}}}]""", duration
    elif query_num == 15:
        return f"""[{{"weight": {results}}}]""", duration
    
    #convert results from [dict()] to [[]] 
    results = [[v for k,v in res.items()] for res in results]
    #convert results to string
    mapping = result_mapping[query_num]
    result_tuples = [
            {
                result_descriptor["name"]: convert_value_to_string(result[i], result_descriptor["type"])
                for i, result_descriptor in enumerate(mapping)
            }
            for result in results
        ]

    return json.dumps(result_tuples), duration


def run_queries(query_variants, results_file, timings_file, batch_date, args):
    sf = os.environ.get("SF")
    if sf is None:
        print("${SF} environment variable must be set")
        exit(1)
    start = time.time()
    for query_variant in query_variants:
        print(f"========================= Q{query_variant} =========================")
        query_num = int(re.sub("[^0-9]", "", query_variant))
        parameters_csv = csv.DictReader(open(args.para / f'bi-{query_variant}.csv'), delimiter='|')
        parameters = [{"name": t[0], "type": t[1]} for t in [f.split(":") for f in parameters_csv.fieldnames]]

        for i,query_parameters in enumerate(parameters_csv):
            query_parameters_split = {k.split(":")[0]: v for k, v in query_parameters.items()}
            query_parameters_in_order = json.dumps(query_parameters_split)

            query_parameters = {k.split(":")[0]: cast_parameter_to_driver_input(v, k.split(":")[1]) for k, v in query_parameters.items()}
            if args.test:
                print(f'Q{query_variant}: {query_parameters}')
            # Q1 parameter name is conflict with TG data type keyword 'datetime' 
            if query_num == 1: query_parameters = {'date': query_parameters['datetime']}
            results, duration = run_query(args.endpoint, query_num, query_parameters)

            results_file.write(f"{query_num}|{query_variant}|{query_parameters_in_order}|{results}\n")
            results_file.flush()
            timings_file.write(f"TigerGraph|{sf}|{batch_date}|{query_variant}|{query_parameters_in_order}|{duration:.6f}\n")
            timings_file.flush()
            # test run: 1 query, regular run: 10 queries
            if args.test:
                print(f"-> {duration:.4f} seconds")
                print(f"-> {results}")
            if args.test or i == args.nruns-1:
                break

    return time.time() - start

def run_precompute(args):
    t0 = time.time()
    print(f"==================== Precompute for BI 19,4,6,20 ======================")
    # compute values and print to files
    for q in [4,6,20]:
        t1 = time.time()
        requests.get(f'{args.endpoint}/query/ldbc_snb/precompute_bi{q}', headers=HEADERS)
        print(f'precompute_bi{q}:\t\t{time.time()-t1:.4f} s')

    # precompute q19
    t1 = time.time()
    requests.get(f'{args.endpoint}/query/ldbc_snb/cleanup_bi19', headers=HEADERS)
    print(f'cleanup_bi19:\t\t{time.time()-t1:.4f} s')
    start = datetime.date(2010,1,1)
    nbatch = 12 # can be smaller if memory is sufficient
    for i in range(nbatch):
      t1 = time.time()
      end = start + datetime.timedelta(days=365*3//nbatch + 1)
      output = Path('/home/tigergraph/reply_count')
      out_file = output / f'part_{i:04d}.csv'
      params = {'startDate':start, 'endDate': end, 'file': str(out_file)}
      requests.get(f'{args.endpoint}/query/ldbc_snb/precompute_bi19', params = params, headers=HEADERS)
      print(f'precompute_bi19({start},{end}):{time.time()-t1:.4f} s')
      start = end

    # load the files (this is faster in large SF)
    t1 = time.time()
    if not args.cluster:
        subprocess.run(f"docker exec --user tigergraph snb-bi-tg bash -c '/home/tigergraph/tigergraph/app/cmd/gsql -g ldbc_snb RUN LOADING JOB load_precompute'", shell=True)
    else:
        subprocess.run(f'gsql -g ldbc_snb RUN LOADING JOB load_precompute', shell=True)
    print(f'load_precompute:\t\t{time.time()-t1:.4f} s')
    return time.time() - t0

# main functions
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='BI query driver')
    parser.add_argument('--cluster', action='store_true', help='load concurrently on cluster')
    parser.add_argument('--para', type=Path, default=Path('../parameters'), help='parameter folder')
    parser.add_argument('--skip', action='store_true', help='skip precomputation')
    parser.add_argument('--test', action='store_true', help='test mode only run one time')
    parser.add_argument('--temp', type=Path, default=Path('/tmp'), help='folder for temparoty files')
    parser.add_argument('--nruns', '-n', type=int, default=10, help='number of runs')
    parser.add_argument('--endpoint', type=str, default='http://127.0.0.1:9000',help='tigergraph endpoints')
    args = parser.parse_args()
    
    output = Path('output')
    output.mkdir(exist_ok=True)
    results_file = open(output/'results.csv', 'w')
    timings_file = open(output/'timings.csv', 'w')
    timings_file.write(f"tool|sf|day|q|parameters|time\n")
    query_variants = ["1", "2a", "2b", "3", "4", "5", "6", "7", "8a", "8b", "9", "10a", "10b", "11", "12", "13", "14a", "14b", "15a", "15b", "16a", "16b", "17", "18", "19a", "19b", "20a", "20b"]
    if not args.skip: run_precompute(args)
    run_queries(query_variants, results_file, timings_file, 'None', args)
    results_file.close()
    timings_file.close()
