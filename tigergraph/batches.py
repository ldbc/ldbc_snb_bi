import argparse
import subprocess
from pathlib import Path
import time
import requests
from datetime import datetime, date, timedelta
from glob import glob
import json
import ast

VERTICES = ['Comment', 'Forum', 'Person', 'Post']
EDGES = ['Comment_hasCreator_Person', 'Comment_hasTag_Tag', 'Comment_isLocatedIn_Country',
    'Comment_replyOf_Comment', 'Comment_replyOf_Post', 'Forum_containerOf_Post',
    'Forum_hasMember_Person', 'Forum_hasModerator_Person', 'Forum_hasTag_Tag',
    'Person_hasInterest_Tag', 'Person_isLocatedIn_City', 'Person_knows_Person',
    'Person_likes_Comment', 'Person_likes_Post', 'Person_studyAt_University',
    'Person_workAt_Company', 'Post_hasCreator_Person', 'Post_hasTag_Tag',
    'Post_isLocatedIn_Country']
DEL_EDGES = ['Person_knows_Person', 'Person_likes_Comment', 'Person_likes_Post',
    'Forum_hasMember_Person']
NAMES = VERTICES + EDGES


def run_query(name, parameters, endpoint):
    HEADERS = {'GSQL-TIMEOUT': '36000000'}
    start = time.time()
    response = requests.get(f'{endpoint}/query/ldbc_snb/{name}', headers=HEADERS, params=parameters).json()
    end = time.time()
    duration = end - start
    return response['results'][0]['result'], duration

"""
Load data using restpp endpoints
""" 
def load_by_restpp(job, data_dir, names, batch_dir, endpoint):
    for name in names:
        print(f'{name}:')
        folder = (data_dir/'dynamic'/name/batch_dir)
        if not folder.is_dir(): 
            print("!!! No changes occured")
            continue
        for f in folder.iterdir():
            print(f'- {f}')
            url = f'{endpoint}/ddl/ldbc_snb?tag={job}&filename=file_{name}&sep=%7C&ack=all'
            curl = f'curl -X POST  --data-binary  @{f} "{url}"'
            res = subprocess.run(curl, shell=True, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
            res = json.loads(res.stdout.decode("utf-8"))
            nlines = res["results"][0]["statistics"]["validLine"]
            print(f'> {nlines} changes')

def run_batch_updates(start_date, end_date, timing_file, args):
    header = '_with_header' if args.header else ''
    docker_data = Path('/data')
    batch_size = timedelta(days=1)
    batch_date = start_date
    while batch_date < end_date:
        tot_ins_time = 0
        tot_del_time = 0
        
        batch_id = batch_date.strftime('%Y-%m-%d')
        batch_dir = f"batch_id={batch_id}"
        print(f"#################### {batch_dir} ####################")

        print("## Inserts")
        t0 = time.time()
        load_by_restpp(f'insert_vertex{header}', args.data_dir/'inserts', VERTICES, batch_dir, args.endpoint)
        load_by_restpp(f'insert_edge{header}', args.data_dir/'inserts', EDGES, batch_dir, args.endpoint)
        t1 = time.time()
        tot_ins_time += t1-t0
        timing_file.write(f'{batch_date}|insert|{tot_ins_time:.6f}\n')
        timing_file.flush()
        print("## Deletes")
        for vertex in VERTICES:
            print(f"{vertex}:")
            path = args.data_dir/'deletes'/'dynamic'/vertex/batch_dir 
            docker_path = docker_data/'deletes'/'dynamic'/vertex/batch_dir
            print(path)
            if not path.exists(): 
                print("!!! No changes occured")
                continue
            for fp in path.glob('*.csv'):
                if fp.is_file():
                    print(f'- {fp}')
                    result, duration = run_query(f'del_{vertex}', {'file':str(docker_path/fp.name), 'header':args.header}, args.endpoint)
                    print(f'> {result} changes')
            tot_del_time += duration
        t0 = time.time()
        load_by_restpp(f'delete_edge{header}', args.data_dir/'deletes', DEL_EDGES, batch_dir, args.endpoint)
        t1 = time.time()
        tot_del_time += t1 - t0
        batch_date = batch_date + batch_size
        timing_file.write(f'{batch_date}|delete|{tot_del_time:.6f}\n')
        timing_file.flush()

# main functions
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Batch updates for TigerGraph BI workloads')
    parser.add_argument('data_dir', type=Path, help='The directory to load data from')
    parser.add_argument('--header', action='store_true', help='whether data has the header')
    parser.add_argument('--endpoint', type=str, default = 'http://127.0.0.1:9000', help='tigergraph rest port')
    args = parser.parse_args()

    timing_file = open('output/batch_timing.csv', 'w')
    timing_file.write(f'date|operation|time\n')
    network_start_date = date(2012, 11, 29)
    network_end_date = date(2013, 1, 1)
    run_batch_updates(network_start_date, network_end_date, timing_file, args)