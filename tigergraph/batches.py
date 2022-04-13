import argparse
import subprocess
from pathlib import Path
import time
import requests
from datetime import datetime, date, timedelta
from glob import glob
import json
import ast

parser = argparse.ArgumentParser(description='Batch updates for TigerGraph BI workloads')
parser.add_argument('data_dir', type=Path, help='The machine (default:ANY) and directory to load data from, e.g. "/home/tigergraph/data" or "ALL:/home/tigergraph/data"')
parser.add_argument('--header', action='store_true', help='whether data has the header')
parser.add_argument('--machine', '-m', type=str, default = 'ANY', help='which machine to load the data')
parser.add_argument('--endpoint', type=str, default = 'http://127.0.0.1:9000', help='tigergraph rest port')
parser.add_argument('--container', type=str, default = 'snb-bi-tg', help='tigergraph container name')

args = parser.parse_args()

VERTICES = [
    'Comment',
    'Forum',
    'Person',
    'Post',
]
EDGES = [
    'Comment_hasCreator_Person',
    'Comment_hasTag_Tag',
    'Comment_isLocatedIn_Country',
    'Comment_replyOf_Comment',
    'Comment_replyOf_Post',
    'Forum_containerOf_Post',
    'Forum_hasMember_Person',
    'Forum_hasModerator_Person',
    'Forum_hasTag_Tag',
    'Person_hasInterest_Tag',
    'Person_isLocatedIn_City',
    'Person_knows_Person',
    'Person_likes_Comment',
    'Person_likes_Post',
    'Person_studyAt_University',
    'Person_workAt_Company',
    'Post_hasCreator_Person',
    'Post_hasTag_Tag',
    'Post_isLocatedIn_Country',
]
DEL_EDGES = [
    'Person_knows_Person',
    'Person_likes_Comment',
    'Person_likes_Post',
    'Forum_hasMember_Person',
]
NAMES = VERTICES + EDGES


def run_query(name, parameters):
    ENDPOINT = f'{args.endpoint}/query/ldbc_snb/'
    HEADERS = {'GSQL-TIMEOUT': '36000000'}
    start = time.time()
    response = requests.get(ENDPOINT + name, headers=HEADERS, params=parameters).json()
    end = time.time()
    duration = end - start
    return response['results'][0]['result'], duration

"""
Load data using restpp endpoints
""" 
def load_by_restpp(job, data_dir, names, batch_dir):
    endpoint = f'{args.endpoint}/ddl/ldbc_snb/'
    for name in names:
        print(f'{name}:')
        folder = (data_dir/'dynamic'/name/batch_dir)
        if not folder.is_dir(): 
            print("!!! No changes occured")
            continue
        for f in folder.iterdir():
            print(f'- {f}')
            url = f'{endpoint}?tag={job}&filename=file_{name}&sep=%7C&ack=all'
            curl = f'curl -X POST  --data-binary  @{f} "{url}"'
            res = subprocess.run(curl, shell=True, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
            res = json.loads(res.stdout.decode("utf-8"))
            nlines = res["results"][0]["statistics"]["validLine"]
            print(f'> {nlines} changes')

"""
Load using gsql command from docker container
Can loading data from the distributed cluster

def load_distributed():
    file_paths = [(data_dir /tag / name) for name in names]
    gsql = f'RUN LOADING JOB {job} USING '
    gsql += ', '.join(f'file_{name}=\\"{machine}:{file_path}\\"' for name, file_path in zip(names, file_paths))
    # submit loading job in the TigerGraph host so that it supports distributed loading
    cmd = 'export PATH=/home/tigergraph/tigergraph/app/cmd:$PATH;'
    cmd += f'gsql -g ldbc_snb "{gsql}"'
    cmdl = f'docker exec --user tigergraph --interactive --tty {args.container} bash -c \'{cmd}\''
    #print(cmdl)
    #subprocess.run(cmdl, shell=True)
"""

header = '_with_header' if args.header else ''
docker_data = Path('/data')
network_start_date = date(2012, 11, 29)
network_end_date = date(2013, 1, 1)
batch_size = timedelta(days=1)
batch_start_date = network_start_date
while batch_start_date < network_end_date:
    tot_ins_time = 0
    tot_del_time = 0
    
    batch_id = batch_start_date.strftime('%Y-%m-%d')
    batch_dir = f"batch_id={batch_id}"
    print(f"#################### {batch_dir} ####################")

    print("## Inserts")
    t0 = time.time()
    load_by_restpp(f'insert_vertex{header}', args.data_dir/'inserts', VERTICES, batch_dir)
    load_by_restpp(f'insert_edge{header}', args.data_dir/'inserts', EDGES, batch_dir)
    t1 = time.time()
    tot_ins_time += t1-t0
    print(f'insert time: {tot_ins_time} s')
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
                result, duration = run_query(f'del_{vertex}', {'file':str(docker_path/fp.name), 'header':args.header})
                print(f'> {result} changes')
        tot_del_time += duration
    t0 = time.time()
    load_by_restpp(f'delete_edge{header}', args.data_dir/'deletes', DEL_EDGES, batch_dir)
    t1 = time.time()
    tot_del_time += t1 - t0
    batch_start_date = batch_start_date + batch_size
    print(f'delete time: {tot_del_time}')
