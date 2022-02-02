import argparse
import json
import os
import subprocess
import sys
from pathlib import Path
import time
from timeit import default_timer as timer
import ast
import requests
import re
from datetime import datetime, timedelta
from random import randrange, choice
from glob import glob

parser = argparse.ArgumentParser(description='Batch updates for TigerGraph BI workloads')
parser.add_argument('data_dir', type=Path, help='The machine (default:ANY) and directory to load data from, e.g. "/home/tigergraph/data" or "ALL:/home/tigergraph/data"')
parser.add_argument('--header', action='store_true', help='whether data has the header')
parser.add_argument('--machine', '-m', type=str, default = 'ANY', help='which machine to load the data')
args = parser.parse_args()

STATIC_NAMES = [
    'Organisation',
    'Organisation_isLocatedIn_Place',
    'Place',
    'Place_isPartOf_Place',
    'Tag',
    'TagClass',
    'TagClass_isSubclassOf_TagClass',
    'Tag_hasType_TagClass',
]
DYNAMIC_VERTICES = [
    'Comment',
    'Forum',
    'Person',
    'Post',
]
DYNAMIC_EDGES = [
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
DYNAMIC_NAMES = DYNAMIC_VERTICES + DYNAMIC_EDGES


"""
Load data
    - job: load_static, load_dynamic, delete_dynamic
    - machine : node, ANY | ALL | m1
    - data_dir: file path
    - tag : dynamic | static
    - names: STATIC_NAMES | DYNAMIC_NAMES | DEL_EDGES
    - suffix : str
    - date : None
""" 
def load_data(job, machine, data_dir, tag, names, date):
    file_paths = [(data_dir /tag / name) for name in names]
    gsql = f'RUN LOADING JOB {job} USING '
    gsql += ', '.join(f'file_{name}="{machine}:{file_path}"' for name, file_path in zip(names, file_paths))
    subprocess.run(f'gsql -g ldbc_snb \'{gsql}\'', shell=True)


verbose = True
res_dir = 'results'
os.makedirs(res_dir, exist_ok=True)
timelog = res_dir/'timelog.csv'
header = '_with_header' if args.header else ''

network_start_date = date(2012, 9, 13)
network_end_date = date(2012, 12, 31)
batch_size = relativedelta(days=1)
batch_start_date = network_start_date
while batch_start_date < network_end_date:

    batch_id = batch_start_date.strftime('%Y-%m-%d')
    batch_dir = f"batch_id={batch_id}"
    print(f"#################### {batch_dir} ####################")

    print("## Inserts")
    for entity in insert_entities:
    t0 = timer()
    load_data(f'insert_vertex{header}', args.machine, args.data_dir/'inserts', 'dynamic', DYNAMIC_VERTICES, date)
    load_data(f'insert_edge{header}', args.machine, args.data_dir/'inserts', 'dynamic', DYNAMIC_EDGES, date)
    t1 = timer()
    tot_ins_time += t1-t0
    if args.verbose: logf.write('insert,' + toStr(quick_stat())+ f',{t1-t0}\n')
    
    print('======== deletion for ' + date.strftime('%Y-%m-%d') + '========')
    for vertex, workload in zip(DEL_VERTICES, DEL_WORKLOADS):
        t0 = timer()
        path = args.data_dir/'deletes'/'dynamic'/vertex/date.strftime('batch_id=%Y-%m-%d') 
        if not path.exists(): continue
        for fp in path.glob('*.csv'):
            if fp.is_file(): 
                result = workload.run({'file':str(fp)})
                print(f'Deleting {vertex}: {result.result}')
        t1 = timer()
        tot_del_time += t1 - t0
        if args.verbose: logf.write(f'{vertex},' + toStr(quick_stat())+ f',{t1-t0}\n')
    t0 = timer()
    load_data('delete_edge' + header, args.machine, args.data_dir/'deletes', 'dynamic', DEL_EDGES, args.suffix, date)
    t1 = timer()
    tot_del_time += t1 - t0
    if args.verbose: logf.write('delete_edge,' + toStr(quick_stat())+ f',{t1-t0}\n')
    
    

date = begin 
tot_ins_time = 0
tot_del_time = 0
dateStr = date.strftime('%Y-%m-%d')
output = args.output/dateStr

logf = open(timelog, 'w')
cols = ['date'] + stat_name + ['ins','del','gen'] + [f'bi{i}' for i in range(1,21)]
logf.write(','.join(cols)+'\n')
stat_dict = cmd_stat(args)
batch_log = f'{dateStr},' + toStr([stat_dict[n] for n in stat_name]) 
batch_log += ',' + toStr([tot_ins_time, tot_del_time])
header = '_with_header' if args.header else ''
if args.read_interval > 0: 
    # run query 
    durations = cmd_run(args, output = output)
    batch_log += ',' + toStr(durations)
logf.write(batch_log+'\n')
logf.flush()
while date < end:
    print('======== insertion for ' + date.strftime('%Y-%m-%d') + '========')
    t0 = timer()
    load_data('insert_vertex' + header, args.machine, args.data_dir/'inserts', 'dynamic', DYNAMIC_VERTICES, args.suffix, date)
    load_data('insert_edge' + header, args.machine, args.data_dir/'inserts', 'dynamic', DYNAMIC_EDGES, args.suffix, date)
    t1 = timer()
    tot_ins_time += t1-t0
    if args.verbose: logf.write('insert,' + toStr(quick_stat())+ f',{t1-t0}\n')
    
    print('======== deletion for ' + date.strftime('%Y-%m-%d') + '========')
    for vertex,workload in zip(DEL_VERTICES, DEL_WORKLOADS):
        t0 = timer()
        path = args.data_dir/'deletes'/'dynamic'/vertex/date.strftime('batch_id=%Y-%m-%d') 
        if not path.exists(): continue
        for fp in path.glob('*.csv'):
            if fp.is_file(): 
                result = workload.run({'file':str(fp)})
                print(f'Deleting {vertex}: {result.result}')
        t1 = timer()
        tot_del_time += t1 - t0
        if args.verbose: logf.write(f'{vertex},' + toStr(quick_stat())+ f',{t1-t0}\n')
    t0 = timer()
    load_data('delete_edge' + header, args.machine, args.data_dir/'deletes', 'dynamic', DEL_EDGES, args.suffix, date)
    t1 = timer()
    tot_del_time += t1 - t0
    if args.verbose: logf.write('delete_edge,' + toStr(quick_stat())+ f',{t1-t0}\n')
    
    date += delta    
    dateStr = date.strftime('%Y-%m-%d')
    output = args.output/dateStr
        
    # run query 
    if args.read_interval and (date - begin).days % args.read_interval == 0: 
        stat_dict = cmd_stat(args)
        batch_log = f'{dateStr},' + toStr([stat_dict[n] for n in stat_name]) 
        batch_log += ',' + toStr([tot_ins_time, tot_del_time])
        batch_log += ',' + toStr(cmd_run(args, output = output))
    else:
        batch_log = f'{dateStr},' + toStr(quick_stat()) 
        batch_log += ',' + toStr([tot_ins_time, tot_del_time])
    
    logf.write(batch_log+'\n')
    logf.flush()
    if args.read_interval == 0 or (date - begin).days % args.read_interval == 0:
        tot_ins_time = 0
        tot_del_time = 0
    
logf.close()

#=============
def write_txn_fun(tx, query_spec, batch, csv_file):
    result = tx.run(query_spec, batch=batch, csv_file=csv_file)
    return result.value()

def run_update(session, query_spec, batch, csv_file):
    start = time.time()
    result = session.write_transaction(write_txn_fun, query_spec, batch, csv_file)
    end = time.time()
    duration = end - start

    num_changes = result[0]
    return num_changes


if len(sys.argv) < 2:
    print("Usage: batches.py <NEO4J_DATA_DIRECTORY> [--compressed]")
    exit(1)

data_dir = sys.argv[1]
compressed = len(sys.argv) == 3 and sys.argv[2] == "--compressed"

if compressed:
    csv_extension = ".csv.gz"
else:
    csv_extension = ".csv"

# to ensure that all inserted edges have their endpoints at the time of their insertion, we insert nodes first and edges second
insert_nodes = ["Comment", "Forum", "Person", "Post"]
insert_edges = ["Comment_hasCreator_Person", "Comment_hasTag_Tag", "Comment_isLocatedIn_Country", "Comment_replyOf_Comment", "Comment_replyOf_Post", "Forum_containerOf_Post", "Forum_hasMember_Person", "Forum_hasModerator_Person", "Forum_hasTag_Tag", "Person_hasInterest_Tag", "Person_isLocatedIn_City", "Person_knows_Person", "Person_likes_Comment", "Person_likes_Post", "Person_studyAt_University", "Person_workAt_Company", "Post_hasCreator_Person", "Post_hasTag_Tag", "Post_isLocatedIn_Country"]
insert_entities = insert_nodes + insert_edges

# set the order of deletions to reflect the dependencies between node labels (:Comment)-[:REPLY_OF]->(:Post)<-[:CONTAINER_OF]-(:Forum)-[:HAS_MODERATOR]->(:Person)
delete_nodes = ["Comment", "Post", "Forum", "Person"]
delete_edges = ["Forum_hasMember_Person", "Person_knows_Person", "Person_likes_Comment", "Person_likes_Post"]
delete_entities = delete_nodes + delete_edges

insert_queries = {}
for entity in insert_entities:
    with open(f"dml/ins-{entity}.cypher", "r") as insert_query_file:
        insert_queries[entity] = insert_query_file.read()

delete_queries = {}
for entity in delete_entities:
    with open(f"dml/del-{entity}.cypher", "r") as delete_query_file:
        delete_queries[entity] = delete_query_file.read()

driver = GraphDatabase.driver("bolt://localhost:7687")
session = driver.session()

network_start_date = date(2012, 9, 13)
network_end_date = date(2012, 12, 31)
batch_size = relativedelta(days=1)

batch_start_date = network_start_date
while batch_start_date < network_end_date:
    # format date to yyyy-mm-dd
    batch_id = batch_start_date.strftime('%Y-%m-%d')
    batch_dir = f"batch_id={batch_id}"
    print(f"#################### {batch_dir} ####################")

    print("## Inserts")
    for entity in insert_entities:
        batch_path = f"{data_dir}/inserts/dynamic/{entity}/{batch_dir}"
        if not os.path.exists(batch_path):
            continue

        print(f"{entity}:")
        for csv_file in [f for f in os.listdir(batch_path) if f.endswith(csv_extension)]:
            print(f"- inserts/dynamic/{entity}/{batch_dir}/{csv_file}")
            num_changes = run_update(session, insert_queries[entity], batch_dir, csv_file)
            if num_changes == 0:
                print("!!! No changes occured")
            else:
                print(f"> {num_changes} changes")
            print()

    print("## Deletes")
    for entity in delete_entities:
        batch_path = f"{data_dir}/deletes/dynamic/{entity}/{batch_dir}"
        if not os.path.exists(batch_path):
            continue

        print(f"{entity}:")
        for csv_file in [f for f in os.listdir(batch_path) if f.endswith(csv_extension)]:
            print(f"- deletes/dynamic/{entity}/{batch_dir}/{csv_file}")
            num_changes = run_update(session, delete_queries[entity], batch_dir, csv_file)
            if num_changes == 0:
                print("!!! No changes occured")
            else:
                print(f"> {num_changes} changes")
            print()

    batch_start_date = batch_start_date + batch_size

session.close()
driver.close()
