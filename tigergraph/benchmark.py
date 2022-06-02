import argparse
from pathlib import Path
from datetime import date, timedelta
from queries import run_queries, precompute, cleanup
from batches import run_batch_update
import os
import time
import re
import requests

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='LDBC TigerGraph BI workload Benchmark')
    parser.add_argument('data_dir', type=Path, help='The directory to load data from')
    parser.add_argument('--header', action='store_true', help='whether data has the header')
    parser.add_argument('--cluster', action='store_true', help='load concurrently on cluster')
    parser.add_argument('--skip', action='store_true', help='skip precompute')
    parser.add_argument('--para', type=Path, default=Path('../parameters'), help='parameter folder')
    parser.add_argument('--test', action='store_true', help='test mode only run one time')
    parser.add_argument('--nruns', '-n', type=int, default=10, help='number of runs')
    parser.add_argument('--endpoint', type=str, default = 'http://127.0.0.1:9000', help='tigergraph rest port')
    args = parser.parse_args()

    sf = os.environ.get("SF")
    results_file = open('output/results.csv', 'w')
    timings_file = open('output/timings.csv', 'w')
    timings_file.write(f"tool|sf|day|q|parameters|time\n")
    query_variants = ["1", "2a", "2b", "3", "4", "5", "6", "7", "8a", "8b", "9", "10a", "10b", "11", "12", "13", "14a", "14b", "15a", "15b", "16a", "16b", "17", "18", "19a", "19b", "20a", "20b"]
    query_nums = [int(re.sub("[^0-9]", "", query_variant)) for query_variant in query_variants]
    start_date = date(2012, 11, 29)
    end_date = date(2013, 1, 1)
    batch_size = timedelta(days=1)
    needClean = False
    batch_date = start_date
    while batch_date < end_date:
        start = time.time()
        duration = run_batch_update(batch_date, args)
        if needClean:
            for query_num in [19,20]:
                if query_num in query_nums:
                    cleanup(query_num, args.endpoint)
        needClean = False
        
        for query_num in [4,6,19,20]:
            if query_num in query_nums:
                # we wait for the rebuild before run bi19precompute query, bi19precompute is sometimes aborted due to OOM                
                if query_num == 19: requests.get(f'{args.endpoint}/rebuildnow', headers={'GSQL-TIMEOUT': '36000000'})
                precompute(query_num, args.endpoint)
        needClean = True
        writes_time = time.time() - start
        timings_file.write(f"TigerGraph|{sf}|{batch_date}|writes||{writes_time:.6f}\n")
        reads_time = run_queries(query_variants, results_file, timings_file, batch_date, args)
        timings_file.write(f"TigerGraph|{sf}|{batch_date}|reads||{reads_time:.6f}\n")
        batch_date = batch_date + batch_size

    results_file.close()
    timings_file.close()
