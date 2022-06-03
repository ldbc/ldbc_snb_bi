import argparse
from pathlib import Path
from datetime import datetime, date, timedelta
from queries import run_queries
from batches import run_batch_updates

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='LDBC TigerGraph BI workload Benchmark')
    parser.add_argument('data_dir', type=Path, help='The directory to load data from')
    parser.add_argument('--header', action='store_true', help='whether data has the header')
    parser.add_argument('--cluster', action='store_true', help='load concurrently on cluster')
    parser.add_argument('--skip', action='store_true', help='skip precompute')
    parser.add_argument('--para', type=Path, default=Path('../parameters'), help='parameter folder')
    parser.add_argument('--test', action='store_true', help='test mode only run one time')
    parser.add_argument('--endpoint', type=str, default = 'http://127.0.0.1:9000', help='tigergraph rest port')
    args = parser.parse_args()

    results_file = open('output/results.csv', 'w')
    timings_file = open('output/timings-old.csv', 'w')
    batch_timing = open('output/batch_timing.csv', 'w')
    timings_file.write(f"sf|q|parameters|time\n")
    batch_timing.write(f'date|operation|time\n')
    query_variants = ["1", "2a", "2b", "3", "4", "5", "6", "7", "8a", "8b", "9", "10a", "10b", "11", "12", "13", "14a", "14b", "15a", "15b", "16a", "16b", "17", "18", "19a", "19b", "20"]

    start_date = date(2012, 11, 29)
    end_date = date(2012, 12, 2)
    batch_size = timedelta(days=1)
    while start_date < end_date:
        print()
        print(f"----------------> Batch date: {start_date} <---------------")
        next_date = start_date + batch_size
        run_batch_updates(start_date, next_date, batch_timing, args)
        run_queries(query_variants, results_file, timings_file, args)
        start_date = next_date

    results_file.close()
    timings_file.close()
