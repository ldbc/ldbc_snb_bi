from datetime import datetime
import csv
import re
import psycopg2
import time
import signal
import sys
from contextlib import contextmanager

@contextmanager
def timeout(t):
    signal.signal(signal.SIGALRM, raise_timeout)
    signal.alarm(t)

    try:
        yield
    except TimeoutError:
        raise
    finally:
        signal.signal(signal.SIGALRM, signal.SIG_IGN)

def raise_timeout(signum, frame):
    raise TimeoutError

def run_query(con, query_id, query_spec, query_parameters, test):
    if test:
        print(f'Q{query_id}: {query_parameters}')
    start = time.time()
    cur = con.cursor()

    for key in query_parameters.keys():
        query_spec = query_spec.replace(f":{key}", query_parameters[key])

    try:
        with timeout(300):
            cur.execute(query_spec)
    except TimeoutError:
        return
    results = cur.fetchall()
    end = time.time()
    duration = end - start
    if test:
        print(f"-> {duration:.4f} seconds")
        print(f"-> {results}")
    return (results, duration)

def convert_to_datetime(timestamp):
    dt = datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%S.%f+00:00")
    return f"'{dt}'::timestamp"

def convert_to_date(timestamp):
    dt = datetime.strptime(timestamp, '%Y-%m-%d')
    return f"'{dt}'::date"


if len(sys.argv) < 2:
    print("Usage: bi.py <sf> [--test]")

sf = sys.argv[1]

test = False
if len(sys.argv) > 2:
    if sys.argv[2] == "--test":
        test = True

con = psycopg2.connect(host="localhost", port=5432, user="postgres", password="mysecretpassword", dbname="ldbcsnb")


for query_variant in ["1", "2a", "2b", "3", "4", "5", "6", "7", "8a", "8b", "9", "10a", "10b", "11", "12", "13", "14a", "14b", "15a", "15b", "16a", "16b", "17", "18"]: #, "19a", "19b", "20"
    query_num = int(re.sub("[^0-9]", "", query_variant))
    query_subvariant = re.sub("[^ab]", "", query_variant)

    print(f"========================= Q {query_num:02d}{query_subvariant.rjust(1)} =========================")
    query_file = open(f'queries/bi-{query_num}.sql', 'r')
    query_spec = query_file.read()

    parameters_csv = csv.DictReader(open(f'../parameters/bi-{query_variant}.csv'), delimiter='|')

    i = 0
    for query_parameters in parameters_csv:
        i = i + 1

        # convert fields based on type designators
        query_parameters = {k: f"{v}::bigint"         if re.match('.*:(ID|LONG)', k)       else v for k, v in query_parameters.items()}
        query_parameters = {k: convert_to_date(v)     if re.match('.*:DATE$', k)           else v for k, v in query_parameters.items()}
        query_parameters = {k: convert_to_datetime(v) if re.match('.*:DATETIME', k)        else v for k, v in query_parameters.items()}
        query_parameters = {k: f"'{v}'"               if re.match('.*:STRING([^[]|$)', k)  else v for k, v in query_parameters.items()}
        query_parameters = {k:
            "ARRAY["
            + ', '.join([f"'{e}'" for e in v.split(';') ])
            + "]::varchar[]"
            if re.findall('\[\]$', k) else v for k, v in query_parameters.items()}
        # drop type designators
        type_pattern = re.compile(':.*')
        query_parameters = {type_pattern.sub('', k): v for k, v in query_parameters.items()}
        run_query(con, query_variant, query_spec, query_parameters, test)
        if test or i == 10:
            break

con.close()
