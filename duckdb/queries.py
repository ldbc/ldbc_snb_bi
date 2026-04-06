import datetime
import json
import os
import re
import time

def run_script(con, filename):
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
            con.execute(query)
            start = time.time()
            end = time.time()
            duration = end - start
