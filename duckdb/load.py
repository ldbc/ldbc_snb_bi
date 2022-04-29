import sys

print(sys.path)
sys.path.remove('/usr/lib/python3.8/site-packages/duckdb-0.2.2.dev6998+g8f0019d39-py3.8-linux-x86_64.egg')
print(sys.path)
import duckdb

print(duckdb.__file__)
print(duckdb.__version__)


con = duckdb.connect(":memory:")
con.execute("CREATE TABLE Customer(cid bigint not null, name string)");
con.execute("CREATE TABLE Transfers(tid bigint, from_id bigint, to_id bigint, amount bigint);")
con.execute("INSERT INTO Customer VALUES (0, 'A'), (1, 'B'), (2, 'C'), (3, 'D'), (4, 'E'), (5, 'F'), (6, 'G');")
con.execute("INSERT INTO Transfers VALUES "
            " (955, 0, 1, 150), "
            " (769, 0, 3, 50), "
            " (607, 1, 4, 250), "
            " (184, 1, 6, 350), "
            "  (955, 2, 5, 150), "
            " (769, 3, 0, 100), "
            "  (769, 3, 2, 50), "
            " (607, 4, 5, 250), "
            "  (955, 5, 2, 150), "
            "  (769, 6, 2, 50), "
            " (769, 6, 3, 50), "
            "  (769, 6, 4, 50);")

con.execute("SELECT CREATE_CSR_VERTEX( "
            "0,"
            "v.vcount, "
            "sub.dense_id, "
            "sub.cnt "
            ") AS numEdges "
            "FROM ( "
            "SELECT c.rowid as dense_id, count(t.from_id) as cnt "
            "FROM Customer c "
            "LEFT JOIN  Transfers t ON t.from_id = c.cid "
            "GROUP BY c.rowid "
            ") sub,  (SELECT count(c.cid) as vcount FROM Customer c) v")

con.execute("SELECT min(CREATE_CSR_EDGE(0, (SELECT count(c.cid) as vcount FROM Customer c), "
            "CAST ((SELECT sum(CREATE_CSR_VERTEX(0, (SELECT count(c.cid) as vcount FROM Customer c), "
            "sub.dense_id , sub.cnt )) AS numEdges "
            "FROM ( "
            "    SELECT c.rowid as dense_id, count(t.from_id) as cnt "
            "    FROM Customer c "
            "    LEFT JOIN  Transfers t ON t.from_id = c.cid "
            "    GROUP BY c.rowid "
            ") sub) AS BIGINT), "
            "src.rowid, dst.rowid)) "
            "FROM "
            "  Transfers t "
            "JOIN Customer src ON t.from_id = src.cid "
            "  JOIN Customer dst ON t.to_id = dst.cid; ")

con.execute("CREATE TABLE src_dest(id int, v_size bigint, src bigint, dst bigint);")

con.execute("INSERT INTO src_dest VALUES(0, 7, 0, 5), (0, 7, 1, 5), (0, 7, 1, 6), (0, 7, 1, 1), (0, 7, 1, 0), (0, 7, 3, 0), (0, 7, 0, 2), (0, 7, 0, 6);")

con.execute("SELECT s.src, s.dst, cheapest_path(s.id, false, v_size, s.src, s.dst) FROM src_dest s")

# import sys
# import os
# import re
# import time
#
# if len(sys.argv) < 2:
#     print("Umbra loader script")
#     print("Usage: load.py <UMBRA_DATA_DIR> [--compressed]")
#     exit(1)
#
# def run_script(cur, filename):
#     TODO
#
#
# run_script(cur, "ddl/drop-tables.sql")
# run_script(cur, "ddl/schema-composite-merged-fk.sql")
# print("Load initial snapshot")
#
# # initial snapshot
# static_path = f"{data_dir}/initial_snapshot/static"
# dynamic_path = f"{data_dir}/initial_snapshot/dynamic"
# static_entities = ["Organisation"]
# dynamic_entities = ["Person", "Person_knows_Person", "Person_studyAt_University", "Person_workAt_Company"]
#
# dbs_data_dir => set this based on the ${...}/out-sf0.1/csv/bi/composite-merged-fk
#
# if local:
#     dbs_data_dir = data_dir
# else:
#     dbs_data_dir = '/data'
#
# print("## Static entities")
# for entity in static_entities:
#     for csv_file in [f for f in os.listdir(f"{static_path}/{entity}") if f.startswith("part-") and f.endswith(".csv")]:
#         csv_path = f"{entity}/{csv_file}"
#         print(f"- {csv_path}")
#         cur.execute(f"COPY {entity} FROM '{dbs_data_dir}/initial_snapshot/static/{entity}/{csv_file}' (DELIMITER '|', HEADER, FORMAT csv)")
#         pg_con.commit()
# print("Loaded static entities.")
#
# print("## Dynamic entities")
# for entity in dynamic_entities:
#     for csv_file in [f for f in os.listdir(f"{dynamic_path}/{entity}") if f.startswith("part-") and f.endswith(".csv")]:
#         csv_path = f"{entity}/{csv_file}"
#         print(f"- {csv_path}")
#         cur.execute(f"COPY {entity} FROM '{dbs_data_dir}/initial_snapshot/dynamic/{entity}/{csv_file}' (DELIMITER '|', HEADER, FORMAT csv)")
#         if entity == "Person_knows_Person":
#             cur.execute(f"COPY {entity} (creationDate, Person2id, Person1id) FROM '{dbs_data_dir}/initial_snapshot/dynamic/{entity}/{csv_file}' (DELIMITER '|', HEADER, FORMAT csv)")
#         pg_con.commit()
# print("Loaded dynamic entities.")
