import os
import duckdb

temporal_parquet_path = "temporal/"
con = duckdb.connect(database='temporal.duckdb')

print("============ Loading the temporal tables ============")
for entity in ["Person", "Person_knows_Person"]:
    print(f"{entity}")
    for parquet_file in [f for f in os.listdir(f"{temporal_parquet_path}{entity}") if f.endswith(".snappy.parquet")]:
        print(f"- {parquet_file}")
        con.execute(f"DROP TABLE IF EXISTS {entity}_temporal;")
        con.execute(f"CREATE TABLE {entity}_temporal AS SELECT * FROM '{temporal_parquet_path}{entity}/{parquet_file}';")
