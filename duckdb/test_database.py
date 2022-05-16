import duckdb
import load

print(duckdb.__version__)

def main():
    sf = "0.1"
    con = duckdb.connect("snb_benchmark.duckdb", read_only=False)
    load.run_script(con, "ddl/drop-tables.sql")
    load.run_script(con, "ddl/schema-composite-merged-fk.sql")
    data_dir = f'../../ldbc_snb_datagen_spark/out-sf{sf}/graphs/csv/bi/composite-merged-fk'

    load.load_entities(con, data_dir, 20) # TODO Add query
    # file_location = f"queries/test_database.sql"
    # queries = open(file_location).read().split(";")
    #
    # for query in queries:
    #     print(con.execute(query))

if __name__ == "__main__":
    main()