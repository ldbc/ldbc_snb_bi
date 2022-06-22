import logging
import os

from load import load_entities_csv, run_script, process_arguments
import duckdb
import sys


def main(argv):
    sf, query, _, _, _, _, _, _ = process_arguments(argv)

    for sf in [10]:
        con = duckdb.connect("snb_benchmark.duckdb", read_only=False)
        run_script(con, "ddl/drop-tables.sql")
        run_script(con, "ddl/schema-composite-merged-fk.sql")
        data_dir = f'../../ldbc_snb_datagen_spark/out-sf{sf}/graphs/csv/bi/composite-merged-fk'
        load_entities_csv(con, data_dir, query)
        logging.debug("writing to parquet")
        out_dir = f'../../ldbc_snb_datagen_spark/out-sf{sf}/parquet'
        if not os.path.exists(out_dir):
            os.makedirs(out_dir)

        con.execute(f"EXPORT DATABASE '{out_dir}' (FORMAT PARQUET);")


if __name__ == "__main__":
    logging.basicConfig(format='%(process)d-%(levelname)s-%(message)s', level=logging.DEBUG)
    main(sys.argv[1:])
