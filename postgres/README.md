# LDBC SNB PostgreSQL implementation

[PostgreSQL](https://www.postgresql.org/) implementation of the [LDBC SNB BI benchmark](https://github.com/ldbc/ldbc_snb_docs).

## Start the database

```bash
export POSTGRES_DATA_DIR=`pwd`/../../ldbc_snb_data_converter/data/csv-composite-merged-fk/
./start.sh
./load.sh
```
