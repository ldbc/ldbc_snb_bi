# LDBC SNB PostgreSQL implementation

[PostgreSQL](https://www.postgresql.org/) implementation of the [LDBC SNB BI benchmark](https://github.com/ldbc/ldbc_snb_docs).

## Generating the data set

The Postgres implementation expects the data to be in `composite-merged-fk` CSV layout, with headers and without quoted fields.
To generate data that confirms this requirement, run Datagen without any layout or formatting arguments (`--explode-*` or `--format-options`).

In Datagen's directory (`ldbc_snb_datagen_spark`), issue the following commands:

```bash
tools/build.sh

# set the desired SF and generate
export SF=0.003
rm -rf sf${SF}/csv/bi/composite-merged-fk/
tools/run.py ./target/ldbc_snb_datagen_${PLATFORM_VERSION}-${DATAGEN_VERSION}.jar -- \
    --format csv --scale-factor ${SF} --mode bi --output-dir sf${SF}
export POSTGRES_CSV_DIR=`pwd`/sf${SF}/csv/bi/composite-merged-fk/
```

## Loading the data

To load the data, issue the following commands in this directory:

```
# initialize the database
scripts/start.sh
scripts/create-db.sh

# load and apply the microbatches
python3 load.py ${POSTGRES_CSV_DIR}

To load the microbatches, run:

```bash
python3 batches.py ${POSTGRES_CSV_DIR}
```
