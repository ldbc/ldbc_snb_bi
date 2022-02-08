# LDBC SNB BI PostgreSQL implementation

[PostgreSQL](https://www.postgresql.org/) implementation of the [LDBC SNB BI benchmark](https://github.com/ldbc/ldbc_snb_docs).

## Generating the data set

The Postgres implementation expects the data to be in `composite-merged-fk` CSV layout, with headers and without quoted fields.
To generate data that confirms this requirement, run Datagen without any layout or formatting arguments (`--explode-*` or `--format-options`).

In Datagen's directory (`ldbc_snb_datagen_spark`), issue the following commands:

```bash
tools/build.sh

# set the desired SF and generate
export SF=0.003
rm -rf sf${SF}/
tools/run.py ./target/ldbc_snb_datagen_${PLATFORM_VERSION}-${DATAGEN_VERSION}.jar -- \
    --format csv --scale-factor ${SF} --mode bi --output-dir sf${SF}
```

## Loading the data

Set the `POSTGRES_CSV_DIR` environment variable.

```bash
export POSTGRES_CSV_DIR=${DATAGEN_DIRECTORY}/sf${SF}/graphs/csv/bi/composite-merged-fk/
```

If the data is compressed, set:

```bash
export POSTGRES_CSV_FLAGS="--compressed"
```

To use the sample data set, run:

```bash
wget -q https://ldbcouncil.org/ldbc_snb_datagen_spark/social-network-sf0.003-bi-composite-merged-fk-postgres-compressed.zip
unzip -q social-network-sf0.003-bi-composite-merged-fk-postgres-compressed.zip
export POSTGRES_CSV_DIR=`pwd`/social-network-sf0.003-bi-composite-merged-fk-postgres-compressed/graphs/csv/bi/composite-merged-fk/
export POSTGRES_CSV_FLAGS="--compressed"
```

Load the data:

```bash
scripts/load-in-one-step.sh
```

## Microbatches

Test loading the microbatches:

```bash
scripts/batches.sh
```

## Queries

To run the queries, issue:

```bash
scripts/bi.sh
```
