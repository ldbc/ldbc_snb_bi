# LDBC SNB BI Umbra implementation

[Umbra](https://umbra-db.com/) implementation of the [LDBC Social Network Benchmark's BI workload](https://github.com/ldbc/ldbc_snb_docs).

## Building the container

Set the `UMBRA_URL` environment variable and build the container:

```bash
export UMBRA_URL=
scripts/build-container.sh
```

### Loading the data set

Umbra uses the same format as [Postgres](../postgres/README.md#generating-the-data-set), however, it currently does not support loading from compressed files (`.csv.gz`).

1. Set the `${UMBRA_CSV_DIR}` environment variable to point to the data set, e.g.:

    ```bash
    export UMBRA_CSV_DIR=
    ```

    To use the test data set, run:

    ```bash
    wget -q https://ldbcouncil.org/ldbc_snb_datagen_spark/social-network-sf0.003-bi-composite-merged-fk.zip
    unzip -q social-network-sf0.003-bi-composite-merged-fk.zip
    export UMBRA_CSV_DIR=`pwd`/social-network-sf0.003-bi-composite-merged-fk/graphs/csv/bi/composite-merged-fk/
    ```

1. To start the DBMS, create a database and load the data, run:

    ```bash
    scripts/load-in-one-step.sh
    ```

## Microbatches

Test loading the microbatches:

```bash
scripts/batches.sh
```

:warning: Deletions currently do not work, see the `#TODO` entries in `batches.py`.

## Queries

To run the queries, issue:

```bash
scripts/bi.sh
```

## Running queries interactively

To connect to the database through the SQL console, use:

```bash
scripts/connect.sh
```
