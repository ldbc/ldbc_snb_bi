# LDBC SNB BI Umbra implementation

[Umbra](https://umbra-db.com/) implementation of the [LDBC Social Network Benchmark's BI workload](https://github.com/ldbc/ldbc_snb_docs).

## Building the container

Set the `UMBRA_URL` environment variable and build the container:

```bash
export UMBRA_URL=
scripts/build.sh
```

### Loading the data set

Umbra uses the same format as [Postgres](../postgres/README.md#generating-the-data-set)

1. Set the `${UMBRA_CSV_DIR}` environment variable to point to the data set, e.g.:

    ```bash
    export UMBRA_CSV_DIR=`pwd`/../postgres/test-data/
    ```

2. To start the DBMS, create a database and load the data, run:

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
