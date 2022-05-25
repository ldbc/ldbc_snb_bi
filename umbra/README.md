# LDBC SNB BI Umbra implementation

[Umbra](https://umbra-db.com/) implementation of the [LDBC Social Network Benchmark's BI workload](https://github.com/ldbc/ldbc_snb_docs).

## Getting the container

The Umbra container is currently available upon request.

## Generating the data set

The Umbra implementation expects the data to be in `composite-merged-fk` CSV layout, with headers and without quoted fields.
To generate data that confirms this requirement, run Datagen without any layout or formatting arguments (`--explode-*` or `--format-options`).

In Datagen's directory (`ldbc_snb_datagen_spark`), issue the following commands. We assume that the Datagen project is built and the `${PLATFORM_VERSION}`, `${DATAGEN_VERSION}` environment variables are set correctly.

```bash
export SF=desired_scale_factor
export LDBC_SNB_DATAGEN_MAX_MEM=available_memory
```

```bash
rm -rf out-sf${SF}/
tools/run.py \
    --cores $(nproc) \
    --memory ${LDBC_SNB_DATAGEN_MAX_MEM} \
    ./target/ldbc_snb_datagen_${PLATFORM_VERSION}-${DATAGEN_VERSION}.jar \
    -- \
    --format csv \
    --scale-factor ${SF} \
    --mode bi \
    --output-dir out-sf${SF} \
    --generate-factors
```

## Loading the data set

Note that unlike Postgres, Umbra does not support directly loading from compressed files (`.csv.gz`).

1. Set the `${UMBRA_CSV_DIR}` environment variable to point to the data set.

    * To use a locally generated data set, set the `${LDBC_SNB_DATAGEN_DIR}` and `${SF}` environment variables and run:

        ```bash
        export UMBRA_CSV_DIR=${LDBC_SNB_DATAGEN_DIR}/out-sf${SF}/graphs/csv/bi/composite-merged-fk/
        ```

        Or, simply run:

        ```bash
        . scripts/use-datagen-data-set.sh
        ```

    * To download and use the sample data set, run:

        ```bash
        wget -q https://ldbcouncil.org/ldbc_snb_datagen_spark/social-network-sf0.003-bi-composite-merged-fk.zip
        unzip -q social-network-sf0.003-bi-composite-merged-fk.zip
        export UMBRA_CSV_DIR=`pwd`/social-network-sf0.003-bi-composite-merged-fk/graphs/csv/bi/composite-merged-fk/
        ```

        Or, simply run:

        ```
        scripts/get-sample-data-set.sh
        . scripts/use-sample-data-set.sh
        ```

1. To start the DBMS, create a database and load the data, run:

    ```bash
    scripts/load-in-one-step.sh
    ```

1. The substitution parameters should be generated using the [`paramgen`](../paramgen).

## Queries

To run the queries, issue:

```bash
scripts/queries.sh
```

For a test run, use:

```bash
scripts/queries.sh ${SF} --test
```

## Benchmark

To run the queries and the batches alternately, as specified by the benchmark, run:

```bash
scripts/benchmark.sh
```

## Running queries interactively

To connect to the database through the SQL console, use:

```bash
scripts/connect.sh
```
