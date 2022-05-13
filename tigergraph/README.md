# LDBC SNB BI TigerGraph/GSQL implementation
**The instruction is for single-node benchmark.**
We provide two methods for cluster setup.
  1. [k8s/README.md](./k8s) - Deploy TG containers using [kubernetes (k8s)](https://kubernetes.io) 
  1. [benchmark_on_cluster/README.md](./benchmark_on_cluster) - Manually install and configure TigerGraph

[[Old Benchmark link]](https://github.com/tigergraph/ecosys/tree/ldbc/ldbc_benchmark/tigergraph/queries_v3)

## Generating the data set

1. The TigerGraph implementation expects the data to be in `composite-projected-fk` CSV layout. To generate data that confirms this requirement, run Datagen with the `--explode-edges` option.  In Datagen's directory (`ldbc_snb_datagen_spark`), issue the following commands. We assume that the Datagen project is built and the `${PLATFORM_VERSION}`, `${DATAGEN_VERSION}` environment variables are set correctly.

    ```bash
    export SF=desired_scale_factor
    ```

    ```bash
    rm -rf out-sf${SF}/
    tools/run.py \
        --cores 4 \
        --memory 8G \
        ./target/ldbc_snb_datagen_${PLATFORM_VERSION}-${DATAGEN_VERSION}.jar \
        -- \
        --format csv \
        --scale-factor ${SF} \
        --explode-edges \
        --mode bi \
        --output-dir out-sf${SF}/ \
        --generate-factors \
        --format-options compression=gzip
    ```

## Load data

1. To download and use the sample data set, run:

    ```bash
    wget -q https://ldbcouncil.org/ldbc_snb_datagen_spark/social-network-sf0.003-bi-composite-projected-fk.zip
    unzip -q social-network-sf0.003-bi-composite-projected-fk.zip
    . scripts/use-sample-data-set.sh 
    ```

    To use other data sets, adjust the variables in [scripts/use-sample-data-set.sh](scripts/use-sample-data-set.sh):

    * `TG_DATA_DIR` - a folder containing `initial_snapshot`, `inserts` and `deletes`.
    * `TG_LICENSE` - optional, trial license is used if not specified, sufficient for SF-30 and smaller.
    * If CSV files have headers, set `export TG_HEADER=true`, otherwise set to `false`.
    * Run:

        ```bash
        . scripts/use-sample-data-set.sh
        ```

1. Load the data:

    ```bash
    ./load-in-one-step.sh
    ```

    This step may take a while, as it is responsible for defining the schema, loading the data and installing the queries. The TigerGraph container terminal can be accessed via:
    
    ```bash
    docker exec --user tigergraph -it snb-bi-tg bash
    ```

    If a web browser is available, TigerGraph GraphStudio can be accessed via <http://localhost:14240/>.

1. The substitution parameters should be generated using the [`paramgen`](../paramgen).

## Microbatches

Test loading the microbatches:

```bash
scripts/batches.sh
```

:warning: Note that the data in TigerGraph database is modified. Therefore, **the database needs to be reloaded or restored from backup before each run**. Use the provided `scripts/backup-database.sh` and `scripts/restore-database.sh` scripts to achieve this.

## Queries

To run the queries, issue:

```bash
scripts/queries.sh
```

For a test run, use:

```bash
scripts/queries.sh --test
```

Results are written to `output/results.csv` and `output/timings.csv`.

## Benchmarks

To run the benchmark, issue:

```bash
scripts/benchmark.sh
```

## About the TigerGraph Implementation
1. Because the current TigerGraph datetime use ecpoch in seconds but the datetime in LDBC SNB benchmarks is in milliseconds. So we store the datetime as INT64 in the datatime and write user defined functions to do conversion. The dateime value in the dataset is considered as the local time. INT64 datetime in millisecond `value` can be converted to datetime using `datetime_to_epoch(value/1000)`.
1. The user defined function is in `ExprFunctions.hpp` (for query) and `TokenBank.cpp` (for loader).
1. We add additional attribute `maxMember` in Forum for pre-computation of BI-4, and attribute `popularityScore` in Person for pre-computation of BI-6
1. We also added additional edges `KNOWS15`, `KNOWS19` and `KNOWS20` to store the weight on KNOWS edges. The edge weight can be pre-computed for BI-19 and BI-20. For BI-15, the edge weight need to be calculated every time before the query run. 
1. TigerGraph uses accumulators and the vertex-attached local accumulators give good performance on clusters. Most aggregation operations are achieved using local accumulators. Currently, the path patterns in TigerGraph are executed from left to right hand side. To filter paths, it is important to start from a highly selective endpoints and then reach out to others.