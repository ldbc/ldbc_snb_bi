# LDBC SNB BI TigerGraph/GSQL implementation

** This section is for single-node benchmark. For benchmark on cluster, refer to the k8s setup in `k8s/`.**
[TigerGraph](https://www.tigergraph.com) implementation of the [LDBC SNB benchmark](https://github.com/ldbc/ldbc_snb_docs). The scripts are modified from [Old version driver](https://github.com/tigergraph/ecosys/tree/ldbc/ldbc_benchmark/tigergraph/queries_v3)

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
        ./target/ldbc_snb_datagen_${PLATFORM_VERSION}-${DATAGEN_VERSION}.jar -- \
        -- \
        --format csv \
        --scale-factor ${SF} \
        --explode-edges \
        --mode bi \
        --output-dir out-sf${SF}/ \
        --generate-factors \
        --format-options header=false
    ```

1. To download and use the sample data set, run:

    ```bash
    wget -q https://ldbcouncil.org/ldbc_snb_datagen_spark/social-network-sf0.003-bi-composite-projected-fk.zip
    unzip -q social-network-sf0.003-bi-composite-projected-fk.zip
    ```

## Load data

1. In `scripts/var.sh`, set 
    * `TG_DATA_DIR` - a folder containing `initial_snapshot`, `inserts` and `deletes`. 
        - for the sample data, is `[Download Location]/social-network-sf0.003-bi-composite-projected-fk/graphs/csv/bi/composite-projected-fk/`
    * `TG_LICENSE` - optional, trial license is sufficient for SF-30 and smaller.
    * `SF` - optional
    * If your CSVs have headers, set `TG_HEADER` to `true`.
    
2. Load the data 
    ```bash
    ./load-in-one-step.sh
    ```
    This step may take a while. This step is responisble for defining the schema, loading data, and installing queries. The tigergraph container's terminal can be accessed via using Docker command `docker exec --user tigergraph -it snb-bi-tg bash`. If web browser is availble, you can explore the graph using TigerGraph GraphStudio in the browser: `http://localhost:14240/`.

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

Results are written to `output/results.csv` and `output/time.csv`.
