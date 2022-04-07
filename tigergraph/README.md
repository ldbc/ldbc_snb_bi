# LDBC SNB BI TigerGraph/GSQL implementation

[TigerGraph](https://www.tigergraph.com) implementation of the [LDBC SNB benchmark](https://github.com/ldbc/ldbc_snb_docs). The scripts are modified from [Old version driver](https://github.com/tigergraph/ecosys/tree/ldbc/ldbc_benchmark/tigergraph/queries_v3)

## Generating the data set

The TigerGraph implementation expects the data to be in `composite-projected-fk` CSV layout. To generate data that confirms this requirement, run Datagen with the `--explode-edges` option. Both data with headers and without headers (`--format-options header=false`) are supported.

In Datagen's directory (`ldbc_snb_datagen_spark`), issue the following commands. We assume that the Datagen project is built and the `${PLATFORM_VERSION}`, `${DATAGEN_VERSION}` environment variables are set correctly.

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

## Loading the data

Set the `${TG_DATA_DIR}` environment variable. E.g., assuming that your `${LDBC_SNB_DATAGEN_DIR}` and `${SF}` environment variables are set, run:

```bash
export TG_DATA_DIR=${LDBC_SNB_DATAGEN_DIR}/out-sf${SF}/csv/bi/composite-projected-fk/
```

To download and use the sample data set, run:

```bash
wget -q https://ldbcouncil.org/ldbc_snb_datagen_spark/social-network-sf0.003-bi-composite-projected-fk.zip
unzip -q social-network-sf0.003-bi-composite-projected-fk.zip
export TG_DATA_DIR=`pwd`/social-network-sf0.003-bi-composite-projected-fk/graphs/csv/bi/composite-projected-fk/
```

In the default setting, the driver assumes that the CSV files do not have headers. If your CSVs have headers, please modify TG_HEADER in `scripts/vars.sh`.

Start the TigerGraph Docker container. For data larger than 50G, a license need to be applied after running `start.sh`. If you work on a cluster, refer to the k8s setup in `k8s/`.

```bash
scripts/stop-docker.sh #if there is an existing container
scripts/start-docker.sh
```

Load the data. This step may take a while (several minutes), as it is responsible for defining the queries, loading jobs, loading the data, installing queries and pre-compute the edge weights for BI 19 and 20. After the database is ready, you can explore the graph using TigerGraph GraphStudio in the browser: `http://localhost:14240/`. By default, the docker terminal can be accessed via `ssh tigergraph@localhost -p 14022` with password tigergraph, or using Docker command `docker exec --user tigergraph -it snb-interactive-tigergraph bash`.

```bash
scripts/setup.sh
```

The above scripts can be executed with a single command:
```bash
scripts/load-in-one-step.sh
```

## Microbatches

Test loading the microbatches:

```bash
scripts/batches.sh
```

:warning: Note that this script uses the data sets in the `${TG_DATA_DIR}` directory on the host machine. The data is modified here. Therefore, **the database needs to be reloaded or restored from backup before each run**. Use the provided `scripts/backup-database.sh` and `scripts/restore-database.sh` scripts to achieve this.
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
