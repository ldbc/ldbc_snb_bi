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

## Use th sample data set
To download and use the sample data set, run:

```bash
wget -q https://ldbcouncil.org/ldbc_snb_datagen_spark/social-network-sf0.003-bi-composite-projected-fk.zip
unzip -q social-network-sf0.003-bi-composite-projected-fk.zip
export TG_DATA_DIR=`pwd`/social-network-sf0.003-bi-composite-projected-fk/graphs/csv/bi/composite-projected-fk/
```


## Loading the data

If you work on a cluster, refer to the k8s setup in `k8s/`. This section is for single-node benchmark.

Edit `scripts/var.sh`, and set the `TG_DATA_DIR` to your data directory:
```bash
export TG_DATA_DIR=${LDBC_SNB_DATAGEN_DIR}/out-sf${SF}/csv/bi/composite-projected-fk/
```

In the default setting, TigerGraph uses trial license and this license can hold at most 100GB data. For SF-100 and larger, please set `$TG_LICENSE` to the license string in `scripts/var.sh`. 
In the default setting, the driver assumes that the CSV files do not have headers. If your CSVs have headers, please modify `$TG_HEADER` in `scripts/vars.sh`. To load data

```bash
./load-in-one-step.sh
```

This step may take a while, as it starts a TigerGraph server container in docker. In the container, we define the schema, load data, and install queries. By default, the container's terminal can be accessed via using Docker command `docker exec --user tigergraph -it snb-bi-tg bash`. If web browser is availble, you can explore the graph using TigerGraph GraphStudio in the browser: `http://localhost:14240/`.

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
