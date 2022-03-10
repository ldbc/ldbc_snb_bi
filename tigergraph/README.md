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

In the default setting, the driver assumes that the CSV files do not have headers. **If your CSVs have headers,** set:

```bash
export TG_HEADER=true
```

Start the TigerGraph Docker container. For data larger than 50G, you need a license. If you work on a cluster, you can install TigerGraph manually and skip this step.

```bash
scripts/stop-docker.sh #if there is an existing container
scripts/start-docker.sh
```

Load the data. This step may take a while (several minutes), as it is responsible for defining the queries, loading jobs, loading the data, installing (optimizing and compiling on the server) queries and pre-compute the edge weights for BI 19 and 20.

```bash
scripts/setup.sh
```

## Microbatches

Test loading the microbatches:

```bash
scripts/batches.sh
```

## Queries

To run and validate the queries.

```bash
scripts/validate.sh
```

Results are written to `results/validation_params.csv`.
