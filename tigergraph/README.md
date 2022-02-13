# LDBC SNB BI TigerGraph/GSQL implementation

[TigerGraph](https://www.tigergraph.com) implementation of the [LDBC SNB benchmark](https://github.com/ldbc/ldbc_snb_docs). The scripts are modified from [Old version driver](https://github.com/tigergraph/ecosys/tree/ldbc/ldbc_benchmark/tigergraph/queries_v3)

## Generating the data set

The TigerGraph implementation expects the data to be in `composite-projected-fk` CSV layout, without headers and with quoted fields.
To generate data that confirms this requirement, run Datagen with the `--explode-edges` and the `--format-options header=false,quoteAll=true` options. Both data with headers and without headers are supported. 

In Datagen's directory (`ldbc_snb_datagen_spark`), issue the following commands:

```bash
tools/build.sh

# set the desired SF and generate
export SF=0.003
rm -rf sf${SF}/
tools/run.py ./target/ldbc_snb_datagen_${PLATFORM_VERSION}-${DATAGEN_VERSION}.jar -- \
    --format csv --scale-factor ${SF} --mode bi --explode-edges --format-options header=false,quoteAll=true --output-dir sf${SF}
```

## Loading the data

Set the `$TG_DATA_DIR` environment variable.
```bash
export TG_DATA_DIR=`pwd`/sf${SF}/csv/bi/composite-projected-fk/
```

In the default setting, the driver consider the dataset does not have headers. If the data has header, set `export TG_HEADER=true`.

Start TigerGraph docker container. For data larger than 50G, you need a license. If you work on a cluster, you can install TigerGraph manually and skip this step.
```
scripts/stop-docker.sh #if there is an existing container
scripts/start-docker.sh
```

Load the data, This step may take a while (several minutes), as it is responsible for defining the queries, loading jobs, loading the data, installing (optimizing and compiling on the server) queries and pre-compute the edge weights for BI 19 and 20.

```bash
scripts/setup.sh
```

## Microbatches

Test loading the microbatches:
```bash
scripts/batches.sh
```

## Queries

To run and validate the queries:

```bash
scripts/validate.sh
```