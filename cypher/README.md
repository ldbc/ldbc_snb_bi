# LDBC SNB Cypher implementation

[Cypher](http://www.opencypher.org/) implementation of the [LDBC SNB benchmark](https://github.com/ldbc/ldbc_snb_docs).
Note that some BI queries are not expressed using pure Cypher, instead, they make use of the [APOC](https://neo4j.com/labs/apoc/) and [Graph Data Science](https://neo4j.com/product/graph-data-science-library/) Neo4j libraries.

## Loading the data in Neo4j

The Neo4j instance is run in Docker.

## Generating the data set

The Neo4j implementation expects the data to be in `composite-projected-fk` CSV layout, without headers and with quoted fields.
To generate data that confirms this requirement, run Datagen with the `--explode-edges` and the `--format-options header=false,quoteAll=true` options.

(Rationale: Files should not have headers as these are provided separately (in the `headers/` directory) and quoting the fields in the CSV is required to [preserve trailing spaces](https://neo4j.com/docs/operations-manual/4.2/tools/neo4j-admin-import/#import-tool-header-format).)

In Datagen's directory (`ldbc_snb_datagen_spark`), issue the following command:

```bash
# build
tools/build.sh

# set the desired SF and generate
export SF=0.003
rm -rf sf${SF}/
tools/run.py ./target/ldbc_snb_datagen_${PLATFORM_VERSION}-${DATAGEN_VERSION}.jar -- --format csv --scale-factor ${SF} --mode bi --explode-edges --format-options header=false,quoteAll=true --output-dir sf${SF}
export NEO4J_CSV_DIR=`pwd`/sf${SF}/csv/bi/composite-projected-fk/
```

## Loading the data

To load the data, issue the following commands in this repository:

```bash
# initialize variables
. scripts/environment-variables-default.sh

# load
scripts/load-in-one-step.sh
```

To load the microbatches, run:

```bash
# perform microbatch loading
python3 batches-cypher.py ${NEO4J_CSV_DIR}
```
