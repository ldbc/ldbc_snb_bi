# LDBC SNB BI Neo4j/Cypher implementation

[Cypher](http://www.opencypher.org/) implementation of the [LDBC SNB benchmark](https://github.com/ldbc/ldbc_snb_docs).
Note that some BI queries are not expressed (efficiently) in Cypher so they make use of the [APOC](https://neo4j.com/labs/apoc/) and [Graph Data Science](https://neo4j.com/product/graph-data-science-library/) Neo4j libraries.

## Generating the data set

The Neo4j implementation expects the data to be in `composite-projected-fk` CSV layout, without headers and with quoted fields.
To generate data that confirms this requirement, run Datagen with the `--explode-edges` and the `--format-options header=false,quoteAll=true` options.

(Rationale: Files should not have headers as these are provided separately (in the `headers/` directory) and quoting the fields in the CSV is required to [preserve trailing spaces](https://neo4j.com/docs/operations-manual/4.3/tools/neo4j-admin-import/#import-tool-header-format).)

In Datagen's directory (`ldbc_snb_datagen_spark`), issue the following commands:

```bash
export SF=desired_scale_factor
```

```bash
rm -rf out-sf${SF}/
export SF=1
tools/build.sh
tools/run.py --cores 4 --memory 8G target/ldbc_snb_datagen_2.12_spark3.1-0.5.0-SNAPSHOT.jar -- --format csv --scale-factor ${SF} --explode-edges --mode bi --output-dir out-sf${SF}/ --generate-factors --format-options header=false,quoteAll=true
```

## Loading the data

Set the `${NEO4J_CSV_DIR}` environment variable.

```bash
export NEO4J_CSV_DIR=${LDBC_SNB_DATAGEN_DIR}/out-sf${SF}/graphs/csv/bi/composite-projected-fk/
```

If the data is compressed, set the following flag:

```bash
export NEO4J_CSV_FLAGS="--compressed"
```

To use the sample data set, run

```bash
wget -q https://ldbcouncil.org/ldbc_snb_datagen_spark/social-network-sf0.003-bi-composite-projected-fk-neo4j-compressed.zip
unzip -q social-network-sf0.003-bi-composite-projected-fk-neo4j-compressed.zip
export NEO4J_CSV_DIR=`pwd`/social-network-sf0.003-bi-composite-projected-fk-neo4j-compressed/graphs/csv/bi/composite-projected-fk/
export NEO4J_CSV_FLAGS="--compressed"
```

Load the data:

```bash
scripts/load-in-one-step.sh
```

## Microbatches

Test loading the microbatches:

```bash
scripts/batches.sh
```

:warning: Note that this script uses the data sets in the `${NEO4J_CSV_DIR}` directory on the host machine but maps the paths relative to the `/import` directory in the Docker container (Neo4j's dedicated import directory which it uses as the basis of the import paths in the `LOAD CSV` Cypher commands).
For example, the `${NEO4J_CSV_DIR}/deletes/dynamic/Post/batch_id=2012-09-13/part-x.csv` path is translated to the `deletes/dynamic/Post/batch_id=2012-09-13/part-x.csv` relative path.

## Queries

To run the queries, issue:

```bash
scripts/bi.sh ${SF}
```

For a test run, use:

```bash
scripts/bi.sh ${SF} --test
```

## Working with the database

To start a database that has already been loaded, run:

```bash
scripts/start.sh
```
