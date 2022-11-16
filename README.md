![LDBC_LOGO](https://raw.githubusercontent.com/wiki/ldbc/ldbc_snb_datagen/images/ldbc-logo.png)

# LDBC SNB Business Intelligence (BI) workload implementations

[![Build Status](https://circleci.com/gh/ldbc/ldbc_snb_bi.svg?style=svg)](https://circleci.com/gh/ldbc/ldbc_snb_bi)

Implementations for the BI workload of the [LDBC Social Network Benchmark](https://ldbcouncil.org/ldbc_snb_docs/ldbc-snb-specification.pdf).

To get started with the LDBC SNB benchmarks, check out our introductory presentation: [The LDBC Social Network Benchmark](https://docs.google.com/presentation/d/1p-nuHarSOKCldZ9iEz__6_V3sJ5kbGWlzZHusudW_Cc/) ([PDF](https://ldbcouncil.org/docs/presentations/ldbc-snb-2021-12.pdf)).

:scroll: If you wish to cite the LDBC SNB, please refer to the [documentation repository](https://github.com/ldbc/ldbc_snb_docs#how-to-cite-ldbc-benchmarks) ([bib snippet](https://github.com/ldbc/ldbc_snb_docs/blob/dev/bib/specification.bib)).

## Implementations

The repository contains the following implementations:

* [`cypher`](cypher/): an implementation using the [Neo4j graph database management system](https://dbdb.io/db/neo4j) with queries expressed in the [Cypher language](https://neo4j.com/developer/cypher/)
* [`umbra`](umbra/): an implementation using the [Umbra JIT-compiled columnar relational database management system](https://dbdb.io/db/umbra) with expressed in SQL queries written in the PostgreSQL dialect
* [`tigergraph`](tigergraph/): an implementation using the [TigerGraph graph database management system](https://dbdb.io/db/tigergraph) with queries expressed in the [GSQL language](https://www.tigergraph.com/gsql/)

All implementations use Docker containers for ease of setup and execution. However, the setups can be adjusted to use a non-containerized DBMS.

## Reproducing SNB BI experiments

Running an SNB BI experiment requires the following steps.

1. Pick a system, e.g. [Umbra](umbra/). Make sure you have the required binaries and licenses available.

1. Generate the data sets using the [SNB Datagen](https://github.com/ldbc/ldbc_snb_datagen_spark/) according to the format described in the system's README.

1. Generate the substitution parameters using the [`paramgen`](paramgen/) tool.

1. Load the data set: set the required environment variables and run the tool's `scripts/load-in-one-step.sh` script.

1. Run the benchmark: set the required environment variables and run the tool's `scripts/benchmark.sh` script.

1. Collect the results in the [`output`](output/) directory of the tool.

:warning:
Note that deriving official LDBC results requires commissioning an _audited benchmark_, which is a more complex process as it entails code review, cross-validation, etc.
See [the specification's Auditing chapter](https://ldbcouncil.org/ldbc_snb_docs/ldbc-snb-specification.pdf#chapter.7) for details.

## Cross-validation

To cross-validate the results of two implementations, use two systems.
Load the data into both, then run the benchmark in validation mode, e.g. [Cypher](cypher/) and [Umbra](umbra/) results.
Then, run:

```bash
export SF=10

cd cypher
scripts/benchmark.sh --validate
cd ..

cd umbra
scripts/benchmark.sh --validate
cd ..

scripts/cross-validate.sh cypher umbra
```

Note that the cross-validation uses the [numdiff](scripts/numdiff.md) CLI tool.

## Usage

See [`.circleci/config.yml`](.circleci/config.yml) for an up-to-date example on how to use the projects in this repository.

## Data sets

[Pre-generated data sets and parameters](snb-bi-pre-generated-data-sets.md) are now available for
SF1-SF10,000 (projected FK variant) and
SF1-SF1,000 (merged FK variant).

Data sets are also being uploaded to the [SURF CWI repository](https://repository.surfsara.nl/datasets/cwi/snb) and expected to be published there in 2023.
