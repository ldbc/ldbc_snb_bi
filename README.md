![LDBC_LOGO](https://raw.githubusercontent.com/wiki/ldbc/ldbc_snb_datagen/images/ldbc-logo.png)

# LDBC SNB Business Intelligence (BI) workload implementations

[![Build Status](https://circleci.com/gh/ldbc/ldbc_snb_bi.svg?style=svg)](https://circleci.com/gh/ldbc/ldbc_snb_bi)

Implementations for the BI workload of the [LDBC Social Network Benchmark](https://ldbcouncil.org/ldbc_snb_docs/).

To get started with the LDBC SNB benchmarks, check out our introductory presentation: [The LDBC Social Network Benchmark](https://docs.google.com/presentation/d/1p-nuHarSOKCldZ9iEz__6_V3sJ5kbGWlzZHusudW_Cc/) ([PDF](https://ldbcouncil.org/docs/presentations/ldbc-snb-2021-12.pdf)).

:scroll: If you wish to cite the LDBC SNB, please refer to the [documentation repository](https://github.com/ldbc/ldbc_snb_docs#how-to-cite-ldbc-benchmarks) ([bib snippet](https://github.com/ldbc/ldbc_snb_docs/blob/dev/bib/specification.bib)).

## Implementations

The repository contains the following implementations:

* [`cypher`](cypher/): queries are expressed in the [Cypher language](https://neo4j.com/developer/cypher/) and run in the [Neo4j graph database management system](https://dbdb.io/db/neo4j)
* [`umbra`](umbra/): queries are expressed in SQL and run in [Umbra JIT-compiled columnar relational database management system](https://dbdb.io/db/umbra).
* [`tigergraph`](tigergraph/): queries are expressed in the [GSQL language](https://www.tigergraph.com/gsql/) and run in the [TigerGraph graph database management system](https://tigergraph.com/)

All implementations use Docker containers for ease of setup and execution. However, the setups can be adjusted to use a non-containerized DBMS.

## Reproducing SNB BI experiments

Running an SNB BI experiment requires the following steps.

1. Pick a tool, e.g. [Umbra](umbra/). Make sure you have the required binaries and licenses available.

1. Generate the data sets using the [SNB Datagen](https://github.com/ldbc/ldbc_snb_datagen_spark/) according to the format described in the tool's README.

1. Generate the substitution parameters using the [`paramgen`](paramgen/) tool.

1. Load the data set: set the required environment variables and run the tool's `scripts/load-in-one-step.sh` script.

1. Run the benchmark: set the required environment variables and run the tool's `scripts/benchmark.sh` script.

1. Collect the results in the [`output`](output/) directory of the tool.

:warning:
Note that deriving official LDBC results requires commissioning an _audited benchmark_, which is a more complex process as it entails code review, ACID tests, etc.
See [the specification's Auditing chapter](https://ldbcouncil.org/ldbc_snb_docs/ldbc-snb-specification.pdf#chapter.7) for details.

## Cross-validation

To cross-validate the results of two implementations, run the power test for both tools, e.g. [Cypher](cypher/) and [Umbra](umbra/) results.
Then, run:

```bash
scripts/cross-validate.sh cypher umbra
```

Note that the cross-validation uses the [numdiff](scripts/numdiff.md) tool.

## Usage

See [`.circleci/config.yml`](.circleci/config.yml) for an up-to-date example on how to use the projects in this repository.

## Data sets

BI sets are being uploaded to the [SURF CWI repository](https://repository.surfsara.nl/datasets/cwi/snb).
(See [download instructions](https://github.com/ldbc/data-sets-surf-repository/).)
