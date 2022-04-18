![LDBC_LOGO](https://raw.githubusercontent.com/wiki/ldbc/ldbc_snb_datagen/images/ldbc-logo.png)

# LDBC SNB Business Intelligence (BI) workload implementations

[![Build Status](https://circleci.com/gh/ldbc/ldbc_snb_bi.svg?style=svg)](https://circleci.com/gh/ldbc/ldbc_snb_bi)

Implementations for the BI workload of the [LDBC Social Network Benchmark](https://ldbcouncil.org/ldbc_snb_docs/).

To get started with the LDBC SNB benchmarks, check out our introductory presentation: [The LDBC Social Network Benchmark](https://docs.google.com/presentation/d/1p-nuHarSOKCldZ9iEz__6_V3sJ5kbGWlzZHusudW_Cc/) ([PDF](https://ldbcouncil.org/docs/presentations/ldbc-snb-2021-12.pdf)).

:scroll: If you wish to cite the LDBC SNB, please refer to the [documentation repository](https://github.com/ldbc/ldbc_snb_docs#how-to-cite-ldbc-benchmarks) ([bib snippet](https://github.com/ldbc/ldbc_snb_docs/blob/dev/bib/specification.bib)).

:warning: Implementations in this repository are preliminary, i.e. they are unaudited and - in rare cases - do not pass validation. For details, feel free to contact us through an issue or email.

## Implementations

The repository contains the following implementations:

* [`cypher`](cypher/): queries are expressed in the [Cypher language](https://neo4j.com/developer/cypher/) and run in the [Neo4j graph database management system](https://dbdb.io/db/neo4j)
* [`umbra`](umbra/): queries are expressed in SQL and run in [Umbra JIT-compiled columnar database management system](https://dbdb.io/db/umbra) (limitation: weighted shortest path queries, Q19 and Q20, are not supported)
* [`tigergraph`](tigergraph/): queries are expressed in the [GSQL language](https://www.tigergraph.com/gsql/) and run in the [TigerGraph graph database management system](https://tigergraph.com/)

All implementations use Docker for ease of setup and execution. However, the setups can be adjusted to use a non-containerized DBMS.

## Cross-validation

To cross-validate the results of two implementations, run the queries (whose results are saved in the implementation's `output/result.csv` file). Then, use the [numdiff](scripts/numdiff.md) tool:

```bash
numdiff \
    --separators='\|\n;,<>' \
    --absolute-tolerance 0.001 \
    cypher/output/results.csv \
    umbra/output/results.csv
```

Or, simply run:

```bash
scripts/cross-validate.sh
```

## Parameter generation

The query input parameter generator is implemented in the [`paramgen/`](paramgen/) directory.

## Usage

See [`.circleci/config.yml`](.circleci/config.yml) for an up-to-date example on how to use the projects in this repository.

