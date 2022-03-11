![LDBC_LOGO](https://raw.githubusercontent.com/wiki/ldbc/ldbc_snb_datagen/images/ldbc-logo.png)

# LDBC SNB Business Intelligence (BI) workload implementations

[![Build Status](https://circleci.com/gh/ldbc/ldbc_snb_bi.svg?style=svg)](https://circleci.com/gh/ldbc/ldbc_snb_bi)

Implementations for the BI workload of the [LDBC Social Network Benchmark](https://ldbcouncil.org/ldbc_snb_docs/).

To get started with the LDBC SNB benchmarks, check out our introductory presentation: [The LDBC Social Network Benchmark](https://docs.google.com/presentation/d/1p-nuHarSOKCldZ9iEz__6_V3sJ5kbGWlzZHusudW_Cc/) ([PDF](https://ldbcouncil.org/docs/presentations/ldbc-snb-2021-12.pdf)).

:scroll: If you wish to cite the LDBC SNB, please refer to the [documentation repository](https://github.com/ldbc/ldbc_snb_docs#how-to-cite-ldbc-benchmarks) ([bib snippet](https://github.com/ldbc/ldbc_snb_docs/blob/dev/bib/specification.bib)).

:warning: Implementations in this repository are preliminary, i.e. they are unaudited and - in rare cases - do not pass validation. For details, feel free to contact us through an issue or email.

## Implementations

The repository contains the following implementations:

* [`cypher`](cypher/): queries are expressed in the Cypher language and run in the Neo4j graph database management system (version 4) using its stored procedure libraries (e.g. Graph Data Science)
* [`umbra`](umbra/): queries are implemented in SQL and run in [Umbra JIT-compiled columnar database management system](https://umbra-db.com/) (incomplete, see [related issues](https://github.com/ldbc/ldbc_snb_bi/labels/umbra))

All implementations use Docker for ease of setup and execution. However, the setups can be adjusted to use a non-containerized DBMS.

## Parameter generation

The query input parameter generator is implemented in the [`paramgen/`](paramgen/) directory.

## Usage

See [`.circleci/config.yml`](.circleci/config.yml) for an up-to-date example on how to use the projects in this repository.
