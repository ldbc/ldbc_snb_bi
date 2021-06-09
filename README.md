![LDBC_LOGO](https://raw.githubusercontent.com/wiki/ldbc/ldbc_snb_datagen/images/ldbc-logo.png)

# LDBC SNB Business Intelligence (BI) workload implementations

[![Build Status](https://circleci.com/gh/ldbc/ldbc_snb_bi.svg?style=svg)](https://circleci.com/gh/ldbc/ldbc_snb_bi)

Implementations for the BI workload of the [LDBC Social Network Benchmark](https://ldbc.github.io/ldbc_snb_docs/).

:scroll: If you wish to cite the LDBC SNB, please refer to the [documentation repository](https://github.com/ldbc/ldbc_snb_docs#how-to-cite-ldbc-benchmarks) ([bib snippet](https://github.com/ldbc/ldbc_snb_docs/blob/dev/bib/specification.bib)).

:warning: Implementations in this repository are preliminary, i.e. they are unaudited and - in rare cases - do not pass validation. For details, feel free to contact us through an issue or email.

The repository contains two reference implementations:

* `cypher`: queries are expressed in the Cypher language and run in the Neo4j graph database management system (version 4) and its stored procedure libraries (e.g. Graph Data Science)
* `postgres`: queries are implemented in SQL and run in PostgreSQL relational database management system (version 13)

Both implementations use Docker for ease of setup and execution. However, the setups can be adjusted to use a non-containerized DBMS.
