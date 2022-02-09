#!/bin/bash

mvn package

java -cp target/neo4j-driver-1.0-SNAPSHOT.jar Main

Rscript ./scripts/plot.R
