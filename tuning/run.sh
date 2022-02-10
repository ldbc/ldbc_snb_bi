#!/bin/bash

mvn package

java -cp target/tuning-1.0-SNAPSHOT.jar Main all $1 false

Rscript ./scripts/plot.R all
