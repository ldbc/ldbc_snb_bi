#!/bin/bash

mvn package

java -cp target/tuning-1.0-SNAPSHOT.jar Main

Rscript ./scripts/plot.R
