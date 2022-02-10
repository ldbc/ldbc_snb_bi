#!/bin/bash

mvn package

java -cp target/tuning-1.0-SNAPSHOT.jar Main $1 $2 $3