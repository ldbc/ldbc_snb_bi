#!/bin/bash

source ../../CLionProjects/duckdb-pgq/.venv/bin/activate

#for i in 10 3 1 0.3 0.1
#do
  for j in {1..5}
  do
#    echo $i $j
    python3 load.py -s 10 -q 20 -l 0 -w bi
  done
#done



