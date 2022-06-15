#!/bin/bash

source ../../CLionProjects/duckdb-pgq/.venv/bin/activate

for i in 10000
do
  for j in {1..50}
  do
    echo $i $j
    python3 load.py -s $i -q 13 -l 0 -w interactive -t 96 -e 1 -a 1024
  done
done



