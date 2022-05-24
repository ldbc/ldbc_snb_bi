#!/bin/bash

source ../../duckdb-pgq/.venv/bin/activate

for j in {1..50..1}
do
  for i in 3 10 30 100 300
  do
    echo $i $j
    python3 load.py -s $i -q 20
  done
done

deactivate 


