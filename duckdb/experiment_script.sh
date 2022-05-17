#!/bin/bash

source ../../CLionProjects/duckdb-pgq/.venv/bin/activate

for _ in {1..5}
do
  for i in 0.1 0.3 1 3 10
  do
    python3 load.py -s $i -q 20
  done
done

