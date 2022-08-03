# Instruction manual 

`load.py` can be used to benchmark the DuckPGQ implementation. 

To do this: 

1. Clone [LDBC SNB Datagen Spark](https://github.com/ldbc/ldbc_snb_datagen_spark)
   1. Load the datasets in this repository
2. Generate the parameters and place them in the parameters folder.  
3. Create a python package of DuckPGQ
   1. Clone [DuckPGQ](https://github.com/cwida/duckdb-pgq.old/tree/path_length)
   2. Run `create_python_package.sh` found in the top level of the DuckPGQ repository. This will create a .venv folder in the same level.
   3. Activate the virtualenvironment using `source .venv/bin/activate` (on Linux). __The implementation has not been tested on any other OS__ 
4. Run `python load.py -s <scale factor> -w <workload (bi or interactive)> -q <subquery> -l <_only_ load database (1 or 0)> -a <number of lanes> -t <number of threads> -e <log results (1 or 0)>`

