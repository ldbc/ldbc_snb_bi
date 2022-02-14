# Paramgen Tuning #

## Preliminaries ##

Environment variables:
```
export LDBC_SNB_BI=`bi-dir`
```

Run paramgen.

## Scripts ##

Run each query `n` times and plot `dbHits`, `records`, and `runtime`.
```
./run.sh <n>
```

Run a single query  `n` times and print summary.
```
./scripts/execute-query.sh <query> <n> <print_summary>
```

Make plots for query `n`
```
./scripts/plot.sh <query>
```
