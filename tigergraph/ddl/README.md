# TigerGraph BI implementation
1. Because the current TigerGraph datetime use ecpoch in seconds but the datetime in LDBC SNB benchmarks is in milliseconds. So we store the datetime as INT64 in the datatime and write user defined functions to do conversion. The dateime value in the dataset is considered as the local time. INT64 datetime in millisecond `value` can be converted to datetime using `datetime_to_epoch(value/1000)`.
2. The user defined function is in `ExprFunctions.hpp` (for query) and `TokenBank.cpp` (for loader).
3. `queries/pre-19.gsql` and `queries/pre-20.gsql` are queries to pre-compute the edges weights in BI-19 and BI-20.
