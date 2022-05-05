# Naïve parameter generation

This variant of the paramgen wilfully disregards concerns for [parameter curation](https://research.vu.nl/en/publications/parameter-curation-for-benchmark-queries) and just uses uniform random sampling. We use it to demonstrate the difference between a naïve and a curating parameter generator.

Using the bad paramgen requires the same steps as the [curating paramgen](../paramgen/) but there are no temporal attributes. Omitting them is part of being bad!

:warning: The goal of this parameter generator is to demonstrate how to generate **bad parameters**. Therefore, it should not be used for official benchmarks.
