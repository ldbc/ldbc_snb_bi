#!/bin/bash

Rscript ./scripts/plot.R $1
open graphics/bi-$1-summary.pdf

