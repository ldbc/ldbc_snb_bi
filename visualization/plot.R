library(ggplot2)
library(tidyverse)
library(plyr)
library(stringr)
library(scales)

# load

results = read.csv("../cypher/output/timings.csv", sep="|")
results

# preprocess

# --

# results

gm_mean = function(x, na.rm=TRUE) {
  exp(sum(log(x[x > 0]), na.rm=na.rm) / length(x))
}

#results$op <- as.numeric(gsub("[^0-9]*([0-9]+)[^0-9]*$", "\\1", results$operation_type))
#results$op <- sprintf("%02d", results$op)

ggplot(results, aes(x=sf, y=execution_duration_MILLISECONDS)) +
  geom_point() +
  facet_wrap(~q, ncol=1, scales="free") +
  theme_bw()
