library(ggplot2)
library(dplyr)
library(tidyr)
library(readr)
library(stringr)

files_umbra = list.files(path = "../umbra/output/", recursive = TRUE, pattern = "timings.*.csv$", full.names = TRUE)

# ----------------------------------------------- Umbra -----------------------------------------------

results <- readr::read_delim(files_umbra, delim = '|')
results$sf = as.factor(results$sf)
results = results[str_detect(results$q, "^[0-9]|writes"), ]
results$q_id = as.numeric(str_extract(results$q, "[0-9]+"))
results$q_variant = str_extract(results$q, "[ab]+")
results$q_variant[is.na(results$q_variant)] = ""
results$my_q = paste(sprintf("%02d", results$q_id), results$q_variant, sep="")
results$my_q[results$q == "writes"] = " writes"
results <- results[order(results$my_q), ]

# make query variants use the same y axis
variants <- results[results$q_variant != "", ]
variants$var_a <- sprintf("%02da", variants$q_id)
variants$var_b <- sprintf("%02db", variants$q_id)

variants_extremes <- variants %>%
  group_by(tool, sf, q_id, var_a, var_b) %>%
  summarize(min_time = min(time), max_time = max(time), .groups = "keep") %>%
  pivot_longer(
    cols=c(min_time, max_time),
    names_to = "minmax",
    values_to = "time"
  ) %>%
  pivot_longer(
    cols=c(var_a, var_b),
    names_to = "var",
    values_to = "my_q"
  )

# geom blank inserts an invisible point
ggplot(results, aes(x=sf, y=time, col=tool)) +
  geom_jitter(alpha=0.75, position=position_jitter(0.075)) +
  geom_blank(data = variants_extremes, aes(x = sf, y = time)) + 
  facet_wrap(~my_q, ncol=7, scales="free_y") +
  xlab("scale factor") +
  ylab("time [seconds]") +
  theme_bw() +
  theme(legend.position="none",
        plot.margin = margin(0.1, 0.1, -0.45, 0.1, "cm"),
        legend.margin = margin(0.0, 0.0, 0.0, 0.0, "cm"),
  )
ggsave(file="umbra-results.pdf", width=170, height=160, units="mm", useDingbats=F)
