if (!require(ggplot2)) install.packages('ggplot2')
if (!require(readr)) install.packages('readr')
if (!require(patchwork)) install.packages('patchwork')

# load data
queries = c("1", "2a", "2b", "3", "4", "5", "6", "7", "8a", "8b", "9", "10a", "10b", "11", "12", "13", "14a", "14b", "15a", "15b", "16a", "16b", "17",
            "18", "19a", "19b", "20")

for (q in queries) {
  file = paste0("./data/bi-", q,"-summary.csv")

  df = suppressMessages(read_csv(file = file, col_names = T))

  # histogram
  p1 = ggplot(df, aes(x=dbHits)) +
    geom_histogram(color="black", fill="white",bins=30) +
    theme_bw()

  p2 = ggplot(df, aes(x=records)) +
    geom_histogram(color="black", fill="white",bins=30) +
    theme_bw()

  combined <- p1 + p2  & theme(text = element_text(size = 20))
  combined + plot_layout(guides = "collect")

  ggsave(paste0("./graphics/bi-", q, "-summary.pdf"),width = 12, height = 6,device = "pdf")
}
