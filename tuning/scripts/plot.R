if (!require(ggplot2)) install.packages('ggplot2')
if (!require(readr)) install.packages('readr')
if (!require(patchwork)) install.packages('patchwork')

args = commandArgs(trailingOnly=TRUE)
query = args[1]

# load data
if (query == "all") {
  queries = c("1", "2a", "2b", "3", "4", "5", "6", "7", "8a", "8b", "9", "10a", "10b", "11", "12", "13", "14a", "14b", "15a", "15b", "16a", "16b", "17",
            "18", "19a", "19b", "20")
} else {
  queries = query
}

q = "2a"

for (q in queries) {
  file = paste0("./data/bi-", q,"-summary.csv")

  df = suppressMessages(read_csv(file = file, col_names = T))

  # discard first 5 results
  df = df[6:nrow(df),] 
  
  # histogram
  p1 = ggplot(df, aes(x=dbHits)) +
    geom_histogram(color="black", fill="white",bins=30) +
    theme_bw()

  p2 = ggplot(df, aes(x=records)) +
    geom_histogram(color="black", fill="white",bins=30) +
    theme_bw()
  
  p3 = ggplot(df, aes(x=runtime)) +
    geom_histogram(color="black", fill="white",bins=30) +
    theme_bw()

  combined <- p1 + p2 + p3 & theme(text = element_text(size = 20))
  combined + plot_layout(guides = "collect")

  ggsave(paste0("./graphics/bi-", q, "-summary.pdf"),width = 18, height = 6,device = "pdf")
}
