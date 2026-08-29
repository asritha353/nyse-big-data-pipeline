args <- commandArgs(trailingOnly = TRUE)
project_root <- if (length(args) >= 1) args[[1]] else normalizePath("..")

input_path <- file.path(project_root, "output", "nyse_for_r.csv")
output_dir <- file.path(project_root, "output", "r_analysis")
dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)

column_names <- c(
  "date", "symbol", "company", "sector", "close",
  "daily_return_pct", "volume"
)

nyse <- read.csv(
  input_path,
  header = FALSE,
  col.names = column_names,
  stringsAsFactors = FALSE,
  check.names = FALSE
)

nyse$date <- as.Date(nyse$date)
nyse$close <- as.numeric(nyse$close)
nyse$daily_return_pct <- as.numeric(nyse$daily_return_pct)
nyse$volume <- as.numeric(nyse$volume)

required_columns <- c("date", "symbol", "close", "daily_return_pct", "sector")
stopifnot(all(required_columns %in% names(nyse)))
stopifnot(nrow(nyse) == 15120)
stopifnot(length(unique(nyse$symbol)) == 20)
stopifnot(!anyNA(nyse[required_columns]))

all_dates <- sort(unique(nyse$date))
all_symbols <- sort(unique(nyse$symbol))

to_matrix <- function(values) {
  result <- matrix(
    NA_real_,
    nrow = length(all_dates),
    ncol = length(all_symbols),
    dimnames = list(as.character(all_dates), all_symbols)
  )
  result[cbind(match(nyse$date, all_dates), match(nyse$symbol, all_symbols))] <- values
  result
}

price_matrix <- to_matrix(nyse$close)
return_matrix <- to_matrix(nyse$daily_return_pct)

stopifnot(!anyNA(price_matrix))
stopifnot(!anyNA(return_matrix))

price_covariance <- cov(price_matrix)
price_correlation <- cor(price_matrix)
return_covariance <- cov(return_matrix)
return_correlation <- cor(return_matrix)

write.csv(price_matrix, file.path(output_dir, "closing_price_matrix.csv"))
write.csv(price_covariance, file.path(output_dir, "closing_price_covariance.csv"))
write.csv(price_correlation, file.path(output_dir, "closing_price_correlation.csv"))
write.csv(return_covariance, file.path(output_dir, "daily_return_covariance.csv"))
write.csv(return_correlation, file.path(output_dir, "daily_return_correlation.csv"))

date_caption <- paste(
  format(min(nyse$date), "%Y-%m-%d"),
  "to",
  format(max(nyse$date), "%Y-%m-%d")
)

# Plot 1 contract: compare five representative companies over time with a
# multi-series line chart. Color and line type both distinguish each series.
focus_symbols <- c("AAPL", "BAC", "JNJ", "KO", "NKE")
focus_colors <- c("#2F5D8C", "#C08B2C", "#D17A3F", "#7A7F45", "#C06C84")
focus_line_types <- c(1, 2, 3, 4, 5)

png(
  file.path(output_dir, "stock_price_trends.png"),
  width = 1800,
  height = 1200,
  res = 150
)
par(
  mfrow = c(2, 3),
  mar = c(4, 4, 3, 1),
  oma = c(3, 3, 6, 1),
  family = "sans"
)
focus_prices <- price_matrix[, focus_symbols, drop = FALSE]
for (i in seq_along(focus_symbols)) {
  plot(
    all_dates,
    focus_prices[, i],
    type = "l",
    col = focus_colors[[i]],
    lty = focus_line_types[[i]],
    lwd = 2,
    xlab = "",
    ylab = "USD",
    main = focus_symbols[[i]],
    col.axis = "#343A40",
    col.lab = "#343A40",
    col.main = "#20252A"
  )
  grid(col = "#E4E7EA", lty = 1)
  lines(
    all_dates,
    focus_prices[, i],
    col = focus_colors[[i]],
    lty = focus_line_types[[i]],
    lwd = 2
  )
}
plot.new()
text(
  0.5,
  0.58,
  "Independent y-axis\nfor each company",
  cex = 1.15,
  col = "#343A40"
)
text(0.5, 0.38, "Actual closing prices (USD)", cex = 0.9, col = "#5B6268")
mtext("Stock Closing-Price Trends", outer = TRUE, side = 3, line = 3.5,
      cex = 1.5, font = 2, col = "#20252A")
mtext(
  paste("Five sector-representative companies |", date_caption),
  outer = TRUE,
  side = 3,
  line = 1.8,
  cex = 0.95,
  col = "#5B6268"
)
mtext("Trading date", outer = TRUE, side = 1, line = 1.2,
      cex = 1, col = "#343A40")
dev.off()

# Plot 2 contract: show the shape and tails of all open-to-close daily returns
# with a histogram, a zero reference, and a directly labeled mean reference.
png(
  file.path(output_dir, "daily_return_distribution.png"),
  width = 1800,
  height = 1050,
  res = 150
)
par(mar = c(5, 6, 5, 2), family = "sans")
return_mean <- mean(nyse$daily_return_pct)
hist(
  nyse$daily_return_pct,
  breaks = 60,
  col = "#DCE8F2",
  border = "#2F5D8C",
  main = "Daily Return Distribution",
  xlab = "Open-to-close daily return (%)",
  ylab = "Trading records",
  col.axis = "#343A40",
  col.lab = "#343A40",
  col.main = "#20252A"
)
abline(v = 0, col = "#4D5358", lty = 2, lwd = 2)
abline(v = return_mean, col = "#C7762B", lty = 1, lwd = 2)
legend(
  "topright",
  legend = c("Zero return", sprintf("Mean: %.4f%%", return_mean)),
  col = c("#4D5358", "#C7762B"),
  lty = c(2, 1),
  lwd = 2,
  bty = "n"
)
mtext(
  paste(format(nrow(nyse), big.mark = ","), "records |", date_caption),
  side = 3,
  line = 0.5,
  cex = 0.85,
  col = "#5B6268"
)
dev.off()

# Plot 3 contract: compare all pairwise daily-return correlations in a clustered
# heatmap. The fixed -1 to +1 scale prevents visual exaggeration.
correlation_order <- hclust(as.dist(1 - return_correlation))$order
ordered_correlation <- return_correlation[
  correlation_order,
  correlation_order,
  drop = FALSE
]
correlation_palette <- colorRampPalette(c("#2F5D8C", "#F7F7F4", "#C7762B"))(101)

png(
  file.path(output_dir, "correlation_heatmap.png"),
  width = 1900,
  height = 1700,
  res = 170
)
par(mar = c(9, 9, 6, 3), family = "sans")
symbol_count <- ncol(ordered_correlation)
image(
  x = seq_len(symbol_count),
  y = seq_len(symbol_count),
  z = t(ordered_correlation[symbol_count:1, , drop = FALSE]),
  col = correlation_palette,
  zlim = c(-1, 1),
  axes = FALSE,
  xlab = "",
  ylab = "",
  main = "Daily-Return Correlation Heatmap"
)
axis(
  1,
  at = seq_len(symbol_count),
  labels = colnames(ordered_correlation),
  las = 2,
  tick = FALSE,
  cex.axis = 0.75
)
axis(
  2,
  at = seq_len(symbol_count),
  labels = rev(rownames(ordered_correlation)),
  las = 2,
  tick = FALSE,
  cex.axis = 0.75
)
box(col = "#4D5358")
mtext(
  paste("Open-to-close returns | 20 symbols |", date_caption),
  side = 3,
  line = 1.2,
  cex = 0.85,
  col = "#5B6268"
)
mtext(
  "Blue = negative    Neutral = zero    Orange = positive",
  side = 1,
  line = 7,
  cex = 0.8,
  col = "#5B6268"
)
dev.off()

correlation_for_ranking <- return_correlation
diag(correlation_for_ranking) <- NA_real_
max_pair_index <- arrayInd(
  which.max(abs(correlation_for_ranking)),
  dim(correlation_for_ranking)
)
max_pair_symbols <- c(
  rownames(correlation_for_ranking)[max_pair_index[1]],
  colnames(correlation_for_ranking)[max_pair_index[2]]
)
max_pair_value <- correlation_for_ranking[max_pair_index[1], max_pair_index[2]]

summary_lines <- c(
  paste("Input rows:", nrow(nyse)),
  paste("Symbols:", length(all_symbols)),
  paste("Trading dates:", length(all_dates)),
  paste("Date range:", date_caption),
  sprintf("Mean daily return: %.6f%%", return_mean),
  sprintf("Daily return standard deviation: %.6f%%", sd(nyse$daily_return_pct)),
  sprintf(
    "Strongest absolute off-diagonal return correlation: %s/%s = %.6f",
    max_pair_symbols[[1]],
    max_pair_symbols[[2]],
    max_pair_value
  )
)
writeLines(summary_lines, file.path(output_dir, "analysis_summary.txt"))

cat(paste(summary_lines, collapse = "\n"), "\n")
cat("R analytics outputs written to:", output_dir, "\n")
