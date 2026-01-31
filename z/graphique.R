library(ggplot2)
library(dplyr)

df <- data.frame(
  Category = c("SSA Avg", "LIC Threshold", "DRC"),
  Value = c(60.39, 55, 25.061)
)

# Uniformiser à 1 décimale
df$Value <- round(df$Value, 1)

df <- df %>% arrange(Value)

ggplot(df, aes(x = Value, y = reorder(Category, Value))) +
  geom_col(fill = c("#4B9CD3", "#FFC857", "#2E4057")) +
  geom_text(aes(label = sprintf("%.1f", Value)),   # <-- FORCÉ 1 DÉCIMALE
            hjust = -0.1, size = 5) +
  labs(
    subtitle = "General Government Gross Debt – Comparative Context (% GDP)",
    x = "Value",
    y = NULL,
    caption = "Source : IMF WEO database : April 2025"
  ) +
  theme_minimal(base_size = 14) +
  theme(
    plot.title = element_text(face = "bold"),
    panel.grid.major.y = element_blank(),
    plot.caption = element_text(face = "italic", size = 10)
  ) +
  xlim(0, max(df$Value) * 1.15)
