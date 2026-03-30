# [TidyTuesday Explorer](https://jmclawson.github.io/tidytuesday-explorer/)

Search, compare, and explore variables across TidyTuesday datasets

## About

Ever wonder how many [TidyTuesday](https://github.com/rfordatascience/tidytuesday) datasets have columns for "genus" and "species"? (*Answer: 3 and 15, respectively.*) By collecting the data dictionaries documented for each week, TidyTuesday Explorer helps you find strange *family* connections between [spiders](https://github.com/rfordatascience/tidytuesday/tree/main/data/2021/2021-12-07), [cheeses](https://github.com/rfordatascience/tidytuesday/tree/main/data/2024/2024-06-04), and [boardgames](https://github.com/rfordatascience/tidytuesday/tree/main/data/2019/2019-03-12). Behind the scenes, GitHub Actions fetch new data weekly and run Python scripts to build a Parquet dataset, queried in-browser using DuckDB-WASM.

## Permissions

Code in this project is MIT licensed to make it easier to use. Data is derived from TidyTuesday (CC0 1.0).
