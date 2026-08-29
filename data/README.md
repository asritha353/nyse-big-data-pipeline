# Data directories

Download `prices.csv` and `securities.csv` from the Kaggle dataset
[NYSE](https://www.kaggle.com/datasets/dgawlik/nyse) and place them in
`data/raw/`.

Run `python scripts/prepare_nyse_data.py` to generate the two staging files in
`data/processed/`. Downloaded and generated CSV files are intentionally excluded
from Git because they are reproducible and `prices.csv` is large.
