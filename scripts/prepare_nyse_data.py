import pandas as pd
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]

RAW_DIR = PROJECT_ROOT / "data" / "raw"
PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"

PRICES_FILE = RAW_DIR / "prices.csv"
SECURITIES_FILE = RAW_DIR / "securities.csv"

PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

SELECTED_SYMBOLS = [
    "AAPL", "MSFT", "IBM", "ORCL", "INTC",
    "JPM", "GS", "BAC", "C", "WFC",
    "JNJ", "PFE", "MRK", "UNH", "ABT",
    "KO", "PEP", "MCD", "NKE", "SBUX"
]

START_DATE = "2014-01-01"

print("Loading prices.csv...")
prices = pd.read_csv(PRICES_FILE)

print("Loading securities.csv...")
securities = pd.read_csv(SECURITIES_FILE)

print("\nOriginal prices shape:", prices.shape)
print("Original securities shape:", securities.shape)

# Standardize symbol column names
if "symbol" not in prices.columns and "Symbol" in prices.columns:
    prices = prices.rename(columns={"Symbol": "symbol"})

if "Ticker symbol" in securities.columns:
    securities = securities.rename(columns={"Ticker symbol": "symbol"})
elif "Symbol" in securities.columns:
    securities = securities.rename(columns={"Symbol": "symbol"})

# Convert date column
prices["date"] = pd.to_datetime(prices["date"], format="mixed")
# Filter selected stocks and date range
filtered_prices = prices[
    (prices["symbol"].isin(SELECTED_SYMBOLS))
    & (prices["date"] >= START_DATE)
].copy()

filtered_securities = securities[
    securities["symbol"].isin(SELECTED_SYMBOLS)
].copy()

# Sort for clean downstream processing
filtered_prices = filtered_prices.sort_values(["symbol", "date"])

# Convert date back to YYYY-MM-DD
filtered_prices["date"] = filtered_prices["date"].dt.strftime("%Y-%m-%d")

prices_output = PROCESSED_DIR / "prices_staging.csv"
securities_output = PROCESSED_DIR / "securities_staging.csv"

filtered_prices.to_csv(prices_output, index=False)
filtered_securities.to_csv(securities_output, index=False)

print("\nFiltered prices shape:", filtered_prices.shape)
print("Filtered securities shape:", filtered_securities.shape)

print("\nSymbols found in prices:")
print(sorted(filtered_prices["symbol"].unique()))

print("\nFiles created:")
print(prices_output)
print(securities_output)