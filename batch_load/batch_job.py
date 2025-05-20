import pandas as pd
import yfinance as yf
from datetime import datetime, timedelta
from tqdm import tqdm
import s3fs

# === User Input ===
YEARS = int(input("Enter number of years: "))

# === Tickers ===
TICKERS = [
    'AAPL', 'MSFT', 'GOOGL', 'AMZN', 'NVDA',
    'TSLA', 'META', 'BRK-B', 'LLY', 'JPM',
    'V', 'UNH', 'AVGO', 'MA', 'JNJ',
    'HD', 'XOM', 'PG', 'COST', 'BAC'
]

# === Time Range ===
end_date = datetime.today() - timedelta(days=1)
start_date = end_date - timedelta(days=365 * YEARS)

# === S3 Path Setup (MinIO) ===
fs = s3fs.S3FileSystem(
    key='admin',
    secret='password',
    client_kwargs={'endpoint_url': 'http://127.0.0.1:9000'}
)

BUCKET_NAME = "lakehouse"
OBJECT_PREFIX = "stock_batch"
S3_PATH = f"{BUCKET_NAME}/{OBJECT_PREFIX}/daily_stock_data.parquet"

# === Adjust Yahoo Ticker Format (BRK-B → BRK.B) ===
yf_tickers = [t.replace('-', '.') for t in TICKERS]

# === Download Data ===
print("Downloading stock data from Yahoo Finance...")
df = yf.download(
    tickers=yf_tickers,
    start=start_date,
    end=end_date,
    group_by='ticker',
    threads=True,
    auto_adjust=False
)

# === Transform to Flat Records ===
records = []
multi_index = isinstance(df.columns, pd.MultiIndex)

print("Processing ticker data...")
for i in tqdm(range(len(TICKERS))):
    ticker = TICKERS[i]
    yf_ticker = yf_tickers[i]

    try:
        if multi_index:
            if yf_ticker not in df.columns.levels[0]:
                continue
            ticker_df = df[yf_ticker].dropna().reset_index()
        else:
            if yf_ticker != yf_tickers[0]:
                continue
            ticker_df = df.dropna().reset_index()

        for _, row in ticker_df.iterrows():
            trade_date = row['Date'].strftime('%Y-%m-%d')
            open_price = round(float(row['Open']), 2)
            close_price = round(float(row['Close']), 2)
            pct_change = round((close_price - open_price) / open_price * 100, 2)

            payload = {
                "trade_date": trade_date,
                "ticker": ticker,
                "open_price": open_price,
                "close_price": close_price,
                "pct_change": pct_change
            }
            records.append(payload)
    except Exception as e:
        print(f"Skipping {ticker}: {e}")
        continue

# === Final DataFrame ===
if records:
    final_df = pd.DataFrame(records)
    final_df.sort_values(by=["ticker", "trade_date"], inplace=True)

    # === Write to MinIO as Parquet ===
    print(f"Writing to MinIO: s3://{S3_PATH}")
    final_df.to_parquet(
        f"s3://{S3_PATH}",
        index=False,
        compression="snappy",
        storage_options={
            "key": "admin",
            "secret": "password",
            "client_kwargs": {"endpoint_url": "http://127.0.0.1:9000"}
        }
    )

    print("Parquet written to MinIO successfully.")
else:
    print("No valid data found to write.")
