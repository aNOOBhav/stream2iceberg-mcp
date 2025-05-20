
# 📈 Stock Daily Data Pipeline to Apache Iceberg on MinIO

This project automates the retrieval, transformation, and loading of daily stock price data from Yahoo Finance into an Apache Iceberg table stored on MinIO. The data is queryable using Dremio.

## 🚀 Features

- Fetches daily stock OHLC data for the last _n_ years using `yfinance`.
- Calculates daily percent change for each stock.
- Writes data in Parquet format with Snappy compression.
- Stores the data in MinIO under a designated bucket and prefix.
- Iceberg-compatible format for data lake query engines like Dremio.
- TQDM progress bar for monitoring download status.

---

## 🧾 Tech Stack

- **Python** with `yfinance`, `pandas`, `s3fs`, and `pyarrow`
- **Apache Iceberg** format (optional Iceberg table promotion)
- **MinIO** for S3-compatible object storage
- **Dremio** as query engine for SQL analytics

---

## 📂 Project Structure

```
stock_daily_pipeline/
│
├── daily_pipeline.py        # Main ETL script
├── requirements.txt         # Dependencies
├── README.md                # Project documentation
```

---

## 🛠️ Setup Instructions

### 1. Install Requirements

```bash
pip install -r requirements.txt
```

### 2. Configure MinIO Connection

Make sure MinIO is running at `http://127.0.0.1:9000` with:

```python
fs = s3fs.S3FileSystem(
    key='admin',
    secret='password',
    client_kwargs={'endpoint_url': 'http://127.0.0.1:9000'}
)
```

### 3. Set Bucket and Object Prefix

```python
BUCKET_NAME = "lakehouse"
OBJECT_PREFIX = "stock_batch"
```

---

## ✅ How to Run

```bash
python daily_pipeline.py
```

You will be prompted to enter the number of years (e.g., `2`), and the script will:

1. Fetch historical data for 20 large-cap tickers.
2. Generate daily metrics (`trade_date`, `open_price`, `close_price`, `pct_change`).
3. Write to: `s3://lakehouse/stock_batch/daily_stock_data.parquet` in Parquet format.

---

## 📊 Example Query in Dremio

```sql
SELECT
    trade_date,
    ticker,
    open_price,
    close_price,
    pct_change
FROM lakehouse.stock_batch
WHERE trade_date < DATE '2025-05-16';
```

> If `trade_date` is a string, wrap it with `TO_DATE(trade_date, 'YYYY-MM-DD')`.

---

## 🧪 Sample Output Format

```json
{
  "trade_date": "2025-05-15",
  "ticker": "AAPL",
  "open_price": 189.55,
  "close_price": 191.23,
  "pct_change": 0.89
}
```

---

## 📈 Ticker List

The script currently processes the following tickers:

```
AAPL, MSFT, GOOGL, AMZN, NVDA, TSLA, META, BRK-B, LLY, JPM,
V, UNH, AVGO, MA, JNJ, HD, XOM, PG, COST, BAC
```

(Note: `BRK-B` is internally converted to `BRK.B` for Yahoo Finance.)

---

## 🔄 LLD

![image](https://github.com/user-attachments/assets/a0874d5a-60e0-48ee-9979-05b4f7ebb130)


## 🔄 Future Enhancements

- Partition data by `year` or `ticker` for faster querying.
- Automate Iceberg table promotion using Dremio REST API or Nessie.
- Schedule with Prefect or Airflow for production orchestration.

---

## 🧾 License

MIT License

---

## 🙋‍♂️ Questions?

Feel free to raise issues or reach out for improvements or support!

