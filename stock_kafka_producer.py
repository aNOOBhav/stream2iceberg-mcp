from confluent_kafka import Producer
import yfinance as yf
import json
from datetime import datetime, timedelta

conf = {'bootstrap.servers': '127.0.0.1:9092'}
producer = Producer(conf)

TICKERS = [ 'AAPL', 'MSFT', 'GOOGL', 'AMZN', 'NVDA',
            'TSLA', 'META', 'BRK-B', 'LLY', 'JPM',
            'V', 'UNH', 'AVGO', 'MA', 'JNJ',
            'HD', 'XOM', 'PG', 'COST', 'BAC' ]

def delivery_report(err, msg):
    if err:
        print(f"Delivery failed for {msg.key()}: {err}")
    else:
        print(f"Delivered {msg.key().decode()} to {msg.topic()} [{msg.partition()}]")

for ticker in TICKERS:
    data = yf.download(ticker, period="3d", interval="1d", progress=False)
    if len(data) < 2:
        continue
    row = data.iloc[-1]
    trade_date = row.name.strftime('%Y-%m-%d')

    payload = {
        "trade_date": trade_date,
        "ticker": ticker,
        "open_price": round(float(row['Open']), 2),
        "close_price": round(float(row['Close']), 2),
        "pct_change": round(float((row['Close'] - row['Open']) / row['Open']) * 100, 2)
    }

    producer.produce("stockdaily-events", key=ticker.encode(), value=json.dumps(payload), callback=delivery_report)

producer.flush()
