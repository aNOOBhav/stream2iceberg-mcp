from confluent_kafka import Consumer
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import s3fs
import json
from datetime import datetime

#MinIO S3-compatible connection (IPv4-safe)
fs = s3fs.S3FileSystem(
    key='admin',
    secret='password',
    client_kwargs={'endpoint_url': 'http://127.0.0.1:9000'}  # use 127.0.0.1 instead of localhost
)

#Confluent Kafka Consumer config (binds to IPv4 explicitly)
conf = {
    'bootstrap.servers': '127.0.0.1:9092',
    'group.id': 'stock-consumer-new-group',
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(conf)
consumer.subscribe(['stockdaily-events'])  # Make sure topic exists and is populated

print("Listening to Confluent Kafka topic...")

# Buffer for partitioned writes
message_buffer = {}

def write_partition_to_minio(trade_date, records):
    if not records:
        return
    df = pd.DataFrame(records)
    table = pa.Table.from_pandas(df)

    file_path = f"lakehouse/stock_stream/trade_date={trade_date}/stocks.parquet"
    with fs.open(file_path, 'wb') as f:
        pq.write_table(table, f)

    print(f"Wrote {len(df)} records to MinIO: {file_path}")

try:
    while True:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            print("Consumer error:", msg.error())
            continue

        try:
            value = json.loads(msg.value().decode('utf-8'))
            print("Received message:", value)

            trade_date = value.get('trade_date')
            if not trade_date:
                print("Missing trade_date. Skipping message.")
                continue

            if trade_date not in message_buffer:
                message_buffer[trade_date] = []
            message_buffer[trade_date].append(value)

            if len(message_buffer[trade_date]) >= 20:
                write_partition_to_minio(trade_date, message_buffer[trade_date])
                message_buffer[trade_date] = []

        except Exception as e:
            print("Error processing message:", e)

except KeyboardInterrupt:
    print("Stopping consumer and flushing remaining data...")
    for trade_date, records in message_buffer.items():
        write_partition_to_minio(trade_date, records)

finally:
    consumer.close()
