from prefect import task, flow
import subprocess
import requests
import os
from dotenv import load_dotenv
# from pyspark.sql import SparkSession # Uncomment if using Spark

load_dotenv()  # Load environment variables from .env

@task
def run_kafka_producer():
    subprocess.run(["python", "stock_kafka_producer.py"], check=True)

@task
def run_kafka_consumer():
    subprocess.run(["python", "stock_kafka_consumer.py"], check=True)

@task
def run_dremio_insert():
    sql = """
    INSERT INTO nessie.stock_stream.daily_snapshot (
        trade_date,
        ticker,
        open_price,
        close_price,
        pct_change
    )
    SELECT
        trade_date,
        ticker,
        open_price,
        close_price,
        pct_change
    FROM lakehouse.lakehouse.stock_stream
    WHERE trade_date = DATE_ADD(CURRENT_DATE, -1);
    """

    response = requests.post(
        "http://localhost:9047/apiv2/sql",
        headers={"Content-Type": "application/json"},
        json={"sql": sql, "context": []},
        auth=(os.getenv("DREMIO_USER"), os.getenv("DREMIO_PASSWORD"))
    )

    if response.status_code == 200:
        print("Dremio Insert Success")
    else:
        print(f"Dremio Insert Failed: {response.status_code} {response.text}")
        raise Exception("Dremio Insert Failed")

@flow(name="daily_stock_pipeline")
def daily_pipeline():
    run_kafka_producer()
    run_kafka_consumer()
    run_dremio_insert()

if __name__ == "__main__":
    if not os.getenv("DREMIO_USER") or not os.getenv("DREMIO_PASSWORD"):
        raise EnvironmentError("DREMIO_USER and DREMIO_PASSWORD must be set.")
    daily_pipeline()
