from prefect import task, flow
import requests
import os
from dotenv import load_dotenv

load_dotenv()

@task
def run_batch_dremio_insert():
    sql = """
    INSERT INTO nessie.stock_stream.daily_snapshot (
        trade_date,
        ticker,
        open_price,
        close_price,
        pct_change
    )
    SELECT trade_date, ticker, open_price, close_price, pct_change
    FROM (
      SELECT * FROM 
      (
      SELECT trade_date, ticker, open_price, close_price, pct_change
      FROM (
        SELECT TO_DATE(trade_date, 'YYYY-MM-DD', 1) AS trade_date, ticker, open_price, close_price, pct_change
        FROM lakehouse.stock_batch
      ) nested_0
      ORDER BY trade_date DESC
      )
      WHERE trade_date < DATE_ADD(CURRENT_DATE,-2)
    ) nested_0
    ORDER BY trade_date DESC;
    """

    response = requests.post(
        "http://localhost:9047/apiv2/sql",
        headers={"Content-Type": "application/json"},
        json={"sql": sql, "context": []},
        auth=(os.getenv("DREMIO_USER"), os.getenv("DREMIO_PASSWORD"))
    )

    if response.status_code == 200:
        print("Batch Dremio Insert Success")
    else:
        print(f"Batch Insert Failed: {response.status_code} {response.text}")
        raise Exception("Batch Dremio Insert Failed")
