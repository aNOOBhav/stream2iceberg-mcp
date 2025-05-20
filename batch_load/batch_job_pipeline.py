from prefect import flow, task
import subprocess
from batch_dremio_insert import run_batch_dremio_insert
import os
from dotenv import load_dotenv

load_dotenv()

@task
def run_batch_job():
    subprocess.run(["python", "batch_job.py"], check=True)

@flow(name="batch_stock_pipeline")
def batch_pipeline():
    run_batch_job()
    run_batch_dremio_insert()

if __name__ == "__main__":
    if not os.getenv("DREMIO_USER") or not os.getenv("DREMIO_PASSWORD"):
        raise EnvironmentError("DREMIO_USER and DREMIO_PASSWORD must be set.")
    batch_pipeline()