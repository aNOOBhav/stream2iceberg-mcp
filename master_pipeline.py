from prefect import flow, task
import subprocess
from batch_dremio_insert import run_batch_dremio_insert
import os
from dotenv import load_dotenv

load_dotenv()

@task
def run_daily_pipeline():
    print("🚀 Running Daily Pipeline...")
    subprocess.run(["python", "daily_pipeline.py"], check=True)

@task
def run_batch_job():
    print("📦 Running Batch Job...")
    subprocess.run(["python", "batch_job.py"], check=True)

@flow(name="unified_stock_pipeline")
def stock_pipeline(mode: str = "daily"):
    mode = mode.lower()

    if mode == "daily":
        run_daily_pipeline()
    elif mode == "batch":
        run_batch_job()
        run_batch_dremio_insert()
    elif mode == "both":
        run_daily_pipeline()
        run_batch_job()
        run_batch_dremio_insert()
    else:
        raise ValueError("Invalid mode. Choose from 'daily', 'batch', or 'both'.")

if __name__ == "__main__":
    import sys
    mode = sys.argv[1] if len(sys.argv) > 1 else "daily"
    stock_pipeline(mode)
