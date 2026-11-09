from pathlib import Path
import subprocess
import sys
import time

from ingestion import ingest_data
from silver_transformation import run_silver_transformation
from gold_transformation import run_gold_transformation

PROJECT_ROOT = Path(__file__).resolve().parent.parent

def run_pipeline():
    start_time = time.time()

    print("\n" + "=" * 65)
    print("STARTING COMPLETE E-COMMERCE DATA PIPELINE")
    print("=" * 65)

    stages = []

    def execute_stage(name, function):
        print(f"\n{'=' * 65}")
        print(f"STAGE: {name}")
        print("=" * 65)

        stage_start = time.time()
        function()
        duration = time.time() - stage_start

        stages.append((name, duration))
        print(f"\n{name} completed in {duration:.2f} seconds.")

    try:
        execute_stage(
            "1. DATASET GENERATION",
            lambda: subprocess.run(
                [sys.executable, "scripts/generate_dataset.py"],
                cwd=PROJECT_ROOT,
                check=True,
            ),
        )

        execute_stage("2. BRONZE INGESTION", ingest_data)
        execute_stage("3. SILVER TRANSFORMATION", run_silver_transformation)
        execute_stage("4. GOLD TRANSFORMATION", run_gold_transformation)

        execute_stage(
            "5. AUTOMATED DATA QUALITY TESTS",
            lambda: subprocess.run(
                [sys.executable, "tests/test_gold_quality.py"],
                cwd=PROJECT_ROOT,
                check=True,
            ),
        )

        execute_stage(
            "6. SQL ANALYTICS",
            lambda: subprocess.run(
                [sys.executable, "scripts/query_analytics.py"],
                cwd=PROJECT_ROOT,
                check=True,
            ),
        )

    except Exception as error:
        print("\n" + "=" * 65)
        print("PIPELINE FAILED")
        print("=" * 65)
        print(f"Error: {error}")
        raise SystemExit(1)

    total_duration = time.time() - start_time

    print("\n" + "=" * 65)
    print("COMPLETE PIPELINE EXECUTION SUMMARY")
    print("=" * 65)

    for name, duration in stages:
        print(f"{name}: {duration:.2f} seconds")

    print(f"\nTotal execution time: {total_duration:.2f} seconds")
    print("ALL PIPELINE STAGES COMPLETED SUCCESSFULLY!")

if __name__ == "__main__":
    run_pipeline()
