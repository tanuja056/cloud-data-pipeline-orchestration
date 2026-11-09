from datetime import datetime
from pathlib import Path
import subprocess

from airflow.sdk import dag, task

PROJECT_ROOT = Path(__file__).resolve().parent.parent
PROJECT_PYTHON = PROJECT_ROOT / ".venv" / "bin" / "python"


def run_script(script_path):
    subprocess.run(
        [str(PROJECT_PYTHON), str(PROJECT_ROOT / script_path)],
        cwd=str(PROJECT_ROOT),
        check=True,
    )


@dag(
    dag_id="ecommerce_data_pipeline",
    description="End-to-end ecommerce data engineering pipeline",
    schedule=None,
    start_date=datetime(2026, 11, 9),
    catchup=False,
    tags=["data-engineering", "pyspark", "ecommerce"],
)
def ecommerce_pipeline():

    @task
    def generate_dataset():
        run_script("scripts/generate_dataset.py")

    @task
    def bronze_ingestion():
        run_script("scripts/ingestion.py")

    @task
    def silver_transformation():
        run_script("scripts/silver_transformation.py")

    @task
    def gold_transformation():
        run_script("scripts/gold_transformation.py")

    @task
    def data_quality_tests():
        run_script("tests/test_gold_quality.py")

    @task
    def sql_analytics():
        run_script("scripts/query_analytics.py")

    generation = generate_dataset()
    bronze = bronze_ingestion()
    silver = silver_transformation()
    gold = gold_transformation()
    quality = data_quality_tests()
    analytics = sql_analytics()

    generation >> bronze >> silver >> gold >> quality >> analytics


ecommerce_pipeline()
