"""orders_dag -- daily order processing pipeline.

Each task creates its own SparkSession, reads from storage, transforms,
and writes back. DataFrames never cross task boundaries.

dfguard catches schema mismatches at the function call, before any
processing begins.
"""

import os
import sys
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

# Make the pipeline package importable
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

INPUT_PATH   = "/tmp/orders_data/raw_orders.parquet"
ENRICH_PATH  = "/tmp/orders_data/enriched_orders.parquet"
SUMMARY_PATH = "/tmp/orders_data/customer_summary.parquet"


def task_enrich(**context):
    from pipeline.schemas import RawOrderSchema
    from pipeline.transforms import enrich
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.master("local").appName("orders-enrich").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    raw = spark.read.parquet(INPUT_PATH)

    # Validate right after loading -- catches upstream schema drift immediately
    RawOrderSchema.assert_valid(raw)

    enriched = enrich(raw)   # dfg.arm() guards the function call
    enriched.write.mode("overwrite").parquet(ENRICH_PATH)
    spark.stop()


def task_summarise(**context):
    from pipeline.schemas import EnrichedOrderSchema
    from pipeline.transforms import summarise
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.master("local").appName("orders-summarise").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    enriched = spark.read.parquet(ENRICH_PATH)

    # Catches drift: if enrich task wrote unexpected columns, fails here
    EnrichedOrderSchema.assert_valid(enriched)

    summary = summarise(enriched)   # @dfg.enforce(subset=False) -- exact match required
    summary.write.mode("overwrite").parquet(SUMMARY_PATH)
    spark.stop()


with DAG(
    dag_id="orders_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["orders", "pyspark"],
) as dag:

    enrich_task = PythonOperator(
        task_id="enrich_orders",
        python_callable=task_enrich,
    )

    summarise_task = PythonOperator(
        task_id="summarise_orders",
        python_callable=task_summarise,
    )

    enrich_task >> summarise_task
