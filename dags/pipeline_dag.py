from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
}

with DAG(
    dag_id='etl_pipeline',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    description='ETL pipeline: Extract from PostgreSQL, process with Spark, load to Hive, reindex ElasticSearch',
) as dag:

    extract_postgres = BashOperator(
        task_id='extract_postgres',
        bash_command='python /app/scripts/extract_postgres.py',
    )

    process_spark = BashOperator(
        task_id='process_spark',
        bash_command='spark-submit /app/spark_analysis.py',
    )

    load_hive = BashOperator(
        task_id='load_hive',
        bash_command='python /app/scripts/load_to_hive.py',
    )

    reindex_elasticsearch = BashOperator(
        task_id='reindex_elasticsearch',
        bash_command='python /app/scripts/reindex_elasticsearch.py',
        trigger_rule='all_done',
    )

    extract_postgres >> process_spark >> load_hive >> reindex_elasticsearch
