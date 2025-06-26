"""
DAG: etl_postgres_to_hive_spark_es.py

- Extrae datos de PostgreSQL
- Transforma con Spark
- Carga en Hive
- Reindexa ElasticSearch si cambia el catálogo de productos
"""
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import os

# Configuración
POSTGRES_CONN = "postgresql://sa:password1234@postgres:5432/DBTC01"
HIVE_CONN = "hive://hive-server:10000/default"
SPARK_SCRIPT = "/app/scripts/transform_postgres_to_hive.py"
EXPORT_PATH = "/app/data/exported_data.csv"

# DAG
with DAG(
    dag_id="etl_postgres_to_hive_spark_es",
    default_args={
        "owner": "airflow",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    description="ETL: Postgres -> Spark -> Hive -> ElasticSearch",
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
) as dag:

    # 1. Extraer datos de PostgreSQL a CSV
    extract_postgres = BashOperator(
        task_id="extract_postgres",
        bash_command=f"psql {POSTGRES_CONN} -c \"COPY (SELECT * FROM products) TO STDOUT WITH CSV HEADER\" > {EXPORT_PATH}",
    )

    # 2. Transformar con Spark (ejemplo: script PySpark)
    transform_spark = BashOperator(
        task_id="transform_spark",
        bash_command="docker exec bd2project2-spark-1 /opt/bitnami/spark/bin/spark-submit /app/scripts/transform_postgres_to_hive.py /app/data/exported_data.csv",
    )

    # 2.1. Inicializar esquema de Hive ejecutando sentencias directamente
    init_hive_schema = BashOperator(
        task_id="init_hive_schema",
        bash_command="docker exec bd2project2-hive-server-1 beeline -u 'jdbc:hive2://localhost:10000' -e \"CREATE DATABASE IF NOT EXISTS restaurant_db; USE restaurant_db; CREATE TABLE IF NOT EXISTS pedidos (pedido_id INT, usuario_id INT, producto_id INT, reserva_id INT, fecha DATE, cantidad INT, total DECIMAL(10,2)) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE; CREATE TABLE IF NOT EXISTS dim_usuarios (usuario_id INT, nombre STRING, email STRING, genero STRING, fecha_nacimiento DATE, ubicacion STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE; CREATE TABLE IF NOT EXISTS dim_productos (producto_id INT, nombre STRING, tipo STRING, precio DECIMAL(10,2)) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE; CREATE TABLE IF NOT EXISTS dim_reservas (reserva_id INT, usuario_id INT, fecha DATE, hora STRING, cantidad_personas INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE; CREATE TABLE IF NOT EXISTS dim_tiempo (fecha DATE, anio INT, mes INT, dia INT, trimestre INT, dia_semana STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE;\"",
    )

    # 3. Cargar en Hive (usando Beeline o PyHive)
    load_hive = BashOperator(
        task_id="load_hive",
        bash_command="docker exec bd2project2-hive-server-1 beeline -u 'jdbc:hive2://localhost:10000/restaurant_db' -e \"LOAD DATA LOCAL INPATH '/app/data/exported_data.csv' OVERWRITE INTO TABLE dim_productos\"",
    )

    # 4. Reindexar ElasticSearch si cambia el catálogo de productos
    def reindex_es():
        # Aquí deberías poner la lógica real de reindexado
        print("Reindexando ElasticSearch...")

    reindex_elasticsearch = PythonOperator(
        task_id="reindex_elasticsearch",
        python_callable=reindex_es,
    )

    extract_postgres >> transform_spark >> init_hive_schema >> load_hive >> reindex_elasticsearch
