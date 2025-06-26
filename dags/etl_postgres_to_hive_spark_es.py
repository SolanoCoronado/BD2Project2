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

    # 0. Poblar PostgreSQL con datos sintéticos usando Faker antes de extraer
    populate_postgres_faker = BashOperator(
        task_id="populate_postgres_faker",
        bash_command="python /app/scripts/populate_postgres_with_faker.py",
    )

    # (Opcional) Poblar con synthetic_data.sql (comentado)
    # populate_postgres = BashOperator(
    #     task_id="populate_postgres",
    #     bash_command=f"psql {POSTGRES_CONN} -f /app/scripts/synthetic_data.sql",
    # )

    # 1. Extraer datos de PostgreSQL a CSV (todas las tablas relevantes)
    extract_products = BashOperator(
        task_id="extract_products",
        bash_command=f"psql {POSTGRES_CONN} -c \"COPY (SELECT * FROM products) TO STDOUT WITH CSV HEADER\" > /app/data/products.csv",
    )
    extract_orders = BashOperator(
        task_id="extract_orders",
        bash_command=f"psql {POSTGRES_CONN} -c \"COPY (SELECT * FROM orders) TO STDOUT WITH CSV HEADER\" > /app/data/orders.csv",
    )
    extract_reservations = BashOperator(
        task_id="extract_reservations",
        bash_command=f"psql {POSTGRES_CONN} -c \"COPY (SELECT * FROM reservations) TO STDOUT WITH CSV HEADER\" > /app/data/reservations.csv",
    )
    extract_menus = BashOperator(
        task_id="extract_menus",
        bash_command=f"psql {POSTGRES_CONN} -c \"COPY (SELECT * FROM menus) TO STDOUT WITH CSV HEADER\" > /app/data/menus.csv",
    )
    extract_restaurants = BashOperator(
        task_id="extract_restaurants",
        bash_command=f"psql {POSTGRES_CONN} -c \"COPY (SELECT * FROM restaurants) TO STDOUT WITH CSV HEADER\" > /app/data/restaurants.csv",
    )

    # 1.1. Inicializar esquema Hive (modelo estrella)
    init_hive_schema = BashOperator(
        task_id="init_hive_schema",
        bash_command='''docker exec bd2project2-hive-server-1 beeline -u "jdbc:hive2://localhost:10000" -e "CREATE DATABASE IF NOT EXISTS restaurant_db; CREATE TABLE IF NOT EXISTS restaurant_db.dim_tiempo (fecha DATE, anio INT, mes INT, dia INT) STORED AS PARQUET; CREATE TABLE IF NOT EXISTS restaurant_db.dim_producto (producto_id STRING, nombre STRING, price DOUBLE, menu_id STRING) STORED AS PARQUET; CREATE TABLE IF NOT EXISTS restaurant_db.dim_restaurante (restaurante_id STRING, nombre STRING, address STRING) STORED AS PARQUET; CREATE TABLE IF NOT EXISTS restaurant_db.fact_pedidos (pedido_id STRING, user_id STRING, restaurante_id STRING, producto_id STRING, fecha DATE, total DOUBLE) STORED AS PARQUET;"'''
    )

    # 2. Transformar, crear modelo estrella y cubos OLAP en Hive con Spark
    transform_spark_olap = BashOperator(
        task_id="transform_spark_olap",
        bash_command="docker exec bd2project2-spark-1 /opt/bitnami/spark/bin/spark-submit /app/scripts/spark_analysis.py",
    )

    # Dependencias: init_hive_schema antes de Spark
    extract_products >> init_hive_schema
    extract_orders >> init_hive_schema
    extract_reservations >> init_hive_schema
    extract_menus >> init_hive_schema
    extract_restaurants >> init_hive_schema

    init_hive_schema >> transform_spark_olap

    # Dependencias: poblar antes de extraer
    populate_postgres_faker >> [extract_products, extract_orders, extract_reservations, extract_menus, extract_restaurants]
    # (Si quieres usar el SQL clásico, cambia a: populate_postgres >> [...])
