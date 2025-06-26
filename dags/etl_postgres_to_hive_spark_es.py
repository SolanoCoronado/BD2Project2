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

    # 2. Transformar con Spark (ejemplo: script PySpark)
    transform_spark = BashOperator(
        task_id="transform_spark",
        bash_command="docker exec bd2project2-spark-1 /opt/bitnami/spark/bin/spark-submit /app/scripts/spark_olap_analysis.py",
    )

    # 2.1. Inicializar esquema de Hive ejecutando sentencias directamente
    init_hive_schema = BashOperator(
        task_id="init_hive_schema",
        bash_command="docker exec bd2project2-hive-server-1 beeline -u 'jdbc:hive2://localhost:10000' -e \"CREATE DATABASE IF NOT EXISTS restaurant_db; USE restaurant_db; CREATE TABLE IF NOT EXISTS pedidos (pedido_id INT, usuario_id INT, producto_id INT, reserva_id INT, fecha DATE, cantidad INT, total DECIMAL(10,2)) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE; CREATE TABLE IF NOT EXISTS dim_usuarios (usuario_id INT, nombre STRING, email STRING, genero STRING, fecha_nacimiento DATE, ubicacion STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE; CREATE TABLE IF NOT EXISTS dim_productos (producto_id INT, nombre STRING, tipo STRING, precio DECIMAL(10,2)) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE; CREATE TABLE IF NOT EXISTS dim_reservas (reserva_id INT, usuario_id INT, fecha DATE, hora STRING, cantidad_personas INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE; CREATE TABLE IF NOT EXISTS dim_tiempo (fecha DATE, anio INT, mes INT, dia INT, trimestre INT, dia_semana STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE; CREATE TABLE IF NOT EXISTS dim_menus (menu_id INT, restaurant_id INT, nombre STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE; CREATE TABLE IF NOT EXISTS dim_restaurantes (restaurant_id INT, nombre STRING, direccion STRING, owner_id STRING, is_available BOOLEAN, table_quantity INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE;\"",
    )

    # 3. Cargar en Hive (usando Beeline o PyHive) para cada tabla relevante
    load_hive_productos = BashOperator(
        task_id="load_hive_productos",
        bash_command="docker exec bd2project2-hive-server-1 beeline -u 'jdbc:hive2://localhost:10000/restaurant_db' -e \"LOAD DATA LOCAL INPATH '/app/data/products.csv' OVERWRITE INTO TABLE dim_productos\"",
    )
    load_hive_pedidos = BashOperator(
        task_id="load_hive_pedidos",
        bash_command="docker exec bd2project2-hive-server-1 beeline -u 'jdbc:hive2://localhost:10000/restaurant_db' -e \"LOAD DATA LOCAL INPATH '/app/data/orders.csv' OVERWRITE INTO TABLE pedidos\"",
    )
    load_hive_reservas = BashOperator(
        task_id="load_hive_reservas",
        bash_command="docker exec bd2project2-hive-server-1 beeline -u 'jdbc:hive2://localhost:10000/restaurant_db' -e \"LOAD DATA LOCAL INPATH '/app/data/reservations.csv' OVERWRITE INTO TABLE dim_reservas\"",
    )
    load_hive_menus = BashOperator(
        task_id="load_hive_menus",
        bash_command="docker exec bd2project2-hive-server-1 beeline -u 'jdbc:hive2://localhost:10000/restaurant_db' -e \"LOAD DATA LOCAL INPATH '/app/data/menus.csv' OVERWRITE INTO TABLE dim_menus\"",
    )
    load_hive_restaurantes = BashOperator(
        task_id="load_hive_restaurantes",
        bash_command="docker exec bd2project2-hive-server-1 beeline -u 'jdbc:hive2://localhost:10000/restaurant_db' -e \"LOAD DATA LOCAL INPATH '/app/data/restaurants.csv' OVERWRITE INTO TABLE dim_restaurantes\"",
    )

    # Eliminar la tarea y dependencia de reindex_elasticsearch si no usas ElasticSearch
    # def reindex_es():
    #     print("Reindexando ElasticSearch...")
    # reindex_elasticsearch = PythonOperator(
    #     task_id="reindex_elasticsearch",
    #     python_callable=reindex_es,
    # )
    # load_hive_productos >> reindex_es

    # 5. Cargar resultados OLAP en Hive (comentado temporalmente)
    # load_hive_olap_tendencias = BashOperator(
    #     task_id="load_hive_olap_tendencias",
    #     bash_command="docker exec bd2project2-hive-server-1 beeline -u 'jdbc:hive2://localhost:10000/restaurant_db' -e \"CREATE TABLE IF NOT EXISTS olap_tendencias_consumo (order_year INT, order_month INT, ventas_totales DOUBLE) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE; LOAD DATA LOCAL INPATH '/data/olap_tendencias_consumo.csv' OVERWRITE INTO TABLE olap_tendencias_consumo\"",
    # )
    # load_hive_olap_horarios = BashOperator(
    #     task_id="load_hive_olap_horarios",
    #     bash_command="docker exec bd2project2-hive-server-1 beeline -u 'jdbc:hive2://localhost:10000/restaurant_db' -e \"CREATE TABLE IF NOT EXISTS olap_horarios_pico (order_hour INT, cantidad_pedidos INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE; LOAD DATA LOCAL INPATH '/data/olap_horarios_pico.csv' OVERWRITE INTO TABLE olap_horarios_pico\"",
    # )
    # load_hive_olap_crecimiento = BashOperator(
    #     task_id="load_hive_olap_crecimiento",
    #     bash_command="docker exec bd2project2-hive-server-1 beeline -u 'jdbc:hive2://localhost:10000/restaurant_db' -e \"CREATE TABLE IF NOT EXISTS olap_crecimiento_mensual (order_year INT, order_month INT, ventas_totales DOUBLE, ventas_previas DOUBLE, crecimiento_mensual DOUBLE) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE; LOAD DATA LOCAL INPATH '/data/olap_crecimiento_mensual.csv' OVERWRITE INTO TABLE olap_crecimiento_mensual\"",
    # )
    # load_hive_olap_ventas_tipo = BashOperator(
    #     task_id="load_hive_olap_ventas_tipo",
    #     bash_command="docker exec bd2project2-hive-server-1 beeline -u 'jdbc:hive2://localhost:10000/restaurant_db' -e \"CREATE TABLE IF NOT EXISTS olap_ventas_por_tipo (tipo STRING, ventas_totales DOUBLE) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE; LOAD DATA LOCAL INPATH '/data/olap_ventas_por_tipo.csv' OVERWRITE INTO TABLE olap_ventas_por_tipo\"",
    # )
    # load_hive_olap_actividad_ubicacion = BashOperator(
    #     task_id="load_hive_olap_actividad_ubicacion",
    #     bash_command="docker exec bd2project2-hive-server-1 beeline -u 'jdbc:hive2://localhost:10000/restaurant_db' -e \"CREATE TABLE IF NOT EXISTS olap_actividad_ubicacion (ubicacion STRING, actividad INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE; LOAD DATA LOCAL INPATH '/data/olap_actividad_ubicacion.csv' OVERWRITE INTO TABLE olap_actividad_ubicacion\"",
    # )
    # load_hive_olap_frecuencia_usuarios = BashOperator(
    #     task_id="load_hive_olap_frecuencia_usuarios",
    #     bash_command="docker exec bd2project2-hive-server-1 beeline -u 'jdbc:hive2://localhost:10000/restaurant_db' -e \"CREATE TABLE IF NOT EXISTS olap_frecuencia_usuarios (user_id INT, num_operaciones INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE; LOAD DATA LOCAL INPATH '/data/olap_frecuencia_usuarios.csv' OVERWRITE INTO TABLE olap_frecuencia_usuarios\"",
    # )

    # --- Eliminar temporalmente las tareas de mover archivos OLAP y cargar a Hive ---
    # (Puedes descomentar y restaurar estas tareas cuando quieras cargar los resultados OLAP a Hive)

    # --- Eliminar dependencias relacionadas ---
    # init_hive_schema >> move_olap_tendencias >> load_hive_olap_tendencias
    # init_hive_schema >> move_olap_horarios >> load_hive_olap_horarios
    # init_hive_schema >> move_olap_crecimiento >> load_hive_olap_crecimiento
    # init_hive_schema >> move_olap_ventas_tipo >> load_hive_olap_ventas_tipo
    # init_hive_schema >> move_olap_actividad_ubicacion >> load_hive_olap_actividad_ubicacion
    # init_hive_schema >> move_olap_frecuencia_usuarios >> load_hive_olap_frecuencia_usuarios

    # Dependencias
    extract_products >> transform_spark
    extract_orders >> transform_spark
    extract_reservations >> transform_spark
    extract_menus >> transform_spark
    extract_restaurants >> transform_spark

    transform_spark >> init_hive_schema
    init_hive_schema >> [load_hive_productos, load_hive_pedidos, load_hive_reservas, load_hive_menus, load_hive_restaurantes]  # Agrega aquí load_hive_menus, load_hive_restaurantes si las usas

    # load_hive_productos >> reindex_es (si implementas la tarea de reindexado)

    # Dependencias para OLAP y OLAP adicional (eliminadas temporalmente)
    # transform_spark >> init_hive_schema
    # init_hive_schema >> [
    #     load_hive_olap_tendencias,
    #     load_hive_olap_horarios,
    #     load_hive_olap_crecimiento,
    #     load_hive_olap_ventas_tipo,
    #     load_hive_olap_actividad_ubicacion,
    #     load_hive_olap_frecuencia_usuarios
    # ]

    # Dependencias: poblar antes de extraer
    populate_postgres_faker >> [extract_products, extract_orders, extract_reservations, extract_menus, extract_restaurants]
    # (Si quieres usar el SQL clásico, cambia a: populate_postgres >> [...])
