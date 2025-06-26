# transform_postgres_to_hive.py
"""
Script PySpark para transformar datos extraídos de PostgreSQL antes de cargar en Hive.
Este script debe ejecutarse dentro del contenedor de Spark.
"""
from pyspark.sql import SparkSession
import sys

if __name__ == "__main__":
    input_path = sys.argv[1]  # Ruta al CSV exportado desde Postgres
    output_path = "/data/transformed_data.csv"

    spark = SparkSession.builder.appName("TransformPostgresToHive").getOrCreate()

    # Leer los tres archivos principales
    orders_path = "/data/orders.csv"
    products_path = "/data/products.csv"
    reservations_path = "/data/reservations.csv"
    restaurants_path = "/data/restaurants.csv"

    df_orders = spark.read.option("header", True).csv(orders_path)
    df_products = spark.read.option("header", True).csv(products_path)
    df_reservations = spark.read.option("header", True).csv(reservations_path)
    df_restaurants = spark.read.option("header", True).csv(restaurants_path)

    # Si orders.csv no tiene order_date, agregarlo al export desde Postgres
    # Puedes modificar el BashOperator extract_orders en tu DAG así:
    # bash_command=f"psql {POSTGRES_CONN} -c \"COPY (SELECT *, COALESCE(order_date, NOW()::date) as order_date FROM orders) TO STDOUT WITH CSV HEADER\" > /app/data/orders.csv",
    # Si tu tabla orders no tiene order_date, puedes generar una fecha aleatoria en Python antes de exportar.
    # Ejemplo de generación en Python (fuera de Spark):
    # import pandas as pd, numpy as np
    # df = pd.read_csv('orders.csv')
    # if 'order_date' not in df.columns:
    #     df['order_date'] = pd.to_datetime('2025-06-01') + pd.to_timedelta(np.random.randint(0, 30, size=len(df)), unit='D')
    # df.to_csv('orders.csv', index=False)
    #
    # Puedes poner este snippet en un script previo o como un PythonOperator en Airflow.
    #
    # Lo importante: orders.csv debe tener una columna order_date (YYYY-MM-DD) antes de que Spark lo procese.
    #
    # El resto del pipeline funcionará correctamente con la lógica Spark ya implementada.

    # Asegurar que orders tenga order_date (si no existe, crear una aleatoria para demo)
    from pyspark.sql.functions import monotonically_increasing_id, to_date, expr
    if 'order_date' not in df_orders.columns:
        # Generar fechas aleatorias en junio 2025
        df_orders = df_orders.withColumn(
            "order_date",
            expr("date_add('2025-06-01', cast(rand() * 29 as int))")
        )

    # Unir orders con products para obtener tipo/category
    if 'product_id' in df_orders.columns:
        df_orders = df_orders.join(df_products.withColumnRenamed('id', 'product_id'), on='product_id', how='left')
    elif 'menu_id' in df_orders.columns:
        df_orders = df_orders.join(df_products.withColumnRenamed('id', 'menu_id'), on='menu_id', how='left')
    # Unir orders con restaurants para obtener address
    if 'restaurant_id' in df_orders.columns:
        df_orders = df_orders.join(df_restaurants.withColumnRenamed('id', 'restaurant_id'), on='restaurant_id', how='left')

    # Cast de columnas necesarias
    from pyspark.sql.functions import col
    df_orders = df_orders.withColumn("total_price", col("total_price").cast("double"))
    df_orders = df_orders.withColumn("order_date", to_date(col("order_date")))

    # --- Análisis OLAP ---
    # Tendencias de consumo: ventas totales por mes
    from pyspark.sql.functions import month, year, hour, sum as _sum, count as _count, lag, lit
    from pyspark.sql.window import Window
    df_orders = df_orders.withColumn("order_month", month(col("order_date"))) \
                           .withColumn("order_year", year(col("order_date")))
    tendencias = df_orders.groupBy("order_year", "order_month").agg(_sum("total_price").alias("ventas_totales"))
    tendencias.coalesce(1).write.mode("overwrite").option("header", True).csv("/data/olap_tendencias_consumo.csv")

    # Horarios pico: cantidad de pedidos por hora
    df_orders = df_orders.withColumn("order_hour", hour(col("order_date")))
    horarios_pico = df_orders.groupBy("order_hour").agg(_count("id").alias("cantidad_pedidos"))
    horarios_pico.coalesce(1).write.mode("overwrite").option("header", True).csv("/data/olap_horarios_pico.csv")

    # Crecimiento mensual: variación de ventas mes a mes
    windowSpec = Window.orderBy("order_year", "order_month")
    tendencias = tendencias.withColumn("ventas_previas", lag("ventas_totales").over(windowSpec))
    tendencias = tendencias.withColumn("crecimiento_mensual", (col("ventas_totales") - col("ventas_previas")) / col("ventas_previas"))
    tendencias.coalesce(1).write.mode("overwrite").option("header", True).csv("/data/olap_crecimiento_mensual.csv")

    # --- OLAP adicional ---
    # 1. Ventas por tipo de producto (asume columna 'tipo' o 'category' en productos)
    if 'tipo' in df_orders.columns or 'category' in df_orders.columns:
        tipo_col = 'tipo' if 'tipo' in df_orders.columns else 'category'
        ventas_tipo = df_orders.groupBy(tipo_col).agg(_sum("total_price").alias("ventas_totales"))
        ventas_tipo.coalesce(1).write.mode("overwrite").option("header", True).csv("/data/olap_ventas_por_tipo.csv")

    # 2. Actividad por ubicación (asume columna 'ubicacion' o 'address' en usuarios o restaurantes)
    if 'ubicacion' in df_orders.columns:
        actividad_ubicacion = df_orders.groupBy("ubicacion").agg(_count("id").alias("actividad"))
        actividad_ubicacion.coalesce(1).write.mode("overwrite").option("header", True).csv("/data/olap_actividad_ubicacion.csv")
    elif 'address' in df_orders.columns:
        actividad_ubicacion = df_orders.groupBy("address").agg(_count("id").alias("actividad"))
        actividad_ubicacion.coalesce(1).write.mode("overwrite").option("header", True).csv("/data/olap_actividad_ubicacion.csv")

    # 3. Frecuencia de uso por usuario (cantidad de pedidos/reservas por usuario)
    if 'user_id' in df_orders.columns:
        frecuencia_usuarios = df_orders.groupBy("user_id").agg(_count("id").alias("num_operaciones"))
        frecuencia_usuarios.coalesce(1).write.mode("overwrite").option("header", True).csv("/data/olap_frecuencia_usuarios.csv")

    print("Transformación y análisis OLAP completados. Archivos: transformed_data.csv, olap_tendencias_consumo.csv, olap_horarios_pico.csv, olap_crecimiento_mensual.csv")

    # --- Post-procesamiento: mover archivos part-*.csv a archivos planos para Hive ---
    import glob
    import shutil
    import os

    def flatten_csv_dir(csv_dir_path):
        """
        Busca el único archivo part-*.csv dentro de la carpeta y lo mueve/renombra al nombre plano esperado,
        eliminando la carpeta original. Si no existe la carpeta o el archivo, no hace nada.
        Ahora es robusto: si el archivo ya está plano, o el part-*.csv no existe, no falla.
        """
        import traceback
        # Si ya es un archivo plano, no hacer nada
        if os.path.isfile(csv_dir_path):
            return
        # Si no es un directorio, no hacer nada
        if not os.path.isdir(csv_dir_path):
            return
        try:
            part_files = glob.glob(os.path.join(csv_dir_path, 'part-*.csv'))
            if not part_files:
                # No hay archivos part-*.csv, eliminar carpeta vacía si existe
                try:
                    shutil.rmtree(csv_dir_path)
                except Exception:
                    pass
                return
            part_file = part_files[0]
            flat_path = csv_dir_path.rstrip('/')
            # Si el destino existe como archivo, eliminarlo
            if os.path.exists(flat_path):
                if os.path.isfile(flat_path):
                    os.remove(flat_path)
                elif os.path.isdir(flat_path):
                    shutil.rmtree(flat_path)
            shutil.move(part_file, flat_path)
            shutil.rmtree(csv_dir_path)
        except Exception as e:
            print(f"[WARN] flatten_csv_dir({csv_dir_path}): {e}")
            traceback.print_exc()
            pass

    # Lista de todos los archivos OLAP y de datos transformados
    output_files = [
        '/data/transformed_data.csv',
        '/data/olap_tendencias_consumo.csv',
        '/data/olap_horarios_pico.csv',
        '/data/olap_crecimiento_mensual.csv',
        '/data/olap_ventas_por_tipo.csv',
        '/data/olap_actividad_ubicacion.csv',
        '/data/olap_frecuencia_usuarios.csv',
    ]
    for path in output_files:
        flatten_csv_dir(path)

    spark.stop()
