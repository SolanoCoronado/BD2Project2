# transform_postgres_to_hive.py
"""
Script PySpark para transformar datos extraídos de PostgreSQL antes de cargar en Hive.
Este script debe ejecutarse dentro del contenedor de Spark.
"""
from pyspark.sql import SparkSession
import sys

if __name__ == "__main__":
    input_path = sys.argv[1]  # Ruta al CSV exportado desde Postgres
    output_path = "/app/data/transformed_data.csv"

    spark = SparkSession.builder.appName("TransformPostgresToHive").getOrCreate()

    # Leer CSV exportado de Postgres
    df = spark.read.option("header", True).csv(input_path)

    # Limpieza: eliminar filas con valores nulos en columnas clave
    df_clean = df.dropna(subset=["id", "menu_id", "name", "price"])

    # Conversión de tipos
    df_typed = df_clean.withColumn("id", df_clean["id"].cast("int")) \
                        .withColumn("menu_id", df_clean["menu_id"].cast("int")) \
                        .withColumn("price", df_clean["price"].cast("double"))

    # Filtro más suave: solo precios positivos y no nulos
    df_filtered = df_typed.filter(df_typed.price > 0)

    # Guardar datos transformados completos
    df_filtered.coalesce(1).write.mode("overwrite").option("header", True).csv(output_path)

    # --- Análisis OLAP ---
    from pyspark.sql.functions import month, year, hour, sum as _sum, count as _count, col, lag, lit
    from pyspark.sql.window import Window

    # Tendencias de consumo: ventas totales por mes
    df_orders = df_filtered  # Asume que el input es orders.csv o similar
    if 'total_price' in df_orders.columns and 'id' in df_orders.columns:
        df_orders = df_orders.withColumn("total_price", col("total_price").cast("double"))
        if 'order_date' in df_orders.columns:
            df_orders = df_orders.withColumn("order_month", month(col("order_date"))) \
                                   .withColumn("order_year", year(col("order_date")))
        elif 'reservation_time' in df_orders.columns:
            df_orders = df_orders.withColumn("order_month", month(col("reservation_time"))) \
                                   .withColumn("order_year", year(col("reservation_time")))
        else:
            df_orders = df_orders.withColumn("order_month", lit(None)).withColumn("order_year", lit(None))

        tendencias = df_orders.groupBy("order_year", "order_month").agg(_sum("total_price").alias("ventas_totales"))
        tendencias.coalesce(1).write.mode("overwrite").option("header", True).csv("/app/data/olap_tendencias_consumo.csv")

        # Horarios pico: cantidad de pedidos por hora
        if 'order_date' in df_orders.columns:
            df_orders = df_orders.withColumn("order_hour", hour(col("order_date")))
        elif 'reservation_time' in df_orders.columns:
            df_orders = df_orders.withColumn("order_hour", hour(col("reservation_time")))
        else:
            df_orders = df_orders.withColumn("order_hour", lit(None))
        horarios_pico = df_orders.groupBy("order_hour").agg(_count("id").alias("cantidad_pedidos"))
        horarios_pico.coalesce(1).write.mode("overwrite").option("header", True).csv("/app/data/olap_horarios_pico.csv")

        # Crecimiento mensual: variación de ventas mes a mes
        windowSpec = Window.orderBy("order_year", "order_month")
        tendencias = tendencias.withColumn("ventas_previas", lag("ventas_totales").over(windowSpec))
        tendencias = tendencias.withColumn("crecimiento_mensual", (col("ventas_totales") - col("ventas_previas")) / col("ventas_previas"))
        tendencias.coalesce(1).write.mode("overwrite").option("header", True).csv("/app/data/olap_crecimiento_mensual.csv")

    # --- OLAP adicional ---
    # 1. Ventas por tipo de producto (asume columna 'tipo' o 'category' en productos)
    if 'tipo' in df_orders.columns or 'category' in df_orders.columns:
        tipo_col = 'tipo' if 'tipo' in df_orders.columns else 'category'
        ventas_tipo = df_orders.groupBy(tipo_col).agg(_sum("total_price").alias("ventas_totales"))
        ventas_tipo.coalesce(1).write.mode("overwrite").option("header", True).csv("/app/data/olap_ventas_por_tipo.csv")

    # 2. Actividad por ubicación (asume columna 'ubicacion' o 'address' en usuarios o restaurantes)
    if 'ubicacion' in df_orders.columns:
        actividad_ubicacion = df_orders.groupBy("ubicacion").agg(_count("id").alias("actividad"))
        actividad_ubicacion.coalesce(1).write.mode("overwrite").option("header", True).csv("/app/data/olap_actividad_ubicacion.csv")
    elif 'address' in df_orders.columns:
        actividad_ubicacion = df_orders.groupBy("address").agg(_count("id").alias("actividad"))
        actividad_ubicacion.coalesce(1).write.mode("overwrite").option("header", True).csv("/app/data/olap_actividad_ubicacion.csv")

    # 3. Frecuencia de uso por usuario (cantidad de pedidos/reservas por usuario)
    if 'user_id' in df_orders.columns:
        frecuencia_usuarios = df_orders.groupBy("user_id").agg(_count("id").alias("num_operaciones"))
        frecuencia_usuarios.coalesce(1).write.mode("overwrite").option("header", True).csv("/app/data/olap_frecuencia_usuarios.csv")

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
        '/app/data/transformed_data.csv',
        '/app/data/olap_tendencias_consumo.csv',
        '/app/data/olap_horarios_pico.csv',
        '/app/data/olap_crecimiento_mensual.csv',
        '/app/data/olap_ventas_por_tipo.csv',
        '/app/data/olap_actividad_ubicacion.csv',
        '/app/data/olap_frecuencia_usuarios.csv',
    ]
    for path in output_files:
        flatten_csv_dir(path)

    spark.stop()
