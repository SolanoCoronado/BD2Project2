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

    # Ejemplo de transformación: filtrar productos con precio > 100
    df_transformed = df_typed.filter(df_typed.price > 100)

    # Guardar resultado transformado (sin particionar, un solo archivo CSV)
    df_transformed.coalesce(1).write.mode("overwrite").option("header", True).csv(output_path)
    print(f"Transformación completada. Archivo: {output_path}")
    spark.stop()
