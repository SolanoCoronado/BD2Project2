"""
Script Spark ETL + OLAP completo para arquitectura estrella
- Lee los CSV de /data (orders, products, restaurants, reservations, menus)
- Limpia y transforma los datos usando solo los campos presentes
- Crea tablas de hecho y dimensiones (modelo estrella) en Hive
- Crea 5 cubos/vistas OLAP en Hive
- Todo listo para automatizar en el DAG
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month, dayofmonth, hour, sum as _sum, count as _count, avg as _avg

spark = SparkSession.builder \
    .appName("ETL Restaurante OLAP") \
    .enableHiveSupport() \
    .getOrCreate()

# 1. Leer CSVs
orders = spark.read.option("header", True).csv("/data/orders.csv")
products = spark.read.option("header", True).csv("/data/products.csv")
restaurants = spark.read.option("header", True).csv("/data/restaurants.csv")
reservations = spark.read.option("header", True).csv("/data/reservations.csv")
menus = spark.read.option("header", True).csv("/data/menus.csv")

# 2. Limpieza y transformación básica
def safe_cast(df, colname, dtype):
    if colname in df.columns:
        return df.withColumn(colname, col(colname).cast(dtype))
    return df
orders = safe_cast(orders, "order_date", "date")
orders = safe_cast(orders, "total_price", "double")
products = safe_cast(products, "price", "double")
restaurants = safe_cast(restaurants, "table_quantity", "int")

# 3. Crear dimensiones (solo con campos presentes)
dim_tiempo = orders.select(col("order_date").alias("fecha")).dropna().distinct() \
    .withColumn("anio", year(col("fecha"))) \
    .withColumn("mes", month(col("fecha"))) \
    .withColumn("dia", dayofmonth(col("fecha")))
dim_tiempo.write.mode("overwrite").format("hive").saveAsTable("restaurant_db.dim_tiempo")

dim_producto = products.select(
    col("id").alias("producto_id"),
    col("name").alias("nombre"),
    col("price"),
    col("menu_id")
).dropna()
dim_producto.write.mode("overwrite").format("hive").saveAsTable("restaurant_db.dim_producto")

dim_restaurante = restaurants.select(
    col("id").alias("restaurante_id"),
    col("name").alias("nombre"),
    col("address")
).dropna()
dim_restaurante.write.mode("overwrite").format("hive").saveAsTable("restaurant_db.dim_restaurante")

# 4. Crear tabla de hechos (pedidos)
fact_pedidos = orders.select(
    col("id").alias("pedido_id"),
    col("user_id"),
    col("restaurant_id").alias("restaurante_id"),
    col("product_id").alias("producto_id"),
    col("order_date").alias("fecha"),
    col("total_price").alias("total")
).dropna()
fact_pedidos.write.mode("overwrite").format("hive").saveAsTable("restaurant_db.fact_pedidos")

# 5. Cubos/vistas OLAP en Hive (usando SparkSQL)
spark.sql("""
CREATE OR REPLACE VIEW restaurant_db.cubo_ventas_tiempo AS
SELECT t.anio, t.mes, SUM(f.total) AS total_ventas, COUNT(f.pedido_id) AS num_pedidos
FROM restaurant_db.fact_pedidos f
JOIN restaurant_db.dim_tiempo t ON f.fecha = t.fecha
GROUP BY t.anio, t.mes
ORDER BY t.anio, t.mes
""")

spark.sql("""
CREATE OR REPLACE VIEW restaurant_db.cubo_ventas_producto AS
SELECT p.nombre, SUM(f.total) AS total_ventas, COUNT(f.pedido_id) AS num_pedidos
FROM restaurant_db.fact_pedidos f
JOIN restaurant_db.dim_producto p ON f.producto_id = p.producto_id
GROUP BY p.nombre
ORDER BY total_ventas DESC
""")

spark.sql("""
CREATE OR REPLACE VIEW restaurant_db.cubo_ventas_restaurante AS
SELECT r.nombre, SUM(f.total) AS total_ventas, COUNT(f.pedido_id) AS num_pedidos
FROM restaurant_db.fact_pedidos f
JOIN restaurant_db.dim_restaurante r ON f.restaurante_id = r.restaurante_id
GROUP BY r.nombre
ORDER BY total_ventas DESC
""")

spark.sql("""
CREATE OR REPLACE VIEW restaurant_db.cubo_frecuencia_usuario AS
SELECT user_id, COUNT(pedido_id) AS pedidos_realizados, SUM(total) AS total_gastado
FROM restaurant_db.fact_pedidos
GROUP BY user_id
ORDER BY pedidos_realizados DESC
""")

spark.sql("""
CREATE OR REPLACE VIEW restaurant_db.cubo_ventas_diarias AS
SELECT fecha, SUM(total) AS total_ventas, COUNT(pedido_id) AS num_pedidos
FROM restaurant_db.fact_pedidos
GROUP BY fecha
ORDER BY fecha
""")

# Exportar cubos/vistas OLAP a CSV para análisis externo o visualización
spark.sql("SELECT * FROM restaurant_db.cubo_ventas_tiempo").toPandas().to_csv("/data/cubo_ventas_tiempo.csv", index=False)
spark.sql("SELECT * FROM restaurant_db.cubo_ventas_producto").toPandas().to_csv("/data/cubo_ventas_producto.csv", index=False)
spark.sql("SELECT * FROM restaurant_db.cubo_ventas_restaurante").toPandas().to_csv("/data/cubo_ventas_restaurante.csv", index=False)
spark.sql("SELECT * FROM restaurant_db.cubo_frecuencia_usuario").toPandas().to_csv("/data/cubo_frecuencia_usuario.csv", index=False)
spark.sql("SELECT * FROM restaurant_db.cubo_ventas_diarias").toPandas().to_csv("/data/cubo_ventas_diarias.csv", index=False)

spark.stop()
