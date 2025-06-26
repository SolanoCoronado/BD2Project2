"""
Script Spark: ETL y modelo estrella OLAP para restaurante
- Lee CSVs de /data
- Limpia y transforma datos con DataFrames y SparkSQL
- Crea tablas de hecho y dimensiones (modelo estrella)
- Carga a Hive
- Crea cubos/vistas OLAP en Hive
- Realiza 3 análisis: tendencias de consumo, horarios pico, crecimiento mensual
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

# 2. Limpieza y transformación
orders = orders.withColumn("order_date", col("order_date").cast("date"))
orders = orders.withColumn("total_price", col("total_price").cast("double"))
products = products.withColumn("price", col("price").cast("double"))

# 3. Crear dimensiones
# Tiempo
dim_tiempo = orders.select(col("order_date").alias("fecha")) \
    .dropna().distinct() \
    .withColumn("anio", year(col("fecha"))) \
    .withColumn("mes", month(col("fecha"))) \
    .withColumn("dia", dayofmonth(col("fecha")))
dim_tiempo.write.mode("overwrite").format("hive").saveAsTable("restaurant_db.dim_tiempo")

# Producto
dim_producto = products.select(
    col("id").alias("producto_id"),
    col("name").alias("nombre"),
    col("price"),
    col("menu_id")
).dropna()
dim_producto.write.mode("overwrite").format("hive").saveAsTable("restaurant_db.dim_producto")

# Restaurante
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

# 6. Análisis requeridos
# Tendencias de consumo
spark.sql("SELECT * FROM restaurant_db.cubo_ventas_tiempo").show()
# Horarios pico (si tienes hora en los datos, puedes agregarla a dim_tiempo y agrupar por hora)
# Crecimiento mensual
spark.sql("SELECT anio, mes, total_ventas, LAG(total_ventas) OVER (ORDER BY anio, mes) AS ventas_previas, (total_ventas - LAG(total_ventas) OVER (ORDER BY anio, mes)) / LAG(total_ventas) OVER (ORDER BY anio, mes) AS crecimiento FROM restaurant_db.cubo_ventas_tiempo").show()

spark.stop()
