from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("OLAP desde CSVs").getOrCreate()

# Cargar los CSV como DataFrames
orders = spark.read.option("header", True).csv("/data/orders.csv")
products = spark.read.option("header", True).csv("/data/products.csv")
menus = spark.read.option("header", True).csv("/data/menus.csv")
reservations = spark.read.option("header", True).csv("/data/reservations.csv")
restaurants = spark.read.option("header", True).csv("/data/restaurants.csv")

# Preprocesamiento: aseguramos tipos y columnas necesarias
orders = orders.withColumn("order_date", col("order_date").cast("date"))
orders = orders.withColumn("total_price", col("total_price").cast("double"))

# Registrar DataFrames como tablas temporales para Spark SQL
orders.createOrReplaceTempView("orders")
products.createOrReplaceTempView("products")
menus.createOrReplaceTempView("menus")
reservations.createOrReplaceTempView("reservations")
restaurants.createOrReplaceTempView("restaurants")

# Tendencias de consumo (ventas por mes y tipo de producto)
tendencias_sql = spark.sql("""
    SELECT
        YEAR(o.order_date) AS anio,
        MONTH(o.order_date) AS mes,
        p.category,
        SUM(o.total_price) AS total_ventas,
        COUNT(o.id) AS num_pedidos,
        AVG(o.total_price) AS promedio_venta
    FROM orders o
    LEFT JOIN products p ON o.product_id = p.id
    GROUP BY anio, mes, p.category
    ORDER BY anio, mes, total_ventas DESC
""")
tendencias_sql.coalesce(1).write.mode("overwrite").option("header", True).csv("/data/tendencias_consumo.csv")

# Horarios pico (pedidos por hora)
horarios_sql = spark.sql("""
    SELECT
        HOUR(order_date) AS order_hour,
        COUNT(id) AS cantidad_pedidos
    FROM orders
    GROUP BY order_hour
    ORDER BY cantidad_pedidos DESC
""")
horarios_sql.coalesce(1).write.mode("overwrite").option("header", True).csv("/data/horarios_pico.csv")

# Crecimiento mensual (ventas y pedidos por mes)
crecimiento_sql = spark.sql("""
    SELECT
        YEAR(order_date) AS anio,
        MONTH(order_date) AS mes,
        SUM(total_price) AS ingresos_totales,
        COUNT(id) AS total_pedidos
    FROM orders
    GROUP BY anio, mes
    ORDER BY anio, mes
""")
crecimiento_sql.coalesce(1).write.mode("overwrite").option("header", True).csv("/data/crecimiento_mensual.csv")

spark.stop()
