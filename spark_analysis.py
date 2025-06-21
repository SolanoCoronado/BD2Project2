from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, count, avg

# Inicializa Spark con soporte Hive
spark = SparkSession.builder \
    .appName("Procesamiento Restaurante") \
    .enableHiveSupport() \
    .getOrCreate()

# Carga las tablas Hive como DataFrames
pedidos = spark.table("restaurant_db.pedidos")
productos = spark.table("restaurant_db.dim_productos")
reservas = spark.table("restaurant_db.dim_reservas")
tiempo = spark.table("restaurant_db.dim_tiempo")

# 1. Tendencias de consumo: ventas por mes y tipo de producto
tendencias = pedidos.join(productos, "producto_id") \
    .join(tiempo, pedidos.fecha == tiempo.fecha) \
    .groupBy("anio", "mes", "tipo") \
    .agg(
        sum("total").alias("total_ventas"),
        count("pedido_id").alias("num_pedidos"),
        avg("total").alias("promedio_venta")
    ) \
    .orderBy("anio", "mes", "total_ventas", ascending=False)

tendencias.show()
tendencias.write.mode("overwrite").csv("/data/tendencias_consumo", header=True)

# 2. Horarios pico: reservas por hora
horarios_pico = reservas.groupBy("hora") \
    .agg(count("reserva_id").alias("total_reservas")) \
    .orderBy("total_reservas", ascending=False)

horarios_pico.show()
horarios_pico.write.mode("overwrite").csv("/data/horarios_pico", header=True)

# 3. Crecimiento mensual: ingresos y pedidos por mes y año
crecimiento = pedidos.join(tiempo, pedidos.fecha == tiempo.fecha) \
    .groupBy("anio", "mes") \
    .agg(
        sum("total").alias("ingresos_totales"),
        count("pedido_id").alias("total_pedidos")
    ) \
    .orderBy("anio", "mes")

crecimiento.show()
crecimiento.write.mode("overwrite").csv("/data/crecimiento_mensual", header=True)

spark.stop()
