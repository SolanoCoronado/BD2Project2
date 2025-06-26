import pandas as pd
from pyhive import hive

# Configuración de conexión a Hive
conn = hive.Connection(host='hive-server', port=10000, username='root')

# Lee el CSV procesado por Spark
input_path = '/app/data/pedidos_from_postgres.csv'
df = pd.read_csv(input_path)

# Crea la tabla en Hive (ajusta columnas según tu esquema)
create_table_query = '''
CREATE TABLE IF NOT EXISTS pedidos_spark (
    id INT,
    usuario_id INT,
    producto_id INT,
    cantidad INT,
    fecha STRING
) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE
'''

cursor = conn.cursor()
cursor.execute(create_table_query)

# Inserta los datos (esto es un ejemplo simple, para grandes volúmenes usa LOAD DATA)
for _, row in df.iterrows():
    insert_query = f"""
    INSERT INTO TABLE pedidos_spark VALUES ({row['id']}, {row['usuario_id']}, {row['producto_id']}, {row['cantidad']}, '{row['fecha']}')
    """
    cursor.execute(insert_query)

print("Datos cargados en Hive desde Spark.")
cursor.close()
conn.close()
