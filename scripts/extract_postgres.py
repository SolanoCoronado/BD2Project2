import pandas as pd
import psycopg2

# Configuración de conexión
conn = psycopg2.connect(
    host='superset-db',
    port=5432,
    dbname='superset',
    user='superset',
    password='superset'
)

# Consulta de ejemplo (ajusta según tus tablas)
query = """
SELECT * FROM pedidos;
"""

df = pd.read_sql(query, conn)

# Exporta a CSV para Spark
output_path = '/app/data/pedidos_from_postgres.csv'
df.to_csv(output_path, index=False)

print(f"Datos extraídos y guardados en {output_path}")
conn.close()
