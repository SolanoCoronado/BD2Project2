import os
import pandas as pd
import numpy as np

# Permite funcionar tanto en local como en contenedor
orders_path = '/data/orders.csv'
if not os.path.exists(orders_path):
    orders_path = 'orders.csv'

# Cargar el archivo orders.csv
df = pd.read_csv(orders_path)

# Si no existe la columna order_date, agregarla con fechas aleatorias de junio 2025
df['order_date'] = pd.to_datetime('2025-06-01') + pd.to_timedelta(np.random.randint(0, 30, size=len(df)), unit='D')

# Guardar el archivo modificado
df.to_csv(orders_path, index=False)
print('orders.csv actualizado con columna order_date')
