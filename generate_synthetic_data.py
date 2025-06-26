import csv
import random
from datetime import datetime, timedelta

# Configuración
NUM_RESTAURANTS = 50
NUM_MENUS = 200
NUM_PRODUCTS = 1000
NUM_USERS = 500
NUM_RESERVATIONS = 5000
NUM_ORDERS = 10000

# Generar usuarios
users = [f'user{str(i).zfill(3)}' for i in range(1, NUM_USERS+1)]

# Menús
menus = [(i+1, random.randint(1, NUM_RESTAURANTS), f"Menú Especial {i+1}") for i in range(NUM_MENUS)]

# Productos
product_names = [
    'Carne Asada', 'Ensalada', 'Pizza Margarita', 'Lasaña', 'Sushi Roll', 'Tempura', 'Tacos al Pastor',
    'Quesadillas', 'Hamburguesa Clásica', 'Papas Fritas', 'Pasta Alfredo', 'Ensalada César', 'Costillas BBQ',
    'Chorizo Asado', 'Pollo Teriyaki', 'Arroz Frito', 'Pollo Broaster', 'Ensalada de Pollo', 'Camarones Empanizados',
    'Ceviche', 'Pizza Pepperoni', 'Pizza Hawaiana', 'Sashimi', 'Nigiri', 'Milanesa de Pollo', 'Puré de Papa',
    'Torta Cubana', 'Torta de Jamón', 'Costillas Ahumadas', 'Papas Gajo', 'Hamburguesa Veggie', 'Wrap Vegetariano',
    'Café Americano', 'Croissant', 'Quiche Lorraine', 'Baguette', 'Tapas Variadas', 'Aceitunas', 'Arroz Chaufa',
    'Pollo Saltado'
]

# Generar products.csv
with open('products.csv', 'w', newline='', encoding='utf-8') as f:
    writer = csv.writer(f)
    writer.writerow(['id', 'menu_id', 'name', 'price'])
    for i in range(1, NUM_PRODUCTS+1):
        menu_id = random.randint(1, NUM_MENUS)
        name = random.choice(product_names) + f' {random.randint(1, 20)}'
        price = round(random.uniform(30, 200), 2)
        writer.writerow([i, menu_id, name, price])

# Generar reservations.csv
start_date = datetime(2024, 1, 1)
with open('reservations.csv', 'w', newline='', encoding='utf-8') as f:
    writer = csv.writer(f)
    writer.writerow(['id', 'user_id', 'restaurant_id', 'reservation_time', 'status'])
    for i in range(1, NUM_RESERVATIONS+1):
        user_id = random.choice(users)
        restaurant_id = random.randint(1, NUM_RESTAURANTS)
        days_offset = random.randint(0, 540)
        hour = random.randint(12, 22)
        minute = random.choice([0, 15, 30, 45])
        reservation_time = (start_date + timedelta(days=days_offset, hours=hour, minutes=minute)).strftime('%Y-%m-%d %H:%M:%S')
        status = random.choice(['confirmada', 'cancelada'])
        writer.writerow([i, user_id, restaurant_id, reservation_time, status])

# Generar orders.csv
with open('orders.csv', 'w', newline='', encoding='utf-8') as f:
    writer = csv.writer(f)
    writer.writerow(['id', 'user_id', 'restaurant_id', 'total_price', 'order_status', 'pick_in_site'])
    for i in range(1, NUM_ORDERS+1):
        user_id = random.choice(users)
        restaurant_id = random.randint(1, NUM_RESTAURANTS)
        total_price = round(random.uniform(50, 500), 2)
        order_status = random.choice(['pendiente', 'preparando', 'listo', 'entregado'])
        pick_in_site = random.choice([True, False])
        writer.writerow([i, user_id, restaurant_id, total_price, order_status, pick_in_site])

# Generar menus.csv
with open('menus.csv', 'w', newline='', encoding='utf-8') as f:
    writer = csv.writer(f)
    writer.writerow(['menu_id', 'restaurant_id', 'nombre'])
    for menu in menus:
        writer.writerow(menu)

# Generar restaurants.csv
restaurants = []
for i in range(1, NUM_RESTAURANTS+1):
    nombre = f'Restaurante {i}'
    direccion = f'Calle {i} #{random.randint(1, 1000)}'
    owner_id = f'owner{str(i).zfill(2)}'
    is_available = random.choice([True, False])
    table_quantity = random.randint(5, 25)
    restaurants.append([i, nombre, direccion, owner_id, is_available, table_quantity])

with open('restaurants.csv', 'w', newline='', encoding='utf-8') as f:
    writer = csv.writer(f)
    writer.writerow(['restaurant_id', 'nombre', 'direccion', 'owner_id', 'is_available', 'table_quantity'])
    for r in restaurants:
        writer.writerow(r)

print('Archivos CSV generados: products.csv, reservations.csv, orders.csv, menus.csv, restaurants.csv')
