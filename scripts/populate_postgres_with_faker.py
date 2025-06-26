"""
Script para poblar PostgreSQL con datos sintéticos usando Faker.
Genera datos para restaurants, menus, products, reservations y orders.
Configura la conexión según tu docker-compose (usuario: sa, password: password1234, db: DBTC01, host: postgres o localhost).
"""
import psycopg2
from faker import Faker
import random
from datetime import datetime, timedelta

# Configuración de conexión
DB_HOST = "postgres"  # O "postgres" si corres dentro del contenedor
DB_PORT = 5432
DB_NAME = "DBTC01"
DB_USER = "sa"
DB_PASS = "password1234"

N_RESTAURANTS = 50
N_MENUS = 200
N_PRODUCTS = 1000
N_RESERVATIONS = 5000
N_ORDERS = 10000

fake = Faker()

conn = psycopg2.connect(
    host=DB_HOST,
    port=DB_PORT,
    dbname=DB_NAME,
    user=DB_USER,
    password=DB_PASS
)
cur = conn.cursor()

def reset_tables():
    cur.execute("TRUNCATE orders, reservations, products, menus, restaurants RESTART IDENTITY CASCADE;")
    conn.commit()

def insert_restaurants():
    restaurants = []
    for i in range(N_RESTAURANTS):
        name = fake.company()
        address = fake.address().replace('\n', ', ')
        owner_id = f"owner{i+1:02d}"
        is_available = random.choice([True, False])
        table_quantity = random.randint(5, 25)
        restaurants.append((name, address, owner_id, is_available, table_quantity))
    cur.executemany("INSERT INTO restaurants (name, address, owner_id, is_available, table_quantity) VALUES (%s, %s, %s, %s, %s)", restaurants)
    conn.commit()

def insert_menus():
    menus = []
    for i in range(N_MENUS):
        restaurant_id = random.randint(1, N_RESTAURANTS)
        name = fake.word().capitalize() + " Menu"
        menus.append((restaurant_id, name))
    cur.executemany("INSERT INTO menus (restaurant_id, name) VALUES (%s, %s)", menus)
    conn.commit()

def insert_products():
    products = []
    for i in range(N_PRODUCTS):
        menu_id = random.randint(1, N_MENUS)
        name = fake.word().capitalize() + " " + fake.word().capitalize()
        price = round(random.uniform(30, 200), 2)
        products.append((menu_id, name, price))
    cur.executemany("INSERT INTO products (menu_id, name, price) VALUES (%s, %s, %s)", products)
    conn.commit()

def insert_reservations():
    reservations = []
    for i in range(N_RESERVATIONS):
        user_id = f"user{random.randint(1, 1000):03d}"
        restaurant_id = random.randint(1, N_RESTAURANTS)
        days_offset = random.randint(0, 60)
        time = fake.time_object()
        reservation_time = (datetime(2025, 6, 1) + timedelta(days=days_offset)).replace(hour=time.hour, minute=time.minute, second=0)
        status = random.choice(["confirmada", "cancelada"])
        reservations.append((user_id, restaurant_id, reservation_time, status))
    cur.executemany("INSERT INTO reservations (user_id, restaurant_id, reservation_time, status) VALUES (%s, %s, %s, %s)", reservations)
    conn.commit()

def insert_orders():
    orders = []
    for i in range(N_ORDERS):
        user_id = f"user{random.randint(1, 1000):03d}"
        restaurant_id = random.randint(1, N_RESTAURANTS)
        # Seleccionar productos solo del restaurante correspondiente
        cur.execute("SELECT id FROM products WHERE menu_id IN (SELECT id FROM menus WHERE restaurant_id = %s)", (restaurant_id,))
        product_ids = [row[0] for row in cur.fetchall()]
        if not product_ids:
            # Si no hay productos para ese restaurante, asignar un producto aleatorio global
            cur.execute("SELECT id FROM products ORDER BY RANDOM() LIMIT 1")
            product_id = cur.fetchone()[0]
        else:
            product_id = random.choice(product_ids)
        total_price = round(random.uniform(50, 500), 2)
        order_status = random.choice(["pendiente", "listo", "entregado", "preparando"])
        pick_in_site = random.choice([True, False])
        days_offset = random.randint(0, 60)
        random_hour = random.randint(0, 23)
        random_minute = random.randint(0, 59)
        random_second = random.randint(0, 59)
        order_date = (datetime(2025, 6, 1) + timedelta(days=days_offset, hours=random_hour, minutes=random_minute, seconds=random_second))
        order_date_str = order_date.strftime('%Y-%m-%d %H:%M:%S')
        orders.append((user_id, restaurant_id, product_id, total_price, order_status, pick_in_site, order_date_str))
    cur.executemany("INSERT INTO orders (user_id, restaurant_id, product_id, total_price, order_status, pick_in_site, order_date) VALUES (%s, %s, %s, %s, %s, %s, %s)", orders)
    conn.commit()

if __name__ == "__main__":
    print("Reseteando tablas...")
    reset_tables()
    print("Insertando restaurantes...")
    insert_restaurants()
    print("Insertando menús...")
    insert_menus()
    print("Insertando productos...")
    insert_products()
    print("Insertando reservas...")
    insert_reservations()
    print("Insertando pedidos...")
    insert_orders()
    print("¡Datos sintéticos generados con Faker y cargados en PostgreSQL!")
    cur.close()
    conn.close()
