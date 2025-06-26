-- Crear tabla de restaurantes
CREATE TABLE restaurants (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    address TEXT NOT NULL,
    owner_id VARCHAR(255) NOT NULL,
    is_available BOOLEAN NOT NULL DEFAULT TRUE,
    table_quantity INT NOT NULL DEFAULT 10
);

-- Crear tabla de menús
CREATE TABLE menus (
    id SERIAL PRIMARY KEY,
    restaurant_id INT NOT NULL,
    name VARCHAR(255) NOT NULL,
    FOREIGN KEY (restaurant_id) REFERENCES restaurants(id) ON DELETE CASCADE
);

-- Crear tabla de productos
CREATE TABLE products (
    id SERIAL PRIMARY KEY,
    menu_id INT NOT NULL,
    name VARCHAR(100) NOT NULL,
    price DECIMAL(10,2) NOT NULL,
    FOREIGN KEY (menu_id) REFERENCES menus(id) ON DELETE CASCADE
);

-- Crear tabla de reservas
CREATE TABLE reservations (
    id SERIAL PRIMARY KEY,
    user_id VARCHAR(255) NOT NULL,
    restaurant_id INT NOT NULL,
    reservation_time TIMESTAMP NOT NULL,
    status VARCHAR(50) CHECK (status IN ('confirmada', 'cancelada')) NOT NULL,
    FOREIGN KEY (restaurant_id) REFERENCES restaurants(id) ON DELETE CASCADE
);

-- Crear tabla de pedidos
CREATE TABLE orders (
    id SERIAL PRIMARY KEY,
    user_id VARCHAR(255) NOT NULL,
    restaurant_id INT NOT NULL,
    product_id INT NOT NULL,
    total_price DECIMAL(10,2) NOT NULL,
    order_status VARCHAR(50) CHECK (order_status IN ('pendiente', 'preparando', 'listo', 'entregado')) NOT NULL,
    pick_in_site BOOLEAN NOT NULL DEFAULT FALSE,
    order_date DATE NOT NULL DEFAULT CURRENT_DATE,
    FOREIGN KEY (restaurant_id) REFERENCES restaurants(id) ON DELETE CASCADE,
    FOREIGN KEY (product_id) REFERENCES products(id) ON DELETE CASCADE
);

-- Insertar datos fake en restaurants
INSERT INTO restaurants (name, address, owner_id, is_available, table_quantity) VALUES
('La Parrilla', 'Av. Central 123', 'owner01', TRUE, 15),
('Pizza Express', 'Calle 45 #67', 'owner02', TRUE, 12),
('Sushi House', 'Blvd. Mar 456', 'owner03', FALSE, 8);

-- Insertar datos fake en menus
INSERT INTO menus (restaurant_id, name) VALUES
(1, 'Menú Principal'),
(2, 'Menú Italiano'),
(3, 'Menú Japonés');

-- Insertar datos fake en products
INSERT INTO products (menu_id, name, price) VALUES
(1, 'Carne Asada', 150.00),
(1, 'Ensalada', 50.00),
(2, 'Pizza Margarita', 120.00),
(2, 'Lasaña', 130.00),
(3, 'Sushi Roll', 90.00),
(3, 'Tempura', 80.00);

-- Insertar datos fake en reservations
INSERT INTO reservations (user_id, restaurant_id, reservation_time, status) VALUES
('user01', 1, '2025-06-25 19:00:00', 'confirmada'),
('user02', 2, '2025-06-26 20:00:00', 'cancelada'),
('user03', 3, '2025-06-27 18:30:00', 'confirmada');

-- Insertar datos fake en orders
INSERT INTO orders (user_id, restaurant_id, product_id, total_price, order_status, pick_in_site, order_date) VALUES
('user01', 1, 1, 200.00, 'pendiente', TRUE, '2025-06-25'),
('user02', 2, 3, 120.00, 'listo', FALSE, '2025-06-26'),
('user03', 3, 5, 170.00, 'entregado', TRUE, '2025-06-27');
