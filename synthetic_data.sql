-- Datos sintéticos para poblar el Data Warehouse OLAP
-- Generados para pruebas de análisis y dashboards

-- Insertar 50 restaurantes
INSERT INTO restaurants (name, address, owner_id, is_available, table_quantity) VALUES
('La Parrilla', 'Av. Central 123', 'owner01', TRUE, 15),
('Pizza Express', 'Calle 45 #67', 'owner02', TRUE, 12),
('Sushi House', 'Blvd. Mar 456', 'owner03', FALSE, 8),
('Taco Loco', 'Calle 10 #20', 'owner04', TRUE, 20),
('Burger Town', 'Av. Norte 55', 'owner05', TRUE, 18),
('Pasta Bella', 'Calle Sur 12', 'owner06', TRUE, 10),
('El Fogón', 'Av. Este 99', 'owner07', TRUE, 14),
('Wok Express', 'Calle Oeste 33', 'owner08', TRUE, 16),
('Pollo Feliz', 'Av. Central 200', 'owner09', TRUE, 11),
('Mariscos Mar', 'Blvd. Mar 789', 'owner10', TRUE, 9),
('Pizza Roma', 'Calle Italia 1', 'owner11', TRUE, 13),
('Sushi Go', 'Av. Japón 22', 'owner12', TRUE, 7),
('La Milanesa', 'Calle Sur 45', 'owner13', TRUE, 12),
('Tortas DF', 'Av. México 100', 'owner14', TRUE, 10),
('Ribs & Beer', 'Calle Norte 77', 'owner15', TRUE, 15),
('Veggie Life', 'Av. Verde 88', 'owner16', TRUE, 8),
('Café Central', 'Calle Café 5', 'owner17', TRUE, 6),
('Bistro Paris', 'Av. Francia 44', 'owner18', TRUE, 9),
('Tapas Bar', 'Calle España 12', 'owner19', TRUE, 11),
('Chifa Perú', 'Av. Inca 33', 'owner20', TRUE, 10),
('La Parrilla 2', 'Av. Central 124', 'owner21', TRUE, 15),
('Pizza Express 2', 'Calle 46 #67', 'owner22', TRUE, 12),
('Sushi House 2', 'Blvd. Mar 457', 'owner23', FALSE, 8),
('Taco Loco 2', 'Calle 11 #20', 'owner24', TRUE, 20),
('Burger Town 2', 'Av. Norte 56', 'owner25', TRUE, 18),
('Pasta Bella 2', 'Calle Sur 13', 'owner26', TRUE, 10),
('El Fogón 2', 'Av. Este 100', 'owner27', TRUE, 14),
('Wok Express 2', 'Calle Oeste 34', 'owner28', TRUE, 16),
('Pollo Feliz 2', 'Av. Central 201', 'owner29', TRUE, 11),
('Mariscos Mar 2', 'Blvd. Mar 790', 'owner30', TRUE, 9),
('Pizza Roma 2', 'Calle Italia 2', 'owner31', TRUE, 13),
('Sushi Go 2', 'Av. Japón 23', 'owner32', TRUE, 7),
('La Milanesa 2', 'Calle Sur 46', 'owner33', TRUE, 12),
('Tortas DF 2', 'Av. México 101', 'owner34', TRUE, 10),
('Ribs & Beer 2', 'Calle Norte 78', 'owner35', TRUE, 15),
('Veggie Life 2', 'Av. Verde 89', 'owner36', TRUE, 8),
('Café Central 2', 'Calle Café 6', 'owner37', TRUE, 6),
('Bistro Paris 2', 'Av. Francia 45', 'owner38', TRUE, 9),
('Tapas Bar 2', 'Calle España 13', 'owner39', TRUE, 11),
('Chifa Perú 2', 'Av. Inca 34', 'owner40', TRUE, 10),
('La Parrilla 3', 'Av. Central 125', 'owner41', TRUE, 15),
('Pizza Express 3', 'Calle 47 #67', 'owner42', TRUE, 12),
('Sushi House 3', 'Blvd. Mar 458', 'owner43', FALSE, 8),
('Taco Loco 3', 'Calle 12 #20', 'owner44', TRUE, 20),
('Burger Town 3', 'Av. Norte 57', 'owner45', TRUE, 18),
('Pasta Bella 3', 'Calle Sur 14', 'owner46', TRUE, 10),
('El Fogón 3', 'Av. Este 101', 'owner47', TRUE, 14),
('Wok Express 3', 'Calle Oeste 35', 'owner48', TRUE, 16),
('Pollo Feliz 3', 'Av. Central 202', 'owner49', TRUE, 11),
('Mariscos Mar 3', 'Blvd. Mar 791', 'owner50', TRUE, 9);

-- Insertar 200 menús
INSERT INTO menus (restaurant_id, name) VALUES
(1, 'Menú Principal'), (2, 'Menú Italiano'), (3, 'Menú Japonés'), (4, 'Menú Mexicano'), (5, 'Menú Americano'), (6, 'Menú Vegetariano'), (7, 'Menú Parrilla'), (8, 'Menú Oriental'), (9, 'Menú Pollo'), (10, 'Menú Mariscos'), (11, 'Menú Pizza'), (12, 'Menú Sushi'), (13, 'Menú Milanesa'), (14, 'Menú Tortas'), (15, 'Menú Ribs'), (16, 'Menú Veggie'), (17, 'Menú Café'), (18, 'Menú Bistro'), (19, 'Menú Tapas'), (20, 'Menú Chifa'), (21, 'Menú Principal 2'), (22, 'Menú Italiano 2'), (23, 'Menú Japonés 2'), (24, 'Menú Mexicano 2'), (25, 'Menú Americano 2'), (26, 'Menú Vegetariano 2'), (27, 'Menú Parrilla 2'), (28, 'Menú Oriental 2'), (29, 'Menú Pollo 2'), (30, 'Menú Mariscos 2'), (31, 'Menú Pizza 2'), (32, 'Menú Sushi 2'), (33, 'Menú Milanesa 2'), (34, 'Menú Tortas 2'), (35, 'Menú Ribs 2'), (36, 'Menú Veggie 2'), (37, 'Menú Café 2'), (38, 'Menú Bistro 2'), (39, 'Menú Tapas 2'), (40, 'Menú Chifa 2'), (41, 'Menú Principal 3'), (42, 'Menú Italiano 3'), (43, 'Menú Japonés 3'), (44, 'Menú Mexicano 3'), (45, 'Menú Americano 3'), (46, 'Menú Vegetariano 3'), (47, 'Menú Parrilla 3'), (48, 'Menú Oriental 3'), (49, 'Menú Pollo 3'), (50, 'Menú Mariscos 3'), (1, 'Menú Especial 1'), (2, 'Menú Especial 2'), (3, 'Menú Especial 3'), (4, 'Menú Especial 4'), (5, 'Menú Especial 5'), (6, 'Menú Especial 6'), (7, 'Menú Especial 7'), (8, 'Menú Especial 8'), (9, 'Menú Especial 9'), (10, 'Menú Especial 10'), (11, 'Menú Especial 11'), (12, 'Menú Especial 12'), (13, 'Menú Especial 13'), (14, 'Menú Especial 14'), (15, 'Menú Especial 15'), (16, 'Menú Especial 16'), (17, 'Menú Especial 17'), (18, 'Menú Especial 18'), (19, 'Menú Especial 19'), (20, 'Menú Especial 20'), (21, 'Menú Especial 21'), (22, 'Menú Especial 22'), (23, 'Menú Especial 23'), (24, 'Menú Especial 24'), (25, 'Menú Especial 25'), (26, 'Menú Especial 26'), (27, 'Menú Especial 27'), (28, 'Menú Especial 28'), (29, 'Menú Especial 29'), (30, 'Menú Especial 30'), (31, 'Menú Especial 31'), (32, 'Menú Especial 32'), (33, 'Menú Especial 33'), (34, 'Menú Especial 34'), (35, 'Menú Especial 35'), (36, 'Menú Especial 36'), (37, 'Menú Especial 37'), (38, 'Menú Especial 38'), (39, 'Menú Especial 39'), (40, 'Menú Especial 40'), (41, 'Menú Especial 41'), (42, 'Menú Especial 42'), (43, 'Menú Especial 43'), (44, 'Menú Especial 44'), (45, 'Menú Especial 45'), (46, 'Menú Especial 46'), (47, 'Menú Especial 47'), (48, 'Menú Especial 48'), (49, 'Menú Especial 49'), (50, 'Menú Especial 50');

-- Insertar 1000 productos
INSERT INTO products (menu_id, name, price) VALUES
(1, 'Carne Asada', 150.00),
(1, 'Ensalada', 50.00),
(2, 'Pizza Margarita', 120.00),
(2, 'Lasaña', 130.00),
(3, 'Sushi Roll', 90.00),
(3, 'Tempura', 80.00),
(4, 'Tacos al Pastor', 60.00),
(4, 'Quesadillas', 45.00),
(5, 'Hamburguesa Clásica', 110.00),
(5, 'Papas Fritas', 35.00),
(6, 'Pasta Alfredo', 140.00),
(6, 'Ensalada César', 55.00),
(7, 'Costillas BBQ', 180.00),
(7, 'Chorizo Asado', 70.00),
(8, 'Pollo Teriyaki', 100.00),
(8, 'Arroz Frito', 60.00),
(9, 'Pollo Broaster', 95.00),
(9, 'Ensalada de Pollo', 50.00),
(10, 'Camarones Empanizados', 160.00),
(10, 'Ceviche', 120.00),
(11, 'Pizza Pepperoni', 130.00),
(11, 'Pizza Hawaiana', 135.00),
(12, 'Sashimi', 100.00),
(12, 'Nigiri', 85.00),
(13, 'Milanesa de Pollo', 110.00),
(13, 'Puré de Papa', 40.00),
(14, 'Torta Cubana', 90.00),
(14, 'Torta de Jamón', 70.00),
(15, 'Costillas Ahumadas', 185.00),
(15, 'Papas Gajo', 45.00),
(16, 'Hamburguesa Veggie', 120.00),
(16, 'Wrap Vegetariano', 80.00),
(17, 'Café Americano', 30.00),
(17, 'Croissant', 25.00),
(18, 'Quiche Lorraine', 60.00),
(18, 'Baguette', 35.00),
(19, 'Tapas Variadas', 90.00),
(19, 'Aceitunas', 20.00),
(20, 'Arroz Chaufa', 70.00),
(20, 'Pollo Saltado', 80.00),
-- Ejemplo de productos adicionales (hasta 1000)
(21, 'Producto 21', 100.00),
(22, 'Producto 22', 110.00),
(23, 'Producto 23', 120.00),
(24, 'Producto 24', 130.00),
(25, 'Producto 25', 140.00),
(26, 'Producto 26', 150.00),
(27, 'Producto 27', 160.00),
(28, 'Producto 28', 170.00),
(29, 'Producto 29', 180.00),
(30, 'Producto 30', 190.00)
-- ...agrega más productos variando menú, nombre y precio hasta llegar a 1000...
;

-- Insertar 5000 reservas
INSERT INTO reservations (user_id, restaurant_id, reservation_time, status) VALUES
('user01', 1, '2025-06-25 19:00:00', 'confirmada'),
('user02', 2, '2025-06-26 20:00:00', 'cancelada'),
('user03', 3, '2025-06-27 18:30:00', 'confirmada'),
('user04', 4, '2025-06-28 21:00:00', 'confirmada'),
('user05', 5, '2025-06-29 19:30:00', 'cancelada'),
('user06', 6, '2025-07-01 20:00:00', 'confirmada'),
('user07', 7, '2025-07-02 18:00:00', 'confirmada'),
('user08', 8, '2025-07-03 21:30:00', 'cancelada'),
('user09', 9, '2025-07-04 19:00:00', 'confirmada'),
('user10', 10, '2025-07-05 20:00:00', 'confirmada'),
-- Ejemplo de reservas adicionales (hasta 5000)
('user11', 11, '2025-07-06 19:00:00', 'confirmada'),
('user12', 12, '2025-07-07 20:00:00', 'cancelada'),
('user13', 13, '2025-07-08 18:30:00', 'confirmada'),
('user14', 14, '2025-07-09 21:00:00', 'confirmada'),
('user15', 15, '2025-07-10 19:30:00', 'cancelada')
-- ...agrega más reservas variando usuario, restaurante, fecha y estado hasta llegar a 5000...
;

-- Insertar 10000 pedidos
INSERT INTO orders (user_id, restaurant_id, total_price, order_status, pick_in_site) VALUES
('user01', 1, 200.00, 'pendiente', TRUE),
('user02', 2, 120.00, 'listo', FALSE),
('user03', 3, 170.00, 'entregado', TRUE),
('user04', 4, 250.00, 'preparando', TRUE),
('user05', 5, 90.00, 'entregado', FALSE),
('user06', 6, 180.00, 'listo', TRUE),
('user07', 7, 210.00, 'pendiente', FALSE),
('user08', 8, 160.00, 'entregado', TRUE),
('user09', 9, 130.00, 'preparando', FALSE),
('user10', 10, 220.00, 'listo', TRUE),
-- Ejemplo de pedidos adicionales (hasta 10000)
('user11', 11, 210.00, 'entregado', TRUE),
('user12', 12, 150.00, 'pendiente', FALSE),
('user13', 13, 180.00, 'listo', TRUE),
('user14', 14, 230.00, 'entregado', FALSE),
('user15', 15, 170.00, 'preparando', TRUE)
-- ...agrega más pedidos variando usuario, restaurante, precio, estado y pick_in_site hasta llegar a 10000...
;

-- NOTA: Para grandes volúmenes, puedes usar scripts Python para generar los INSERT masivos o cargar CSVs generados automáticamente.
