-- Datos de muestra para probar el esquema estrella
USE restaurant_db;

-- Insertar datos en dim_tiempo (para 2024)
INSERT INTO dim_tiempo VALUES
('2024-01-15', 2024, 1, 15, 1, 'Lunes'),
('2024-02-20', 2024, 2, 20, 1, 'Martes'),
('2024-03-10', 2024, 3, 10, 1, 'Domingo'),
('2024-04-05', 2024, 4, 5, 2, 'Viernes'),
('2024-05-12', 2024, 5, 12, 2, 'Domingo'),
('2024-06-18', 2024, 6, 18, 2, 'Martes'),
('2024-07-22', 2024, 7, 22, 3, 'Lunes'),
('2024-08-30', 2024, 8, 30, 3, 'Viernes'),
('2024-09-14', 2024, 9, 14, 3, 'Sábado'),
('2024-10-08', 2024, 10, 8, 4, 'Martes'),
('2024-11-25', 2024, 11, 25, 4, 'Lunes'),
('2024-12-31', 2024, 12, 31, 4, 'Martes');

-- Insertar datos en dim_usuarios
INSERT INTO dim_usuarios VALUES
(1, 'Juan Pérez', 'juan.perez@email.com', 'Masculino', '1990-05-15', 'San José'),
(2, 'María González', 'maria.gonzalez@email.com', 'Femenino', '1985-08-22', 'Cartago'),
(3, 'Carlos Rodríguez', 'carlos.rodriguez@email.com', 'Masculino', '1992-03-10', 'Alajuela'),
(4, 'Ana Martínez', 'ana.martinez@email.com', 'Femenino', '1988-12-05', 'Heredia'),
(5, 'Luis Fernández', 'luis.fernandez@email.com', 'Masculino', '1995-07-18', 'San José'),
(6, 'Carmen Jiménez', 'carmen.jimenez@email.com', 'Femenino', '1987-11-30', 'Puntarenas');

-- Insertar datos en dim_productos
INSERT INTO dim_productos VALUES
(1, 'Pizza Margherita', 'Comida Principal', 12.50),
(2, 'Hamburguesa Clásica', 'Comida Principal', 8.75),
(3, 'Ensalada César', 'Entrada', 6.25),
(4, 'Pasta Carbonara', 'Comida Principal', 11.00),
(5, 'Tiramisu', 'Postre', 4.50),
(6, 'Coca Cola', 'Bebida', 2.00),
(7, 'Cerveza Nacional', 'Bebida', 3.25),
(8, 'Sopa de Tomate', 'Entrada', 5.75),
(9, 'Helado de Vainilla', 'Postre', 3.50),
(10, 'Agua Mineral', 'Bebida', 1.50);

-- Insertar datos en dim_reservas
INSERT INTO dim_reservas VALUES
(1, 1, '2024-01-15', '19:30', 2),
(2, 2, '2024-02-20', '20:00', 4),
(3, 3, '2024-03-10', '18:45', 3),
(4, 4, '2024-04-05', '19:15', 2),
(5, 5, '2024-05-12', '20:30', 5),
(6, 6, '2024-06-18', '19:00', 2),
(7, 1, '2024-07-22', '18:30', 3),
(8, 3, '2024-08-30', '20:15', 4),
(9, 2, '2024-09-14', '19:45', 2),
(10, 4, '2024-10-08', '18:00', 6);

-- Insertar datos en pedidos (tabla de hechos)
INSERT INTO pedidos VALUES
(1, 1, 1, 1, '2024-01-15', 2, 25.00),
(2, 1, 6, 1, '2024-01-15', 2, 4.00),
(3, 2, 2, 2, '2024-02-20', 4, 35.00),
(4, 2, 7, 2, '2024-02-20', 4, 13.00),
(5, 3, 4, 3, '2024-03-10', 3, 33.00),
(6, 3, 5, 3, '2024-03-10', 3, 13.50),
(7, 4, 3, 4, '2024-04-05', 2, 12.50),
(8, 4, 9, 4, '2024-04-05', 2, 7.00),
(9, 5, 1, 5, '2024-05-12', 5, 62.50),
(10, 5, 8, 5, '2024-05-12', 5, 28.75),
(11, 6, 2, 6, '2024-06-18', 2, 17.50),
(12, 6, 10, 6, '2024-06-18', 2, 3.00),
(13, 1, 4, 7, '2024-07-22', 3, 33.00),
(14, 1, 6, 7, '2024-07-22', 3, 6.00),
(15, 3, 1, 8, '2024-08-30', 4, 50.00),
(16, 3, 7, 8, '2024-08-30', 4, 13.00),
(17, 2, 3, 9, '2024-09-14', 2, 12.50),
(18, 2, 5, 9, '2024-09-14', 2, 9.00),
(19, 4, 2, 10, '2024-10-08', 6, 52.50),
(20, 4, 6, 10, '2024-10-08', 6, 12.00);

-- Verificar los datos insertados
SELECT 'dim_tiempo' AS tabla, COUNT(*) AS registros FROM dim_tiempo
UNION ALL
SELECT 'dim_usuarios' AS tabla, COUNT(*) AS registros FROM dim_usuarios
UNION ALL
SELECT 'dim_productos' AS tabla, COUNT(*) AS registros FROM dim_productos
UNION ALL
SELECT 'dim_reservas' AS tabla, COUNT(*) AS registros FROM dim_reservas
UNION ALL
SELECT 'pedidos' AS tabla, COUNT(*) AS registros FROM pedidos;