-- Esquema Estrella para Restaurante (Hive SQL)
-- Crear base de datos
CREATE DATABASE IF NOT EXISTS restaurant_db;
USE restaurant_db;

-- Tabla de hechos: pedidos
CREATE TABLE IF NOT EXISTS pedidos (
    pedido_id INT,
    usuario_id INT,
    producto_id INT,
    reserva_id INT,
    fecha DATE,
    cantidad INT,
    total DECIMAL(10,2)
)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

-- Dimensión: usuarios
CREATE TABLE IF NOT EXISTS dim_usuarios (
    usuario_id INT,
    nombre STRING,
    email STRING,
    genero STRING,
    fecha_nacimiento DATE,
    ubicacion STRING
)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

-- Dimensión: productos
CREATE TABLE IF NOT EXISTS dim_productos (
    producto_id INT,
    nombre STRING,
    tipo STRING,
    precio DECIMAL(10,2)
)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

-- Dimensión: reservas
CREATE TABLE IF NOT EXISTS dim_reservas (
    reserva_id INT,
    usuario_id INT,
    fecha DATE,
    hora STRING,
    cantidad_personas INT
)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

-- Dimensión: tiempo
CREATE TABLE IF NOT EXISTS dim_tiempo (
    fecha DATE,
    anio INT,
    mes INT,
    dia INT,
    trimestre INT,
    dia_semana STRING
)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

-- Verificar que las tablas se crearon correctamente
SHOW TABLES;