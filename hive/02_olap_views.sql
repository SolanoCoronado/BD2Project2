-- Vistas OLAP para análisis agregados en Hive
USE restaurant_db;

-- 1. Ventas por mes y tipo de producto
CREATE VIEW IF NOT EXISTS v_ventas_mes_tipo_producto AS
SELECT 
    t.anio, 
    t.mes, 
    p.tipo, 
    SUM(f.total) AS total_ventas,
    COUNT(f.pedido_id) AS num_pedidos,
    AVG(f.total) AS promedio_venta
FROM pedidos f
JOIN dim_productos p ON f.producto_id = p.producto_id
JOIN dim_tiempo t ON f.fecha = t.fecha
GROUP BY t.anio, t.mes, p.tipo
ORDER BY t.anio, t.mes, total_ventas DESC;

-- 2. Frecuencia de reservas por ubicación
CREATE VIEW IF NOT EXISTS v_reservas_ubicacion AS
SELECT 
    u.ubicacion, 
    COUNT(r.reserva_id) AS total_reservas,
    AVG(r.cantidad_personas) AS promedio_personas,
    COUNT(DISTINCT r.usuario_id) AS usuarios_unicos
FROM dim_reservas r
JOIN dim_usuarios u ON r.usuario_id = u.usuario_id
GROUP BY u.ubicacion
ORDER BY total_reservas DESC;

-- 3. Productos más vendidos por trimestre
CREATE VIEW IF NOT EXISTS v_productos_trimestre AS
SELECT 
    t.anio, 
    t.trimestre, 
    p.nombre, 
    p.tipo,
    SUM(f.cantidad) AS cantidad_vendida,
    SUM(f.total) AS ingresos_generados,
    COUNT(f.pedido_id) AS num_pedidos
FROM pedidos f
JOIN dim_productos p ON f.producto_id = p.producto_id
JOIN dim_tiempo t ON f.fecha = t.fecha
GROUP BY t.anio, t.trimestre, p.nombre, p.tipo
ORDER BY t.anio, t.trimestre, cantidad_vendida DESC;

-- 4. Ingresos por usuario y año
CREATE VIEW IF NOT EXISTS v_ingresos_usuario_anio AS
SELECT 
    u.usuario_id, 
    u.nombre, 
    u.ubicacion,
    t.anio, 
    SUM(f.total) AS total_gastado,
    COUNT(f.pedido_id) AS num_pedidos,
    AVG(f.total) AS gasto_promedio_por_pedido
FROM pedidos f
JOIN dim_usuarios u ON f.usuario_id = u.usuario_id
JOIN dim_tiempo t ON f.fecha = t.fecha
GROUP BY u.usuario_id, u.nombre, u.ubicacion, t.anio
ORDER BY t.anio, total_gastado DESC;

-- 5. Frecuencia de pedidos por día de la semana
CREATE VIEW IF NOT EXISTS v_frecuencia_pedidos_dia_semana AS
SELECT 
    t.dia_semana, 
    COUNT(f.pedido_id) AS total_pedidos,
    SUM(f.total) AS ingresos_totales,
    AVG(f.total) AS ingreso_promedio,
    SUM(f.cantidad) AS productos_vendidos
FROM pedidos f
JOIN dim_tiempo t ON f.fecha = t.fecha
GROUP BY t.dia_semana
ORDER BY total_pedidos DESC;

-- 6. Vista adicional: Análisis de clientes por género y ubicación
CREATE VIEW IF NOT EXISTS v_analisis_clientes_genero_ubicacion AS
SELECT 
    u.genero,
    u.ubicacion,
    COUNT(DISTINCT u.usuario_id) AS total_usuarios,
    SUM(f.total) AS ingresos_totales,
    AVG(f.total) AS gasto_promedio_por_pedido,
    COUNT(f.pedido_id) AS total_pedidos
FROM dim_usuarios u
JOIN pedidos f ON u.usuario_id = f.usuario_id
GROUP BY u.genero, u.ubicacion
ORDER BY ingresos_totales DESC;

-- 7. Vista adicional: Tendencia de ventas mensuales
CREATE VIEW IF NOT EXISTS v_tendencia_ventas_mensuales AS
SELECT 
    t.anio,
    t.mes,
    COUNT(f.pedido_id) AS total_pedidos,
    SUM(f.total) AS ingresos_totales,
    SUM(f.cantidad) AS productos_vendidos,
    COUNT(DISTINCT f.usuario_id) AS clientes_unicos,
    AVG(f.total) AS ticket_promedio
FROM pedidos f
JOIN dim_tiempo t ON f.fecha = t.fecha
GROUP BY t.anio, t.mes
ORDER BY t.anio, t.mes;

-- Mostrar las vistas creadas
SHOW TABLES;