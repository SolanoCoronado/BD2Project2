-- Consultas de prueba para validar las vistas OLAP
USE restaurant_db;

-- 1. Probar vista de ventas por mes y tipo de producto
SELECT 'Ventas por mes y tipo de producto:' AS consulta;
SELECT * FROM v_ventas_mes_tipo_producto LIMIT 10;

-- 2. Probar vista de reservas por ubicación
SELECT 'Reservas por ubicación:' AS consulta;
SELECT * FROM v_reservas_ubicacion;

-- 3. Probar vista de productos más vendidos por trimestre
SELECT 'Productos más vendidos por trimestre:' AS consulta;
SELECT * FROM v_productos_trimestre LIMIT 10;

-- 4. Probar vista de ingresos por usuario y año
SELECT 'Ingresos por usuario y año:' AS consulta;
SELECT * FROM v_ingresos_usuario_anio LIMIT 10;

-- 5. Probar vista de frecuencia de pedidos por día de la semana
SELECT 'Frecuencia de pedidos por día de la semana:' AS consulta;
SELECT * FROM v_frecuencia_pedidos_dia_semana;

-- 6. Probar vista de análisis de clientes por género y ubicación
SELECT 'Análisis de clientes por género y ubicación:' AS consulta;
SELECT * FROM v_analisis_clientes_genero_ubicacion;

-- 7. Probar vista de tendencia de ventas mensuales
SELECT 'Tendencia de ventas mensuales:' AS consulta;
SELECT * FROM v_tendencia_ventas_mensuales;

-- Consultas adicionales para análisis más profundo
SELECT 'Top 5 productos más rentables:' AS consulta;
SELECT 
    p.nombre,
    p.tipo,
    SUM(f.total) AS ingresos_totales,
    SUM(f.cantidad) AS cantidad_vendida,
    AVG(f.total/f.cantidad) AS precio_promedio
FROM pedidos f
JOIN dim_productos p ON f.producto_id = p.producto_id
GROUP BY p.nombre, p.tipo
ORDER BY ingresos_totales DESC
LIMIT 5;

SELECT 'Clientes más valiosos:' AS consulta;
SELECT 
    u.nombre,
    u.ubicacion,
    SUM(f.total) AS gasto_total,
    COUNT(f.pedido_id) AS num_pedidos,
    AVG(f.total) AS gasto_promedio_por_pedido
FROM dim_usuarios u
JOIN pedidos f ON u.usuario_id = f.usuario_id
GROUP BY u.nombre, u.ubicacion
ORDER BY gasto_total DESC
LIMIT 5;

SELECT 'Análisis de estacionalidad por trimestre:' AS consulta;
SELECT 
    t.trimestre,
    COUNT(f.pedido_id) AS total_pedidos,
    SUM(f.total) AS ingresos_totales,
    AVG(f.total) AS ticket_promedio
FROM pedidos f
JOIN dim_tiempo t ON f.fecha = t.fecha
GROUP BY t.trimestre
ORDER BY t.trimestre;