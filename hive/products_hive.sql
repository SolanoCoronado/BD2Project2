-- Tabla products para Hive (estructura compatible con el flujo ETL)
CREATE TABLE IF NOT EXISTS products (
    id INT,
    menu_id INT,
    name STRING,
    price DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;
