# Proyecto Hive y Spark - Guía de Configuración

Este README contiene los pasos necesarios para configurar y ejecutar el proyecto con Hive y Spark usando Docker Compose.

## Configuración Inicial

### 1. Detener y levantar los servicios

Primero, asegúrate de detener cualquier instancia previa y levantar los contenedores:

```bash
docker-compose down
docker-compose up -d
```

### 2. Conectar a Hive Server

Accede al contenedor de Hive Server:

```bash
docker-compose exec hive-server bash
```

### 3. Conectar a Beeline

Conecta a Hive usando Beeline (nota: a veces puede fallar, esperar a que el contenedor inicie completamente):

```bash
beeline -u jdbc:hive2://localhost:10000
```

**⚠️ Importante:** Si la conexión falla, espera unos minutos para que el contenedor termine de inicializar completamente antes de intentar nuevamente.

### 4. Ejecutar Scripts SQL

Una vez conectado a Beeline, ejecuta los siguientes scripts en orden:

```sql
!run /opt/hive/scripts/01_schema.sql
!run /opt/hive/scripts/03_sample_data.sql
!run /opt/hive/scripts/02_olap_views.sql
```

**📝 Nota:** Los comandos `!run` pueden no funcionar correctamente. En caso de fallo, ejecuta el contenido de cada archivo manualmente copiando y pegando el código SQL.

### 5. Ejecutar análisis con Spark

Accede al contenedor de Spark:

```bash
docker exec -it bd2project2-spark-1 bash
```

### 6. Ejecutar el script de análisis

Finalmente, ejecuta el script de análisis de Spark:

```bash
spark-submit /app/spark_analysis.py
```

## Visualización de Datos con Superset (usando archivos CSV)

Puedes usar Apache Superset para visualizar los resultados generados por Spark directamente desde los archivos CSV exportados en la carpeta `data/`.

### 1. Agregar Superset al entorno Docker Compose

Agrega este servicio al final de tu `docker-compose.yml`:

```yaml
  superset:
    image: apache/superset:latest
    environment:
      - SUPERSET_LOAD_EXAMPLES=no
      - SUPERSET_SECRET_KEY=supersecretkey
    ports:
      - "8088:8088"
    volumes:
      - ./data:/app/data
    command: "/bin/sh -c 'superset db upgrade && superset fab create-admin --username admin --firstname Admin --lastname User --email admin@admin.com --password admin || true && superset init && superset run -h 0.0.0.0'"
    depends_on:
      - spark
```

Luego reinicia los servicios:

```bash
docker-compose up -d
```

### 2. Acceder a Superset

Abre tu navegador y ve a: [http://localhost:8088](http://localhost:8088)

Usuario: `admin`  
Contraseña: `admin`

### 3. Cargar los archivos CSV como fuentes de datos

1. Entra a Superset y ve a **Data > Upload a CSV**.
2. Sube cada archivo CSV de las carpetas `data/tendencias_consumo/`, `data/horarios_pico/`, y `data/crecimiento_mensual/`.
3. Asigna un nombre a cada tabla y selecciona el motor de base de datos "SQLite" (por defecto) o crea una base de datos local.

### 4. Crear dashboards

- **Ingresos por mes y categoría de producto:** Usa el CSV de tendencias de consumo.
- **Actividad de clientes por zona geográfica:** Usa el CSV de reservas o tendencias si tienes la columna de ubicación.
- **Estadísticas de pedidos completados vs cancelados:** Si tienes ese dato en tus CSV, súbelo; si no, puedes crear un CSV manualmente.

### 5. Construir gráficos y dashboards

1. Ve a **Charts** y crea gráficos usando los datos subidos.
2. Agrupa los gráficos en un dashboard.

---

**Nota:** Puedes usar Metabase o Redash de forma similar, subiendo los CSV como fuentes de datos.

¿Quieres que te agregue el bloque de Superset al `docker-compose.yml` automáticamente? Si necesitas ejemplos de gráficos, dime cuál y te ayudo a configurarlo.

## Troubleshooting

- **Conexión a Beeline falla:** Espera unos minutos adicionales para que Hive Server termine de inicializar
- **Scripts SQL no se ejecutan:** Copia y pega manualmente el contenido de cada archivo SQL
- **Contenedores no responden:** Verifica que todos los servicios estén ejecutándose con `docker-compose ps`

## Estructura del Proyecto

- `01_schema.sql` - Definición del esquema de la base de datos
- `03_sample_data.sql` - Datos de ejemplo para las tablas
- `02_olap_views.sql` - Vistas OLAP para análisis
- `spark_analysis.py` - Script de análisis con Spark