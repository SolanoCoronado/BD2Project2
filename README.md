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

## Troubleshooting

- **Conexión a Beeline falla:** Espera unos minutos adicionales para que Hive Server termine de inicializar
- **Scripts SQL no se ejecutan:** Copia y pega manualmente el contenido de cada archivo SQL
- **Contenedores no responden:** Verifica que todos los servicios estén ejecutándose con `docker-compose ps`

## Estructura del Proyecto

- `01_schema.sql` - Definición del esquema de la base de datos
- `03_sample_data.sql` - Datos de ejemplo para las tablas
- `02_olap_views.sql` - Vistas OLAP para análisis
- `spark_analysis.py` - Script de análisis con Spark