FROM apache/superset:latest

USER root
RUN apt-get update && apt-get install -y build-essential libpq-dev gcc

# Instalar directamente en el entorno de Python principal (sin activar .venv)
RUN pip install --no-cache-dir psycopg2-binary

USER superset