# config.py
import os

# Variables de entorno para controlar la creación de archivos
CREAR_CSV = os.getenv("CREAR_CSV", "true").lower() == "true"
CREAR_PARQUET = os.getenv("CREAR_PARQUET", "true").lower() == "true"
