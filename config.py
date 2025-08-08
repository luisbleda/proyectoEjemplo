# config.py
import os

# Variables de entorno para controlar la creación de archivos
CREAR_FICHERO = os.getenv("CREAR_FICHERO", "true").lower() == "false"
