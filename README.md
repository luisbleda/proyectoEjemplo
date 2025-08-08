# proyectoEjemplo

## Descripción
Proyecto de ejemplo en Python que utiliza PySpark para procesar datos financieros por cliente.  
Incluye una clase `Operativa` para calcular métricas agregadas, con tests automatizados usando `pytest`.

---

## Estructura del proyecto

proyectoEjemplo/
│
├── app.py # Archivo principal de la aplicación
├── version.py # Archivo con la versión del proyecto
├── clases/ # Carpeta con clases principales (ej. Operativa)
│ └── operativa.py
├── test/ # Tests automatizados con pytest
│ └── test_operativa.py
├── utils/ # Utilidades varias (si hay)
├── requirements.txt # Dependencias del proyecto
├── README.md # Este archivo
└── setup.py # Configuración para distribución del paquete

yaml
Copiar
Editar

---

## Instalación

Se recomienda usar un entorno virtual para gestionar las dependencias:

```bash
python -m venv .venv
source .venv/bin/activate      # Linux/Mac
.venv\Scripts\activate         # Windows PowerShell
pip install -r requirements.txt

---

## Uso

Para ejecutar la aplicación principal:

python app.py

---

## Testing

Los tests están escritos con pytest. Para ejecutarlos:

pytest test

---

## Versionado

La versión del proyecto está definida en el archivo version.py en la raíz del proyecto.
