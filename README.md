# proyectoEjemplo

## 📌 Descripción

Proyecto de ejemplo en Python que utiliza **PySpark** para procesar datos financieros por cliente.  
Incluye una clase `Operativa` que calcula métricas agregadas sobre los datos, y cuenta con tests automatizados usando `pytest`.

---

## 📁 Estructura del proyecto

```text
proyectoEjemplo/
├── app.py              # Archivo principal de la aplicación
├── version.py          # Archivo con la versión del proyecto
├── clases/             # Carpeta con clases principales
│   └── operativa.py    # Clase Operativa
├── test/               # Tests automatizados con pytest
│   └── test_operativa.py
├── utils/              # Funciones auxiliares (si existen)
├── requirements.txt    # Dependencias del proyecto
├── README.md           # Este archivo
└── setup.py            # Configuración para distribución del paquete
```

---

## ⚙️ Instalación

Se recomienda usar un entorno virtual para gestionar las dependencias:

```bash
python -m venv .venv
# En Linux/Mac
source .venv/bin/activate

# En Windows PowerShell
.venv\Scripts\activate

# Instalar dependencias
pip install -r requirements.txt
```

## 🧰 Uso

```bash
python app.py
```

## 🧪 Testing

```bash
pytest test
```
