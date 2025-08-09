import pandas as pd
import random
from utils.metodos import fecha_aleatoria
from utils.constantes import (
    RUTA_CSV_PANDAS, RUTA_PARQUET_PANDAS
)


class CreateFilePandas:
    def __init__(self):
        # Configuración interna
        self.num_registros = 10_000

    def run(self):
        # Valores posibles
        clientes_ids = [f"{i:04d}" for i in range(1, 501)]
        codigos_clientes = [
            f"{random.randint(10000000, 99999999)}"
            f"{chr(random.randint(65, 90))}"
            for _ in range(500)
        ]
        monedas = ["EUR", "USD", "GBP", "JPY", "MXN"]
        tipos_operacion = ["Retirada Efectivo", "Tarjeta",
                           "Transferencia", "Bizum"]

        # Generar datos aleatorios con condición para importe_operativa
        datos = []
        for _ in range(self.num_registros):
            tipo_op = random.choice(tipos_operacion)
            importe = round(random.uniform(100, 10000), 2)

            # En "Retirada Efectivo" y "Tarjeta" siempre positivo
            if tipo_op not in ["Retirada Efectivo", "Tarjeta"]:
                # En otros tipos, importe puede ser negativo o positivo
                if random.choice([True, False]):
                    importe = -importe

            datos.append({
                "id_cliente": random.choice(clientes_ids),
                "fecha_saldo": fecha_aleatoria(),
                "codigo_cliente": random.choice(codigos_clientes),
                "importe_operativa": importe,
                "moneda": random.choice(monedas),
                "tipo_operacion": tipo_op
            })

        # Crear DataFrame
        df = pd.DataFrame(datos)

        # Guardar como CSV
        df.to_csv(RUTA_CSV_PANDAS, index=False)

        # Guardar como Parquet
        df.to_parquet(RUTA_PARQUET_PANDAS, index=False)

        print(f"✅ CSV generado en: {RUTA_CSV_PANDAS}")
        print(f"✅ Parquet generado en: {RUTA_PARQUET_PANDAS}")
