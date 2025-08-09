from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, DateType, DoubleType
)
from utils.constantes import RUTA_CSV_SPARK, RUTA_PARQUET_SPARK
import random
from utils.metodos import fecha_aleatoria


class CreateFileSpark:
    def __init__(self):
        self.num_registros = 10_000

    def run(self, spark: SparkSession):
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
            id_cliente = random.choice(clientes_ids)
            fecha = fecha_aleatoria()
            codigo_cliente = random.choice(codigos_clientes)
            tipo_op = random.choice(tipos_operacion)
            importe = round(random.uniform(100, 10000), 2)

            # En "Retirada Efectivo" y "Tarjeta" siempre positivo
            if tipo_op not in ["Retirada Efectivo", "Tarjeta"]:
                # En otros tipos, importe puede ser negativo o positivo
                if random.choice([True, False]):
                    importe = -importe

            moneda = random.choice(monedas)

            datos.append(
                (id_cliente, fecha, codigo_cliente,
                 importe, moneda, tipo_op))

        # Definición del esquema
        schema = StructType([
            StructField("id_cliente", StringType(), True),
            StructField("fecha_saldo", DateType(), True),
            StructField("codigo_cliente", StringType(), True),
            StructField("importe_operativa", DoubleType(), True),
            StructField("moneda", StringType(), True),
            StructField("tipo_operacion", StringType(), True),
        ])

        # Crear DataFrame con esquema explícito
        df = spark.createDataFrame(datos, schema=schema)

        # Guardar como CSV
        df.coalesce(1).write.mode("overwrite").option(
            "header", True).csv(RUTA_CSV_SPARK)
        df.write.mode("overwrite").parquet(RUTA_PARQUET_SPARK)

        print(f"✅ CSV generado en: {RUTA_CSV_SPARK}")
        print(f"✅ Parquet generado en: {RUTA_PARQUET_SPARK}")
