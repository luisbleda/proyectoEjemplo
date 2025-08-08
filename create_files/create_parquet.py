from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, DateType, LongType
)
from datetime import date


class CreateParquet:
    def run(self, spark: SparkSession):
        datos = [
            ("0001", date(2023, 5, 12), "11111111A", 1500, "EUR"),
            ("0001", date(2023, 5, 12), "11111111A", 2000, "EUR"),
            ("0001", date(2023, 5, 12), "11111111A", 3000, "EUR"),
            ("0001", date(2023, 5, 12), "11111111A", 4000, "EUR"),
            ("0002", date(2023, 6, 18), "22222222B", 2300, "USD"),
            ("0003", date(2024, 1, 8), "33333333C", 5000, "GBP"),
            ("0004", date(2022, 11, 25), "44444444D", 3300, "EUR"),
            ("0004", date(2022, 11, 25), "44444444D", 6000, "EUR"),
            ("0005", date(2025, 2, 14), "55555555E", 4200, "JPY"),
            ("0006", date(2024, 8, 1), "66666666F", 1200, "MXN"),
            ("0007", date(2023, 9, 20), "77777777G", 8700, "USD"),
            ("0008", date(2022, 3, 5), "88888888H", 2900, "EUR"),
            ("0009", date(2024, 6, 30), "99999999I", 7600, "GBP"),
            ("0010", date(2025, 7, 19), "11111111B", 4100, "EUR"),
        ]

        schema = StructType([
            StructField("id_cliente", StringType(), True),
            StructField("fecha_saldo", DateType(), True),
            StructField("codigo_cliente", StringType(), True),
            StructField("importe_cuenta", LongType(), True),
            StructField("moneda", StringType(), True),
        ])

        df = spark.createDataFrame(datos, schema=schema)

        df.coalesce(1).write.mode("overwrite").parquet(
            "ficheros_prueba/cuentas_parquet/")
