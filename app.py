from pyspark.sql import SparkSession
from create_files.create_csv import CreateCSV
from create_files.create_parquet import CreateParquet
from clases.operativa import Operativa
from utils.constantes import RUTA_PARQUET, RUTA_CSV
import config


def main():
    spark = SparkSession.builder.appName("CrearArchivos").getOrCreate()

    if config.CREAR_CSV:
        print("Creando archivo CSV...")
        csv_creator = CreateCSV()
        csv_creator.run(spark)
    else:
        print("Creación de CSV deshabilitada.")

    if config.CREAR_PARQUET:
        print("Creando archivo Parquet...")
        parquet_creator = CreateParquet()
        parquet_creator.run(spark)
    else:
        print("Creación de Parquet deshabilitada.")

    df_csv = spark.read.option(
        "header", True).option("inferSchema", True).csv(RUTA_CSV)
    df_parquet = spark.read.parquet(RUTA_PARQUET)

    oper_csv = Operativa(df_csv).run()
    oper_parquet = Operativa(df_parquet).run()

    oper_csv.coalesce(1).write.mode("overwrite").option(
        "header", True).csv("ficheros_prueba/cuentas_csv_operativa/")
    oper_parquet.coalesce(1).write.mode("overwrite").parquet(
        "ficheros_prueba/cuentas_parquet_operativa/")
    spark.stop()


if __name__ == "__main__":
    main()
