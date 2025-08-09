from pyspark.sql import SparkSession
from create_files.create_operativas_parquet_spark import CreateFileSpark
from create_files.create_operativas_parquet_pandas import CreateFilePandas
from clases.operativa_fichero_spark import OperativaSpark
from clases.operativa_fichero_pandas import OperativaPandas
from utils.constantes import (
    RUTA_PARQUET_SPARK, RUTA_CSV_SPARK, RUTA_CLIENTES,
    RUTA_CSV_PANDAS, RUTA_PARQUET_PANDAS, ESC_PARQUET_SPARK, ESC_CSV_SPARK,
    ESC_CSV_PANDAS, ESC_PARQUET_PANDAS, RUTA_CLIENTES_PANDAS
)
import pandas as pd
import config


def main():
    spark = SparkSession.builder.appName("App").getOrCreate()

    if config.CREAR_FICHERO:
        print("Creando archivo CSV y PARQUET...")
        csv_creator_spark = CreateFileSpark()
        csv_creator_spark.run(spark)
        csv_creator_pandas = CreateFilePandas()
        csv_creator_pandas.run()
    else:
        print("Creación Fichero nuevo deshabilitada.")

    # OPERATIVA SPARK
    df_csv = spark.read.option(
        "header", True).option("inferSchema", True).csv(RUTA_CSV_SPARK)
    df_parquet = spark.read.parquet(RUTA_PARQUET_SPARK)
    df_clientes = spark.read.option(
        "header", True).option("inferSchema", True).csv(RUTA_CLIENTES)
    oper_csv = OperativaSpark(df_csv, df_clientes).run()
    oper_parquet = OperativaSpark(df_parquet, df_clientes).run()

    oper_csv.coalesce(1).write.mode("overwrite").option(
        "header", True).csv(ESC_CSV_SPARK)
    oper_parquet.coalesce(1).write.mode("overwrite").parquet(
        ESC_PARQUET_SPARK)

    # OPERATIVA PANDAS
    df_csv_pandas = pd.read_csv(
        RUTA_CSV_PANDAS, sep=",",
        parse_dates=["fecha_saldo"])
    df_parquet_pandas = pd.read_parquet(RUTA_PARQUET_PANDAS)
    df_clientes_pandas = pd.read_csv(
        RUTA_CLIENTES_PANDAS, sep=",",
        parse_dates=["fecha_nacimiento"])

    oper_csv = OperativaPandas(df_csv_pandas, df_clientes_pandas).run()
    oper_parquet = OperativaPandas(df_parquet_pandas, df_clientes_pandas).run()

    oper_csv.to_csv(ESC_CSV_PANDAS, index=False, sep=",")
    oper_parquet.to_parquet(ESC_PARQUET_PANDAS, index=False)
    spark.stop()


if __name__ == "__main__":
    main()
