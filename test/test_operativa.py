import pytest
from pyspark.sql import SparkSession
from clases.operativa import Operativa
from pyspark.sql.functions import col

@pytest.fixture(scope="module")
def spark():
    spark = SparkSession.builder \
        .master("local[1]") \
        .appName("pytest-pyspark-local-testing") \
        .getOrCreate()
    yield spark
    spark.stop()

def test_max_saldo_por_cliente(spark):
    # Datos de ejemplo
    data = [
        (1, "2023-01-01", "A", 100),
        (1, "2023-01-02", "A", 200),
        (2, "2023-01-01", "B", 50),
    ]
    df = spark.createDataFrame(data, ["id_cliente", "fecha_saldo", "codigo_cliente", "importe_cuenta"])

    oper = Operativa(df)
    df_result = oper.max_saldo_por_cliente()

    # Comprobar que la columna max_saldo se agregó correctamente
    result = df_result.select("id_cliente", "max_saldo").distinct().collect()
    expected = [(1, 200), (2, 50)]

    assert sorted([(r.id_cliente, r.max_saldo) for r in result]) == sorted(expected)

def test_fecha_mas_reciente_por_operacion(spark):
    data = [
        (1, "2023-01-01", "A", 100),
        (1, "2023-01-03", "A", 200),
        (1, "2023-01-02", "B", 150),
        (2, "2023-01-01", "B", 50),
        (2, "2023-01-05", "B", 60),
    ]
    df = spark.createDataFrame(data, ["id_cliente", "fecha_saldo", "codigo_cliente", "importe_cuenta"])

    oper = Operativa(df)
    # Primero añade max_saldo (opcional)
    df_with_max_saldo = oper.max_saldo_por_cliente()
    df_result = oper.fecha_mas_reciente_por_operacion(df_with_max_saldo)

    # Comprobar fecha más reciente por id_cliente y codigo_cliente
    result = df_result.select("id_cliente", "codigo_cliente", "fecha_mas_reciente").distinct().collect()

    expected = [
        (1, "A", "2023-01-03"),
        (1, "B", "2023-01-02"),
        (2, "B", "2023-01-05"),
    ]

    assert sorted([(r.id_cliente, r.codigo_cliente, r.fecha_mas_reciente) for r in result]) == sorted(expected)
