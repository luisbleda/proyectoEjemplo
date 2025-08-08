import pytest
from pyspark.sql import SparkSession
from clases.operativa_spark import OperativaSpark
from datetime import datetime
from pyspark.sql.types import (
    StructType, StructField, StringType, DateType
)


@pytest.fixture(scope="module")
def spark():
    spark = SparkSession.builder \
        .master("local[1]") \
        .appName("pytest-pyspark-testing") \
        .getOrCreate()
    yield spark
    spark.stop()


@pytest.fixture
def df_operativa(spark):
    data = [
        (1, "2023-01-01", "A", 100.0),
        (1, "2023-01-02", "A", 200.0),
        (2, "2023-01-01", "B", 50.0),
    ]
    return spark.createDataFrame(data, ["id_cliente", "fecha_saldo", "codigo_cliente", "importe_operativa"])


@pytest.fixture
def df_clientes(spark):
    schema = StructType([
        StructField("codigo_cliente", StringType(), True),
        StructField("nombre", StringType(), True),
        StructField("apellido1", StringType(), True),
        StructField("apellido2", StringType(), True),
        StructField("direccion", StringType(), True),
        StructField("fecha_nacimiento", DateType(), True),
        StructField("dni", StringType(), True),
    ])

    data = [
        ("A", "Ana", "Gómez", "López", "Calle 1", datetime.strptime("2000-01-01", "%Y-%m-%d"), "12345678A"),
        ("B", "Luis", "Pérez", "Martínez", "Calle 2", datetime.strptime("1980-06-15", "%Y-%m-%d"), "87654321B"),
    ]

    return spark.createDataFrame(data, schema=schema)


def test_max_saldo_por_cliente(df_operativa):
    oper = OperativaSpark(df_operativa, None)
    df_result = oper.max_saldo_por_cliente()
    result = df_result.select("id_cliente", "max_saldo").distinct().collect()
    expected = [(1, 200.0), (2, 50.0)]
    assert sorted([(r.id_cliente, r.max_saldo) for r in result]) == sorted(expected)


def test_fecha_mas_reciente_por_operacion(df_operativa):
    oper = OperativaSpark(df_operativa, None)
    df_with_max_saldo = oper.max_saldo_por_cliente()
    df_result = oper.fecha_mas_reciente_por_operacion(df_with_max_saldo)
    result = df_result.select("id_cliente", "codigo_cliente", "fecha_mas_reciente").distinct().collect()
    expected = [
        (1, "A", "2023-01-02"),
        (2, "B", "2023-01-01"),
    ]
    assert sorted([(r.id_cliente, r.codigo_cliente, r.fecha_mas_reciente) for r in result]) == sorted(expected)


def test_calcular_saldo_final(df_operativa):
    oper = OperativaSpark(df_operativa, None)
    df_result = oper.calcular_saldo_final(df_operativa)
    result = df_result.select("codigo_cliente", "saldo_final").distinct().collect()
    expected = [("A", 300.00), ("B", 50.00)]
    assert sorted([(r.codigo_cliente, r.saldo_final) for r in result]) == sorted(expected)


def test_informacion_cliente(df_operativa, df_clientes):
    oper = OperativaSpark(df_operativa, df_clientes)
    df_max = oper.max_saldo_por_cliente()
    df_fecha = oper.fecha_mas_reciente_por_operacion(df_max)
    df_saldo = oper.calcular_saldo_final(df_fecha)
    df_result = oper.informacion_cliente(df_saldo, df_clientes)

    result = df_result.select("codigo_cliente", "nombre_completo", "menor_30") \
                      .orderBy("codigo_cliente") \
                      .collect()

    expected = [
        ("A", "Ana Gómez López", "SI"),
        ("B", "Luis Pérez Martínez", "NO"),
    ]
    assert [(r.codigo_cliente, r.nombre_completo, r.menor_30) for r in result] == expected


def test_run(df_operativa, df_clientes):
    oper = OperativaSpark(df_operativa, df_clientes)
    df_result = oper.run()
    result = df_result.select("codigo_cliente", "nombre_completo", "menor_30") \
                      .orderBy("codigo_cliente") \
                      .collect()

    expected = [
        ("A", "Ana Gómez López", "SI"),
        ("B", "Luis Pérez Martínez", "NO"),
    ]
    assert [(r.codigo_cliente, r.nombre_completo, r.menor_30) for r in result] == expected

