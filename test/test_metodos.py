from datetime import date, datetime, timedelta
from utils.metodos import (
    fecha_aleatoria, enrich_data, temporal_analysis
)
from pyspark.sql import SparkSession
import pytest
from pyspark.sql import functions as F


@pytest.fixture(scope="module")
def spark():
    spark = SparkSession.builder \
        .master("local[1]") \
        .appName("pytest-pyspark-testing-methods") \
        .getOrCreate()
    yield spark
    spark.stop()


def test_fecha_aleatoria_rango():
    for _ in range(1000):  # Repetimos varias veces por ser aleatorio
        fecha = fecha_aleatoria()
        assert isinstance(fecha, date)
        assert date(2022, 1, 1) <= fecha <= date(2025, 12, 31)


def test_fecha_aleatoria_variedad():
    fechas = {fecha_aleatoria() for _ in range(100)}
    assert len(
        fechas) > 1, \
        "El método siempre devuelve la misma fecha, revisar aleatoriedad"


def test_enrich_data_basic(spark):
    ventas = spark.createDataFrame(
        [
            (1, "A", 100),
            (2, "B", 150),
            (3, "C", 200),
        ],
        ["venta_id", "cliente_id", "monto"],
    )

    clientes = spark.createDataFrame(
        [
            ("A", "Cliente A"),
            ("B", "Cliente B"),
            ("D", "Cliente D Extra"),
        ],
        ["cliente_id", "nombre_cliente"],
    )

    clima = spark.createDataFrame(
        [
            ("A", "Soleado"),
            ("B", "Lluvioso"),
            ("C", "Nublado"),
        ],
        ["cliente_id", "clima"],
    )

    enrich_sources = [
        (clientes, ["cliente_id"], True),  # broadcast join
        (clima, ["cliente_id"], False),  # join normal
    ]

    resultado = enrich_data(
        ventas, enrich_sources, repartition_key="cliente_id"
    )

    assert resultado is not None
    assert resultado.count() == 3

    cols_esperadas = set(ventas.columns) | {"nombre_cliente", "clima"}
    assert set(resultado.columns) >= cols_esperadas

    row = resultado.filter(F.col("venta_id") == 1).collect()[0]
    assert row["cliente_id"] == "A"
    assert row["nombre_cliente"] == "Cliente A"
    assert row["clima"] == "Soleado"

    row = resultado.filter(F.col("venta_id") == 3).collect()[0]
    assert row["cliente_id"] == "C"
    assert row["nombre_cliente"] is None
    assert row["clima"] == "Nublado"


def test_temporal_analysis_basic(spark):
    base_time = datetime(2023, 1, 1, 0, 0, 0)
    data = []
    for sensor_id in ["S1", "S2"]:
        for i in range(6):
            timestamp = base_time + timedelta(minutes=5 * i)
            valor = 10 if not (sensor_id == "S1" and i == 3) else 100
            data.append((sensor_id, timestamp, valor))

    df = spark.createDataFrame(data, schema=["sensor_id", "ts", "valor"])

    resultado = temporal_analysis(
        df=df,
        timestamp_col="ts",
        group_cols=["sensor_id"],
        metric_col="valor",
        window_duration="5 minutes",
        slide_duration="5 minutes",
        rolling_window_size=3,
        outlier_threshold=1.0,
    )

    cols_esperadas = {
        "sensor_id",
        "time_window",
        "avg_metric",
        "sum_metric",
        "stddev_metric",
        "rolling_avg",
        "rolling_sum",
        "rolling_stddev",
        "is_outlier",
    }

    # Verificar columnas
    assert set(resultado.columns) >= cols_esperadas

    # Verificar que hay filas para cada sensor
    counts = resultado.groupBy("sensor_id").count().collect()
    assert all(c["count"] > 0 for c in counts)

    # Opcional: imprimir para debug
    resultado.select(
        "sensor_id",
        "time_window",
        "avg_metric",
        "rolling_avg",
        "rolling_stddev",
        "is_outlier"
    ).orderBy("sensor_id", "time_window").show(truncate=False)

    # Verificar que al menos un outlier existe para S1 (valor 100)
    outliers = resultado.filter(
        "sensor_id = 'S1' AND is_outlier = true").count()
    assert outliers > 0
