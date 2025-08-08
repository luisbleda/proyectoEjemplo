import pytest
import pandas as pd
from datetime import datetime
from clases.operativa_pandas import OperativaPandas


@pytest.fixture
def df_operativa():
    data = {
        "id_cliente": [1, 1, 2],
        "fecha_saldo": ["2023-01-01", "2023-01-02", "2023-01-01"],
        "codigo_cliente": ["A", "A", "B"],
        "importe_operativa": [100, 200, 50]
    }
    return pd.DataFrame(data)


@pytest.fixture
def df_clientes():
    data = {
        "codigo_cliente": ["A", "B"],
        "nombre": ["Ana", "Luis"],
        "apellido1": ["Gómez", "Pérez"],
        "apellido2": ["López", "Martínez"],
        "direccion": ["Calle 1", "Calle 2"],
        "fecha_nacimiento": ["1995-01-01", "1980-06-15"],
        "dni": ["12345678A", "87654321B"]
    }
    return pd.DataFrame(data)


def test_max_saldo_por_cliente(df_operativa):
    oper = OperativaPandas(df_operativa, None)
    df_result = oper.max_saldo_por_cliente()
    expected = pd.DataFrame({
        "id_cliente": [1, 1, 2],
        "fecha_saldo": ["2023-01-01", "2023-01-02", "2023-01-01"],
        "codigo_cliente": ["A", "A", "B"],
        "importe_operativa": [100, 200, 50],
        "max_saldo": [200, 200, 50]
    })
    pd.testing.assert_frame_equal(df_result.reset_index(drop=True), expected)


def test_fecha_mas_reciente_por_operacion(df_operativa):
    oper = OperativaPandas(df_operativa, None)
    df_max = oper.max_saldo_por_cliente()
    df_result = oper.fecha_mas_reciente_por_operacion(df_max)
    expected_fechas = ["2023-01-02", "2023-01-02", "2023-01-01"]
    assert list(df_result["fecha_mas_reciente"]) == expected_fechas


def test_calcular_saldo_final(df_operativa):
    oper = OperativaPandas(df_operativa, None)
    df_result = oper.calcular_saldo_final(df_operativa)
    expected_saldos = {"A": 300.00, "B": 50.00}
    for _, row in df_result.iterrows():
        assert round(row["saldo_final"], 2) == expected_saldos[row["codigo_cliente"]]


def test_informacion_cliente(df_operativa, df_clientes):
    oper = OperativaPandas(df_operativa, df_clientes)
    df_max = oper.max_saldo_por_cliente()
    df_fecha = oper.fecha_mas_reciente_por_operacion(df_max)
    df_saldo = oper.calcular_saldo_final(df_fecha)
    df_result = oper.informacion_cliente(df_saldo, df_clientes)

    assert "nombre_completo" in df_result.columns
    assert "menor_30" in df_result.columns
    assert df_result.loc[df_result["codigo_cliente"] == "A", "nombre_completo"].values[0] == "Ana Gómez López"

    fecha_nac = pd.to_datetime("1995-01-01")
    hoy = pd.to_datetime(datetime.today().strftime('%Y-%m-%d'))
    edad = ((hoy - fecha_nac).days / 365)
    esperado = "SI" if edad < 30 else "NO"

    assert df_result.loc[df_result["codigo_cliente"] == "A", "menor_30"].values[0] == esperado
