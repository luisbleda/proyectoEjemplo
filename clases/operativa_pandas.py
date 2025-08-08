import pandas as pd
from datetime import datetime


class OperativaPandas:
    def __init__(self, df, df_clientes):
        self.df = df
        self.df_clientes = df_clientes

    def run(self):
        df_con_max_saldo = self.max_saldo_por_cliente()
        df_con_fechas = self.fecha_mas_reciente_por_operacion(df_con_max_saldo)
        df_con_saldo = self.calcular_saldo_final(df_con_fechas)
        df_info = self.informacion_cliente(df_con_saldo, self.df_clientes)
        return df_info

    def max_saldo_por_cliente(self):
        max_saldos = self.df.groupby("id_cliente")["importe_operativa"].max().reset_index()
        max_saldos.rename(columns={"importe_operativa": "max_saldo"}, inplace=True)
        df_joined = pd.merge(self.df, max_saldos, on="id_cliente", how="left")
        return df_joined

    def fecha_mas_reciente_por_operacion(self, df):
        max_fechas = df.groupby(["id_cliente", "codigo_cliente"])["fecha_saldo"].max().reset_index()
        max_fechas.rename(columns={"fecha_saldo": "fecha_mas_reciente"}, inplace=True)
        df_joined = pd.merge(df, max_fechas, on=["id_cliente", "codigo_cliente"], how="left")
        return df_joined

    def calcular_saldo_final(self, df):
        saldo_por_cliente = df.groupby("codigo_cliente")["importe_operativa"].sum().round(2).reset_index()
        saldo_por_cliente.rename(columns={"importe_operativa": "saldo_final"}, inplace=True)
        df_con_saldo = pd.merge(df, saldo_por_cliente, on="codigo_cliente", how="left")
        return df_con_saldo

    def informacion_cliente(self, df_operativa, df_clientes):
        df_merged = pd.merge(
            df_operativa,
            df_clientes,
            on="codigo_cliente",
            how="left")
        df_merged = df_merged.fillna({
            "nombre": "",
            "apellido1": "",
            "apellido2": "",
            "direccion": "",
            "fecha_nacimiento": pd.NaT,
            "dni": "",
            "importe_operativa": 0,
            "max_saldo": 0,
            "fecha_mas_reciente": pd.NaT,
            "saldo_final": 0
        })
        # Crear nombre completo
        df_merged["nombre_completo"] = df_merged["nombre"] + " " + df_merged["apellido1"] + " " + df_merged["apellido2"]

        # Calcular edad
        hoy = pd.to_datetime(datetime.today().strftime('%Y-%m-%d'))
        df_merged["fecha_nacimiento"] = pd.to_datetime(df_merged["fecha_nacimiento"])
        df_merged["edad"] = ((hoy - df_merged["fecha_nacimiento"]).dt.days / 365).round().astype("Int64")

        # Crear columna menor_30
        df_merged["menor_30"] = df_merged["edad"].apply(lambda x: "SI" if x < 30 else "NO")

        return df_merged.drop(columns=["edad"])
