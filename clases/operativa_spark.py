from pyspark.sql.functions import max
from pyspark.sql import functions as F
from datetime import datetime


class OperativaSpark:
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
        # Agregar columna max_saldo al df original mediante join
        max_saldos = self.df.groupBy("id_cliente").agg(
            max("importe_operativa").alias("max_saldo")
        )
        df_joined = self.df.join(max_saldos,
                                 on="id_cliente",
                                 how="left")
        return df_joined

    def fecha_mas_reciente_por_operacion(self, df):
        # Agregar columna fecha_mas_reciente al df pasado mediante join
        max_fechas = df.groupBy("id_cliente", "codigo_cliente").agg(
            max("fecha_saldo").alias("fecha_mas_reciente")
        )
        df_joined = df.join(max_fechas,
                            on=["id_cliente", "codigo_cliente"],
                            how="left")
        return df_joined

    def calcular_saldo_final(self, df):
        """
        Añade una columna 'saldo_final' al DataFrame con el saldo total
        por cliente (codigo_cliente) sumando importe_operativa,
        redondeado a 2 decimales.
        """
        # Sumar importe_operativa agrupado por codigo_cliente
        saldo_por_cliente = df.groupBy("codigo_cliente").agg(
            F.round(F.sum("importe_operativa"), 2).alias("saldo_final")
        )

        # Unir saldo_final al DataFrame original por codigo_cliente
        df_con_saldo = df.join(saldo_por_cliente, on="codigo_cliente", how="left")

        return df_con_saldo

    def informacion_cliente(self, df_operativa, df_clientes):
        # Join por codigo_cliente
        df_merged = df_operativa.join(
            df_clientes,
            on='codigo_cliente',
            how='left').fillna("").dropDuplicates(["codigo_cliente"])

        # Crear nombre_completo
        df_merged = df_merged.withColumn(
            'nombre_completo',
            F.concat_ws(' ', df_merged['nombre'],
                        df_merged['apellido1'], df_merged['apellido2'])
        )

        # Fecha hoy en formato yyyy-MM-dd
        hoy_str = datetime.today().strftime('%Y-%m-%d')

        # Calcular edad en años con datediff y floor
        df_merged = df_merged.withColumn(
            'edad',
            F.floor(F.datediff(F.lit(hoy_str), F.col('fecha_nacimiento')) / 365)
        )

        # Crear columna menor_30: SI si edad < 30, NO en otro caso
        df_merged = df_merged.withColumn(
            'menor_30',
            F.when(F.col('edad') < 30, F.lit('SI')).otherwise(F.lit('NO'))
        )

        return df_merged.drop('edad')
