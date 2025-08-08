from pyspark.sql.functions import max

class Operativa:
    def __init__(self, df):
        self.df = df

    def run(self):
        df_con_max_saldo = self.max_saldo_por_cliente()
        df_con_fechas = self.fecha_mas_reciente_por_operacion(df_con_max_saldo)
        return df_con_fechas

    def max_saldo_por_cliente(self):
        # Agregar columna max_saldo al df original mediante join
        max_saldos = self.df.groupBy("id_cliente").agg(
            max("importe_cuenta").alias("max_saldo")
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
