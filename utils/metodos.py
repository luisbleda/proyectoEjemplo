from datetime import date, timedelta
import random
from pyspark.sql import functions as F
from pyspark.sql import Window


def fecha_aleatoria():
    """
    Genera una fecha aleatoria entre el 1 de enero de 2022 y el
    31 de diciembre de 2025.
    """
    inicio = date(2022, 1, 1)
    fin = date(2025, 12, 31)
    delta = fin - inicio
    return inicio + timedelta(days=random.randint(0, delta.days))


def enrich_data(main_df, enrich_dfs, repartition_key):
    """
    Enriquecer main_df con varias fuentes de
    datos usando joins optimizados.

    Args:
        main_df: DataFrame principal (p.ej., ventas)
        enrich_dfs: lista de tuplas con (DataFrame a unir,
        lista de columnas clave, broadcast True/False)
        repartition_key: clave para hacer repartition en
        main_df para manejar skew (opcional)

    Returns:
        DataFrame resultante con los datos enriquecidos.
    """

    # Manejar skew: reparticionamos main_df si repartition_key se provee
    if repartition_key:
        main_df = main_df.repartition(
            repartition_key)

    result_df = main_df

    for (df_to_join, join_keys,
         use_broadcast) in enrich_dfs:
        # Si el df pequeño debe ser broadcast
        if use_broadcast:
            df_to_join = F.broadcast(df_to_join)

        # Hacemos join (inner por defecto, puede adaptarse)
        result_df = result_df.join(
            df_to_join, on=join_keys, how='left')

    return result_df


def temporal_analysis(df, timestamp_col, group_cols,
                      metric_col, window_duration, slide_duration,
                      rolling_window_size, outlier_threshold):
    """
    Realiza análisis de series temporales con PySpark:
    - Agrupa por ventanas de tiempo.
    - Calcula métricas móviles (rolling avg y sum).
    - Detecta outliers en base a desviación estándar móvil.

    Args:
        df: DataFrame con datos temporales.
        timestamp_col: nombre de la columna timestamp.
        group_cols: columnas para agrupar (p.ej. sensor_id).
        metric_col: columna numérica para analizar.
        window_duration: duración de la ventana para agrupar
            (e.g. '10 minutes').
        slide_duration: intervalo de desplazamiento para la ventana
            (e.g. '5 minutes').
        rolling_window_size: tamaño de la ventana móvil
            (en número de ventanas).
        outlier_threshold: umbral de desviación estándar para detectar
            outliers.

    Returns:
        DataFrame con columnas agregadas, métricas móviles y flag de
        outlier.
    """

    # Paso 1: Agrupar por ventana de tiempo y group_cols
    df_windowed = df.withColumn(
        "time_window",
        F.window(F.col(timestamp_col), window_duration, slide_duration),
    ).groupBy(
        *group_cols,
        "time_window",
    ).agg(
        F.avg(F.col(metric_col)).alias("avg_metric"),
        F.sum(F.col(metric_col)).alias("sum_metric"),
        F.stddev(F.col(metric_col)).alias("stddev_metric"),
    )

    # Paso 2: Definir ventana para rolling metrics ordenada por window start
    rolling_win = (
        Window.partitionBy(*group_cols)
        .orderBy(F.col("time_window").start)
        .rowsBetween(-rolling_window_size + 1, 0)
    )

    # Paso 3: Calcular rolling avg y rolling sum
    df_rolling = df_windowed.withColumn(
        "rolling_avg", F.avg(F.col("avg_metric")).over(rolling_win)
    ).withColumn(
        "rolling_sum", F.sum(F.col("sum_metric")).over(rolling_win)
    ).withColumn(
        "rolling_stddev", F.stddev(F.col("avg_metric")).over(rolling_win)
    )

    # Paso 4: Detectar outliers (valor promedio fuera de umbral std)
    df_outliers = df_rolling.withColumn(
        "is_outlier",
        F.when(
            F.abs(F.col("avg_metric") - F.col("rolling_avg"))
            > outlier_threshold * F.col("rolling_stddev"),
            F.lit(True),
        ).otherwise(F.lit(False)),
    )

    return df_outliers
