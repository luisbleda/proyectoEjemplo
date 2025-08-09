from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, regexp_extract, to_timestamp, udf,
    desc, hour, date_format
)
from pyspark.sql.types import StringType


class WebLogProcessor:
    """
    Clase para procesar y analizar logs de servidor web
    en formato Apache Common Log Format usando PySpark.
    Este tipo de tarea es común en proyectos de Big Data para:
    - Monitoreo de tráfico
    - Análisis de rendimiento
    - Detección de errores
    - Limpieza de datos masivos
    """

    def __init__(self, app_name="Procesamiento Logs Web",
                 shuffle_partitions=8):
        """
        Inicializa la sesión de Spark.
        :param app_name: Nombre de la aplicación
        Spark (aparece en el UI de Spark).
        :param shuffle_partitions: Número de particiones
        para operaciones de shuffle (optimización de rendimiento).
        """
        self.spark = (
            SparkSession.builder
            .appName(app_name)
            .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
            .getOrCreate()
        )
        self.logs_df = None
        self.clean_df = None

    def load_logs(self, file_path):
        """
        Carga los logs desde un archivo en formato texto.
        Soporta archivos en HDFS, S3, Azure Blob o sistema local.
        """
        self.logs_df = self.spark.read.text(file_path)
        return self

    def parse_logs(self):
        """
        Parsea los logs utilizando expresiones regulares
        basadas en el Apache Common Log Format.
        Extrae: IP, timestamp, metodo HTTP,
        URL, código de respuesta y tamaño de respuesta.
        Convierte el timestamp a formato datetime
        para análisis temporal.
        """
        log_pattern = (
            r'(\S+) '          # IP del cliente
            r'\S+ \S+ '        # Identd y usuario (no se usan en este caso)
            r'\[([^\]]+)\] '   # Fecha y hora de la petición
            r'"(\S+) (\S+) \S+" '  # Metodo HTTP y URL
            r'(\d{3}) '        # Código de respuesta HTTP
            r'(\S+)'           # Tamaño de la respuesta en bytes
        )

        self.logs_df = self.logs_df.select(
            regexp_extract('value',
                           log_pattern, 1).alias('ip'),
            regexp_extract('value',
                           log_pattern, 2).alias('timestamp_str'),
            regexp_extract('value',
                           log_pattern, 3).alias('method'),
            regexp_extract('value',
                           log_pattern, 4).alias('url'),
            regexp_extract('value',
                           log_pattern, 5).alias('status_code'),
            regexp_extract('value',
                           log_pattern, 6).alias('response_size')
        )

        # Conversión de string a formato
        # timestamp (ej. "12/Jan/2024:09:15:32 +0000")
        self.logs_df = self.logs_df.withColumn(
            "timestamp",
            to_timestamp(
                col("timestamp_str"),
                "dd/MMM/yyyy:HH:mm:ss Z")
        )
        return self

    def clean_logs(self):
        """
        Limpia el DataFrame eliminando:
        - Filas con valores nulos o vacíos
        - Duplicados
        También añade una columna con los parámetros de
        consulta (query string) de la URL.
        """
        self.clean_df = self.logs_df.filter(
            (col("ip").isNotNull()) &
            (col("timestamp").isNotNull()) &
            (col("method") != "") &
            (col("url") != "") &
            (col("status_code") != "")
        ).dropDuplicates()

        # UDF para extraer parámetros de la URL si existen
        extract_param_udf = udf(
            lambda url: url.split('?')[1] if '?' in url else None,
            StringType())
        self.clean_df = self.clean_df.withColumn(
            "query_params", extract_param_udf(col("url")))
        return self

    def top_pages(self, n=10):
        """
        Devuelve las páginas más visitadas.
        :param n: Número de resultados a devolver.
        """
        return self.clean_df.groupBy(
            "url").count().orderBy(desc("count")).limit(n)

    def top_errors(self, n=10):
        """
        Devuelve los códigos de error más frecuentes (4xx y 5xx).
        :param n: Número de resultados a devolver.
        """
        return self.clean_df.filter(
            col(
                "status_code").startswith("4") | col(
                "status_code").startswith("5")
        ).groupBy("status_code").count().orderBy(desc("count")).limit(n)

    def traffic_by_hour(self):
        """
        Calcula el volumen de tráfico por cada hora del día.
        Esto permite detectar picos de carga en la aplicación.
        """
        return self.clean_df.groupBy(hour(col("timestamp")).alias("hour")) \
            .count().orderBy("hour")

    def save_clean_data(self, output_path):
        """
        Guarda el DataFrame limpio en formato
        Parquet, particionado por fecha.
        Este formato es óptimo para lecturas posteriores y
        reduce el coste de escaneo de datos.
        """
        self.clean_df = self.clean_df.withColumn("date",
                                                 date_format(col("timestamp"),
                                                             "yyyy-MM-dd"))
        self.clean_df.write.mode(
            "overwrite").partitionBy("date").parquet(output_path)
        return self
