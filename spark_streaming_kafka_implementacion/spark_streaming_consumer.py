from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, StringType, TimestampType
import logging

#dejamos la misma variable para el eventp
send_event = "clickstream_data" # se puede cahnge
url_port = 'localhost:9092' # se define la ruta de excucha

# Configura el nivel de log a WARN para reducir los mensajes INFO
spark = SparkSession.builder \
    .appName("KafkaSparkStreaming") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# estructuramos los datos con el mismo modelo que estados trabajando para enviarlso y no dar problemas
# definimos el (Type value), que pueden aceptar cada variable
schema = StructType([
    StructField("User_ID", IntegerType()),
    StructField("Session_ID", IntegerType()),
    StructField("Timestamp", TimestampType()),
    StructField("Page_URL", StringType()),
    StructField("Time_Spent_seconds", IntegerType()),
    StructField("Clicks", IntegerType()),
    StructField("Browser_Type", StringType()),
    StructField("Device_Type", StringType()),
    StructField("Article_Category", StringType()),
    StructField("Article_Name", StringType()),
    StructField("Interacted_Elements", StringType()),
    StructField("Video_Views", IntegerType()),
    StructField("Geo_Location", StringType())
])

# Crear una sesión de Spark 
spark = SparkSession.builder \
    .appName("SparkStreamingDataAnalysis") \
    .getOrCreate()

# Configurar el lector de streaming para leer desde Kafka
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", url_port) \
    .option("subscribe", send_event) \
    .load()

# Parser load datos JSON
parsed_df = df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

# Calcula las estadisticas por ventana de tiempo (promedio de tiempo en la pgina por categoria y tipo de dispositivo)
windowed_stats = parsed_df \
    .groupBy(window(col("Timestamp"), "1 minute"), "Article_Category", "Device_Type") \
    .agg({"Time_Spent_seconds": "avg", "Clicks Total": "sum"})

# Escribir los resultados en la consola
query = windowed_stats \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

query.awaitTermination()