#Importamos librerias necesarias
from pyspark.sql import SparkSession, functions as F
#from pyspark.sql.functions import col, avg, sum, count as _sum
from pyspark.sql.functions import col, avg, sum, count, max, min, year

"""
docs: https://jackyfu1995.medium.com/data-engineering-introduction-and-implementation-of-spark-python-7391c8da2e2e
Cleaning Data with Apache Spark in Python:
https://www.tutorialspoint.com/cleaning-data-with-apache-spark-in-python
Apache Spark: Data cleaning using PySpark for beginners
https://medium.com/bazaar-tech/apache-spark-data-cleaning-using-pyspark-for-beginners-eeeced351ebf
"""

file_path_csv = "ckickstream_blognews_05.csv"

# Inicializa la sesión de Spark
spark = SparkSession.builder.appName('Tarea3').getOrCreate()

# Define la ruta del archivo .csv en HDFS
file_path = f"hdfs://localhost:9000/Tarea3/{file_path_csv}"

# Lee el archivo .csv
df = spark.read.format('csv').option('header','true').option('inferSchema', 'true').load(file_path)

#imprimimos el esquema
df.printSchema()

# Muestra las primeras filas del DataFrame
df.show()

# Estadisticas básicas
df.summary().show()

# A continuacion vamos a realizar operaciones de analisis de datos con las siguientes fuciones
# 1. Contar el número total de registros
total_records = df.count()
print(f"\nTotal de registros: {total_records}")


# 1. Realizamos operaciones de limpieza
# Verificar si hay valores nulos
print("Valores nulos en cada columna:")
df.select([col(c).isNull().cast("int").alias(c) for c in df.columns]).show()

# en esat primera parte estoy usando el objeto dataFrame

# Eliminar filas con valores nulos
clear_df = df.na.drop()

# 2. Transformaciones
# Convertir 'time_spent_seconds' de segundos a minutos usando la funcion map que pasa por parametro cada dato y lo
# retorno con la transformacion
clear_df = clear_df.withColumn("time_spent_minutes", col("time_spent_seconds") / 60)

# Analisis exploratorio de datos (EDA)
# Numero total de usuarios
total_users = clear_df.select("user_id").distinct().count()
print(f"Numero total de usuarios: {total_users}")

# Sesiones por dispositivo
sessions_by_device = clear_df.groupBy("device_type").count().orderBy("count", ascending=False)
print("Sesiones por dispositivo:")
sessions_by_device.show()

# Numero total de cliks por usuario
clicks_per_user = clear_df.groupBy("user_id").agg(sum("clicks").alias("total_clicks"))
print("\nNumero total de cliks por usuario:")
clicks_per_user.show()

# Numero total de clikss por artículo
clicks_by_article = clear_df.groupBy("article_name").agg(sum("clicks").alias("total_clicks")).orderBy("total_clicks", ascending=False)
print("Total de clicks por articulo:")
clicks_by_article.show()

# Tiempo promedio que los usuarios pasan en las páginas
average_time_spent = clear_df.groupBy("user_id").agg(avg("time_spent_seconds").alias("average_time_spent"))
print("\nTiempo promedio que los usuarios pasan en las paginas (en segundos):")
average_time_spent.show()

# Videos vistos por categoria
video_views_by_category = clear_df.groupBy("article_category").agg(sum("video_views").alias("total_video_views")).orderBy("total_video_views", ascending=False)
print("Videos vistos por categoria:")
video_views_by_category.show()

# Numero de articulos leídos por categoria
articles_per_category = clear_df.groupBy("article_category").agg(count("article_name").alias("articles_read"))
print("\nNumero de articulos leidos por categoria:")
articles_per_category.show()

# Analisis de la ubicacion geografica
geo_location_counts = clear_df.groupBy("Geo_Location").count().orderBy("count", ascending=False)
print("Numero de interacciones por ubicación geografica:")
geo_location_counts.show()

# Filtrar los datos para el año 2024
df_2024 = clear_df.filter(year(col("timestamp")) == 2024)

# Obtener la primera y última interacción por usuario en 2024
user_interactions_2024 = df_2024.groupBy("user_id").agg(
    min("timestamp").alias("Fecha_Inicio"),
    max("timestamp").alias("Fecha_Final")
).orderBy(col("user_id").asc())

# Mostrar el resultado
print("Fecha de inicio y Final de cada usuario que interactuo con la pagina en el 2024")
user_interactions_2024.show(truncate=False)