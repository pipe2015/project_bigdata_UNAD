#Importamos librerias necesarias
from pyspark.sql import SparkSession, functions as F

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


