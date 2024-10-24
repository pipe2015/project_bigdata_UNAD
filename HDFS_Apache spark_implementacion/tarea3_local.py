from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, sum, count, max, min, stddev


"""
no se puede usar hay que instalar y configurar hadoop para windows da errores
"""

# Inicializar SparkSession
spark = SparkSession.builder \
    .appName("Tarea3") \
    .getOrCreate()

# Cargar el archivo CSV en un DataFrame
file_path_csv = "List_csv_data/ckickstream_blognews_05.csv"
df = spark.read.format('csv').option('header','true').option('inferSchema', 'true').load(file_path_csv)

# Mostrar las primeras filas del DataFrame
print("Primeras filas del DataFrame:")
df.show()

# 1. Contar el número total de registros
total_records = df.count()
print(f"\nTotal de registros: {total_records}")
