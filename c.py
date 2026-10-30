from pyspark.sql import SparkSession

# Crear la sesión de Spark
spark = SparkSession.builder.appName("Streaming_Banco").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# Leer el flujo de datos desde Kafka
df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "localhost:9092").option("subscribe", "sensor_data").load()

# Convertir el valor a string
df_string = df.selectExpr("CAST(value AS STRING) as value")

# Calcular el conteo en tiempo real
query = df_string.groupBy().count().writeStream.outputMode("complete").format("console").start()

query.awaitTermination()
