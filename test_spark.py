import findspark
findspark.init()

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("TestPySpark").getOrCreate()

# Vérifier la version de Spark
print(f"Apache Spark version: {spark.version}")

spark.stop()
