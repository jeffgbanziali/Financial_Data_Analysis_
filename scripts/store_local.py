import findspark
findspark.init()

from pyspark.sql import SparkSession

# 📌 Créer une session Spark
spark = SparkSession.builder.appName("StoreLocally").getOrCreate()

# 📌 Charger les données nettoyées
df = spark.read.option("header", "true").option("inferSchema", "true").csv("../data/financial_data_cleaned.csv")

# 📌 Stocker en format `Parquet` (optimisé pour Spark)
parquet_path = "../data/financial_data.parquet"
df.write.mode("overwrite").parquet(parquet_path)

# 📌 Stocker en format `CSV` (plus universel)
csv_path = "../data/financial_data.csv"
df.write.mode("overwrite").option("header", "true").csv(csv_path)

print(f"✅ Données stockées localement :\n - {parquet_path} (Parquet)\n - {csv_path} (CSV)")

# 📌 Fermer Spark
spark.stop()
