import findspark
findspark.init()

from pyspark.sql import SparkSession

# 📌 Créer une session Spark
spark = SparkSession.builder.appName("ReadLocally").getOrCreate()

# 📌 Lire le fichier `Parquet`
parquet_path = "../data/financial_data.parquet"
df = spark.read.parquet(parquet_path)

# 📌 Afficher un aperçu des données
df.show(10)

# 📌 Afficher les statistiques
df.describe().show()

# 📌 Fermer Spark
spark.stop()
