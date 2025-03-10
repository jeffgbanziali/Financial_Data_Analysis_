import findspark
findspark.init()

from pyspark.sql import SparkSession

# Créer une session Spark
spark = SparkSession.builder.appName("FinanceAnalysis").getOrCreate()

# Charger le fichier CSV nettoyé
df = spark.read.option("header", "true").option("inferSchema", "true").csv("../data/financial_data_cleaned.csv")

# Vérifier que les colonnes sont correctes
print("📌 Aperçu des données :")
df.show(5)

# Vérifier le schéma des colonnes
print("📌 Schéma du DataFrame :")
df.printSchema()

# Fermer Spark
spark.stop()
