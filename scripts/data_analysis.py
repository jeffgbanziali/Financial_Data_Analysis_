import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, stddev, lit, expr
from pyspark.sql.window import Window
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegressionModel
import pandas as pd
import datetime
import os

# 📌 Initialisation de la session Spark
spark = SparkSession.builder.appName("StockAnalysis").getOrCreate()

# 📌 Chargement des données historiques
print("📌 Chargement des données historiques...")
df = spark.read.option("header", "true").option("inferSchema", "true").csv("../data/financial_data_cleaned.csv")

# 📌 Sélectionner uniquement les colonnes utiles et partitionner les données par Date pour améliorer les performances
df_selected = df.select("Date", "Close_AAPL", "Close_TSLA", "Close_GOOGL", "Close_MSFT")
df_selected = df_selected.withColumn("Date", col("Date").cast("date"))
df_selected = df_selected.repartition(10, "Date")  # Partitionner par Date

# 📌 Mettre en cache les données pour éviter des recalculs
df_selected.cache()

# 📌 Définition des fenêtres Spark
windowRolling = Window.partitionBy("Date").orderBy("Date").rowsBetween(-5, 0)

# 📌 Calcul des indicateurs SMA et Volatilité
print("📌 Calcul des indicateurs SMA et Volatilité...")
df_features = df_selected.withColumn("SMA_AAPL", avg("Close_AAPL").over(windowRolling)) \
                         .withColumn("SMA_TSLA", avg("Close_TSLA").over(windowRolling)) \
                         .withColumn("SMA_GOOGL", avg("Close_GOOGL").over(windowRolling)) \
                         .withColumn("SMA_MSFT", avg("Close_MSFT").over(windowRolling)) \
                         .withColumn("Volatility_AAPL", stddev("Close_AAPL").over(windowRolling)) \
                         .withColumn("Volatility_TSLA", stddev("Close_TSLA").over(windowRolling)) \
                         .withColumn("Volatility_GOOGL", stddev("Close_GOOGL").over(windowRolling)) \
                         .withColumn("Volatility_MSFT", stddev("Close_MSFT").over(windowRolling))

df_clean = df_features.fillna(0)

# 📌 Charger les modèles
models = {}
stocks = ["AAPL", "TSLA", "GOOGL", "MSFT"]

for stock in stocks:
    model_path = f"../models/stock_price_model_{stock}"
    if os.path.exists(model_path):
        print(f"📌 Chargement du modèle pour {stock}...")
        models[stock] = LinearRegressionModel.load(model_path)
    else:
        print(f"❌ ERREUR : Le modèle {model_path} est introuvable.")

# 📌 Générer les futures dates (Mars 2025 - Mars 2026)
print("📌 Génération des dates futures...")
last_date = df_selected.agg({"Date": "max"}).collect()[0][0]
future_dates = [last_date + datetime.timedelta(days=i) for i in range(1, 366)]  # 1 an de prévisions
future_df = spark.createDataFrame([(d,) for d in future_dates], ["Date"])

# 📌 Vérifier si les données historiques ne sont pas vides
if not df_clean.rdd.isEmpty():
    last_known_values = df_clean.orderBy(col("Date").desc()).limit(1).collect()[0]

    for stock in stocks:
        future_df = future_df.withColumn(f"SMA_{stock}", lit(last_known_values[f"SMA_{stock}"])) \
                             .withColumn(f"Volatility_{stock}", lit(last_known_values[f"Volatility_{stock}"]))

    # 📌 Générer SMA et Volatilité en fonction des valeurs précédentes pour éviter les prédictions constantes
    future_df = future_df.withColumn("SMA_AAPL", expr("SMA_AAPL * (1 + rand() * 0.02 - 0.01)")) \
                         .withColumn("Volatility_AAPL", expr("Volatility_AAPL * (1 + rand() * 0.02 - 0.01)")) \
                         .withColumn("SMA_TSLA", expr("SMA_TSLA * (1 + rand() * 0.02 - 0.01)")) \
                         .withColumn("Volatility_TSLA", expr("Volatility_TSLA * (1 + rand() * 0.02 - 0.01)")) \
                         .withColumn("SMA_GOOGL", expr("SMA_GOOGL * (1 + rand() * 0.02 - 0.01)")) \
                         .withColumn("Volatility_GOOGL", expr("Volatility_GOOGL * (1 + rand() * 0.02 - 0.01)")) \
                         .withColumn("SMA_MSFT", expr("SMA_MSFT * (1 + rand() * 0.02 - 0.01)")) \
                         .withColumn("Volatility_MSFT", expr("Volatility_MSFT * (1 + rand() * 0.02 - 0.01)"))
else:
    print("❌ ERREUR : Les données historiques sont vides. Impossible d'extrapoler les SMA et Volatilité.")

# 📌 Appliquer la prédiction pour chaque action
for stock in stocks:
    if stock in models:
        print(f"📌 Prédiction pour {stock}...")

        assembler = VectorAssembler(inputCols=[f"SMA_{stock}", f"Volatility_{stock}"], outputCol="features", handleInvalid="keep")
        df_spark_pred = assembler.transform(future_df).select("Date", "features")

        predictions = models[stock].transform(df_spark_pred).select("Date", "prediction")
        predictions = predictions.withColumnRenamed("prediction", f"Prediction_{stock}")

        # 📌 Fusionner les prédictions avec `future_df`
        future_df = future_df.join(predictions, on="Date", how="left")

# 📌 Sauvegarder les prédictions
df_predictions = future_df.toPandas()
df_predictions.to_parquet("../results/predictions.parquet", index=False)

print("✅ Prédictions terminées et enregistrées dans `../results/predictions.parquet`")
