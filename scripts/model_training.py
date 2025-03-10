import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, stddev
from pyspark.sql.window import Window
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.sql.utils import AnalysisException

# 📌 Initialisation de la session Spark
spark = SparkSession.builder.appName("StockPricePrediction").getOrCreate()

# 📌 Chargement des données
print("📌 Chargement des données...")
try:
    df = spark.read.parquet("../data/financial_data.parquet")
except AnalysisException as e:
    print(f"❌ ERREUR : Impossible de charger le fichier Parquet. Détail : {e}")
    spark.stop()
    exit()

# 📌 Sélectionner les colonnes utiles
df = df.select("Date", "Close_AAPL", "Close_TSLA", "Close_GOOGL", "Close_MSFT")
df = df.withColumn("Date", col("Date").cast("date"))

# 📌 Définir une fenêtre de temps pour les indicateurs financiers
windowSpec = Window.orderBy("Date").rowsBetween(-5, 0)

# 📌 Ajouter la Moyenne Mobile (SMA) et la Volatilité (Écart-Type)
print("📌 Calcul des indicateurs financiers (SMA & Volatilité)...")
df = df.withColumn("SMA_AAPL", avg("Close_AAPL").over(windowSpec)) \
       .withColumn("SMA_TSLA", avg("Close_TSLA").over(windowSpec)) \
       .withColumn("SMA_GOOGL", avg("Close_GOOGL").over(windowSpec)) \
       .withColumn("SMA_MSFT", avg("Close_MSFT").over(windowSpec)) \
       .withColumn("Volatility_AAPL", stddev("Close_AAPL").over(windowSpec)) \
       .withColumn("Volatility_TSLA", stddev("Close_TSLA").over(windowSpec)) \
       .withColumn("Volatility_GOOGL", stddev("Close_GOOGL").over(windowSpec)) \
       .withColumn("Volatility_MSFT", stddev("Close_MSFT").over(windowSpec))

# 📌 Remplacer les valeurs NULL par 0 pour éviter des erreurs
df = df.fillna(0)

# 📌 Vérification des valeurs nulles avant transformation
print("📌 Vérification des valeurs NULL :")
df.select([col(c).isNull().alias(c) for c in df.columns]).show()

# 📌 Liste des actions à entraîner
stocks = ["AAPL", "TSLA", "GOOGL", "MSFT"]

# 📌 Entraînement et sauvegarde du modèle pour chaque action
for stock in stocks:
    print(f"\n📌 Entraînement du modèle pour {stock}...")

    assembler = VectorAssembler(inputCols=[f"SMA_{stock}", f"Volatility_{stock}"],
                                outputCol="features",
                                handleInvalid="keep")

    df_ml = assembler.transform(df).select("features", col(f"Close_{stock}").alias("label"))

    # 📌 Vérification que les données sont suffisantes
    train_data, test_data = df_ml.randomSplit([0.8, 0.2], seed=42)

    if train_data.count() == 0 or test_data.count() == 0:
        print(f"❌ ERREUR : Pas assez de données pour entraîner le modèle {stock}.")
        continue

    # 📌 Initialiser le Modèle de Régression Linéaire
    lr = LinearRegression(featuresCol="features", labelCol="label", maxIter=50)

    # 📌 Entraîner le Modèle
    try:
        model = lr.fit(train_data)
        print(f"✅ Modèle entraîné avec succès pour {stock}")

        # 📌 Afficher les coefficients du modèle
        print(f"📌 Coefficients du modèle pour {stock} : {model.coefficients}")
        print(f"📌 Intercept du modèle pour {stock} : {model.intercept}")

        # 📌 Évaluer le modèle sur les données test
        predictions = model.transform(test_data)
        print(f"📌 Prédictions des Prix de Clôture pour {stock} :")
        predictions.select("features", "label", "prediction").show(5)

        # 📌 Sauvegarder le Modèle avec `overwrite`
        model.write().overwrite().save(f"../models/stock_price_model_{stock}")
        print(f"📌 Modèle sauvegardé : ../models/stock_price_model_{stock}")

    except Exception as e:
        print(f"❌ ERREUR : Échec de l'entraînement du modèle pour {stock}. Détail : {e}")

print("\n✅ Entraînement terminé avec succès pour toutes les actions.")

# 📌 Fermer Spark
spark.stop()
