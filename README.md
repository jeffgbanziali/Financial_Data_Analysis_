# 📊 Analyse de Données avec Apache Spark

## 📌 Description
Ce projet utilise **Apache Spark** et **PySpark** pour analyser de grandes quantités de données financières. Il inclut le traitement, l'analyse et la prédiction des prix de clôture des actions de plusieurs entreprises (Apple, Tesla, Google et Microsoft) à l'aide de modèles de régression linéaire. Une interface interactive a été développée avec **Dash** pour visualiser les tendances et les prédictions.

## 🚀 Technologies Utilisées
- **Apache Spark / PySpark** : Traitement et analyse des données
- **Python** : Langage principal du projet
- **Dash & Plotly** : Visualisation interactive des résultats
- **Pandas** : Manipulation des données
- **SQL / Parquet / CSV** : Formats de stockage des données
- **Machine Learning** : Modèle de régression linéaire

## 📂 Structure du Projet

```
.
├── README.md  # Documentation du projet
├── data        # Données brutes et nettoyées
│   ├── financial_data.csv
│   ├── financial_data.parquet
│   └── financial_data_cleaned.csv
├── models      # Modèles de Machine Learning pré-entraînés
│   ├── stock_price_model_AAPL
│   ├── stock_price_model_TSLA
│   ├── stock_price_model_GOOGL
│   ├── stock_price_model_MSFT
│   └── stock_price_model
├── results     # Résultats et prédictions
├── scripts     # Scripts de traitement et d'analyse
│   ├── data_collection.py   # Récupération des données
│   ├── data_processing.py   # Nettoyage et préparation des données
│   ├── data_analysis.py     # Analyse et visualisation
│   ├── model_training.py    # Entraînement du modèle
│   ├── data_visualization.py # Dashboard interactif
│   ├── read_local.py        # Lecture des données locales
│   └── store_local.py       # Stockage des données en local
├── test_spark.py  # Test d'exécution Spark
```

## 📥 Installation

1. **Cloner le dépôt** :
   ```sh
   git clone https://github.com/ton-repo/projet_spark.git
   cd projet_spark
   ```
2. **Installer les dépendances** :
   ```sh
   pip install -r requirements.txt
   ```
3. **Configurer Apache Spark** :
   - Assurez-vous que Spark et Hadoop sont installés sur votre machine.
   - Vérifiez votre environnement avec :
     ```sh
     spark-submit --version
     ```

## 📊 Exécution du Projet

### 1️⃣ **Traitement et Nettoyage des Données**
```sh
python scripts/data_processing.py
```

### 2️⃣ **Entraînement du Modèle**
```sh
python scripts/model_training.py
```

### 3️⃣ **Lancer l'Analyse et la Prédiction**
```sh
python scripts/data_analysis.py
```

### 4️⃣ **Lancer le Dashboard Interactif**
```sh
python scripts/data_visualization.py
```
Une fois lancé, ouvrez [http://127.0.0.1:8050/](http://127.0.0.1:8050/) dans votre navigateur.

## 📌 Fonctionnalités
✅ Extraction et nettoyage des données
✅ Entraînement d'un modèle de régression linéaire
✅ Prédiction des prix de clôture des actions
✅ Visualisation interactive avec Dash & Plotly

## 🛠 Améliorations Futures
- Intégration d'autres modèles de Machine Learning (Random Forest, LSTM)
- Optimisation des performances Spark avec la parallélisation
- Intégration de flux de données en temps réel (Kafka, Spark Streaming)

📌 **Auteur** : Jeff Gbanziali
📅 **Date** : Mars 2025

