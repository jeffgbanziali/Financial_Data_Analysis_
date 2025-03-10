import yfinance as yf
import pandas as pd
import os

# Définir les entreprises à analyser
stocks = ["AAPL", "TSLA", "GOOGL", "MSFT"]

# Télécharger les données boursières
data = yf.download(stocks, start="2024-09-01", end="2025-03-01")

# Réinitialiser l'index pour inclure la colonne "Date"
data.reset_index(inplace=True)

# Renommer les colonnes pour éviter les doublons
new_columns = ['Date'] + [f"{col}_{stock}" for stock in stocks for col in ['Open', 'High', 'Low', 'Close', 'Volume']]
data.columns = new_columns

# Créer le dossier data s'il n'existe pas
if not os.path.exists("../data"):
    os.makedirs("../data")

# Sauvegarder en CSV sans index
csv_path = "../data/financial_data_cleaned.csv"
data.to_csv(csv_path, index=False)

print(f"✅ Données nettoyées et sauvegardées dans {csv_path}")
