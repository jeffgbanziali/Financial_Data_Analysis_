import dash
from dash import dcc, html
import plotly.express as px
import pandas as pd
import os

# 📌 Vérifier l'existence du fichier de prédictions
predictions_path = "../results/predictions.csv"
if not os.path.exists(predictions_path):
    raise FileNotFoundError(
        f"❌ ERREUR : Le fichier {predictions_path} est introuvable. Lancez `data_analysis.py` pour générer les prédictions.")

# 🔄 Chargement des prédictions avec conversion des dates
df_pandas = pd.read_csv(predictions_path)
df_pandas["Date"] = pd.to_datetime(df_pandas["Date"])

# 📈 Liste des actions disponibles
stocks = ["AAPL", "TSLA", "GOOGL", "MSFT"]

# 📊 Création de l'application Dash
app = dash.Dash(__name__)

app.layout = html.Div([
    html.H1("📊 Tableau de Bord des Actions - Dash & Spark", style={"textAlign": "center"}),

    dcc.Dropdown(
        id="stock-dropdown",
        options=[{"label": stock, "value": stock} for stock in stocks],
        value="AAPL",
        clearable=False,
        style={"width": "50%", "margin": "auto"}
    ),

    dcc.Graph(id="price-graph"),

    html.Footer("📢 Dashboard interactif avec Dash & Plotly", style={"textAlign": "center", "marginTop": "20px"})
])


@app.callback(
    dash.Output("price-graph", "figure"),
    [dash.Input("stock-dropdown", "value")]
)
def update_graph(selected_stock):
    y_cols = [col for col in [f"Prediction_{selected_stock}"] if col in df_pandas.columns]

    if not y_cols:
        return px.line(title="❌ Aucune donnée disponible pour cette action.")

    fig = px.line(
        df_pandas, x="Date", y=y_cols,
        title=f"📈 Prédiction et Tendances de {selected_stock} (Mars 2025 - Mars 2026)",
        labels={"value": "Prix", "variable": "Type de Données"},
        template="plotly_dark"
    )

    fig.update_traces(mode="lines+markers")
    return fig


# 🚀 Lancer le serveur Dash
if __name__ == "__main__":
    app.run_server(debug=True)
