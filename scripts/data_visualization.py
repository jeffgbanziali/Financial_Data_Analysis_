import dash
import dash_bootstrap_components as dbc
from dash import dcc, html, Input, Output
import plotly.express as px
import pandas as pd

# 📈 Chargement des prédictions
try:
    df_pandas = pd.read_csv("../results/predictions.csv")
    df_pandas["Date"] = pd.to_datetime(df_pandas["Date"])
except FileNotFoundError:
    print("\u274C ERREUR : Le fichier predictions.csv est introuvable !")
    df_pandas = pd.DataFrame()

stocks = ["AAPL", "TSLA", "GOOGL", "MSFT"]

# 🏠 Initialisation de l'application Dash avec Bootstrap
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.LUX])

# 🔧 Layout du tableau de bord
app.layout = dbc.Container([
    html.H1("📊 Tableau de Bord des Actions - Dash & Spark", className="text-center mt-4"),

    dbc.Row([
        dbc.Col([
            dcc.Dropdown(
                id="stock-dropdown",
                options=[{"label": stock, "value": stock} for stock in stocks],
                value="AAPL",
                clearable=False,
                className="mb-3"
            ),
        ], width=4),
        dbc.Col([
            dcc.DatePickerRange(
                id="date-picker",
                min_date_allowed=df_pandas["Date"].min() if not df_pandas.empty else None,
                max_date_allowed=df_pandas["Date"].max() if not df_pandas.empty else None,
                start_date=df_pandas["Date"].min() if not df_pandas.empty else None,
                end_date=df_pandas["Date"].max() if not df_pandas.empty else None,
                className="mb-3"
            )
        ], width=6)
    ], className="mb-4"),

    dbc.Row([
        dbc.Col([
            html.H4("📈 Performance de l'Action"),
            html.P(id="kpi-performance", className="lead text-success"),
        ], width=4),
        dbc.Col([
            html.H4("📊 Volatilité Moyenne"),
            html.P(id="kpi-volatility", className="lead text-warning"),
        ], width=4),
        dbc.Col([
            html.H4("💰 Prix Moyen"),
            html.P(id="kpi-price", className="lead text-primary"),
        ], width=4),
    ], className="mb-4"),

    dbc.Row([
        dbc.Col(dcc.Graph(id="price-graph"), width=6),
        dbc.Col(dcc.Graph(id="volatility-graph"), width=6)
    ], className="mb-4"),

    dbc.Row([
        dbc.Col(dcc.Graph(id="correlation-graph"), width=12)
    ], className="mb-4"),

    html.Footer("🔹 Dashboard interactif avec Dash & Plotly", className="text-center mt-4")
], fluid=True)


# 🎨 Callbacks pour mise à jour dynamique des données
@app.callback(
    [Output("price-graph", "figure"),
     Output("volatility-graph", "figure"),
     Output("correlation-graph", "figure"),
     Output("kpi-performance", "children"),
     Output("kpi-volatility", "children"),
     Output("kpi-price", "children")],
    [Input("stock-dropdown", "value"),
     Input("date-picker", "start_date"),
     Input("date-picker", "end_date")]
)
def update_graphs(selected_stock, start_date, end_date):
    filtered_df = df_pandas[(df_pandas["Date"] >= start_date) & (df_pandas["Date"] <= end_date)]

    # 🔄 Vérifier si les colonnes existent
    y_cols = [col for col in [f"Close_{selected_stock}", f"SMA_{selected_stock}", f"Prediction_{selected_stock}"] if
              col in filtered_df.columns]

    # Graphique des prix
    fig_price = px.line(filtered_df, x="Date", y=y_cols, title=f"Prédiction et Tendances de {selected_stock}",
                        template="plotly_dark")
    fig_price.update_traces(mode="lines+markers")

    # Graphique des volatilités
    fig_volatility = px.histogram(filtered_df, x=f"Volatility_{selected_stock}",
                                  title=f"Distribution de la Volatilité - {selected_stock}", template="plotly_dark")

    # Graphique des corrélations entre actions
    fig_correlation = px.scatter_matrix(filtered_df, dimensions=[f"Close_{s}" for s in stocks if
                                                                 f"Close_{s}" in filtered_df.columns],
                                        title="Corrélation entre les Actions", template="plotly_dark")

    # 📉 KPIs
    performance = f"{filtered_df[f'Prediction_{selected_stock}'].pct_change().mean():.2%}" if f"Prediction_{selected_stock}" in filtered_df.columns else "N/A"
    volatility = f"{filtered_df[f'Volatility_{selected_stock}'].mean():.2f}" if f"Volatility_{selected_stock}" in filtered_df.columns else "N/A"
    price = f"{filtered_df[f'Close_{selected_stock}'].mean():.2f}" if f"Close_{selected_stock}" in filtered_df.columns else "N/A"

    return fig_price, fig_volatility, fig_correlation, performance, volatility, price


# 🛠️ Lancer l'application Dash
if __name__ == "__main__":
    app.run_server(debug=True)