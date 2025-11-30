import os
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import dash
from dash import dcc, html, Input, Output, callback, State
import dash_bootstrap_components as dbc, dash_table
from sqlalchemy import create_engine
from datetime import datetime
# ... imports ...
from dotenv import load_dotenv # Make sure to import this

load_dotenv() # Load the variables

# ─── CONFIG ─────────────────────────────────────────────────────────────
# CONNECT TO RENDER
db_url = os.getenv("DATABASE_URL")
if db_url and db_url.startswith("postgres://"):
    db_url = db_url.replace("postgres://", "postgresql://", 1)

db_engine = create_engine(db_url)

# ... rest of the code ...

LOGO_URL = "https://cdn1.vc4a.com/media/2024/01/Screenshot-2024-01-29-at-09.12.14-500x322.png"

THEME_PALETTES = {
    "light": {
        "primary_accent": "#4f46e5", "sidebar_bg": "#ffffff", "main_bg": "#f8fafc",
        "card_bg": "#ffffff", "text_primary": "#1f293b", "text_muted": "#6b7280",
        "border_color": "#e5e7eb",
        "chart_colors": ["#4f46e5", "#f59e0b", "#10b981", "#ec4899", "#60a5fa", "#fbbf24"]
    },
    "dark": {
        "primary_accent": "#818cf8", "sidebar_bg": "#111827", "main_bg": "#030712",
        "card_bg": "#1f2937", "text_primary": "#f9fafb", "text_muted": "#9ca3af",
        "border_color": "#374151",
        "chart_colors": ["#818cf8", "#f59e0b", "#34d399", "#f472b6", "#60a5fa", "#fbbf24"]
    }
}

# ─── DATA LOADING ───────────────────────────────────────────────────────
def load_data():
    try:
        # Read directly from SQL
        df = pd.read_sql("SELECT * FROM applications", db_engine)
        
        # Standardize Columns
        rename_map = {
            "Email Address": "contact_email", "What's your email?": "applicant_email", 
            "What's your full name?": "applicant_name", "What's your phone number?": "phone_number", 
            "What's your location?": "location", "What's your gender?": "gender", 
            "Which theme is your startup most aligned with?": "theme_primary",
            "What's the name of your startup?": "startup_name", 
            "What is your company making or going to make?": "product_description",
            "What's the URL of your demo video (1-2 minutes), if you have one?": "product_demo",
            "What's your startup's website URL?": "startup_website_url", 
            "What's your startup's founding date?": "founding_date",
            "How many founders does your startup have?": "num_founders",
            "How many female founders does your company have, if any?": "num_female_founders",
            "What is your revenue in USD for each of the past 6 months?": "monthly_revenue_usd",
            "Fundraise Amount ($)": "latest_fundraise_usd",
            "Status": "application_status", "Application Status": "application_status"
        }
        cols_to_rename = {c: rename_map[c] for c in df.columns if c in rename_map}
        df.rename(columns=cols_to_rename, inplace=True)
        
        # Type conversions
        if "created_at" in df.columns:
            df["application_date"] = pd.to_datetime(df["created_at"], errors="coerce")
        if "founding_date" in df.columns:
            df["founding_date"] = pd.to_datetime(df["founding_date"], errors="coerce")
            
        # Numerics
        numerics = ["num_founders", "num_female_founders", "latest_fundraise_usd"]
        for col in numerics:
            if col not in df.columns: df[col] = 0
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
            
        # Derived
        df["Country"] = df.get("Country", df.get("country", df.get("location", "Unknown")))
        df["Industry"] = df.get("Industry", df.get("industry_name", "Unknown"))
        df["Has Female Founder"] = df["num_female_founders"] > 0
        df["Application Status"] = df.get("application_status", "Applied")
        df["Is Revenue Generating"] = df.get("Revenue Generating?", df.get("is_revenue_generating", "No"))
        
        year_map = {"AA0": 2023, "AA1": 2024, "AA2": 2024, "AA3": 2024, "AA4": 2025}
        if "Cohort" in df.columns:
            df["Application Year"] = df["Cohort"].map(year_map)

        # Startup Age
        def calc_age(row):
            try:
                founding = row.get("founding_date")
                if pd.isnull(founding): return 0
                return int((pd.Timestamp.now() - founding).days // 365)
            except: return 0
        df["Startup Age (years)"] = df.apply(calc_age, axis=1)

        return df
    except Exception as e:
        print(f"Error loading database: {e}")
        return pd.DataFrame()

df = load_data()

# ─── DASH APP LAYOUT (Simplified) ───────────────────────────────────────
app = dash.Dash(__name__, external_stylesheets=[
    dbc.themes.BOOTSTRAP,
    "https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap",
    "https://cdn.jsdelivr.net/npm/bootstrap-icons@1.11.3/font/bootstrap-icons.min.css"
])
app.title = "Accelerate Africa | Secured"

sidebar = html.Div([
    html.Div([
        html.Img(src=LOGO_URL, height=40),
        html.H5("Accelerate Africa", className="fw-bold mt-2")
    ], className="text-center mb-4"),
    html.Hr(),
    html.Div([
        html.Label("Cohort Filter"),
        dcc.Dropdown(id="cohort-filter", 
                     options=[{'label': c, 'value': c} for c in sorted(df['Cohort'].dropna().unique())] if not df.empty else [],
                     placeholder="Select Cohort"),
        html.Br(),
        html.Label("Min Funding ($)"),
        dcc.Dropdown(id="funding-filter", 
                     options=[{"label": "≥ $10k", "value": 10000}, {"label": "≥ $100k", "value": 100000}],
                     placeholder="Any Funding"),
    ]),
    html.Hr(),
    dbc.Nav([
        dbc.NavLink("Shortlisted", href="/shortlisted"),
        dbc.NavLink("Approved", href="/approved"),
    ], vertical=True, pills=True)
], style={"width": "250px", "position": "fixed", "top": 0, "left": 0, "bottom": 0, "padding": "2rem", "backgroundColor": "#f8f9fa"})

app.layout = html.Div([
    dcc.Location(id='url'),
    sidebar,
    html.Div(id="page-content", style={"marginLeft": "250px", "padding": "2rem"})
])

@callback(Output('page-content', 'children'), [Input('url', 'pathname'), Input('cohort-filter', 'value')])
def render_content(pathname, cohort):
    # Filter Data
    d = df.copy()
    if cohort: d = d[d["Cohort"] == cohort]
    
    if pathname == "/shortlisted":
        d = d[d["Application Status"] == "Shortlisted"]
        return html.Div([html.H3("Shortlisted Applications"), dbc.Table.from_dataframe(d[["startup_name", "Country", "contact_email"]], striped=True, bordered=True, hover=True)])
    
    # Dashboard
    if d.empty: return html.Div("No data found. Please run secure_update.py first.")
    
    fig_countries = px.bar(d["Country"].value_counts().head(10), orientation='h', title="Top Countries")
    fig_funding = px.pie(names=d["Is Revenue Generating"].unique(), values=d["Is Revenue Generating"].value_counts(), title="Revenue Status")
    
    return html.Div([
        html.H2("Dashboard Overview"),
        dbc.Row([
            dbc.Col(dbc.Card(dbc.CardBody([html.H4(len(d)), html.P("Applications")])), width=3),
            dbc.Col(dbc.Card(dbc.CardBody([html.H4(f"${d['latest_fundraise_usd'].sum()/1e6:.1f}M"), html.P("Total Raised")])), width=3),
        ], className="mb-4"),
        dbc.Row([
            dbc.Col(dcc.Graph(figure=fig_countries), width=6),
            dbc.Col(dcc.Graph(figure=fig_funding), width=6)
        ])
    ])

if __name__ == "__main__":
    app.run(debug=True, port=8050)
