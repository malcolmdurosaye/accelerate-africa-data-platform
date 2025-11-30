import os
import pandas as pd
from sqlalchemy import create_engine, text
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv
import re

# 1. SETUP
load_dotenv()

app = FastAPI(
    title="Accelerate Africa Data API",
    description="API for accessing application data securely.",
    version="1.0"
)

# 2. CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 3. DATABASE CONNECTION
def get_db_engine():
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        raise ValueError("DATABASE_URL is not set")
    if db_url.startswith("postgres://"):
        db_url = db_url.replace("postgres://", "postgresql://", 1)
    return create_engine(db_url)

# HELPER: Find the real column name dynamically
def find_column(available_columns, targets):
    """
    Checks if any 'target' exists exactly in available_columns.
    Returns the quoted column name or None.
    """
    # 1. Check strict exact matches first (Highest Priority)
    for target in targets:
        if target in available_columns:
            return f'"{target}"'
            
    # 2. Check case-insensitive match
    for target in targets:
        for col in available_columns:
            if target.lower() == col.lower():
                return f'"{col}"'
    
    # 3. Last resort: Partial match (Riskier, but better than nothing)
    # We skip this for 'raised' to avoid picking the text question column
    return None

# 4. ENDPOINTS

@app.get("/")
def read_root():
    return {"status": "online", "message": "Accelerate Africa API is running"}

@app.get("/api/applications")
def get_applications(limit: int = 1000):
    try:
        engine = get_db_engine()
        query = f'SELECT * FROM applications LIMIT {limit}'
        df = pd.read_sql(query, engine)
        
        if df.empty:
            return {"count": 0, "data": []}

        df = df.where(pd.notnull(df), None)
        return {"count": len(df), "data": df.to_dict(orient="records")}
        
    except Exception as e:
        print(f"❌ API CRASHED: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/stats")
def get_stats():
    try:
        engine = get_db_engine()
        
        # 1. Get Column Names
        cols_df = pd.read_sql("SELECT * FROM applications LIMIT 0", engine)
        all_cols = list(cols_df.columns)
        
        # 2. Find Country Column
        country_col = find_column(all_cols, ["Country", "country", "location", "Location"])
        
        # 3. Find Funding Column (STRICT LIST ONLY)
        # We removed the fuzzy search so it doesn't pick the text question again.
        raised_col = find_column(all_cols, [
            "total_raised_usd", 
            "total_raised", 
            "latest_fundraise_usd",
            "Fundraise Amount ($)",
            "How much money have you raised from investors, including friends and family, in total in US Dollars? "
        ])
        
        if not country_col: country_col = '"Country"' # Default fallback
        if not raised_col: raised_col = '"total_raised_usd"' # Default fallback

        # 4. SAFE QUERY
        # We use a REGEX check to only sum values that look like numbers.
        # If it contains text like "N/A", it counts as 0.
        query = f"""
            SELECT 
                COUNT(*) as total_apps,
                COUNT(DISTINCT {country_col}) as total_countries,
                SUM(
                    CASE 
                        WHEN {raised_col} ~ '^[0-9\.]+$' THEN CAST({raised_col} AS NUMERIC)
                        ELSE 0 
                    END
                ) as total_raised
            FROM applications
        """
        
        with engine.connect() as conn:
            result = conn.execute(text(query)).mappings().first()
            
        return dict(result)

    except Exception as e:
        print(f"❌ STATS ERROR: {e}")
        raise HTTPException(status_code=500, detail=f"Stats Error: {str(e)}")