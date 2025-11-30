import os
import pandas as pd
from sqlalchemy import create_engine, text
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv

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

# HELPER: Find column safely
def find_column(available_columns, targets):
    # 1. Exact match
    for target in targets:
        if target in available_columns:
            return f'"{target}"'
    # 2. Case-insensitive match
    for target in targets:
        for col in available_columns:
            if target.lower() == col.lower():
                return f'"{col}"'
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

        # Convert NaNs to None
        df = df.where(pd.notnull(df), None)
        return {"count": len(df), "data": df.to_dict(orient="records")}
        
    except Exception as e:
        print(f"❌ API CRASHED: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/stats")
def get_stats():
    try:
        engine = get_db_engine()
        
        # 1. Get available columns
        cols_df = pd.read_sql("SELECT * FROM applications LIMIT 0", engine)
        all_cols = list(cols_df.columns)
        
        # 2. Find Country Column
        country_col = find_column(all_cols, ["Country", "country", "location", "Location"])
        
        # 3. Find Funding Column (Strict List)
        raised_col = find_column(all_cols, [
            "total_raised_usd", 
            "total_raised", 
            "latest_fundraise_usd",
            "Fundraise Amount ($)"
        ])
        
        # Fallbacks if columns are missing
        if not country_col: 
            # If we can't find country, we can't count countries, so we use a dummy string
            country_sql = "'Unknown'" 
        else:
            country_sql = country_col

        if not raised_col:
            # If we can't find the money column, return 0 instead of crashing
            raised_sql = "0"
        else:
            # CLEANING LOGIC:
            # 1. Cast to TEXT (in case it's already numeric)
            # 2. REGEXP_REPLACE: Remove anything that is NOT a digit or a dot (removes $ , letters)
            # 3. NULLIF: If the result is empty string, make it NULL
            # 4. CAST: Finally turn it into a Number
            raised_sql = f"CAST(NULLIF(REGEXP_REPLACE(CAST({raised_col} AS TEXT), '[^0-9.]', '', 'g'), '') AS NUMERIC)"

        # 4. EXECUTE QUERY
        query = f"""
            SELECT 
                COUNT(*) as total_apps,
                COUNT(DISTINCT {country_sql}) as total_countries,
                SUM({raised_sql}) as total_raised
            FROM applications
        """
        
        with engine.connect() as conn:
            result = conn.execute(text(query)).mappings().first()
            
        return dict(result)

    except Exception as e:
        print(f"❌ STATS ERROR: {e}")
        # Return a safe error response instead of a 500 crash
        return {
            "total_apps": 0,
            "total_countries": 0,
            "total_raised": 0,
            "error_details": str(e)
        }