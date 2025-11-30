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

# HELPER: Find the real column name dynamically
def find_column(available_columns, targets, search_term=None):
    """
    1. Checks if any 'target' exists exactly in available_columns.
    2. If not, searches for 'search_term' inside available_columns.
    3. Returns the first match or None.
    """
    # Exact match check
    for target in targets:
        if target in available_columns:
            return f'"{target}"' # Return quoted for SQL safety
            
    # Fuzzy search check (case insensitive)
    if search_term:
        for col in available_columns:
            if search_term.lower() in col.lower():
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
        
        # Safer Approach: Select * to get whatever columns exist
        query = f'SELECT * FROM applications LIMIT {limit}'
        df = pd.read_sql(query, engine)
        
        if df.empty:
            return {"count": 0, "data": []}

        # Convert NaNs to None
        df = df.where(pd.notnull(df), None)
        
        return {
            "count": len(df),
            "data": df.to_dict(orient="records")
        }
        
    except Exception as e:
        print(f"❌ API CRASHED: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/stats")
def get_stats():
    try:
        engine = get_db_engine()
        
        # 1. Inspect Database Columns first
        # We fetch 0 rows just to see the column names
        cols_df = pd.read_sql("SELECT * FROM applications LIMIT 0", engine)
        all_cols = list(cols_df.columns)
        
        # 2. Dynamically find the right columns
        # Country: Look for "Country" or "country" or "Location"
        country_col = find_column(all_cols, ["Country", "country", "location"])
        
        # Total Raised: Look for "total_raised_usd", "total_raised", or anything with "raised"
        raised_col = find_column(all_cols, ["total_raised_usd", "total_raised", "Total Raised"], search_term="raised")
        
        if not country_col or not raised_col:
            # Fallback for debugging if we can't find them
            return {
                "error": "Could not identify columns", 
                "available_columns": all_cols
            }

        # 3. Construct the query using the found names
        query = f"""
            SELECT 
                COUNT(*) as total_apps,
                COUNT(DISTINCT {country_col}) as total_countries,
                SUM(CAST({raised_col} AS NUMERIC)) as total_raised
            FROM applications
        """
        
        with engine.connect() as conn:
            result = conn.execute(text(query)).mappings().first()
            
        return dict(result)

    except Exception as e:
        print(f"❌ STATS ERROR: {e}")
        raise HTTPException(status_code=500, detail=f"Stats Error: {str(e)}")