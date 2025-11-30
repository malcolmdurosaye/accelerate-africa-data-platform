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

# 4. ENDPOINTS

@app.get("/")
def read_root():
    return {"status": "online", "message": "Accelerate Africa API is running"}

@app.get("/api/applications")
def get_applications(limit: int = 1000):
    try:
        engine = get_db_engine()
        
        # FIX: We select ALL columns first to avoid "Column Not Found" errors.
        # This is safer than typing specific names that might be misspelled.
        query = f'SELECT * FROM applications LIMIT {limit}'
        
        df = pd.read_sql(query, engine)
        
        # Handle empty DB
        if df.empty:
            return {"count": 0, "data": []}

        # CLEANUP: Convert NaNs to None for JSON compatibility
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
        
        # FIX: We use double quotes around "Country" because Postgres is case-sensitive.
        # We also added a fallback for "Industry" just in case.
        query = """
            SELECT 
                COUNT(*) as total_apps,
                COUNT(DISTINCT "Country") as total_countries,
                SUM(CAST(total_raised_usd AS NUMERIC)) as total_raised
            FROM applications
        """
        
        with engine.connect() as conn:
            result = conn.execute(text(query)).mappings().first()
            
        return dict(result)

    except Exception as e:
        print(f"❌ STATS ERROR: {e}")
        
        # Fallback: If "Country" (Big C) fails, try "country" (small c)
        # This handles the case where your database might have different casing.
        try:
            engine = get_db_engine()
            query_fallback = """
                SELECT 
                    COUNT(*) as total_apps,
                    COUNT(DISTINCT country) as total_countries,
                    SUM(CAST(total_raised_usd AS NUMERIC)) as total_raised
                FROM applications
            """
            with engine.connect() as conn:
                result = conn.execute(text(query_fallback)).mappings().first()
            return dict(result)
        except:
            raise HTTPException(status_code=500, detail=f"Database Error: {str(e)}")