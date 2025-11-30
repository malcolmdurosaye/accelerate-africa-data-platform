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
    version="2.0"
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
        
        # [cite_start]We now query specific STANDARD columns [cite: 1, 2, 4, 6, 8]
        # This prevents sending internal fields (like airtable_id) if you don't want to.
        query = f"""
            SELECT 
                "SN",
                "Cohort",
                applicant_name,
                startup_name,
                startup_website_url,
                "Country",
                industry_name,
                product_description,
                application_status,
                monthly_revenue_usd,
                total_raised_usd,
                founding_date,
                created_at
            FROM applications 
            LIMIT {limit}
        """
        
        df = pd.read_sql(query, engine)
        
        if df.empty:
            return {"count": 0, "data": []}

        # Convert NaNs to None for JSON
        df = df.where(pd.notnull(df), None)
        
        return {
            "count": len(df),
            "data": df.to_dict(orient="records")
        }
        
    except Exception as e:
        print(f"❌ API CRASHED: {e}")
        # Fallback: If specific columns fail, just grab everything
        try:
            engine = get_db_engine()
            df = pd.read_sql(f"SELECT * FROM applications LIMIT {limit}", engine)
            df = df.where(pd.notnull(df), None)
            return {"count": len(df), "data": df.to_dict(orient="records"), "warning": "Used fallback query"}
        except Exception as e2:
             raise HTTPException(status_code=500, detail=str(e2))

@app.get("/api/stats")
def get_stats():
    """
    [cite_start]Calculates stats using the DB Standard Field Names [cite: 1, 2, 4, 6, 8]
    """
    try:
        engine = get_db_engine()

        # We use the CLEAN column names directly now:
        # 'Country' -> Standardized in secure_update.py
        # 'total_raised_usd' -> Standardized in secure_update.py
        
        query = """
            SELECT 
                COUNT(*) as total_apps,
                COUNT(DISTINCT "Country") as total_countries,
                SUM(
                    -- Safe cleaning: Remove non-digits, treat empty as 0
                    CAST(NULLIF(REGEXP_REPLACE(CAST(total_raised_usd AS TEXT), '[^0-9.]', '', 'g'), '') AS NUMERIC)
                ) as total_raised
            FROM applications
        """
        
        with engine.connect() as conn:
            result = conn.execute(text(query)).mappings().first()
            
        return dict(result)

    except Exception as e:
        print(f"❌ STATS ERROR: {e}")
        # Return 0s so frontend doesn't break
        return {
            "total_apps": 0,
            "total_countries": 0,
            "total_raised": 0,
            "error_details": str(e)
        }