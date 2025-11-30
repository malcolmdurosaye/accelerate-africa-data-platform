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

# 2. CORS (CRITICAL: Allows Frontend to talk to this API)
# In production, replace ["*"] with the specific URL of your frontend (e.g., ["https://my-dashboard.com"])
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allows all origins
    allow_credentials=True,
    allow_methods=["*"],  # Allows all methods
    allow_headers=["*"],  # Allows all headers
)

# 3. DATABASE CONNECTION
def get_db_engine():
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        raise ValueError("DATABASE_URL is not set")
    # Fix Render URL quirk
    if db_url.startswith("postgres://"):
        db_url = db_url.replace("postgres://", "postgresql://", 1)
    return create_engine(db_url)

# 4. ENDPOINTS

@app.get("/")
def read_root():
    return {"status": "online", "message": "Accelerate Africa API is running"}

@app.get("/api/applications")
def get_applications(limit: int = 1000):
    """
    Returns a list of applications.
    Query Param 'limit' defaults to 1000 records.
    """
    try:
        engine = get_db_engine()
        # We limit columns to avoid sending sensitive PII (like phone numbers) if not needed
        # Modify this SQL to select exactly what the frontend needs
        query = f"""
            SELECT 
                airtable_id,
                "Cohort",
                applicant_name,
                startup_name,
                startup_website_url,
                country,
                industry,
                product_description,
                application_status,
                monthly_revenue_usd,
                total_raised_usd,
                "created_at"
            FROM applications 
            LIMIT {limit}
        """
        
        # Use Pandas to fetch and convert to JSON-friendly dict
        df = pd.read_sql(query, engine)
        
        # Convert NaN (empty values) to None so JSON doesn't break
        df = df.where(pd.notnull(df), None)
        
        return {
            "count": len(df),
            "data": df.to_dict(orient="records")
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/stats")
def get_stats():
    """Returns high-level statistics for dashboard widgets"""
    try:
        engine = get_db_engine()
        
        # Example of a fast aggregate query
        query = """
            SELECT 
                COUNT(*) as total_apps,
                COUNT(DISTINCT country) as total_countries,
                SUM(CAST(total_raised_usd AS NUMERIC)) as total_raised
            FROM applications
        """
        with engine.connect() as conn:
            result = conn.execute(text(query)).mappings().first()
            
        return dict(result)

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))