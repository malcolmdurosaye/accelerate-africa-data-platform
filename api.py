import os
import uuid
import pandas as pd
from sqlalchemy import create_engine, text
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional
from dotenv import load_dotenv

# 1. SETUP
load_dotenv()

app = FastAPI(
    title="Accelerate Africa Data API",
    description="Full CRUD API aligned with DB Standard Field Names.",
    version="4.0"
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

# 4. DATA MODEL (Must match the RENAME_MAP in secure_update.py)
class ApplicationModel(BaseModel):
    # These fields match the "DB standard field names" from your document
    applicant_name: Optional[str] = None
    applicant_email: Optional[str] = None
    startup_name: Optional[str] = None
    startup_website_url: Optional[str] = None
    Country: Optional[str] = None
    industry_name: Optional[str] = None
    product_description: Optional[str] = None
    application_status: Optional[str] = "Applied"
    monthly_revenue_usd: Optional[float] = 0.0
    monthly_expenses_usd: Optional[float] = 0.0
    total_raised_usd: Optional[float] = 0.0
    founding_date: Optional[str] = None
    Cohort: Optional[str] = "Manual Entry"

# 5. ENDPOINTS

@app.get("/")
def read_root():
    return {"status": "online", "message": "Accelerate Africa API is running"}

# --- READ ALL ---
@app.get("/api/applications")
def get_applications(limit: int = 1000):
    try:
        engine = get_db_engine()
        # Query using the NEW Standard Names
        query = f"""
            SELECT 
                "SN", "Cohort", 
                applicant_name, applicant_email, 
                startup_name, startup_website_url, "Country", 
                industry_name, product_description, 
                monthly_revenue_usd, total_raised_usd, 
                application_status, founding_date, created_at, airtable_id
            FROM applications 
            LIMIT {limit}
        """
        df = pd.read_sql(query, engine)
        if df.empty: return {"count": 0, "data": []}
        df = df.where(pd.notnull(df), None)
        return {"count": len(df), "data": df.to_dict(orient="records")}
    except Exception as e:
        print(f"❌ READ ERROR: {e}")
        # Fallback query if specific columns fail
        try:
            engine = get_db_engine()
            df = pd.read_sql(f"SELECT * FROM applications LIMIT {limit}", engine)
            df = df.where(pd.notnull(df), None)
            return {"count": len(df), "data": df.to_dict(orient="records")}
        except Exception as e2:
            raise HTTPException(status_code=500, detail=str(e2))

# --- READ SINGLE ---
@app.get("/api/applications/{app_id}")
def get_single_application(app_id: str):
    try:
        engine = get_db_engine()
        query = text('SELECT * FROM applications WHERE airtable_id = :id')
        with engine.connect() as conn:
            result = conn.execute(query, {"id": app_id}).mappings().first()
        if not result:
            raise HTTPException(status_code=404, detail="Application not found")
        return dict(result)
    except HTTPException as he:
        raise he
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# --- CREATE (POST) ---
@app.post("/api/applications", status_code=201)
def create_application(app_data: ApplicationModel):
    try:
        engine = get_db_engine()
        
        # 1. Generate ID (Starts with 'recManual' so secure_update.py knows to rescue it)
        new_id = f"recManual{uuid.uuid4().hex[:10]}"
        
        # 2. Insert using Standard Column Names
        query = text("""
            INSERT INTO applications (
                airtable_id, applicant_name, applicant_email,
                startup_name, startup_website_url, "Country", 
                industry_name, product_description, application_status, 
                monthly_revenue_usd, monthly_expenses_usd, total_raised_usd, 
                founding_date, "Cohort", created_at
            ) VALUES (
                :id, :name, :email,
                :startup, :web, :country, 
                :ind, :desc, :status, 
                :rev, :exp, :raised, 
                :founded, :cohort, NOW()
            )
        """)
        
        params = {
            "id": new_id,
            "name": app_data.applicant_name,
            "email": app_data.applicant_email,
            "startup": app_data.startup_name,
            "web": app_data.startup_website_url,
            "country": app_data.Country,
            "ind": app_data.industry_name,
            "desc": app_data.product_description,
            "status": app_data.application_status,
            "rev": app_data.monthly_revenue_usd,
            "exp": app_data.monthly_expenses_usd,
            "raised": app_data.total_raised_usd,
            "founded": app_data.founding_date,
            "cohort": app_data.Cohort
        }
        
        with engine.begin() as conn:
            conn.execute(query, params)
            
        return {"message": "Created successfully", "id": new_id, "data": app_data}
        
    except Exception as e:
        print(f"❌ CREATE ERROR: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# --- UPDATE (PUT) ---
@app.put("/api/applications/{app_id}")
def update_application(app_id: str, app_data: ApplicationModel):
    try:
        engine = get_db_engine()
        
        query = text("""
            UPDATE applications SET
                applicant_name = :name,
                applicant_email = :email,
                startup_name = :startup,
                startup_website_url = :web,
                "Country" = :country,
                industry_name = :ind,
                product_description = :desc,
                application_status = :status,
                monthly_revenue_usd = :rev,
                monthly_expenses_usd = :exp,
                total_raised_usd = :raised,
                founding_date = :founded,
                "Cohort" = :cohort
            WHERE airtable_id = :id
        """)
        
        params = {
            "id": app_id,
            "name": app_data.applicant_name,
            "email": app_data.applicant_email,
            "startup": app_data.startup_name,
            "web": app_data.startup_website_url,
            "country": app_data.Country,
            "ind": app_data.industry_name,
            "desc": app_data.product_description,
            "status": app_data.application_status,
            "rev": app_data.monthly_revenue_usd,
            "exp": app_data.monthly_expenses_usd,
            "raised": app_data.total_raised_usd,
            "founded": app_data.founding_date,
            "cohort": app_data.Cohort
        }
        
        with engine.begin() as conn:
            result = conn.execute(query, params)
            
        if result.rowcount == 0:
            raise HTTPException(status_code=404, detail="Application not found or no changes made")
            
        return {"message": "Updated successfully", "id": app_id}
        
    except HTTPException as he:
        raise he
    except Exception as e:
        print(f"❌ UPDATE ERROR: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# --- DELETE ---
@app.delete("/api/applications/{app_id}")
def delete_application(app_id: str):
    try:
        engine = get_db_engine()
        query = text('DELETE FROM applications WHERE airtable_id = :id')
        with engine.begin() as conn:
            result = conn.execute(query, {"id": app_id})
        if result.rowcount == 0:
            raise HTTPException(status_code=404, detail="Application not found")
        return {"message": "Deleted successfully", "id": app_id}
    except HTTPException as he:
        raise he
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# --- STATS ---
@app.get("/api/stats")
def get_stats():
    try:
        engine = get_db_engine()
        # Updated to use Standard Columns: "Country" and "total_raised_usd"
        query = """
            SELECT 
                COUNT(*) as total_apps,
                COUNT(DISTINCT "Country") as total_countries,
                SUM(
                    CAST(NULLIF(REGEXP_REPLACE(CAST(total_raised_usd AS TEXT), '[^0-9.]', '', 'g'), '') AS NUMERIC)
                ) as total_raised
            FROM applications
        """
        with engine.connect() as conn:
            result = conn.execute(text(query)).mappings().first()
        return dict(result)
    except Exception as e:
        print(f"❌ STATS ERROR: {e}")
        return {"total_apps": 0, "total_countries": 0, "total_raised": 0, "error": str(e)}