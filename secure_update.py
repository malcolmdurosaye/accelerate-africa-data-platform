import os
import requests
import pandas as pd
from sqlalchemy import create_engine
import json
from dotenv import load_dotenv
import re

# Load environment variables
load_dotenv()

API_KEY = os.getenv("AIRTABLE_API_KEY")
# Fix for Render's database URL format if needed
DB_URL = os.getenv("DATABASE_URL")
if DB_URL and DB_URL.startswith("postgres://"):
    DB_URL = DB_URL.replace("postgres://", "postgresql://", 1)

BASE_ID = "appfVXnWdI41HoytO"
TABLES = [
    "AA3 Application Responses_closed",
    "AA2 Application Responses_closed",
    "AA1 Application Responses_closed",
    "AA4 Application Responses",
    # "AA0 Application Responses_closed"
]

# MAP: Long Question -> Short DB Column
RENAME_MAP = {
    # --- Contact & Bio ---
    "Email Address": "contact_email",
    "What's your email?": "applicant_email", 
    "What's your email address?": "applicant_email",
    "What's your full name?": "applicant_name",
    "What's your phone number?": "phone_number", 
    "What's your location?": "location",
    "Where are you based?": "location",
    "Where's your startup headquartered?": "startup_hq",
    "What's your gender?": "gender",
    "What's your gender": "gender",
    
    # --- Startup Info ---
    "Which theme is your startup most aligned with?": "theme_primary",
    "What's the name of your startup?": "startup_name",
    "What is your company making or going to make?": "product_description",
    "What is your company making / going to make? ": "product_description",
    "Describe what your startup does in 50 words or less.": "product_description",
    "What's the URL of your demo video (1-2 minutes), if you have one?": "product_demo",
    "What's your startup's website URL?": "startup_website_url",
    "What's your startup's founding date?": "founding_date",
    
    # --- Docs ---
    "Please attach your current cap table as it exists today": "cap_table_link",
    "Please upload a copy of your startup's pitch deck": "pitchdeck_link",
    "Please upload a copy of your startup's pitch deck?": "pitchdeck_link",
    
    # --- Team ---
    "How many founders does your startup have?": "num_founders",
    "How many female founders does your company have, if any?": "num_female_founders",
    
    # FIX: Renaming Co-founder details to avoid length crash
    "For each co-founder, please list out their title, email, location, nationality, and LinkedIn URL": "cofounders_details",
    "For each co-founder, please list out their title, email, location, and nationality.": "cofounders_details",
    
    # --- Financials ---
    "What is your revenue in USD for each of the past 6 months?": "monthly_revenue_usd",
    "How much money do you spend per month?": "monthly_expenses_usd",
    "How much money does your startup spend per month?": "monthly_expenses_usd",
    "How long is your runway (months)?": "runway_months",
    "How long is your runway?": "runway_months",
    "Runway (Months)": "runway_months",
    "How much money have you raised from investors, including friends and family, in total in US Dollars?": "total_raised_usd",
    "Fundraise Amount ($)": "latest_fundraise_usd",
    
    # --- Status ---
    "Status": "application_status",
    "Application Status": "application_status",
    
    # --- FIX: The specific column causing your current error ---
    "If you have already participated or committed to participate in an incubator, accelerator, or pre-accelerator program, please tell us about it.": "prior_accelerators",
    "If you have already participated or committed to participate in an incubator, accelerator, or pre-accelerator program, please tell us about it/them.": "prior_accelerators"
}

def clean_column_name(col_name):
    """
    Ensures column names are safe for Postgres:
    1. Truncates to 60 chars
    2. Removes special characters
    """
    # If it's already in our nice map, return that
    if col_name in RENAME_MAP:
        return RENAME_MAP[col_name]
        
    # Otherwise, clean it automatically
    clean = str(col_name).strip()
    # Replace non-alphanumeric with underscore
    clean = re.sub(r'[^a-zA-Z0-9]', '_', clean)
    # Truncate to 60 chars to prevent Postgres crash
    return clean[:60]

def fetch_and_save():
    if not API_KEY:
        print("❌ ERROR: API Key missing. Check .env file.")
        return

    print("🔒 Authenticating...")
    all_records = []
    headers = {"Authorization": f"Bearer {API_KEY}"}
    
    for table in TABLES:
        print(f"   Fetching {table}...")
        url = f"https://api.airtable.com/v0/{BASE_ID}/{table}"
        offset = None
        while True:
            params = {"offset": offset} if offset else {}
            try:
                resp = requests.get(url, headers=headers, params=params)
                if resp.status_code == 404:
                    print(f"   ⚠️ Warning: Table '{table}' skipped (Not Found/Perms).")
                    break
                if resp.status_code != 200:
                    print(f"   ⚠️ Error {resp.status_code}: {resp.text}")
                    break
                    
                data = resp.json()
                records = data.get("records", [])
                for rec in records:
                    fields = rec.get("fields", {})
                    fields["airtable_id"] = rec.get("id")
                    fields["created_at"] = rec.get("createdTime")
                    fields["Cohort"] = table.split()[0]
                    all_records.append(fields)
                
                offset = data.get("offset")
                if not offset: break
            except Exception as e:
                print(f"   ⚠️ Connection Exception: {e}")
                break
    
    if not all_records:
        print("❌ No records found.")
        return

    df = pd.DataFrame(all_records)
    
    print("🧹 Cleaning column names...")
    
    # 1. Apply the Manual Map
    df.rename(columns=RENAME_MAP, inplace=True)
    
    # 2. Auto-Truncate ANY remaining long columns (Safety Net)
    # This prevents the script from ever crashing due to column length again
    new_columns = {}
    for col in df.columns:
        if len(col) > 60:
            new_name = clean_column_name(col)
            new_columns[col] = new_name
    
    if new_columns:
        df.rename(columns=new_columns, inplace=True)

    # 3. Handle Duplicate Columns (e.g. if two questions mapped to 'prior_accelerators')
    # We group by column name and take the first non-null value
    df = df.groupby(lambda x: x, axis=1).first()

    # 4. Convert lists/dicts to strings for SQL
    for col in df.columns:
        if df[col].dtype == object:
            df[col] = df[col].apply(lambda x: json.dumps(x) if isinstance(x, (list, dict)) else str(x) if x else None)

    print(f"💾 Saving {len(df)} records to database...")
    try:
        engine = create_engine(DB_URL)
        df.to_sql('applications', engine, if_exists='replace', index=False)
        print("✅ Success! Database Updated.")
    except Exception as e:
        print(f"❌ Database Error: {e}")

if __name__ == "__main__":
    fetch_and_save()