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

# MASTER RENAMING MAP (Combined from your provided document)
RENAME_MAP = {
    # --- IDS & DATES ---
    "airtable_record_id": "airtable_id",
    "created_time": "application_date",
    "Timestamp": "submitted_at",
    "Created": "created_at",
    "Id": "SN",

    # --- APPLICANT DETAILS ---
    "Question": "applicant_name", # Maps Question -> applicant_name (Standardized)
    "What's your full name?": "applicant_name",
    "What's your email?": "applicant_email", 
    "What's your email address?": "applicant_email",
    "Email Address": "contact_email",
    "What's your job title?": "applicant_role",
    "What's your location?": "location",
    "Where are you based?": "location",
    "What's your phone number?": "phone_number",
    "What's your gender?": "gender",
    "What's your gender": "gender",
    "What's your ethnicity?": "ethnicity",
    "If you answered 'non-African' above, please describe your connection to Africa.": "non_african_connection",

    # --- STARTUP INFO ---
    "Select": "theme_primary",
    "Which theme is your startup most aligned with?": "theme_primary",
    "What's the name of your startup?": "startup_name",
    "What's your startup's website URL?": "startup_website_url",
    "What's your startup's founding date?": "founding_date",
    "Where's your startup headquartered?": "stratup_hq",
    "Describe what your startup does in 50 words or less.": "startup_description",
    "What is your company making / going to make? ": "product_description",
    "What is your company making or going to make?": "product_description",
    "What's the URL of your demo video (1-2 minutes), if you have one?": "product_demo",
    "What's the URL of your demo video (1-2 minutes), if you have one": "product_demo",
    "Please upload a copy of your startup's pitch deck": "pitchdeck_url",
    "Please upload a copy of your startup's pitch deck?": "pitchdeck_url",
    "Industry": "industry_name",
    "If you selected 'Other' above, which of these sectors is your startup operating in?": "sector_tags",

    # --- TEAM ---
    "How many founders does your startup have?": "num_founders",
    "How many female founders does your company have, if any?": "num_female_founders",
    "For each co-founder, please list out their title, email, location, and nationality.": "cofounders",
    "For each co-founder, please list out their title, email, location, nationality, and LinkedIn URL": "cofounders",
    "How long have the founders known one another, and how did you meet? Have any of the founders not met in person?": "founders_relationship",
    "How many of the founders are full-time versus part-time?": "founders_fulltime_vs_parttime_count",
    "How long has each founder been working on this? How much of that has been full-time? Please explain.": "founder_tenure",
    "Who writes code, or does other technical work on your product? Was any of it done by a non-founder?": "technical_contributors",
    "Please enter the URL of a one-minute unlisted (not private) YouTube video introducing the founder(s).": "intro_video_url",
    "Tell us about a conflict you have had in the founding team and how it was resolved?": "founding_conflict_description",
    "What gives you confidence that your founding team is uniquely positioned or qualified to solve this particular problem?": "team_competency_statement",
    "Please tell us about an interesting project or activity, preferably outside of class or work, that two or more of you took part in together. Include URLs if possible.": "shared_project_description",

    # --- BUSINESS & STRATEGY ---
    "How many active users or customers do you have? ": "active_users",
    "Are people actively using your product now?": "has_active_users",
    "What is your revenue in USD for each of the past 6 months?": "monthly_revenue_usd",
    "How much money does your startup spend per month?": "monthly_expenses_usd",
    "How much money do you spend per month?": "monthly_expenses_usd",
    "How long is your runway?": "runway_months",
    "Runway (Months)": "runway_months",
    "How long is your runway (months)?": "runway_months",
    "If you have already participated or committed to participate in an incubator, accelerator, or pre-accelerator program, please tell us about it/them.": "prior_programs",
    "If you have already participated or committed to participate in an incubator, accelerator, or pre-accelerator program, please tell us about it.": "prior_programs",
    "What problem are you solving? ": "problem_statement",
    "What problem are you solving? Why this problem? For who?": "problem_statement",
    "What are others doing to solve this problem today? ": "current_solution_landscape",
    "What are people doing to solve this problem today without your product/service?": "current_solution_landscape",
    "Why now? ": "why_now",
    "What type of customer(s) are you solving this problem for?": "customer_segment",
    "Who is your ideal customer?": "customer_segment",
    "What’s your customer acquisition strategy? ": "customer_acquisition_strategy",
    "Who is your dream customer? ": "dream_customer",
    "What does your company look like in 5-10 years? How big can it get? ": "vision_5_10y",
    "How do you or will you make money? ": "revenue_model",
    "Why did you pick this idea to work on? ": "motivation_for_idea",
    "Why are you trying to solve this problem? ": "motivation_for_idea",
    "Who are your competitors? ": "competitors",
    "Who are your competitors both locally and globally? What do you understand about your area(s) of business that they don't?": "competitors",
    "If you track metrics around user engagement and retention, what are they? ": "engagement_metrics",
    "Please describe the breakdown of the equity ownership in percentages among the founders, employees, and any other stockholders. ": "equity_breakdown",
    "Do you have domain expertise in this area? How do you know people need what you're building?": "evidence_of_demand",
    "Is this problem primarily prevalent in Africa? If not, where else? ": "problem_geography",
    "What has changed about the market that makes your solution possible today? ": "market_triggers",

    # --- FUNDING & LEGAL ---
    "Have you fundraised before? When was it? And at what post-money valuation was your latest fundraise? ": "prior_fundraise",
    "How much money have you raised from investors, including friends and family, in total in US Dollars? ": "total_raised_usd",
    "List any investments your company has received. ": "investments",
    "Fundraise Amount ($)": "latest_fundraise_usd",
    "Revenue Generating": "is_revenue_generating",
    "Revenue Generating?": "is_revenue_generating",
    "Have you formed any local and international (e.g. Delaware) entities? ": "legal_entities_formed",
    "If you answered 'Yes' above, please list the legal name of your startup.": "legal_entity_names",
    "Where are you incorporated? ": "where_incorporated",
    "Please attach your current cap table as it exists today": "cap_table_attachments",
    "If you have a historic and/or forward looking financial model showing your revenues and costs, please attach it here": "financial_model_attachments",

    # --- METADATA & MISC ---
    "Why did you apply to Accelerate Africa? ": "reason_applied",
    "How did you hear about Accelerate Africa? ": "referral_channel",
    "Country": "Country",
    "Do you know any founders who have been through Accelerate Africa or Future Africa? ": "knows_accelerator_alumni",
    "Please share the name of the Future Africa team member, Accelerate Africa Cohort 1 participant, or program partner who referred you": "referrer_name",
    "If you selected 'Other', please explain": "referral_channel_explain",
    "If you answered 'Other' above, please explain": "other_explanation",
    "By completing this form, you acknowledge the below:  ": "acknowledged_terms",
    "How much could your startup make? ": "potential_revenue_usd",
    "How many are paying?": "paying_users",
    "Is this your first company that you have (co-)founded? If not, what company did you found before?": "is_first_time_founder",
    "Is this your first company that you have (co-)founded?": "is_first_time_founder",
    "If you answered 'Yes' above, what company did you found before?": "prior_company_status",
    "Have you pitched to anyone at Future Africa before? ": "has_pitched_to_future_africa_before",
    "Status": "application_status",
    "Note for Screen out": "screen_out_note"
}

def clean_column_name(col_name):
    """
    Ensures column names are safe for Postgres.
    """
    if col_name in RENAME_MAP:
        return RENAME_MAP[col_name]
    
    # Auto-clean unmapped columns to prevent crashes
    clean = str(col_name).strip()
    clean = re.sub(r'[^a-zA-Z0-9]', '_', clean)
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
                    print(f"   ⚠️ Warning: Table '{table}' skipped (Not Found).")
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
    
    # 1. Apply the Master Rename Map
    df.rename(columns=RENAME_MAP, inplace=True)
    
    # 2. Auto-Truncate any remaining long columns to prevent DB Crash
    new_columns = {}
    for col in df.columns:
        if len(col) > 60:
            new_columns[col] = clean_column_name(col)
    if new_columns:
        df.rename(columns=new_columns, inplace=True)

    # 3. Handle Duplicate Columns (e.g., if two questions map to 'prior_programs')
    # We group by column name and keep the first non-null value
    df = df.groupby(lambda x: x, axis=1).first()

    # 4. Convert lists/dicts to strings for SQL
    for col in df.columns:
        if df[col].dtype == object:
            df[col] = df[col].apply(lambda x: json.dumps(x) if isinstance(x, (list, dict)) else str(x) if x else None)

    print(f"💾 Saving {len(df)} records to database...")
    try:
        engine = create_engine(DB_URL)
        df.to_sql('applications', engine, if_exists='replace', index=False)
        print("✅ Success! Database Updated with Standard Names.")
    except Exception as e:
        print(f"❌ Database Error: {e}")

if __name__ == "__main__":
    fetch_and_save()