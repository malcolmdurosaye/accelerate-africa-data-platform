# Accelerate Africa Dashboard (Secure Mode)

## Setup
1. Open the `.env` file and paste your new Airtable API Key (Starts with `pat...`).
2. Install requirements:
   `pip install -r requirements.txt`

## How to use
1. **Update Data:** Run this whenever you want to pull fresh data from Airtable.
   `python secure_update.py`
   *(This creates/updates the 'accelerate_africa.db' file)*

2. **Run Dashboard:**
   `python app.py`
   *(Open http://127.0.0.1:8050 in your browser)*
