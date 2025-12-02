from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import gspread
import pandas as pd
from datetime import datetime, timedelta
import re
import os
import time # Imported for caching

# --- CONFIGURATION ---
app = FastAPI(title="Deposit Dashboard API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], 
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

SPREADSHEET_ID = "1UetwCMeOrrelOW9S6nWGAS4XiteYNtmgT7Upo19lqw4"
SHEET_NAMES = ['M1', 'M2', 'B1', 'B2', 'K1', 'B3', 'B4']
CREDENTIALS_FILE = "credentials.json"

# --- CACHE CONFIGURATION ---
# We will store the data here.
# Structure: { "data": DataFrame, "timestamp": float }
CACHE_STORAGE = {"data": None, "timestamp": 0}
CACHE_DURATION_SECONDS = 600  # 10 Minutes Cache

# --- HELPER FUNCTIONS ---
def clean_date_ordinal(date_str):
    if isinstance(date_str, str):
        return re.sub(r'(\d+)(st|nd|rd|th)', r'\1', date_str)
    return date_str

def get_consolidated_data():
    global CACHE_STORAGE
    
    # 1. CHECK CACHE
    current_time = time.time()
    if CACHE_STORAGE["data"] is not None:
        age = current_time - CACHE_STORAGE["timestamp"]
        if age < CACHE_DURATION_SECONDS:
            print(f"⚡ SERVING FROM CACHE ({age:.1f}s old)")
            return CACHE_STORAGE["data"]

    # 2. FETCH FROM GOOGLE (If cache is empty or expired)
    print("⏳ FETCHING NEW DATA FROM GOOGLE SHEETS...")
    if not os.path.exists(CREDENTIALS_FILE):
        raise FileNotFoundError("Critical Error: credentials.json not found.")

    try:
        gc = gspread.service_account(filename=CREDENTIALS_FILE)
        spreadsheet = gc.open_by_key(SPREADSHEET_ID)
    except Exception as e:
        raise ConnectionError(f"Google Connection Error: {e}")

    all_data = []
    
    for sheet_name in SHEET_NAMES:
        try:
            worksheet = spreadsheet.worksheet(sheet_name)
            data = worksheet.get_all_records()
            df = pd.DataFrame(data)
            if not df.empty:
                df['BRAND'] = sheet_name
                all_data.append(df)
        except Exception:
            pass 

    if not all_data:
        return pd.DataFrame() 

    df_consolidated = pd.concat(all_data, ignore_index=True)

    # Date Cleaning
    df_consolidated['DEPOSIT DATE'] = df_consolidated['DEPOSIT DATE'].apply(clean_date_ordinal)
    df_consolidated['DEPOSIT DATE'] = pd.to_datetime(
        df_consolidated['DEPOSIT DATE'], 
        format='%A, %B %d %Y, %I:%M:%S %p', 
        errors='coerce'
    )
    df_consolidated.dropna(subset=['DEPOSIT DATE'], inplace=True)

    # Filter 7 Days
    seven_days_ago = datetime.now() - timedelta(days=7)
    df_filtered = df_consolidated[df_consolidated['DEPOSIT DATE'] >= seven_days_ago].copy()
    
    # 3. SAVE TO CACHE
    CACHE_STORAGE["data"] = df_filtered
    CACHE_STORAGE["timestamp"] = current_time
    print("✅ DATA CACHED SUCCESSFULLY")
    
    return df_filtered

# --- ENDPOINTS ---
@app.get("/")
def home():
    return {"status": "ok", "message": "Backend running with Cache System."}

@app.get("/api/v1/dashboard")
def dashboard_data():
    try:
        df = get_consolidated_data()
        total_records = len(df)
        
        # 1. KPI: Total
        total_escalations = total_records
        
        # 2. KPI: Max Age
        if df.empty:
            max_age_days = 0
        else:
            oldest_date = df['DEPOSIT DATE'].min()
            time_difference = datetime.now() - oldest_date
            max_age_days = round(time_difference.total_seconds() / 86400, 1)
        
        # 3. Chart: Daily Trend
        daily_trend = df.groupby(df['DEPOSIT DATE'].dt.date)['DEPOSIT ID'].count().reset_index()
        daily_trend.columns = ['date', 'count']
        daily_trend['date'] = daily_trend['date'].astype(str)
        daily_trend_json = daily_trend.to_dict('records')
        
        # 4. Chart: Brand Distribution
        brand_count = df.groupby('BRAND')['DEPOSIT ID'].count().reset_index()
        brand_count.columns = ['brand', 'count']
        brand_count_json = brand_count.to_dict('records')
        
        # 5. Detailed Table
        df_detail = df.sort_values(by='DEPOSIT DATE', ascending=False)
        detail_columns = ['DEPOSIT ID', 'DEPOSIT DATE', 'CUSTOMER NUMBER', 'AMOUNT', 'Status', 'BRAND']
        df_detail = df_detail[detail_columns].copy()
        df_detail['DEPOSIT DATE'] = df_detail['DEPOSIT DATE'].dt.strftime('%Y-%m-%d %H:%M:%S')
        
        detailed_records = df_detail.to_dict('records')

        response_data = {
            "kpis": {
                "total_escalations": int(total_escalations),
                "max_age_days": max_age_days,
                "data_range": 7
            },
            "charts": {
                "daily_trend": daily_trend_json,
                "brand_distribution": brand_count_json
            },
            "detailed_records": detailed_records
        }
        
        return response_data

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Unexpected Error: {e}")