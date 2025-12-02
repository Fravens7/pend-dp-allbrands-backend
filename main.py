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

# --- MODIFICACIÓN EN LA FUNCIÓN DE PROCESAMIENTO ---
def get_consolidated_data():
    global CACHE_STORAGE
    
    current_time = time.time()
    if CACHE_STORAGE["data"] is not None:
        age = current_time - CACHE_STORAGE["timestamp"]
        if age < CACHE_DURATION_SECONDS:
            print(f"⚡ SERVING FROM CACHE ({age:.1f}s old)")
            return CACHE_STORAGE["data"]

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
                
                # --- NUEVO: CONVERTIR DATE POSTED (UNIX) A FECHA LEGIBLE ---
                # Si existe la columna 'DATE POSTED' y es numérica
                if 'DATE POSTED' in df.columns:
                    df['DATE_POSTED_READABLE'] = pd.to_datetime(df['DATE POSTED'], unit='s', errors='coerce').dt.strftime('%Y-%m-%d %H:%M:%S')
                
                all_data.append(df)
        except Exception:
            pass 

    if not all_data:
        return pd.DataFrame() 

    df_consolidated = pd.concat(all_data, ignore_index=True)

    # Limpieza de DEPOSIT DATE (Para el filtro actual)
    df_consolidated['DEPOSIT DATE RAW'] = df_consolidated['DEPOSIT DATE'] # Guardamos la original para verla
    df_consolidated['DEPOSIT DATE'] = df_consolidated['DEPOSIT DATE'].apply(clean_date_ordinal)
    df_consolidated['DEPOSIT DATE'] = pd.to_datetime(
        df_consolidated['DEPOSIT DATE'], 
        format='%A, %B %d %Y, %I:%M:%S %p', 
        errors='coerce'
    )
    
    # IMPORTANTE: Por ahora seguimos filtrando por DEPOSIT DATE, 
    # pero ya tenemos la otra fecha lista para comparar.
    df_consolidated.dropna(subset=['DEPOSIT DATE'], inplace=True)

    seven_days_ago = datetime.now() - timedelta(days=7)
    df_filtered = df_consolidated[df_consolidated['DEPOSIT DATE'] >= seven_days_ago].copy()
    
    CACHE_STORAGE["data"] = df_filtered
    CACHE_STORAGE["timestamp"] = current_time
    return df_filtered

@app.get("/api/v1/dashboard")
def dashboard_data():
    try:
        df = get_consolidated_data()
        total_records = len(df)
        
        # KPIs y Charts (Igual que antes...)
        total_escalations = total_records
        
        if df.empty:
            max_age_days = 0
        else:
            oldest_date = df['DEPOSIT DATE'].min()
            time_difference = datetime.now() - oldest_date
            max_age_days = round(time_difference.total_seconds() / 86400, 1)
        
        daily_trend = df.groupby(df['DEPOSIT DATE'].dt.date)['DEPOSIT ID'].count().reset_index()
        daily_trend.columns = ['date', 'count']
        daily_trend['date'] = daily_trend['date'].astype(str)
        daily_trend_json = daily_trend.to_dict('records')
        
        brand_count = df.groupby('BRAND')['DEPOSIT ID'].count().reset_index()
        brand_count.columns = ['brand', 'count']
        brand_count_json = brand_count.to_dict('records')
        
        # --- AQUÍ ESTÁ EL CAMBIO PARA VER TODO ---
        # Enviamos TODAS las columnas, convertimos las fechas a string para que no falle el JSON
        df_detail = df.sort_values(by='DEPOSIT DATE', ascending=False)
        
        # Convertimos la fecha de objeto a string
        df_detail['DEPOSIT DATE'] = df_detail['DEPOSIT DATE'].dt.strftime('%Y-%m-%d %H:%M:%S')
        
        # .fillna('') reemplaza los valores vacíos/NaN por texto vacío para no romper el frontend
        detailed_records = df_detail.fillna('').to_dict('records')

        response_data = {
            "kpis": { "total_escalations": int(total_escalations), "max_age_days": max_age_days },
            "charts": { "daily_trend": daily_trend_json, "brand_distribution": brand_count_json },
            "detailed_records": detailed_records # Ahora lleva TODAS las columnas
        }
        
        return response_data

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Unexpected Error: {e}")