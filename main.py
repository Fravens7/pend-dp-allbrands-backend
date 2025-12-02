from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import gspread
import pandas as pd
from datetime import datetime, timedelta
import re
import os
import time

# --- CONFIGURACIÓN ---
app = FastAPI(title="Deposit Dashboard API - M1 ONLY")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], 
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

SPREADSHEET_ID = "1UetwCMeOrrelOW9S6nWGAS4XiteYNtmgT7Upo19lqw4"
# CAMBIO: Solo nos enfocamos en esta hoja
TARGET_SHEET = "M1" 
CREDENTIALS_FILE = "credentials.json"

# --- CACHE ---
CACHE_STORAGE = {"data": None, "timestamp": 0}
CACHE_DURATION_SECONDS = 300  # 5 Minutos es suficiente para pruebas

# --- FUNCIONES ---
def clean_date_ordinal(date_str):
    if isinstance(date_str, str):
        return re.sub(r'(\d+)(st|nd|rd|th)', r'\1', date_str)
    return date_str

def get_m1_data():
    global CACHE_STORAGE
    
    # 1. REVISAR CACHÉ
    current_time = time.time()
    if CACHE_STORAGE["data"] is not None:
        age = current_time - CACHE_STORAGE["timestamp"]
        if age < CACHE_DURATION_SECONDS:
            print(f"⚡ SERVING M1 FROM CACHE ({age:.1f}s old)")
            return CACHE_STORAGE["data"]

    print(f"⏳ FETCHING M1 DATA FROM GOOGLE SHEETS...")
    
    if not os.path.exists(CREDENTIALS_FILE):
        raise FileNotFoundError("Critical Error: credentials.json not found.")

    try:
        gc = gspread.service_account(filename=CREDENTIALS_FILE)
        spreadsheet = gc.open_by_key(SPREADSHEET_ID)
        worksheet = spreadsheet.worksheet(TARGET_SHEET)
    except Exception as e:
        raise ConnectionError(f"Google Connection Error: {e}")

    # Obtener datos
    data = worksheet.get_all_records()
    df = pd.DataFrame(data)

    if df.empty:
        return pd.DataFrame()

    df['BRAND'] = TARGET_SHEET

    # --- LA MAGIA: DESCUBRIR LA HORA REAL DE CREACIÓN ---
    # Convertimos el Timestamp Unix (ej: 1760539997) a fecha legible
    if 'DATE POSTED' in df.columns:
        # errors='coerce' pondrá NaT si el campo está vacío o no es un número
        df['SYSTEM_ENTRY_TIME'] = pd.to_datetime(df['DATE POSTED'], unit='s', errors='coerce')
        # Formateamos bonito para el frontend
        df['SYSTEM_ENTRY_TIME_STR'] = df['SYSTEM_ENTRY_TIME'].dt.strftime('%Y-%m-%d %H:%M:%S')
    else:
        df['SYSTEM_ENTRY_TIME_STR'] = "N/A"

    # --- LIMPIEZA DE FECHA DE DEPÓSITO (INPUT DEL USUARIO) ---
    df['USER_DEPOSIT_DATE'] = df['DEPOSIT DATE'].apply(clean_date_ordinal)
    df['USER_DEPOSIT_DATE'] = pd.to_datetime(
        df['USER_DEPOSIT_DATE'], 
        format='%A, %B %d %Y, %I:%M:%S %p', 
        errors='coerce'
    )

    # FILTRO: Usamos la fecha del usuario por ahora, como acordamos
    df.dropna(subset=['USER_DEPOSIT_DATE'], inplace=True)
    seven_days_ago = datetime.now() - timedelta(days=7)
    
    # Filtramos los últimos 7 días
    df_filtered = df[df['USER_DEPOSIT_DATE'] >= seven_days_ago].copy()
    
    # Guardar en Caché
    CACHE_STORAGE["data"] = df_filtered
    CACHE_STORAGE["timestamp"] = current_time
    
    return df_filtered

# --- ENDPOINTS ---
@app.get("/")
def home():
    return {"status": "ok", "message": "M1 Dashboard Backend Running"}

@app.get("/api/v1/dashboard")
def dashboard_data():
    try:
        df = get_m1_data()
        
        # Si no hay datos, retornamos estructura vacía segura
        if df.empty:
            return {
                "kpis": {"total_escalations": 0, "max_age_days": 0},
                "charts": {"daily_trend": [], "brand_distribution": []},
                "detailed_records": []
            }

        # 1. KPIs
        total_escalations = len(df)
        
        oldest_date = df['USER_DEPOSIT_DATE'].min()
        time_difference = datetime.now() - oldest_date
        max_age_days = round(time_difference.total_seconds() / 86400, 1)
        
        # 2. Charts
        daily_trend = df.groupby(df['USER_DEPOSIT_DATE'].dt.date)['DEPOSIT ID'].count().reset_index()
        daily_trend.columns = ['date', 'count']
        daily_trend['date'] = daily_trend['date'].astype(str)
        daily_trend_json = daily_trend.to_dict('records')
        
        # 3. Detailed Records (PREPARADO PARA MOSTRAR TODO)
        # Ordenamos por la fecha más reciente
        df_detail = df.sort_values(by='USER_DEPOSIT_DATE', ascending=False)
        
        # Convertimos la fecha de usuario a string
        df_detail['DEPOSIT DATE'] = df_detail['USER_DEPOSIT_DATE'].dt.strftime('%Y-%m-%d %H:%M:%S')
        
        # Limpieza final para JSON (rellenar nulos con string vacío)
        detailed_records = df_detail.fillna('').to_dict('records')

        return {
            "kpis": {
                "total_escalations": int(total_escalations),
                "max_age_days": max_age_days,
                "data_range": 7
            },
            "charts": {
                "daily_trend": daily_trend_json,
                # Dejamos esto aunque sea solo M1 para mantener compatibilidad con el frontend
                "brand_distribution": [{"brand": "M1", "count": int(total_escalations)}]
            },
            "detailed_records": detailed_records
        }

    except Exception as e:
        print(f"ERROR: {e}") # Ver en logs de Render
        raise HTTPException(status_code=500, detail=f"Server Error: {str(e)}")