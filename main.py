import os
import pandas as pd
import gspread
import numpy as np
import hashlib
import uuid
import time
import uvicorn
import gunicorn
from fastapi import FastAPI, BackgroundTasks, HTTPException, Header
from fastapi.middleware.cors import CORSMiddleware
from supabase import create_client, Client
from datetime import datetime

# --- CONFIGURACIÓN ---
app = FastAPI(title="Deposit Dashboard Worker")

app.add_middleware(
    CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"],
)

# ID DE TU SPREADSHEET
SPREADSHEET_ID = "1i63fQR8_M8eWLdZsa3QPL_aOCg8labb_W9QmK_w8FXY"
SHEET_NAMES = ['M1', 'M2', 'B1', 'B2', 'K1', 'B3', 'B4']

CREDENTIALS_FILE = "credentials.json"
# Usamos os.environ para todo lo sensible
SUPABASE_URL = os.environ.get("SUPABASE_URL") 
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")
# Define una clave secreta en tus variables de entorno en Render para proteger el botón
CRON_SECRET = os.environ.get("CRON_SECRET")

last_execution_info = {
    "status": "Idle",
    "last_run": "Nunca",
    "records_processed": 0
}

def limpiar_valor(valor):
    if pd.isna(valor): return None
    if isinstance(valor, (np.integer, np.int64)): return int(valor)
    if isinstance(valor, (np.floating, np.float64)): return float(valor)
    return valor

def generar_id_unico(row, brand):
    raw_str = f"{brand}_{row.get('DEPOSIT ID')}_{row.get('USERNAME')}_{row.get('AMOUNT')}_{row.get('DATE POSTED')}"
    return hashlib.md5(raw_str.encode()).hexdigest()

# Esta función ya no necesita ser async porque corre en un hilo aparte
def run_sync_process():
    global last_execution_info
    print(f"⏰ [{datetime.now()}] Iniciando Sincronización...")
    last_execution_info["status"] = "Running"

    try:
        # LÓGICA DE CREDENCIALES
        # Opción A: Archivo local (si lo subiste al repo - no recomendado por seguridad)
        if os.path.exists(CREDENTIALS_FILE):
             gc = gspread.service_account(filename=CREDENTIALS_FILE)
        # Opción B: Variables de entorno (Mejor para Render)
        else:
             # Aquí deberías implementar la carga desde JSON en ENV si decides hacerlo más pro
             # Por ahora, asume que fallará si no está el archivo
             print("⚠️ No se encontró credentials.json")
             last_execution_info["status"] = "Error: No credentials"
             return

        sh = gc.open_by_key(SPREADSHEET_ID)
        
        if not SUPABASE_URL or not SUPABASE_KEY:
            print("❌ Faltan variables de entorno SUPABASE")
            return
            
        supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
        
    except Exception as e:
        print(f"❌ Error conexión inicial: {e}")
        last_execution_info["status"] = f"Error connection: {str(e)}"
        return

    total_nuevos = 0
    
    for sheet_name in SHEET_NAMES:
        try:
            print(f"📄 Procesando hoja: {sheet_name}...")
            time.sleep(2) # Pausa anti-bloqueo de Google
            
            ws = sh.worksheet(sheet_name)
            data = ws.get_all_records()
            df = pd.DataFrame(data)

            if df.empty: continue

            # Limpieza de fechas
            df['date_posted_iso'] = pd.to_datetime(df['DATE POSTED'], unit='s', errors='coerce')
            df['date_posted_iso'] = df['date_posted_iso'].dt.strftime('%Y-%m-%d %H:%M:%S%z').replace("NaT", None)

            records_to_upload = []

            for index, row in df.iterrows():
                try:
                    unique_hash = generar_id_unico(row, sheet_name)
                    display_deposit_id = str(row.get('DEPOSIT ID', '')).strip() or f"NO_ID_{index}"
                    
                    raw_amount = pd.to_numeric(str(row.get('AMOUNT', '')).replace(',', ''), errors='coerce')
                    amount_final = limpiar_valor(raw_amount) or 0
                    posted_final = limpiar_valor(pd.to_numeric(row.get('DATE POSTED'), errors='coerce'))
                    raw_json_clean = {k: limpiar_valor(v) for k, v in row.to_dict().items()}

                    record = {
                        "id": str(uuid.uuid5(uuid.NAMESPACE_DNS, unique_hash)),
                        "deposit_id": display_deposit_id,
                        "brand": sheet_name,
                        "username": str(row.get('USERNAME', '')),
                        "amount": amount_final,
                        "status": str(row.get('Status', '')),
                        "deposit_date_user": str(row.get('DEPOSIT DATE', '')),
                        "date_posted_unix": posted_final, 
                        "date_posted_iso": row['date_posted_iso'], 
                        "pg_assign": str(row.get('PG ASSIGN', '')),
                        "raw_json": raw_json_clean,
                        "updated_at": datetime.utcnow().isoformat()
                    }
                    records_to_upload.append(record)
                except Exception:
                    continue

            if records_to_upload:
                try:
                    supabase.table("deposits").upsert(
                        records_to_upload, on_conflict="id", ignore_duplicates=False
                    ).execute()
                    total_nuevos += len(records_to_upload)
                except Exception as e:
                    print(f"❌ Error Supabase {sheet_name}: {e}")

        except Exception as e:
            print(f"❌ Error hoja {sheet_name}: {e}")

    last_execution_info["last_run"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    last_execution_info["status"] = "Idle"
    last_execution_info["records_processed"] = total_nuevos
    print(f"✅ Sincronización fin. Total: {total_nuevos}")

# --- ENDPOINTS ---

@app.get("/")
def home():
    return last_execution_info

# ESTE ES EL NUEVO CEREBRO DEL SISTEMA
@app.get("/trigger-sync")
def trigger_sync(background_tasks: BackgroundTasks, secret: str = None):
    # Seguridad simple para que nadie sature tu API
    if secret != CRON_SECRET:
        raise HTTPException(status_code=401, detail="Clave secreta inválida")
    
    # Esto lanza la función en segundo plano y responde INMEDIATAMENTE al cron
    background_tasks.add_task(run_sync_process)
    
    return {"message": "Sincronización iniciada en segundo plano", "timestamp": datetime.now()}
