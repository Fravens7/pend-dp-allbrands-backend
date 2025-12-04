import os
import pandas as pd
import gspread
import numpy as np
import hashlib
import uuid
import asyncio
import math
import json
from fastapi import FastAPI, BackgroundTasks, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from supabase import create_client, Client
from datetime import datetime, timezone

# --- CONFIGURACIÓN ---
app = FastAPI(title="Deposit Dashboard Worker Ultra-Fast")

app.add_middleware(
    CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"],
)

SPREADSHEET_ID = "1i63fQR8_M8eWLdZsa3QPL_aOCg8labb_W9QmK_w8FXY"
SHEET_NAMES = ['M1', 'M2', 'B1', 'B2', 'K1', 'B3', 'B4']
CREDENTIALS_FILE = "credentials.json"
SUPABASE_URL = os.environ.get("SUPABASE_URL") 
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")
CRON_SECRET = os.environ.get("CRON_SECRET")

last_execution_info = {
    "status": "Idle",
    "last_run": "Nunca",
    "records_processed": 0
}

# --- SANITIZADOR RECURSIVO (LA SOLUCIÓN FINAL) ---
def sanitize_for_json(obj):
    """
    Recorre recursivamente cualquier objeto (dict, list) y reemplaza
    NaNs o Infinitos con None. Esto asegura compatibilidad JSON 100%.
    """
    if isinstance(obj, float):
        if math.isnan(obj) or math.isinf(obj):
            return None
        return obj
    elif isinstance(obj, dict):
        return {k: sanitize_for_json(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [sanitize_for_json(v) for v in obj]
    elif isinstance(obj, (np.integer, np.int64)):
        return int(obj)
    elif isinstance(obj, (np.floating, np.float64)):
        if np.isnan(obj) or np.isinf(obj):
            return None
        return float(obj)
    elif pd.isna(obj):
        return None
    return obj

def generar_id_unico(row, brand):
    # Convertimos a string forzado para evitar errores de hash
    raw_str = f"{brand}_{str(row.get('DEPOSIT ID'))}_{str(row.get('USERNAME'))}_{str(row.get('AMOUNT'))}_{str(row.get('DATE POSTED'))}"
    return hashlib.md5(raw_str.encode()).hexdigest()

# --- PROCESO DE SINCRONIZACIÓN ---
def run_sync_process():
    global last_execution_info
    
    if last_execution_info["status"] == "Running":
        print("⚠️ En curso.")
        return

    cycle_start_time = datetime.now(timezone.utc).isoformat()
    print(f"⚡ [{datetime.now().strftime('%H:%M:%S')}] Iniciando Sync...")
    last_execution_info["status"] = "Running"

    try:
        if os.path.exists(CREDENTIALS_FILE):
             gc = gspread.service_account(filename=CREDENTIALS_FILE)
        else:
             print("⚠️ No credentials.json")
             last_execution_info["status"] = "Error: No credentials"
             return

        sh = gc.open_by_key(SPREADSHEET_ID)
        
        if not SUPABASE_URL or not SUPABASE_KEY:
            print("❌ Faltan variables SUPABASE")
            last_execution_info["status"] = "Error: Env Vars"
            return
            
        supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
        
        # Batch Get
        ranges = [f"{name}!A:Z" for name in SHEET_NAMES]
        try:
            batch_results = sh.values_batch_get(ranges)
        except Exception as e:
            print(f"❌ Error Google API: {e}")
            last_execution_info["status"] = "Error Google API"
            return

    except Exception as e:
        print(f"❌ Error conexión: {e}")
        last_execution_info["status"] = f"Error connection: {str(e)}"
        return

    total_nuevos = 0
    
    for i, result in enumerate(batch_results.get('valueRanges', [])):
        sheet_name = SHEET_NAMES[i]
        values = result.get('values', [])
        
        if len(values) < 2:
            continue

        try:
            original_headers = values.pop(0)
            final_headers = [h.strip() if h.strip() else f"col_extra_{j}" for j, h in enumerate(original_headers)]
            
            df = pd.DataFrame(values, columns=final_headers)
            if df.empty: continue
            
            # --- LIMPIEZA PRELIMINAR ---
            # Forzamos conversión a numérico donde corresponde para detectar NaNs
            if 'AMOUNT' in df.columns:
                df['AMOUNT'] = pd.to_numeric(df['AMOUNT'].astype(str).str.replace(',', ''), errors='coerce')
            
            if 'DATE POSTED' in df.columns:
                timestamps_numeric = pd.to_numeric(df['DATE POSTED'], errors='coerce')
                df['date_posted_iso'] = pd.to_datetime(timestamps_numeric, unit='s', errors='coerce')
                df['date_posted_iso'] = df['date_posted_iso'].dt.strftime('%Y-%m-%d %H:%M:%S%z').replace("NaT", None)
            else:
                df['date_posted_iso'] = None

            records_to_upload = []
            seen_ids = set()

            for index, row in df.iterrows():
                try:
                    # 1. Filtro de Fila Vacía (Si no hay usuario ni monto, adiós)
                    username = str(row.get('USERNAME', '')).strip()
                    amount_val = row.get('AMOUNT')
                    
                    # Chequeo estricto de vacío
                    is_amount_empty = pd.isna(amount_val) or amount_val is None
                    if (not username or username == 'None' or username == 'nan') and is_amount_empty:
                        continue

                    # 2. Generar ID
                    unique_hash = generar_id_unico(row, sheet_name)
                    record_id = str(uuid.uuid5(uuid.NAMESPACE_DNS, unique_hash))
                    
                    if record_id in seen_ids: continue
                    seen_ids.add(record_id)
                    
                    # Datos crudos
                    raw_json_dict = row.to_dict()

                    record = {
                        "id": record_id,
                        "deposit_id": str(row.get('DEPOSIT ID', '')).strip(),
                        "brand": sheet_name,
                        "username": username,
                        "amount": amount_val, # Se limpiará en el paso final
                        "status": str(row.get('Status', '')).strip(),
                        "deposit_date_user": str(row.get('DEPOSIT DATE', '')),
                        "date_posted_unix": row.get('DATE POSTED'), # Se limpiará al final
                        "date_posted_iso": row.get('date_posted_iso'), 
                        "pg_assign": str(row.get('PG ASSIGN', '')),
                        "raw_json": raw_json_dict, 
                        "updated_at": datetime.now(timezone.utc).isoformat()
                    }
                    records_to_upload.append(record)
                    
                except Exception:
                    continue

            if records_to_upload:
                try:
                    # --- EL PASO MAGICO: SANITIZACIÓN TOTAL ---
                    # Esto recorre todo el array y mata cualquier NaN escondido
                    safe_records = sanitize_for_json(records_to_upload)

                    # 1. UPSERT
                    supabase.table("deposits").upsert(
                        safe_records, on_conflict="id", ignore_duplicates=False
                    ).execute()
                    
                    total_nuevos += len(safe_records)

                    # 2. BARRIDO
                    supabase.table("deposits").update({"status": "CLEARED_AUTO"}) \
                        .eq("brand", sheet_name) \
                        .eq("status", "ALREADY FOLLOW UP") \
                        .lt("updated_at", cycle_start_time) \
                        .execute()
                        
                    print(f" ✅ {sheet_name}: OK ({len(safe_records)}).")

                except Exception as e:
                    print(f"❌ Error Supabase {sheet_name}: {e}")

        except Exception as e:
            print(f"❌ Error hoja {sheet_name}: {e}")

    last_execution_info["last_run"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    last_execution_info["status"] = "Idle"
    last_execution_info["records_processed"] = total_nuevos
    print(f"✅ Ciclo terminado. Total: {total_nuevos}")

async def start_periodic_sync():
    print("⏳ Esperando arranque (5s)...")
    await asyncio.sleep(5)
    while True:
        print(f"💓 [{datetime.now().strftime('%H:%M:%S')}] Esperando ciclo...")
        await asyncio.to_thread(run_sync_process)
        await asyncio.sleep(20) 

@app.on_event("startup")
async def startup_event():
    asyncio.create_task(start_periodic_sync())

@app.get("/")
def home(): return last_execution_info

@app.head("/")
def health_check(): return "OK"

@app.get("/trigger-sync")
def trigger_sync(background_tasks: BackgroundTasks, secret: str = None):
    if secret != CRON_SECRET: raise HTTPException(status_code=401, detail="Clave inválida")
    if last_execution_info["status"] == "Running": return {"message": "⚠️ En curso."}
    background_tasks.add_task(run_sync_process)
    return {"message": "Sync iniciada"}
