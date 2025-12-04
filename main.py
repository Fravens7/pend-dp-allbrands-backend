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

# --- SANITIZADOR ---
def sanitize_for_json(obj):
    if isinstance(obj, float):
        if math.isnan(obj) or math.isinf(obj): return None
        return obj
    elif isinstance(obj, dict):
        return {k: sanitize_for_json(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [sanitize_for_json(v) for v in obj]
    elif isinstance(obj, (np.integer, np.int64)):
        return int(obj)
    elif isinstance(obj, (np.floating, np.float64)):
        if np.isnan(obj) or np.isinf(obj): return None
        return float(obj)
    elif pd.isna(obj):
        return None
    return obj

def limpiar_valor(valor):
    if pd.isna(valor): return None
    if isinstance(valor, str):
        if not valor.strip(): return None
        return valor
    if isinstance(valor, (float, np.floating)):
        if math.isnan(valor) or math.isinf(valor): return None
        return float(valor)
    if isinstance(valor, (int, np.integer)): return int(valor)
    return valor

def generar_id_unico(row, brand):
    raw_str = f"{brand}_{str(row.get('DEPOSIT ID'))}_{str(row.get('USERNAME'))}_{str(row.get('AMOUNT'))}_{str(row.get('DATE POSTED'))}"
    return hashlib.md5(raw_str.encode()).hexdigest()

# --- PROCESO ---
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
        
        if len(values) < 2: continue

        try:
            original_headers = values.pop(0)
            final_headers = [h.strip() if h.strip() else f"col_extra_{j}" for j, h in enumerate(original_headers)]
            
            df = pd.DataFrame(values, columns=final_headers)
            if df.empty: continue
            
            # Limpiezas previas
            if 'AMOUNT' in df.columns:
                df['AMOUNT'] = pd.to_numeric(df['AMOUNT'].astype(str).str.replace(',', ''), errors='coerce')
            
            if 'DATE POSTED' in df.columns:
                timestamps_numeric = pd.to_numeric(df['DATE POSTED'], errors='coerce')
                df['date_posted_iso'] = pd.to_datetime(timestamps_numeric, unit='s', errors='coerce')
                df['date_posted_iso'] = df['date_posted_iso'].dt.strftime('%Y-%m-%d %H:%M:%S%z').replace("NaT", None)
            else:
                df['date_posted_iso'] = None

            records_to_upload = []
            
            # --- FILTROS DE DEDUPLICACIÓN ---
            seen_ids = set()          # Para IDs generados (Hash)
            seen_constraints = set()  # NUEVO: Para contenido real (Marca+User+Monto+Fecha)

            for index, row in df.iterrows():
                try:
                    # 1. Filtro basura
                    username = str(row.get('USERNAME', '')).strip()
                    amount_val = row.get('AMOUNT')
                    is_amount_empty = pd.isna(amount_val) or amount_val is None
                    
                    if (not username or username == 'None' or username == 'nan') and is_amount_empty:
                        continue

                    # 2. Generar Hash ID
                    unique_hash = generar_id_unico(row, sheet_name)
                    record_id = str(uuid.uuid5(uuid.NAMESPACE_DNS, unique_hash))
                    
                    if record_id in seen_ids: continue
                    seen_ids.add(record_id)

                    # 3. Limpieza de valores clave
                    amount_final = limpiar_valor(amount_val) or 0
                    date_iso_final = row.get('date_posted_iso')

                    # --- 4. NUEVO FILTRO: CONTENIDO DUPLICADO ---
                    # Creamos una firma única con los datos que le importan a Supabase
                    # Si ya vimos esta combinación exacta en este ciclo, saltamos.
                    constraint_signature = f"{sheet_name}|{username}|{amount_final}|{date_iso_final}"
                    
                    if constraint_signature in seen_constraints:
                        # Detectamos un gemelo. Lo ignoramos silenciosamente para no romper la DB.
                        continue
                    
                    seen_constraints.add(constraint_signature)
                    # ---------------------------------------------

                    posted_final = limpiar_valor(pd.to_numeric(row.get('DATE POSTED'), errors='coerce')) if 'DATE POSTED' in row else None
                    raw_json_clean = {k: limpiar_valor(v) for k, v in row.to_dict().items()}
                    status_value = str(row.get('Status', '')).strip()

                    record = {
                        "id": record_id,
                        "deposit_id": str(row.get('DEPOSIT ID', '')).strip(),
                        "brand": sheet_name,
                        "username": username,
                        "amount": amount_final,
                        "status": status_value, 
                        "deposit_date_user": str(row.get('DEPOSIT DATE', '')),
                        "date_posted_unix": posted_final, 
                        "date_posted_iso": date_iso_final, 
                        "pg_assign": str(row.get('PG ASSIGN', '')),
                        "raw_json": raw_json_clean, 
                        "updated_at": datetime.now(timezone.utc).isoformat()
                    }
                    records_to_upload.append(record)
                    
                except Exception:
                    continue

            if records_to_upload:
                try:
                    safe_records = sanitize_for_json(records_to_upload)

                    # Upsert
                    supabase.table("deposits").upsert(
                        safe_records, on_conflict="id", ignore_duplicates=False
                    ).execute()
                    
                    total_nuevos += len(safe_records)

                    # Barrido
                    supabase.table("deposits").update({"status": "CLEARED_AUTO"}) \
                        .eq("brand", sheet_name) \
                        .eq("status", "ALREADY FOLLOW UP") \
                        .lt("updated_at", cycle_start_time) \
                        .execute()
                        
                    print(f" ✅ {sheet_name}: OK ({len(safe_records)}).")

                except Exception as e:
                    # Imprimir solo una parte del error para no saturar log
                    err_msg = str(e)[:200] 
                    print(f"❌ Error Supabase {sheet_name}: {err_msg}...")

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
