import os
import pandas as pd
import gspread
import numpy as np
import hashlib
import uuid
import asyncio
from fastapi import FastAPI, BackgroundTasks, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from supabase import create_client, Client
from datetime import datetime

# --- CONFIGURACIÓN ---
app = FastAPI(title="Deposit Dashboard Worker Ultra-Fast")

app.add_middleware(
    CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"],
)

# ID DE TU SPREADSHEET
SPREADSHEET_ID = "1i63fQR8_M8eWLdZsa3QPL_aOCg8labb_W9QmK_w8FXY"
# Las hojas que quieres leer
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

def limpiar_valor(valor):
    if pd.isna(valor): return None
    if isinstance(valor, (np.integer, np.int64)): return int(valor)
    if isinstance(valor, (np.floating, np.float64)): return float(valor)
    return valor

def generar_id_unico(row, brand):
    raw_str = f"{brand}_{row.get('DEPOSIT ID')}_{row.get('USERNAME')}_{row.get('AMOUNT')}_{row.get('DATE POSTED')}"
    return hashlib.md5(raw_str.encode()).hexdigest()

# --- PROCESO DE SINCRONIZACIÓN OPTIMIZADO (BATCH) ---
def run_sync_process():
    global last_execution_info
    
    if last_execution_info["status"] == "Running":
        print("⚠️ Ejecución anterior aún en curso. Saltando ciclo.")
        return

    print(f"⚡ [{datetime.now().strftime('%H:%M:%S')}] Iniciando Sincronización Rápida...")
    last_execution_info["status"] = "Running"

    try:
        if os.path.exists(CREDENTIALS_FILE):
             gc = gspread.service_account(filename=CREDENTIALS_FILE)
        else:
             print("⚠️ No se encontró credentials.json")
             last_execution_info["status"] = "Error: No credentials"
             return

        sh = gc.open_by_key(SPREADSHEET_ID)
        
        if not SUPABASE_URL or not SUPABASE_KEY:
            print("❌ Faltan variables SUPABASE")
            last_execution_info["status"] = "Error: Env Vars"
            return
            
        supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
        
        # --- OPTIMIZACIÓN: BATCH GET (1 SOLA LLAMADA A GOOGLE) ---
        # En lugar de pedir 7 veces, pedimos 1 vez todos los rangos
        # Pedimos desde la columna A hasta la Z (o más si necesitas)
        ranges = [f"{name}!A:Z" for name in SHEET_NAMES]
        
        try:
            # Esta es la magia: Trae todo de golpe
            batch_results = sh.values_batch_get(ranges)
        except Exception as e:
            print(f"❌ Error leyendo Google Sheets: {e}")
            last_execution_info["status"] = "Error Google API"
            return

    except Exception as e:
        print(f"❌ Error conexión inicial: {e}")
        last_execution_info["status"] = f"Error connection: {str(e)}"
        return

    total_nuevos = 0
    
    # Iteramos sobre los resultados en memoria (ya no llamamos a Google)
    for i, result in enumerate(batch_results.get('valueRanges', [])):
        sheet_name = SHEET_NAMES[i]
        values = result.get('values', [])
        
        if len(values) < 2:
            continue

        try:
            # Procesamiento de datos en Pandas (Igual que antes)
            original_headers = values.pop(0)
            final_headers = [h.strip() if h.strip() else f"col_extra_{j}" for j, h in enumerate(original_headers)]
            
            df = pd.DataFrame(values, columns=final_headers)
            if df.empty: continue
            
            if 'DATE POSTED' in df.columns:
                timestamps_numeric = pd.to_numeric(df['DATE POSTED'], errors='coerce')
                df['date_posted_iso'] = pd.to_datetime(timestamps_numeric, unit='s', errors='coerce')
                df['date_posted_iso'] = df['date_posted_iso'].dt.strftime('%Y-%m-%d %H:%M:%S%z').replace("NaT", None)
            else:
                df['date_posted_iso'] = None

            records_to_upload = []
            
            # Usamos un set para evitar procesar duplicados internos de la misma carga
            seen_ids = set()

            for index, row in df.iterrows():
                try:
                    unique_hash = generar_id_unico(row, sheet_name)
                    record_id = str(uuid.uuid5(uuid.NAMESPACE_DNS, unique_hash))
                    
                    if record_id in seen_ids: continue
                    seen_ids.add(record_id)
                    
                    # Limpieza rápida
                    raw_amount = pd.to_numeric(str(row.get('AMOUNT', '')).replace(',', ''), errors='coerce')
                    amount_final = limpiar_valor(raw_amount) or 0
                    posted_final = limpiar_valor(pd.to_numeric(row.get('DATE POSTED'), errors='coerce')) if 'DATE POSTED' in row else None
                    raw_json_clean = {k: limpiar_valor(v) for k, v in row.to_dict().items()}
                    status_value = str(row.get('Status', '')).strip()

                    record = {
                        "id": record_id,
                        "deposit_id": str(row.get('DEPOSIT ID', '')).strip() or f"NO_ID_{index}",
                        "brand": sheet_name,
                        "username": str(row.get('USERNAME', '')),
                        "amount": amount_final,
                        "status": status_value, 
                        "deposit_date_user": str(row.get('DEPOSIT DATE', '')),
                        "date_posted_unix": posted_final, 
                        "date_posted_iso": row.get('date_posted_iso'), 
                        "pg_assign": str(row.get('PG ASSIGN', '')),
                        "raw_json": raw_json_clean, 
                        "updated_at": datetime.utcnow().isoformat()
                    }
                    records_to_upload.append(record)
                    
                except Exception:
                    continue

            if records_to_upload:
                try:
                    # Upsert masivo a Supabase
                    supabase.table("deposits").upsert(
                        records_to_upload, on_conflict="id", ignore_duplicates=False
                    ).execute()
                    total_nuevos += len(records_to_upload)
                except Exception as e:
                    print(f"❌ Error Supabase {sheet_name}: {e}")

        except Exception as e:
            print(f"❌ Error procesando data {sheet_name}: {e}")

    last_execution_info["last_run"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    last_execution_info["status"] = "Idle"
    last_execution_info["records_processed"] = total_nuevos
    print(f"✅ Sync Batch Finalizada. Registros: {total_nuevos}")

# --- AUTO-LOOP OPTIMIZADO (NO BLOQUEANTE) ---
async def start_periodic_sync():
    print("⏳ Esperando arranque del servidor...")
    await asyncio.sleep(5)
    
    while True:
        # CORRECCIÓN: Usamos 'to_thread' para no congelar el servidor
        # Esto envía la sincronización a un hilo separado
        print("🚀 Lanzando sincronización en hilo secundario...")
        await asyncio.to_thread(run_sync_process)
        
        print("⏳ Esperando 20 segundos...")
        await asyncio.sleep(20)

@app.on_event("startup")
async def startup_event():
    # Lanza el bucle en segundo plano
    asyncio.create_task(start_periodic_sync())

# --- ENDPOINTS ---
@app.get("/")
def home():
    return last_execution_info

# Endpoint para los HEAD checks de Render (Evita el error 405 en logs)
@app.head("/")
def health_check():
    return "OK"

@app.get("/trigger-sync")
def trigger_sync(background_tasks: BackgroundTasks, secret: str = None):
    if secret != CRON_SECRET:
        raise HTTPException(status_code=401, detail="Clave secreta inválida")
    
    if last_execution_info["status"] == "Running":
         return {"message": "⚠️ Ya en curso.", "timestamp": datetime.now()}
    
    background_tasks.add_task(run_sync_process)
    return {"message": "Sync iniciada", "timestamp": datetime.now()}
