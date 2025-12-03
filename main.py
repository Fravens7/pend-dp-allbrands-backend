import os
import pandas as pd
import gspread
import numpy as np
import hashlib
import uvicorn
import gunicorn
import uuid
import time
import asyncio  # <--- NEW IMPORT
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
    # Genera un hash único basado en los datos de la fila para evitar duplicados
    raw_str = f"{brand}_{row.get('DEPOSIT ID')}_{row.get('USERNAME')}_{row.get('AMOUNT')}_{row.get('DATE POSTED')}"
    return hashlib.md5(raw_str.encode()).hexdigest()

# --- PROCESO DE SINCRONIZACIÓN (SYNC) ---
def run_sync_process():
    global last_execution_info
    
    # --- BLOQUEO DE SEGURIDAD INTERNO ---
    if last_execution_info["status"] == "Running":
        print("⚠️ Intento de ejecución superpuesta bloqueado.")
        return

    print(f"⏰ [{datetime.now()}] Iniciando Sincronización Automática...")
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
            print("❌ Faltan variables de entorno SUPABASE")
            last_execution_info["status"] = "Error: Env Vars"
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
            all_values = ws.get_all_values()
            
            if len(all_values) < 2: 
                print(f"   ⚠️ Hoja {sheet_name} vacía o sin datos.")
                continue

            original_headers = all_values.pop(0)
            final_headers = []
            empty_counter = 1
            for h in original_headers:
                if not h.strip():
                    final_headers.append(f"columna_extra_{empty_counter}")
                    empty_counter += 1
                else:
                    final_headers.append(h.strip())
            
            df = pd.DataFrame(all_values, columns=final_headers)
            if df.empty: continue
            
            if 'DATE POSTED' in df.columns:
                timestamps_numeric = pd.to_numeric(df['DATE POSTED'], errors='coerce')
                df['date_posted_iso'] = pd.to_datetime(timestamps_numeric, unit='s', errors='coerce')
                df['date_posted_iso'] = df['date_posted_iso'].dt.strftime('%Y-%m-%d %H:%M:%S%z').replace("NaT", None)
            else:
                df['date_posted_iso'] = None

            records_map = {} 
            original_count = len(df)

            for index, row in df.iterrows():
                try:
                    unique_hash = generar_id_unico(row, sheet_name)
                    # Generamos un UUID basado en el contenido para que sea siempre el mismo si el contenido es igual
                    record_id = str(uuid.uuid5(uuid.NAMESPACE_DNS, unique_hash))
                    
                    display_deposit_id = str(row.get('DEPOSIT ID', '')).strip() or f"NO_ID_{index}"
                    raw_amount = pd.to_numeric(str(row.get('AMOUNT', '')).replace(',', ''), errors='coerce')
                    amount_final = limpiar_valor(raw_amount) or 0
                    posted_final = limpiar_valor(pd.to_numeric(row.get('DATE POSTED'), errors='coerce')) if 'DATE POSTED' in row else None
                    raw_json_clean = {k: limpiar_valor(v) for k, v in row.to_dict().items()}
                    
                    # IMPORTANTE: Mapeamos STATUS desde la columna 'Status' del Excel
                    # Asegúrate que en tu Excel la columna se llame 'Status' (o ajusta aquí)
                    status_value = str(row.get('Status', '')).strip()

                    record = {
                        "id": record_id,
                        "deposit_id": display_deposit_id,
                        "brand": sheet_name,
                        "username": str(row.get('USERNAME', '')),
                        "amount": amount_final,
                        "status": status_value, # <--- AQUÍ VA EL STATUS CLAVE
                        "deposit_date_user": str(row.get('DEPOSIT DATE', '')),
                        "date_posted_unix": posted_final, 
                        "date_posted_iso": row.get('date_posted_iso'), 
                        "pg_assign": str(row.get('PG ASSIGN', '')),
                        "raw_json": raw_json_clean, 
                        "updated_at": datetime.utcnow().isoformat()
                    }
                    records_map[record_id] = record
                    
                except Exception as e_row:
                    continue

            records_to_upload = list(records_map.values())

            if records_to_upload:
                try:
                    # UPSERT: La clave para arreglar duplicados y actualizar estados
                    supabase.table("deposits").upsert(
                        records_to_upload, on_conflict="id", ignore_duplicates=False
                    ).execute()
                    
                    print(f" ✅ {sheet_name}: {len(records_to_upload)} registros procesados.")
                    total_nuevos += len(records_to_upload)
                except Exception as e:
                    print(f"❌ Error Supabase {sheet_name}: {e}")

        except Exception as e:
            print(f"❌ Error general en hoja {sheet_name}: {e}")

    last_execution_info["last_run"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    last_execution_info["status"] = "Idle"
    last_execution_info["records_processed"] = total_nuevos
    print(f"✅ Sincronización finalizada. Total registros procesados: {total_nuevos}")

# --- AUTO-LOOP (EL RELOJ INTERNO) ---
# Esta función se ejecuta sola en segundo plano al arrancar el servidor
async def start_periodic_sync():
    while True:
        # Ejecuta la sincronización
        run_sync_process()
        
        # Espera X segundos antes de la siguiente vuelta
        # 120 segundos = 2 minutos. (Recomendado para cuidar quota de Google)
        # Si quieres 1 minuto exacto, pon 60.
        print("⏳ Esperando 2 minutos para la siguiente sincronización...")
        await asyncio.sleep(120) 

@app.on_event("startup")
async def startup_event():
    # Inicia el bucle en segundo plano sin bloquear la API
    asyncio.create_task(start_periodic_sync())

# --- ENDPOINTS (API) ---

@app.get("/")
def home():
    return last_execution_info

@app.get("/trigger-sync")
def trigger_sync(background_tasks: BackgroundTasks, secret: str = None):
    # Este endpoint sigue sirviendo para el botón "Force Update" manual del Admin
    if secret != CRON_SECRET:
        raise HTTPException(status_code=401, detail="Clave secreta inválida")
    
    if last_execution_info["status"] == "Running":
         return {"message": "⚠️ Ya se está ejecutando.", "timestamp": datetime.now()}
    
    background_tasks.add_task(run_sync_process)
    return {"message": "Sincronización forzada iniciada", "timestamp": datetime.now()}
