import os
import asyncio
import pandas as pd
import gspread
import numpy as np
from fastapi import FastAPI, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from supabase import create_client, Client
import uuid
from datetime import datetime

# --- CONFIGURACIÓN ---
app = FastAPI(title="Deposit Dashboard Worker")

app.add_middleware(
    CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"],
)

SPREADSHEET_ID = "1UetwCMeOrrelOW9S6nWGAS4XiteYNtmgT7Upo19lqw4"
SHEET_NAMES = ['M1', 'M2', 'B1', 'B2', 'K1', 'B3', 'B4']
CREDENTIALS_FILE = "credentials.json"
SUPABASE_URL = os.environ.get("SUPABASE_URL", "TU_SUPABASE_PROJECT_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "TU_SUPABASE_SERVICE_ROLE_KEY")

LAST_UPDATE_TIME = "Nunca"
STATUS = "Idle"

def limpiar_valor(valor):
    if pd.isna(valor): return None
    if isinstance(valor, (np.integer, np.int64)): return int(valor)
    if isinstance(valor, (np.floating, np.float64)): return float(valor)
    return valor

def run_sync_process():
    global LAST_UPDATE_TIME, STATUS
    print(f"⏰ [{datetime.now()}] Iniciando Sincronización...")
    STATUS = "Syncing..."

    if not os.path.exists(CREDENTIALS_FILE):
        print("❌ Falta credentials.json")
        return

    try:
        gc = gspread.service_account(filename=CREDENTIALS_FILE)
        sh = gc.open_by_key(SPREADSHEET_ID)
        supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
    except Exception as e:
        print(f"❌ Error conexión: {e}")
        return

    total_nuevos_ciclo = 0
    
    for sheet_name in SHEET_NAMES:
        try:
            ws = sh.worksheet(sheet_name)
            data = ws.get_all_records()
            df = pd.DataFrame(data)

            if df.empty: continue

            df['date_posted_iso'] = pd.to_datetime(df['DATE POSTED'], unit='s', errors='coerce')
            df['date_posted_iso'] = df['date_posted_iso'].dt.strftime('%Y-%m-%d %H:%M:%S%z').replace("NaT", None)

            records_to_upload = []

            for index, row in df.iterrows():
                try:
                    # --- CAMBIO IMPORTANTE: MANEJO DE ID ---
                    dep_id = str(row.get('DEPOSIT ID', '')).strip()
                    
                    # Si no tiene ID, NO lanzamos error. Generamos uno para no perder la plata.
                    if not dep_id:
                        # Creamos un ID único para que entre a la BD
                        generated_id = f"NO_ID_{uuid.uuid4().hex[:8]}"
                        # print(f"   ⚠️ Fila {index+2} sin ID en {sheet_name}. Asignado: {generated_id}")
                        dep_id = generated_id
                    
                    # Limpieza de Monto
                    raw_amount = pd.to_numeric(str(row.get('AMOUNT', '')).replace(',', ''), errors='coerce')
                    amount_final = limpiar_valor(raw_amount) or 0
                    
                    posted_final = limpiar_valor(pd.to_numeric(row.get('DATE POSTED'), errors='coerce'))
                    raw_json_clean = {k: limpiar_valor(v) for k, v in row.to_dict().items()}

                    record = {
                        "deposit_id": dep_id, # Aquí va el ID real o el generado
                        "brand": sheet_name,  # Se mantiene la marca correcta (M1, etc)
                        "username": str(row.get('USERNAME', '')),
                        "amount": amount_final,
                        "status": str(row.get('Status', '')),
                        "deposit_date_user": str(row.get('DEPOSIT DATE', '')),
                        "date_posted_unix": posted_final, 
                        "date_posted_iso": row['date_posted_iso'], 
                        "pg_assign": str(row.get('PG ASSIGN', '')),
                        "reference_no": str(row.get('REFERENCE NO', '')),
                        "customer_number": str(row.get('CUSTOMER NUMBER', '')),
                        "agent_number": str(row.get('AGENT NUMBER', '')),
                        "image_link": str(row.get('IMAGE LINK', '')),
                        "payment_remarks": str(row.get('Payment Remarks', '')),
                        "raw_json": raw_json_clean
                    }
                    records_to_upload.append(record)

                except Exception as e_row:
                    # Solo si falla algo catastrófico (ej. formato de memoria) va a SYSTEM
                    print(f"   ⚠️ Error grave en fila {index+2}: {e_row}. A SYSTEM.")
                    fallback = {
                        "deposit_id": f"ERR_{uuid.uuid4().hex[:8]}",
                        "brand": "SYSTEM",
                        "amount": 0,
                        "username": "DATA_ERROR",
                        "raw_json": {"error": str(e_row)}
                    }
                    records_to_upload.append(fallback)

            if records_to_upload:
                try:
                    # Usamos ignore_duplicates=True.
                    # Si el ID generado (NO_ID_...) es nuevo, entrará.
                    supabase.table("deposits").upsert(
                        records_to_upload, 
                        on_conflict="deposit_id, brand", 
                        ignore_duplicates=True
                    ).execute()
                    
                    total_nuevos_ciclo += len(records_to_upload)
                except Exception as e:
                    print(f"   ❌ Error subiendo lote {sheet_name}: {e}")

        except Exception as e:
            print(f"❌ Error hoja {sheet_name}: {e}")

    LAST_UPDATE_TIME = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    STATUS = "Idle"
    print(f"✅ Sincronización completada. (Incluyendo filas sin ID)")

async def schedule_updates():
    while True:
        await asyncio.to_thread(run_sync_process)
        print("💤 Durmiendo 5 minutos...")
        await asyncio.sleep(300)

@app.on_event("startup")
async def startup_event():
    asyncio.create_task(schedule_updates())

@app.get("/")
def home():
    return {"status": STATUS, "last_update": LAST_UPDATE_TIME}

@app.get("/update-now")
def trigger_update(background_tasks: BackgroundTasks):
    background_tasks.add_task(run_sync_process)
    return {"message": "Actualización forzada iniciada."}