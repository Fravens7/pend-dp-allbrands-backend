import os
import asyncio
import pandas as pd
import gspread
import numpy as np
import hashlib  # Para IDs únicos
import uuid
import time     # Para la pausa de seguridad
from fastapi import FastAPI, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from supabase import create_client, Client
from datetime import datetime

# --- CONFIGURACIÓN ---
app = FastAPI(title="Deposit Dashboard Worker")

app.add_middleware(
    CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"],
)

# ID DE TU SPREADSHEET
SPREADSHEET_ID = "1UetwCMeOrrelOW9S6nWGAS4XiteYNtmgT7Upo19lqw4"
SHEET_NAMES = ['M1', 'M2', 'B1', 'B2', 'K1', 'B3', 'B4']

CREDENTIALS_FILE = "credentials.json"
SUPABASE_URL = os.environ.get("SUPABASE_URL") 
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

LAST_UPDATE_TIME = "Nunca"
STATUS = "Idle"

def limpiar_valor(valor):
    if pd.isna(valor): return None
    if isinstance(valor, (np.integer, np.int64)): return int(valor)
    if isinstance(valor, (np.floating, np.float64)): return float(valor)
    return valor

# --- FUNCIÓN DE HASH ÚNICO ---
def generar_id_unico(row, brand):
    # Combina: Marca + ID + Usuario + Monto + Fecha Sistema
    raw_str = f"{brand}_{row.get('DEPOSIT ID')}_{row.get('USERNAME')}_{row.get('AMOUNT')}_{row.get('DATE POSTED')}"
    return hashlib.md5(raw_str.encode()).hexdigest()

def run_sync_process():
    global LAST_UPDATE_TIME, STATUS
    print(f"⏰ [{datetime.now()}] Iniciando Sincronización Inteligente...")
    STATUS = "Syncing..."

    # 1. PRIMERO NOS CONECTAMOS (ANTES DEL BUCLE)
    try:
        if os.path.exists(CREDENTIALS_FILE):
             print("🔑 Usando credentials.json local...")
             gc = gspread.service_account(filename=CREDENTIALS_FILE)
        else:
             # Si usas variables de entorno para las credenciales de Google en Render, configúralo aquí
             # Por ahora asume que el archivo existe o usa la config por defecto
             print("⚠️ Buscando credentials.json...")
             gc = gspread.service_account(filename=CREDENTIALS_FILE)

        sh = gc.open_by_key(SPREADSHEET_ID)
        
        if not SUPABASE_URL or not SUPABASE_KEY:
            print("❌ Error: Faltan variables de entorno SUPABASE_URL o SUPABASE_KEY")
            return
            
        supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
        
    except Exception as e:
        print(f"❌ Error conexión inicial: {e}")
        return

    total_nuevos = 0
    
    # 2. AHORA SÍ ENTRAMOS AL BUCLE
    for sheet_name in SHEET_NAMES:
        try:
            print(f"📄 Procesando hoja: {sheet_name}...")
            
            # --- PAUSA DE SEGURIDAD (CRÍTICO PARA EVITAR ERROR 429) ---
            time.sleep(2) 
            # ---------------------------------------------------------

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
                    # Generar Hash y ID Visual
                    unique_hash = generar_id_unico(row, sheet_name)
                    
                    display_deposit_id = str(row.get('DEPOSIT ID', '')).strip()
                    if not display_deposit_id:
                        display_deposit_id = f"NO_ID_{index}"

                    # Limpieza numérica
                    raw_amount = pd.to_numeric(str(row.get('AMOUNT', '')).replace(',', ''), errors='coerce')
                    amount_final = limpiar_valor(raw_amount) or 0
                    posted_final = limpiar_valor(pd.to_numeric(row.get('DATE POSTED'), errors='coerce'))
                    
                    raw_json_clean = {k: limpiar_valor(v) for k, v in row.to_dict().items()}

                    record = {
                        "id": str(uuid.uuid5(uuid.NAMESPACE_DNS, unique_hash)), # ID Técnico (Hash)
                        "deposit_id": display_deposit_id,                       # ID Visual
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

                except Exception as e_row:
                    print(f" ⚠️ Fila corrupta {index}: {e_row}")

            if records_to_upload:
                try:
                    # Upsert usando el HASH ('id') para recuperar duplicados visuales
                    supabase.table("deposits").upsert(
                        records_to_upload, 
                        on_conflict="id", 
                        ignore_duplicates=False
                    ).execute()
                    
                    total_nuevos += len(records_to_upload)
                    print(f" ✅ {sheet_name}: {len(records_to_upload)} registros procesados.")
                except Exception as e:
                    print(f" ❌ Error subiendo {sheet_name}: {e}")

        except Exception as e:
            print(f"❌ Error en hoja {sheet_name}: {e}")

    LAST_UPDATE_TIME = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    STATUS = "Idle"
    print(f"✅ Sincronización completada. Total registros procesados: {total_nuevos}")

async def schedule_updates():
    while True:
        await asyncio.to_thread(run_sync_process)
        print("💤 Esperando 5 minutos...")
        await asyncio.sleep(300)

@app.on_event("startup")
async def startup_event():
    asyncio.create_task(schedule_updates())

@app.get("/")
def home():
    return {"status": STATUS, "last_update": LAST_UPDATE_TIME}