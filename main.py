import os
import asyncio
import pandas as pd
import gspread
import numpy as np
import hashlib  # <--- NUEVO: Para generar IDs únicos reales y no perder datos
import uuid
from fastapi import FastAPI, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from supabase import create_client, Client
from datetime import datetime

# --- CONFIGURACIÓN ---
app = FastAPI(title="Deposit Dashboard Worker")

app.add_middleware(
    CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"],
)

# ID DE TU SPREADSHEET (GSheet)
SPREADSHEET_ID = "1UetwCMeOrrelOW9S6nWGAS4XiteYNtmgT7Upo19lqw4"
SHEET_NAMES = ['M1', 'M2', 'B1', 'B2', 'K1', 'B3', 'B4'] # Procesamos todas las marcas

# CREDENTIALS: En Render esto puede manejarse via archivo secreto o variable de entorno JSON.
# Si usas archivo físico en local:
CREDENTIALS_FILE = "credentials.json"

# SUPABASE: Leyendo desde Variables de Entorno de Render
SUPABASE_URL = os.environ.get("SUPABASE_URL") 
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

LAST_UPDATE_TIME = "Nunca"
STATUS = "Idle"

def limpiar_valor(valor):
    if pd.isna(valor): return None
    if isinstance(valor, (np.integer, np.int64)): return int(valor)
    if isinstance(valor, (np.floating, np.float64)): return float(valor)
    return valor

# --- NUEVA FUNCIÓN MÁGICA: Generar ID Único (Hash) ---
def generar_id_unico(row, brand):
    # Creamos una firma única combinando: Marca + ID + Usuario + Monto + Fecha Sistema
    # Si el usuario manda el mismo ID 5 minutos después, la 'DATE POSTED' cambia,
    # por lo tanto el hash cambia y Supabase lo guarda como NUEVO registro.
    raw_str = f"{brand}_{row.get('DEPOSIT ID')}_{row.get('USERNAME')}_{row.get('AMOUNT')}_{row.get('DATE POSTED')}"
    return hashlib.md5(raw_str.encode()).hexdigest()

def run_sync_process():
    global LAST_UPDATE_TIME, STATUS
    print(f"⏰ [{datetime.now()}] Iniciando Sincronización Inteligente...")
    STATUS = "Syncing..."

    # Verificación básica de credenciales
    try:
        if os.path.exists(CREDENTIALS_FILE):
             gc = gspread.service_account(filename=CREDENTIALS_FILE)
        else:
             # Si estás en Render y usas una variable de entorno para el JSON de google, configúralo aquí
             # Por ahora asumimos que subiste el archivo credentials.json o usas dictConfig
             print("⚠️ Advertencia: Buscando credentials.json...")
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
    
    for sheet_name in SHEET_NAMES:
        try:
            print(f"📄 Procesando hoja: {sheet_name}...")
            ws = sh.worksheet(sheet_name)
            data = ws.get_all_records()
            df = pd.DataFrame(data)

            if df.empty: continue

            # Limpieza de fechas para formato ISO
            df['date_posted_iso'] = pd.to_datetime(df['DATE POSTED'], unit='s', errors='coerce')
            df['date_posted_iso'] = df['date_posted_iso'].dt.strftime('%Y-%m-%d %H:%M:%S%z').replace("NaT", None)

            records_to_upload = []

            for index, row in df.iterrows():
                try:
                    # 1. Generamos el HASH único
                    unique_hash = generar_id_unico(row, sheet_name)
                    
                    # 2. Generamos el ID visual (el que ve el humano)
                    display_deposit_id = str(row.get('DEPOSIT ID', '')).strip()
                    if not display_deposit_id:
                        display_deposit_id = f"NO_ID_{index}"

                    # 3. Limpieza de números
                    raw_amount = pd.to_numeric(str(row.get('AMOUNT', '')).replace(',', ''), errors='coerce')
                    amount_final = limpiar_valor(raw_amount) or 0
                    posted_final = limpiar_valor(pd.to_numeric(row.get('DATE POSTED'), errors='coerce'))
                    
                    raw_json_clean = {k: limpiar_valor(v) for k, v in row.to_dict().items()}

                    record = {
                        # UUID determinista basado en el contenido único.
                        # Si el contenido es idéntico, el UUID es idéntico. Si cambia un segundo, el UUID cambia.
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

                except Exception as e_row:
                    print(f" ⚠️ Fila corrupta {index}: {e_row}")

            if records_to_upload:
                try:
                    # EL SECRETO: on_conflict="id".
                    # Como 'id' ahora es un hash único de todo el contenido, 
                    # Supabase guardará los duplicados de 'deposit_id' siempre que tengan distinta hora/monto.
                    supabase.table("deposits").upsert(
                        records_to_upload, 
                        on_conflict="id", 
                        ignore_duplicates=False
                    ).execute()
                    
                    total_nuevos += len(records_to_upload)
                except Exception as e:
                    print(f" ❌ Error subiendo {sheet_name}: {e}")

        except Exception as e:
            print(f"❌ Error en hoja {sheet_name}: {e}")

    LAST_UPDATE_TIME = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    STATUS = "Idle"
    print(f"✅ Sincronización completada. Total registros procesados: {total_nuevos}")

async def schedule_updates():
    while True:
        # Ejecutamos el barrido en un hilo aparte para no bloquear
        await asyncio.to_thread(run_sync_process)
        print("💤 Esperando 5 minutos para el siguiente barrido...")
        await asyncio.sleep(300)

@app.on_event("startup")
async def startup_event():
    asyncio.create_task(schedule_updates())

@app.get("/")
def home():
    return {"status": STATUS, "last_update": LAST_UPDATE_TIME, "mode": "Supabase Sync"}