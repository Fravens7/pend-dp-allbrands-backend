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
app = FastAPI(title="Deposit Dashboard Worker & API")

# Configuración CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], 
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Credenciales
SPREADSHEET_ID = "1UetwCMeOrrelOW9S6nWGAS4XiteYNtmgT7Upo19lqw4"
SHEET_NAMES = ['M1', 'M2', 'B1', 'B2', 'K1', 'B3', 'B4'] 
CREDENTIALS_FILE = "credentials.json"
SUPABASE_URL = os.environ.get("SUPABASE_URL", "TU_SUPABASE_URL_SI_PRUEBAS_LOCAL") 
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "TU_SUPABASE_SERVICE_ROLE_KEY_SI_PRUEBAS_LOCAL")

# Estado Global para Monitoreo
LAST_UPDATE_TIME = "Nunca"
TOTAL_RECORDS_SYNCED = 0
STATUS = "Idle"

# --- FUNCIONES DE LIMPIEZA ---
def limpiar_valor(valor):
    if pd.isna(valor): return None
    if isinstance(valor, (np.integer, np.int64)): return int(valor)
    if isinstance(valor, (np.floating, np.float64)): return float(valor)
    return valor

# --- LÓGICA DE SINCRONIZACIÓN (ETL) ---
def run_sync_process():
    global LAST_UPDATE_TIME, TOTAL_RECORDS_SYNCED, STATUS
    
    print(f"⏰ [{datetime.now()}] Iniciando Sincronización Automática...")
    STATUS = "Syncing..."
    
    # 1. Conexiones
    if not os.path.exists(CREDENTIALS_FILE):
        print("❌ Error: Falta credentials.json")
        STATUS = "Error: No Credentials"
        return

    try:
        gc = gspread.service_account(filename=CREDENTIALS_FILE)
        sh = gc.open_by_key(SPREADSHEET_ID)
        # Usamos la Service Role Key para poder escribir
        supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
    except Exception as e:
        print(f"❌ Error de conexión: {e}")
        STATUS = f"Connection Error: {str(e)}"
        return

    total_nuevos_ciclo = 0
    
    # 2. Procesar cada marca
    for sheet_name in SHEET_NAMES:
        try:
            ws = sh.worksheet(sheet_name)
            data = ws.get_all_records()
            df = pd.DataFrame(data)

            if df.empty: continue

            # Conversión Fecha Sistema
            df['date_posted_iso'] = pd.to_datetime(df['DATE POSTED'], unit='s', errors='coerce')
            df['date_posted_iso'] = df['date_posted_iso'].dt.strftime('%Y-%m-%d %H:%M:%S%z').replace("NaT", None)

            records_to_upload = []

            for index, row in df.iterrows():
                try:
                    # Limpieza Prioritaria
                    dep_id = str(row.get('DEPOSIT ID', '')).strip()
                    if not dep_id: raise ValueError("Sin ID")

                    raw_amount = pd.to_numeric(str(row.get('AMOUNT', '')).replace(',', ''), errors='coerce')
                    amount_final = limpiar_valor(raw_amount) or 0
                    posted_final = limpiar_valor(pd.to_numeric(row.get('DATE POSTED'), errors='coerce'))
                    raw_json_clean = {k: limpiar_valor(v) for k, v in row.to_dict().items()}

                    record = {
                        "deposit_id": dep_id,
                        "brand": sheet_name,
                        "username": str(row.get('USERNAME', '')),
                        "amount": amount_final,
                        "status": str(row.get('Status', '')),
                        "deposit_date_user": str(row.get('DEPOSIT DATE', '')),
                        "date_posted_unix": posted_final, 
                        "date_posted_iso": row['date_posted_iso'], 
                        "pg_assign": str(row.get('PG ASSIGN', '')), # Mantenemos aunque no se use en frontend, por si acaso
                        "reference_no": str(row.get('REFERENCE NO', '')),
                        "customer_number": str(row.get('CUSTOMER NUMBER', '')),
                        "agent_number": str(row.get('AGENT NUMBER', '')),
                        "image_link": str(row.get('IMAGE LINK', '')),
                        "payment_remarks": str(row.get('Payment Remarks', '')),
                        "raw_json": raw_json_clean
                    }
                    records_to_upload.append(record)

                except Exception as e_row:
                    # --- RED DE SEGURIDAD "SYSTEM" ---
                    # Si la fila es un desastre, la guardamos aquí para no perderla
                    print(f"   ⚠️ Error en fila {index+2} de {sheet_name}: {e_row}. Enviando a SYSTEM.")
                    fallback_record = {
                        "deposit_id": f"ERR_{uuid.uuid4().hex[:8]}",
                        "brand": "SYSTEM",
                        "amount": 0,
                        "username": "DATA_ERROR",
                        "payment_remarks": f"Error: {str(e_row)} | Origen: {sheet_name}",
                        "raw_json": {k: str(v) for k, v in row.to_dict().items()}
                    }
                    records_to_upload.append(fallback_record)

            # Subida a Supabase
            if records_to_upload:
                try:
                    # ignore_duplicates=True: Si el ID ya existe, no hace nada. Si es nuevo, lo guarda.
                    supabase.table("deposits").upsert(
                        records_to_upload, 
                        on_conflict="deposit_id, brand", 
                        ignore_duplicates=True
                    ).execute()
                    
                    # Nota: Upsert no devuelve count exacto de insertados si ignoró duplicados, 
                    # pero asumimos éxito del lote.
                    total_nuevos_ciclo += len(records_to_upload)
                except Exception as e:
                    print(f"   ❌ Error subiendo lote {sheet_name}: {e}")

        except Exception as e:
            print(f"❌ Error leyendo hoja {sheet_name}: {e}")

    # Actualizar estado global
    LAST_UPDATE_TIME = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    TOTAL_RECORDS_SYNCED = total_nuevos_ciclo # Esto es un aproximado de procesados
    STATUS = "Idle (Waiting next cycle)"
    print(f"✅ Sincronización finalizada a las {LAST_UPDATE_TIME}")

# --- SCHEDULER (LOOP AUTOMÁTICO) ---
async def schedule_updates():
    while True:
        # Ejecutamos la sincronización en un hilo aparte para no bloquear la API
        await asyncio.to_thread(run_sync_process)
        
        # Esperar 5 minutos (300 segundos) antes de la próxima
        print("💤 Durmiendo 5 minutos...")
        await asyncio.sleep(300)

@app.on_event("startup")
async def startup_event():
    # Esto arranca el loop infinito cuando el servidor se enciende
    asyncio.create_task(schedule_updates())

# --- ENDPOINTS INFORMATIVOS ---

@app.get("/")
def home():
    return {
        "status": STATUS,
        "last_update": LAST_UPDATE_TIME,
        "message": "El Worker está vivo y actualizando Supabase automáticamente."
    }

@app.get("/update-now")
def trigger_update(background_tasks: BackgroundTasks):
    """
    Endpoint de emergencia para forzar una actualización YA.
    """
    background_tasks.add_task(run_sync_process)
    return {"message": "Actualización forzada iniciada en segundo plano."}

@app.get("/api/v1/status")
def get_status():
    """
    Para que el frontend pueda consultar si el sistema está saludable (opcional)
    """
    return {
        "backend_status": "online",
        "last_sync": LAST_UPDATE_TIME,
        "db_provider": "Supabase"
    }