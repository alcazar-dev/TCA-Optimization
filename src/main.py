from fastapi import FastAPI
from .database import init_db, get_connection

app = FastAPI()

@app.on_event("startup")
def startup():
    init_db()

@app.get("/universo-trabajo")
def obtener_universo():
    con = get_connection()
    # Tu Paso 1: Snapshot de inicialización
    df = con.execute("""
        SELECT h.h_num_hab, b.c_edif, b.c_piso
        FROM hothsp h
        JOIN hothab b ON h.h_num_hab = b.h_num_hab
        WHERE h.h_status = '50'
    """).df() # .df() lo convierte a DataFrame de Pandas automáticamente
    return df.to_dict(orient="records")