import duckdb
import os
import re
from difflib import SequenceMatcher

# --- CONFIGURACIÓN ---
DATA_PATH = "data/charts"
DICT_PATH = r"D:\TCA_App\Optimization\TCA-Optimization\data\dict"
OUTPUT_REPORT = "reporte_relevancia_optimizacion.txt"
SIMILARITY_THRESHOLD = 0.7

# Tablas que mencionaste como relevantes para el algoritmo
RELEVANT_TABLES = [
    'hothsp', 'hotstp', 'hothab', 'hotcth', 
    'hottra', 'hotreq', 'hotfbl', 'hotvta', 'hotest'
]

# Tu lógica de negocio para enriquecer el reporte
BUSINESS_LOGIC = {
    'hothsp': "Crítico: Define el universo de trabajo (estatus 50/10) y ventanas de tiempo (h_hra_lld).",
    'hotstp': "Semáforo: Alimenta la disponibilidad visual del sistema.",
    'hothab': "Operativo: Define ubicación física (piso/edificio) y factores de limpieza (cuna/fac_ama).",
    'hotcth': "Prioridad: Proxy de complejidad de limpieza por tipo de habitación.",
    'hotreq': "Especiales: Requerimientos específicos (R/B/E) que afectan el bloque horario.",
    'hotfbl': "Bloqueos: Define prioridad automática si el bloqueo viene de Recepción.",
    'hotvta': "Predicción: Serie temporal para entrenamiento de modelos SARIMA/Prophet.",
    'hotest': "Ground Truth: Validación de ocupación real vs disponible."
}

def get_descriptions(file_name):
    desc_map = {}
    dict_file = os.path.join(DICT_PATH, file_name.replace('.parquet', '.txt'))
    if not os.path.exists(dict_file): return desc_map
    try:
        with open(dict_file, 'r', encoding='latin-1') as f:
            for line in f:
                if '/*' in line and '*/' in line:
                    parts = line.split('/*')
                    comment = parts[1].split('*/')[0].strip()
                    words = re.findall(r'\b([a-zA-Z_]\w*)\b', parts[0])
                    if words: desc_map[words[-1]] = comment
    except Exception: pass
    return desc_map

def generar_reporte_relevante():
    if not os.path.exists(DATA_PATH): return

    files = [f for f in os.listdir(DATA_PATH) if f.endswith('.parquet') 
             and f.replace('.parquet', '').lower() in RELEVANT_TABLES]
    
    schemas, descriptions = {}, {}
    con = duckdb.connect(':memory:')

    print(f"Analizando las {len(files)} tablas clave para el VRPTW...")

    for file in files:
        t_name = file.replace('.parquet', '').lower()
        schema = con.execute(f"DESCRIBE SELECT * FROM read_parquet('{os.path.join(DATA_PATH, file)}')").df()
        schemas[t_name] = dict(zip(schema['column_name'], schema['column_type']))
        descriptions[t_name] = get_descriptions(file)

    tablas = list(schemas.keys())
    conflictos, similitudes = [], []

    for i in range(len(tablas)):
        for j in range(i + 1, len(tablas)):
            t1, t2 = tablas[i], tablas[j]
            for col1, type1 in schemas[t1].items():
                for col2, type2 in schemas[t2].items():
                    ratio = SequenceMatcher(None, col1, col2).ratio()
                    if col1 == col2 and type1 != type2:
                        conflictos.append({'col': col1, 't1': t1, 'type1': type1, 't2': t2, 'type2': type2})
                    elif ratio >= SIMILARITY_THRESHOLD and col1 != col2:
                        similitudes.append({'ratio': ratio, 't1': t1, 'col1': col1, 'type1': type1, 't2': t2, 'col2': col2, 'type2': type2})

    similitudes.sort(key=lambda x: x['ratio'])

    with open(OUTPUT_REPORT, "w", encoding="utf-8") as f:
        f.write("====================================================\n")
        f.write("REPORTE FILTRADO: COLUMNAS RELEVANTES PARA OPTIMIZACIÓN\n")
        f.write("====================================================\n\n")

        f.write("RESUMEN DE RELEVANCIA POR TABLA:\n")
        for t, desc in BUSINESS_LOGIC.items():
            f.write(f"- {t.upper()}: {desc}\n")
        
        f.write("\n" + "="*52 + "\n\n")

        f.write(f"CONFLICTOS DE TIPO EN TABLAS CLAVE ({len(conflictos)})\n")
        for c in conflictos:
            f.write(f"COLUMNA: '{c['col']}'\n")
            f.write(f"  -> {c['t1']} ({c['type1']}): {descriptions[c['t1']].get(c['col'], 'N/A')}\n")
            f.write(f"  -> {c['t2']} ({c['type2']}): {descriptions[c['t2']].get(c['col'], 'N/A')}\n")
            f.write("."*30 + "\n")

        f.write("\n\n SIMILITUDES EN TABLAS CLAVE (Menor a Mayor)\n")
        for s in similitudes:
            f.write(f"[{int(s['ratio']*100)}%] {s['t1']}.{s['col1']} ({s['type1']}) <-> {s['t2']}.{s['col2']} ({s['type2']})\n")
            f.write(f"  Desc 1: {descriptions[s['t1']].get(s['col1'], 'N/A')}\n")
            f.write(f"  Desc 2: {descriptions[s['t2']].get(s['col2'], 'N/A')}\n")
            f.write("."*30 + "\n")

    print(f" Reporte de relevancia generado: {OUTPUT_REPORT}")

if __name__ == "__main__":
    generar_reporte_relevante()