import pandas as pd
import re

habitaciones = [
    '1 K', '1 K 2 SC', '1 K 3 SC', '1 K T', '1 KG', '1 KG 1 CI', '1 KG 1 IND',
    '1 KG 1 IND 1SF', '1 KG 1 SF 1 LT2', '1 KG 1IND 2SF', '1 KG 1LT2', '1 KG 1LT3',
    '1 KG 1SF', '1 KG 1SF 1LT2', '1 KG 2 IND', '1 KG 2SF', '1 KG 2SF 1LT2',
    '1 KG 3SF', '1K 2I 2SC T', '1K 2SC L2', '1K 3I 2SC', '1KG', '1KG 1I 1SF',
    '2 C M', '2 C M 1LT2', '2 C M 2LT2', '2 CM 1LT2', '2 CM 1LT3', '2 CM 2 SF',
    '2 IND', '2 IND 3S', '2 KG', '2 KG 1S', '2 KG 5 IND 2 SF', '2 M 1 IND',
    '2 M 1IND', '2 M 1SF', '2 SC 1 IND', '2 SF', '2KG 1L T', '2M 1L'
]

# Tokens de mayor a menor longitud para evitar matches parciales (LT3 antes que LT)
PESOS = [
    ('LT3', 22), ('LT2', 15), ('L2', 15),
    ('KG', 15), ('K', 15),
    ('CM', 12),
    ('IND', 8), ('CI', 8),
    ('SC', 5), ('SF', 5),
    ('LT', 15), ('L', 15),
    ('M', 12),
    ('I', 8),
    ('S', 5),
    ('T', 5),
]
PESOS_DICT = dict(PESOS)
TIEMPO_BASE = 15

# Regex que captura tanto "2 KG" como "KG" (sin número → asume 1)
TOKEN_PATTERN = re.compile(
    r'(\d+)?\s*(' + '|'.join(re.escape(t) for t, _ in PESOS) + r')\b',
    re.IGNORECASE
)

def calcular_tiempo(hab_string):
    s = hab_string.upper().strip()
    s = re.sub(r'\bC\s+M\b', 'CM', s)  # '2 C M' → '2 CM'

    tiempo_total = TIEMPO_BASE
    desglose = []

    for m in TOKEN_PATTERN.finditer(s):
        cant = int(m.group(1)) if m.group(1) else 1   # sin número → 1
        codigo = m.group(2).upper()
        peso = PESOS_DICT.get(codigo, 0)
        parcial = cant * peso
        tiempo_total += parcial
        desglose.append(f"{cant}x{codigo}({parcial}m)")

    return tiempo_total, " + ".join(desglose) if desglose else "Solo base"


datos = []
for hab in sorted(set(habitaciones)):
    tiempo, desglose = calcular_tiempo(hab)
    datos.append({'Configuración': hab, 'Tiempo Estimado (min)': tiempo, 'Desglose': desglose})

df = pd.DataFrame(datos)
print(df.to_string())

df.to_csv('matriz_tiempos_limpieza.csv', index=False, encoding='utf-8-sig')
print("\nArchivo 'matriz_tiempos_limpieza.csv' generado.")