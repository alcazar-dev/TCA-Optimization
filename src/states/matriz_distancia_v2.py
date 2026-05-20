import pandas as pd
import math

# ─────────────────────────────────────────────
# TOPOLOGÍA DEL COMPLEJO (coordenadas en metros)
# Origen: punta noroeste (edif 13, fila exterior norte)
# Eje X: oeste (0) → este / mountain (80)
# Eje Y: norte (0) → sur (30)
#
#   OCEAN (oeste) ←─────────────────────── MOUNTAIN (este)
#
#   [13][12] [  11  ] [  10  ] [   8  ] [      1      ]   ← brazo norte
#                                        [             ]   ← patio (jardín/piscina)
#   [ 6][ 5] [   4  ] [   3  ] [   2  ] [      7      ]   ← brazo sur
#
# Pares lado a lado (norte: 13 sobre 12, sur: 6 sobre 5)
# Bloques dobles (ocupan altura de dos): 11, 10, 8 / 4, 3, 2
# Fondo extendido hasta mitad del patio: 1 (norte) y 7 (sur)
# ─────────────────────────────────────────────

COORDENADAS_EDIFICIO = {
    # Brazo norte
    13: (0,   0),   # punta norte exterior (par con 12)
    12: (0,  10),   # punta norte interior (par con 13)
    11: (20, 10),   # bloque doble norte
    10: (40, 10),   # bloque doble norte (pisos 1 y 2)
     9: (50, 10),   # bloque intermedio entre 10 y 8 (2 pisos, 13 habs)
     8: (60, 10),   # esquina norte
    # Fondo
     1: (80, 10),   # fondo norte (POOL)
     7: (80, 30),   # fondo sur (GARDEN)
    # Brazo sur
     2: (60, 30),   # esquina sur
     3: (40, 30),   # bloque doble sur
     4: (20, 30),   # bloque doble sur
     6: (0,  20),   # punta sur exterior (par con 5)
     5: (0,  30),   # punta sur interior (par con 6)
}

# ─────────────────────────────────────────────
# PARÁMETROS DE TIEMPO
# ─────────────────────────────────────────────
VELOCIDAD_CAMINATA = 1.2   # metros por segundo (~4.3 km/h)
TIEMPO_PISO        = 30    # segundos por piso de diferencia
TIEMPO_HAB         = 10    # segundos por número de habitación de diferencia

# ─────────────────────────────────────────────
# LISTA DE HABITACIONES
# ─────────────────────────────────────────────
habitaciones = [
    '10101','10102','10103','10104','10105','10106','10107',
    '10201','10202','10203','10204','10205','10206','10207','10208',
    '1101','1102','1103',
    '11101','11102','11103','11104','11105','11106','11107','11108',
    '11201','11202','11203',
    '11301','11302','11303',
    '1201','1202','1203',
    '12101','12102','12103','12104','12105','12106','12107','12108',
    '12202','12203','12205',
    '12301','12302',
    '13101','13102','13103','13104','13105','13106','13107','13108',
    '13201','13202','13203','13204','13205','13206','13207','13208','13209','13210','13211',
    '13301','13302','13303','13304','13305',
    '2101','2102','2103','2104','2105','2106','2107','2108','2109','2110','2111','2112',
    '2201','2202','2203','2204','2205','2206',
    '3101','3102','3103','3104','3105','3106','3107','3108','3109','3110','3111','3112','3113','3114','3115',
    '3201','3202','3203','3204','3205','3206','3207','3208','3209','3210','3211','3212','3213','3214','3215','3216','3217','3218','3219',
    '3301','3302','3303','3304','3305','3306','3307','3308',
    '4101','4102','4103','4104','4105','4106','4107','4108','4109','4110','4111','4112','4113',
    '4201','4202','4203','4204','4205','4206','4207','4208','4209','4210','4211','4212','4213','4214','4215','4216','4217','4218',
    '4301','4302','4303','4304','4305','4306','4307','4308','4309','4310','4311',
    '5101','5102','5103','5104','5105','5106','5107','5108',
    '5201','5202','5203','5204','5205','5206','5207','5208','5209','5210','5211',
    '5301','5302',
    '6101','6102','6103','6104','6105','6106','6107','6108','6109','6110','6111','6112',
    '6201','6202','6203','6204','6205','6206','6207','6208','6209','6210','6211','6212','6213',
    '6301','6302','6303','6304','6305',
    '7101','7102','7104','7202',
    '8101','8102','8103','8104','8105','8106',
    '8201','8202','8203','8204','8205',
    '8301','8302','8304','8305','8306','8307','8308','8309','8311','8312','8314',
    '8401','8402','8403','8404','8405','8407',
    '8502','8503',
    '9101','9102','9103','9104','9105','9106','9107',
    '9201','9202','9203','9205','9206','9208',
]

# ─────────────────────────────────────────────
# PARSEO DE CÓDIGO DE HABITACIÓN
# Formato: XYZZ  donde X..=edificio, Y=piso, ZZ=hab
# ─────────────────────────────────────────────
def parse_habitacion(num_hab):
    s = str(num_hab)
    hab    = int(s[-2:])
    piso   = int(s[-3])
    edificio = int(s[:-3])
    return edificio, piso, hab

# ─────────────────────────────────────────────
# CÁLCULO DE TIEMPO DE TRASLADO
# ─────────────────────────────────────────────
def calcular_tiempo_traslado(hab1, hab2):
    e1, p1, h1 = parse_habitacion(hab1)
    e2, p2, h2 = parse_habitacion(hab2)

    if e1 == e2:
        # Mismo edificio: solo piso y número de habitación
        t_edificio = 0
    else:
        # Distancia euclidiana entre edificios (en metros)
        x1, y1 = COORDENADAS_EDIFICIO[e1]
        x2, y2 = COORDENADAS_EDIFICIO[e2]
        distancia_m = math.sqrt((x1 - x2) ** 2 + (y1 - y2) ** 2)
        t_edificio = distancia_m / VELOCIDAD_CAMINATA  # segundos

    t_piso = abs(p1 - p2) * TIEMPO_PISO
    t_hab  = abs(h1 - h2) * TIEMPO_HAB

    return round(t_edificio + t_piso + t_hab)

# ─────────────────────────────────────────────
# CONSTRUCCIÓN DE LA MATRIZ
# ─────────────────────────────────────────────
print("Calculando matriz de distancias (272 x 272)...")

matriz_datos = {}
for origen in habitaciones:
    fila = {}
    for destino in habitaciones:
        if origen == destino:
            fila[destino] = 0
        else:
            fila[destino] = calcular_tiempo_traslado(origen, destino)
    matriz_datos[origen] = fila

df_distancias = pd.DataFrame.from_dict(matriz_datos, orient='index')

# Muestra
print("\nMuestra de tiempos (segundos):")
sample_cols = ['1101', '8301', '13101', '4201', '10101']
sample_rows = ['1101', '8301', '13101', '4201', '10101']
print(df_distancias.loc[sample_rows, sample_cols].to_string())

# Validaciones básicas
print("\nValidaciones:")
print(f"  Diagonal = 0:        {(df_distancias.values.diagonal() == 0).all()}")
print(f"  Simétrica:           {df_distancias.equals(df_distancias.T)}")
print(f"  Tiempo máx (seg):    {df_distancias.values.max()}")
print(f"  Tiempo máx (min):    {df_distancias.values.max()/60:.1f}")
print(f"  Tiempo medio (seg):  {df_distancias.values[df_distancias.values > 0].mean():.1f}")

# Exportar
nombre_archivo = 'matriz_traslados_segundos.csv'
df_distancias.to_csv(nombre_archivo, encoding='utf-8-sig')
print(f"\n¡Listo! Archivo '{nombre_archivo}' generado.")