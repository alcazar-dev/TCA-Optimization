import pandas as pd
import json

# Ruta de tu archivo CSV
ruta_csv = r'C:\Users\luis\Desktop\TCA\TCA-Optimization\data\habitaciones.csv'

try:
    df = pd.read_csv(ruta_csv)
    
    # Agregamos num_piso, num_edif y vista a las columnas requeridas
    columnas_requeridas = ['desc', 'num_hab', 'tpo_hab', 'num_piso', 'num_edif', 'vista']
    
    # Verificar que existan todas las columnas
    faltantes = [col for col in columnas_requeridas if col not in df.columns]
    if faltantes:
        print(f"Error: Faltan las siguientes columnas en el CSV: {faltantes}")
    else:
        # Rellenar posibles valores nulos para evitar que se omitan en el conteo
        df['desc'] = df['desc'].fillna('SIN_DESCRIPCION')
        df['vista'] = df['vista'].fillna('SIN_VISTA')
        
        agrupado = df.groupby('desc')
        analisis_completo = {}
        
        print("=== ANÁLISIS DETALLADO POR DESCRIPCIÓN ===\n")
        
        for desc, grupo in agrupado:
            nombre_desc = str(desc)
            total_habs = len(grupo)
            
            # 1. Obtener la lista de habitaciones
            habitaciones = grupo[['num_hab', 'tpo_hab']].to_dict(orient='records')
            
            # 2. Realizar los conteos
            conteo_pisos = grupo['num_piso'].value_counts().to_dict()
            conteo_edificios = grupo['num_edif'].value_counts().to_dict()
            conteo_vistas = grupo['vista'].value_counts().to_dict()
            
            # Guardar en el diccionario estructurado
            analisis_completo[nombre_desc] = {
                'total_habitaciones': total_habs,
                'distribucion_edificios': conteo_edificios,
                'distribucion_pisos': conteo_pisos,
                'distribucion_vistas': conteo_vistas,
                'lista_habitaciones': habitaciones
            }
            
            # 3. Imprimir el resumen en consola
            print(f"[{nombre_desc}] - Total: {total_habs} habitaciones")
            
            print("  ▶ Distribución por Edificio:")
            for edif, cant in conteo_edificios.items():
                print(f"    ├─ Edificio {edif}: {cant} habs")
                
            print("  ▶ Distribución por Piso:")
            for piso, cant in conteo_pisos.items():
                print(f"    ├─ Piso {piso}: {cant} habs")
                
            print("  ▶ Distribución por Vista:")
            for vista, cant in conteo_vistas.items():
                print(f"    ├─ {vista}: {cant} habs")
                
            print("  ▶ Listado de Cuartos (Muestra):")
            # Mostramos solo las primeras 5 para no saturar la consola
            for hab in habitaciones[:5]: 
                print(f"    ├─ Hab: {hab['num_hab']} \t| Tipo: {hab['tpo_hab']}")
            if total_habs > 5:
                print(f"    └─ ... y {total_habs - 5} habitaciones más (revisar JSON).")
                
            print("-" * 60)
            
        # 4. Exportar el análisis completo a JSON
        ruta_salida = r'C:\Users\luis\Desktop\TCA\TCA-Optimization\data\analisis_habitaciones.json'
        with open(ruta_salida, 'w', encoding='utf-8') as f:
            json.dump(analisis_completo, f, indent=4, ensure_ascii=False)
            
        print(f"\n¡Listo! El análisis detallado se ha exportado exitosamente a:\n{ruta_salida}")

except FileNotFoundError:
    print(f"No se encontró el archivo en la ruta: {ruta_csv}")
except Exception as e:
    print(f"Ocurrió un error inesperado: {e}")