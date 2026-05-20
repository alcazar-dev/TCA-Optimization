import csv
import json
import os
from typing import Any


def analyze_csv(file_path: str) -> None:
    """Analiza la estructura, tipos de datos y valores únicos de un archivo CSV."""
    print(f"\n{'='*60}")
    print(f"ANÁLISIS DE CSV: {os.path.basename(file_path)}")
    print(f"{'='*60}")

    try:
        with open(file_path, mode="r", encoding="utf-8") as f:
            reader = csv.reader(f)
            header = next(reader, None)

            if not header:
                print("El archivo está vacío.")
                return

            columns = {col: [] for col in header}
            rows = list(reader)

            if not rows:
                print(f"Estructura (Columnas): {header}")
                print("No contiene filas de datos.")
                return

            for row in rows:
                for i, value in enumerate(row):
                    if i < len(header):
                        columns[header[i]].append(value.strip())

            for col in header:
                values = columns[col]
                unique_values = sorted(list(set(values)))

                types = set()
                for val in values:
                    if val == "":
                        types.add("None/Empty")
                        continue
                    try:
                        int(val)
                        types.add("int")
                    except ValueError:
                        try:
                            float(val)
                            types.add("float")
                        except ValueError:
                            types.add("str")

                print(f"\n• Columna: {col}")
                print(f"  - Tipos detectados: {', '.join(types)}")

                # Imprime solo los primeros 15 valores únicos
                if len(unique_values) > 15:
                    print(f"  - Valores únicos ({len(unique_values)}): {unique_values[:15]} ... [+ {len(unique_values) - 15} más]")
                else:
                    print(f"  - Valores únicos ({len(unique_values)}): {unique_values}")


    except Exception as e:
        print(f"Error al procesar el archivo: {e}")


def analyze_json_structure(data: Any, path: str = "root") -> None:
    """Recorre de forma recursiva el JSON para mapear su estructura y tipos de datos."""
    if isinstance(data, dict):
        print(f"• {path} -> Tipo: Dict/Objeto (Llaves: {list(data.keys())})")
        for key, value in data.items():
            analyze_json_structure(value, f"{path}.{key}")

    elif isinstance(data, list):
        print(f"• {path} -> Tipo: List/Array (Longitud: {len(data)})")
        if len(data) > 0:
            print(f"  [Muestra de estructura interna de la lista '{path}']:")
            analyze_json_structure(data[0], f"{path}[0]")

            if all(isinstance(x, (int, float, str, bool)) for x in data):
                unique_vals = sorted(list(set(data)))
                if len(unique_vals) <= 10:
                    print(f"  - Valores únicos en la lista: {unique_vals}")
    else:
        print(f"• {path} -> Tipo: {type(data).__name__} | Valor: {data}")


def analyze_json(file_path: str) -> None:
    """Carga un archivo JSON y desencadena su análisis estructural."""
    print(f"\n{'='*60}")
    print(f"ANÁLISIS DE JSON: {os.path.basename(file_path)}")
    print(f"{'='*60}")

    try:
        with open(file_path, mode="r", encoding="utf-8") as f:
            data = json.load(f)
            analyze_json_structure(data)
    except Exception as e:
        print(f"Error al procesar el archivo: {e}")


if __name__ == "__main__":
    files = [
        "C:\\Users\\luis\\Desktop\\TCA\\TCA-Optimization\\data\\habitaciones.csv",
        "C:\\Users\\luis\\Desktop\\TCA\\TCA-Optimization\\data\\hotel_staff_schedule.json",
        "C:\\Users\\luis\\Desktop\\TCA\\TCA-Optimization\\data\\reservaciones.csv",
    ]

    for file_path in files:
        if not os.path.exists(file_path):
            print(f"\n[!] Archivo no encontrado: {file_path}")
            continue

        if file_path.endswith(".csv"):
            analyze_csv(file_path)
        elif file_path.endswith(".json"):
            analyze_json(file_path)