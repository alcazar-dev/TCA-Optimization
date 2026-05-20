import pandas as pd
import random

def generate_fleet_csv_with_shifts(n_employees, filename="fleet.csv"):
    horizon_days = 30
    base_names = [
        "Miguel", "Miguelito", "Miguelón", "Migue", "Mike", 
        "Miguel Ángel", "Miguelín", "Miguelazo", "Mickey", "Miguel Arturo"
    ]

    data = []
    
    for i in range(1, n_employees + 1):
        emp_id = f"id_{i}"
        name = base_names[(i - 1) % len(base_names)]
        if i > len(base_names):
            name += f" {i}"
            
        hist_vac = random.randint(0, 5)
        
        # Asignar un turno fijo al empleado (Mañana o Tarde)
        shift_type = random.choice(["MORNING", "AFTERNOON"])
        
        # Definir los horarios basados en el turno
        if shift_type == "MORNING":
            shift_start = "07:00"
            shift_end = "17:00"
            break_min_hour = 10
            break_max_hour = 14
        else: # AFTERNOON
            shift_start = "14:00"
            shift_end = "22:00"
            break_min_hour = 16 # Descanso entre las 4 PM y las 8 PM
            break_max_hour = 20

        for day in range(1, horizon_days + 1):
            day_str = f"day_{day}"
            status_choice = random.choices(["WORK", "OFF", "VACATION"], weights=[0.75, 0.20, 0.05])[0]

            if status_choice == "WORK":
                data.append({
                    "employee_id": emp_id,
                    "employee_name": name,
                    "shift_type": shift_type,  # Nueva columna
                    "historical_vacation_used": hist_vac,
                    "day": day_str,
                    "status": status_choice,
                    "shift_start": shift_start,
                    "shift_end": shift_end,
                    "break_hour": random.randint(break_min_hour, break_max_hour),
                    "break_duration_minutes": 30
                })
            else:
                data.append({
                    "employee_id": emp_id,
                    "employee_name": name,
                    "shift_type": shift_type,
                    "historical_vacation_used": hist_vac,
                    "day": day_str,
                    "status": status_choice,
                    "shift_start": None,
                    "shift_end": None,
                    "break_hour": None,
                    "break_duration_minutes": None
                })

    df = pd.DataFrame(data)
    df.to_csv(filename, index=False)
    return filename

csv_file = generate_fleet_csv_with_shifts(15)
print(f"File generated: {csv_file}")