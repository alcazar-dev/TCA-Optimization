# tca_optimization/loaders.py
## Datos Externos

import pandas as pd
from .entities import Room, StaffMember
from .config import hhmm_a_seg

class Loader:
    @staticmethod
    def desde_reservaciones(path_reservaciones: str, fecha: str, solo_sucias: bool = True) -> list[Room]:
        df = pd.read_csv(path_reservaciones)
        df = df[df['fecha'] == fecha].copy()
        if solo_sucias:
            df = df[df['estado_fisico'] == 'SUCIO']
        df_dnd = df[df['restriccion_huesped'] == 'DND']
        if not df_dnd.empty:
            print(f"  {len(df_dnd)} hab con DND excluidas: {df_dnd['num_hab'].tolist()}")
        df = df[df['restriccion_huesped'] != 'DND']

        rooms = []
        for _, row in df.iterrows():
            vent_ini = vent_fin = 0
            if row.get('restriccion_huesped') == 'VENTANA_SOLICITADA':
                try:
                    vent_ini = hhmm_a_seg(str(row['ventana_inicio'])[:5])
                    vent_fin = hhmm_a_seg(str(row['ventana_fin'])[:5])
                except (ValueError, TypeError):
                    pass
            rooms.append(Room(
                num_hab             = str(int(row['num_hab'])),
                tpo_cama            = str(row['tpo_cama']).strip(),
                desc                = str(row['desc']).strip(),
                cpo                 = int(row['cpo']),
                vista               = str(row.get('vista', '')),
                nom_hsp             = str(row.get('nom_hsp', '')),
                num_per             = int(row.get('num_per', 0)),
                evolucion           = str(row.get('evolucion', 'PERMANENCIA')),
                estado_fisico       = str(row.get('estado_fisico', 'SUCIO')),
                ocupacion           = str(row.get('ocupacion', 'LIBRE')),
                restriccion_huesped = str(row.get('restriccion_huesped', 'NINGUNA')),
                ventana_inicio_s    = vent_ini,
                ventana_fin_s       = vent_fin,
            ))
        print(f"  {len(rooms)} habitaciones cargadas para {fecha}")
        return rooms

    @staticmethod
    def desde_fleet(path_fleet: str, day: str, pos_inicio: str = '8101') -> list[StaffMember]:
        df = pd.read_csv(path_fleet)
        df = df[(df['day'] == day) & (df['status'] == 'WORK')].copy()
        staff = []
        for _, row in df.iterrows():
            turno_ini = hhmm_a_seg(str(row['shift_start']))
            turno_fin = hhmm_a_seg(str(row['shift_end']))
            break_ini = int(row['break_hour']) * 3600
            break_dur = int(row['break_duration_minutes']) * 60
            staff.append(StaffMember(
                employee_id    = str(row['employee_id']),
                employee_name  = str(row['employee_name']),
                pos_inicio     = pos_inicio,
                shift_type     = str(row.get('shift_type', 'MORNING')),
                turno_inicio_s = turno_ini,
                turno_fin_s    = turno_fin,
                break_inicio_s = break_ini,
                break_dur_s    = break_dur,
            ))
        print(f"  {len(staff)} empleados activos para {day}")
        return staff