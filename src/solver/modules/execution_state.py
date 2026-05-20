# tca_optimization/execution_state.py
## Memoria de los empleados

from typing import Optional
from .entities import StaffMember, Room
from .config import seg_a_hhmm

class ExecutionState:
    def __init__(self, staff: StaffMember):
        self.staff             = staff
        self.tiempo_actual_s   = staff.turno_inicio_s
        self.tiempo_ocupado_s  = 0
        self.pos_actual:  str        = staff.pos_inicio
        self.ruta:        list[str]  = []
        self.log:         list[dict] = []

    @property
    def employee_id(self) -> str:    return self.staff.employee_id
    @property
    def employee_name(self) -> str:  return self.staff.employee_name
    @property
    def shift_type(self) -> str:     return self.staff.shift_type
    @property
    def turno_inicio_s(self) -> int: return self.staff.turno_inicio_s
    @property
    def turno_fin_s(self) -> int:    return self.staff.turno_fin_s
    @property
    def break_inicio_s(self) -> int: return self.staff.break_inicio_s
    @property
    def break_dur_s(self) -> int:    return self.staff.break_dur_s
    @property
    def break_fin_s(self) -> int:    return self.staff.break_fin_s

    @property
    def disponible_s(self) -> int:
        return self.staff.tiempo_libre_s - self.tiempo_ocupado_s

    def puede_asignar(self, traslado_s: int, limpieza_s: int) -> bool:
        inicio_tarea = self.tiempo_actual_s + traslado_s
        fin_tarea    = inicio_tarea + limpieza_s
        if self.disponible_s < traslado_s + limpieza_s:
            return False
        if inicio_tarea < self.break_fin_s and fin_tarea > self.staff.break_inicio_s:
            return False
        if fin_tarea > self.staff.turno_fin_s:
            return False
        return True

    def avanzar_reloj(self, traslado_s: int, limpieza_s: int, num_hab: str):
        inicio_traslado = self.tiempo_actual_s
        inicio_tarea    = inicio_traslado + traslado_s
        if self.staff.break_inicio_s <= inicio_tarea < self.break_fin_s:
            inicio_tarea = self.break_fin_s
        fin_tarea = inicio_tarea + limpieza_s
        self.log.append({
            'num_hab':         num_hab,
            'traslado_inicio': seg_a_hhmm(inicio_traslado),
            'tarea_inicio':    seg_a_hhmm(inicio_tarea),
            'tarea_fin':       seg_a_hhmm(fin_tarea),
            'traslado_s':      traslado_s,
            'limpieza_s':      limpieza_s,
        })
        self.tiempo_actual_s   = fin_tarea
        self.tiempo_ocupado_s += traslado_s + limpieza_s

    def sincronizar_pos_post_opt(self):
        if self.ruta:
            self.pos_actual = self.ruta[-1]

    def reset(self):
        self.tiempo_actual_s  = self.staff.turno_inicio_s
        self.tiempo_ocupado_s = 0
        self.pos_actual       = self.staff.pos_inicio
        self.ruta             = []
        self.log              = []


class PlanContext:
    def __init__(self, staff: list[StaffMember]):
        self._estados: dict[str, ExecutionState] = {
            s.employee_id: ExecutionState(s) for s in staff
        }

    def estado(self, employee_id: str) -> ExecutionState:
        return self._estados[employee_id]

    @property
    def todos(self) -> list[ExecutionState]:
        return list(self._estados.values())

    @property
    def activos(self) -> list[ExecutionState]:
        return [e for e in self._estados.values() if e.ruta]

    def reset(self):
        for e in self._estados.values():
            e.reset()

    def rutas(self) -> dict[str, list[str]]:
        return {eid: e.ruta[:] for eid, e in self._estados.items()}

    def sincronizar_rooms(self, rooms: list[Room]) -> None:
        asignacion: dict[str, str] = {}
        for e in self._estados.values():
            for hab in e.ruta:
                asignacion[hab] = e.employee_id

        for room in rooms:
            eid = asignacion.get(room.num_hab)
            if eid is not None:
                room.asignada_a = eid
                room.estado     = 'ASIGNADA'
            elif room.estado not in ('DND',):
                room.asignada_a = None
                room.estado     = 'SIN_ASIGNAR'