# tca_optimization/entities.py
## Definiciones datos estaticos

from dataclasses import dataclass
from typing import Optional
from .config import parse_num_hab

@dataclass
class Room:
    num_hab:  str
    tpo_cama: str
    desc:     str
    cpo:      int
    vista:    str = ''
    nom_hsp:  str = ''
    num_per:  int = 0
    evolucion: str = 'PERMANENCIA'
    estado_fisico: str = 'SUCIO'
    ocupacion:     str = 'LIBRE'
    restriccion_huesped: str = 'NINGUNA'
    ventana_inicio_s:    int = 0
    ventana_fin_s:       int = 0
    prioridad:         float = 0.0
    tiempo_limpieza_s: int   = 0
    estado:            str   = 'SUCIA'
    asignada_a: Optional[str] = None

    def __post_init__(self):
        e, p, h       = parse_num_hab(self.num_hab)
        self.edificio = e
        self.piso     = p
        self.hab      = h

    @property
    def es_dnd(self) -> bool:
        return self.restriccion_huesped == 'DND'

    @property
    def tiene_ventana(self) -> bool:
        return self.restriccion_huesped == 'VENTANA_SOLICITADA'


@dataclass(frozen=True)
class StaffMember:
    employee_id:    str
    employee_name:  str
    pos_inicio:     str
    shift_type:     str = 'MORNING'
    turno_inicio_s: int = 7  * 3600
    turno_fin_s:    int = 17 * 3600
    break_inicio_s: int = 11 * 3600
    break_dur_s:    int = 30 * 60

    @property
    def tiempo_libre_s(self) -> int:
        return (self.turno_fin_s - self.turno_inicio_s) - self.break_dur_s

    @property
    def break_fin_s(self) -> int:
        return self.break_inicio_s + self.break_dur_s