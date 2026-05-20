# tca_optimization/prioritizer.py
## Manejo VIP, y cambios entre cuartos - pisos - edificios

from .entities import Room
from .graph import HotelGraph
from .config import SchedulerConfig

class Prioritizer:
    @classmethod
    def score(cls, room: Room, graph: HotelGraph, config: SchedulerConfig, max_limpieza_s: int = 5700) -> float:
        bonus_evol = config.bonus_evolucion.get(room.evolucion, 0.0)
        if room.evolucion == 'CAMBIO':
            return bonus_evol
        vip_bonus      = config.w_vip if graph.vip_multiplier(room.desc) > 1.0 else 0.0
        piso_rel       = max(0, config.max_piso - room.piso)
        piso_score     = config.w_piso * piso_rel
        limpieza_norm  = 1.0 - (room.tiempo_limpieza_s / max(max_limpieza_s, 1))
        limpieza_score = config.w_limpieza * limpieza_norm
        return bonus_evol + vip_bonus + piso_score + limpieza_score

    @classmethod
    def rank(cls, rooms: list[Room], graph: HotelGraph, config: SchedulerConfig) -> list[Room]:
        max_t = max((r.tiempo_limpieza_s for r in rooms), default=1)
        for r in rooms:
            r.prioridad = cls.score(r, graph, config, max_t)
        return sorted(rooms, key=lambda r: -r.prioridad)