# tca_optimization/vns.py
## PARTE 3 AJUSTE - Variable Neighborhood Search

import time
from .config import SchedulerConfig
from .entities import Room
from .execution_state import ExecutionState, PlanContext
from .graph import HotelGraph
from .metrics import MetricsCollector
from .route_utils import route_cost, simular_ruta

class VNS:
    def __init__(self, config: SchedulerConfig | None = None):
        self._cfg = config or SchedulerConfig()

    @property
    def max_vns_sec(self) -> float:
        return self._cfg.max_vns_sec

    @staticmethod
    def plan_cost(estados: list[ExecutionState], graph: HotelGraph, limpieza_map: dict[str, int]) -> int:
        return sum(route_cost(e.ruta, graph, limpieza_map, e.staff.pos_inicio) for e in estados)

    @staticmethod
    def _or_opt(e: ExecutionState, graph: HotelGraph, limpieza_map: dict[str, int]) -> bool:
        best_cost = route_cost(e.ruta, graph, limpieza_map, e.staff.pos_inicio)
        n = len(e.ruta)
        for i in range(n):
            hab   = e.ruta[i]
            resto = e.ruta[:i] + e.ruta[i + 1:]
            for j in range(len(resto) + 1):
                nueva = resto[:j] + [hab] + resto[j:]
                if nueva == e.ruta:
                    continue
                c = route_cost(nueva, graph, limpieza_map, e.staff.pos_inicio)
                if c < best_cost and simular_ruta(e, nueva, limpieza_map, graph):
                    e.ruta    = nueva
                    best_cost = c
                    return True
        return False

    @staticmethod
    def _relocate(ea: ExecutionState, eb: ExecutionState, graph: HotelGraph, limpieza_map: dict[str, int], rooms_idx: dict[str, Room]) -> bool:
        costo_actual = (route_cost(ea.ruta, graph, limpieza_map, ea.staff.pos_inicio) + route_cost(eb.ruta, graph, limpieza_map, eb.staff.pos_inicio))
        for i, hab in enumerate(ea.ruta):
            lim_hab      = limpieza_map.get(hab, 0)
            ruta_a_nueva = ea.ruta[:i] + ea.ruta[i + 1:]
            for j in range(len(eb.ruta) + 1):
                ruta_b_nueva = eb.ruta[:j] + [hab] + eb.ruta[j:]
                tsl_test = graph.traslado(eb.staff.pos_inicio, hab)
                if not eb.puede_asignar(tsl_test, lim_hab):
                    continue
                if not simular_ruta(eb, ruta_b_nueva, limpieza_map, graph):
                    continue
                if ruta_a_nueva and not simular_ruta(ea, ruta_a_nueva, limpieza_map, graph):
                    continue
                costo_nuevo = (route_cost(ruta_a_nueva, graph, limpieza_map, ea.staff.pos_inicio) + route_cost(ruta_b_nueva, graph, limpieza_map, eb.staff.pos_inicio))
                if costo_nuevo < costo_actual:
                    ea.ruta = ruta_a_nueva
                    eb.ruta = ruta_b_nueva
                    room_obj = rooms_idx.get(hab)
                    if room_obj:
                        room_obj.asignada_a = eb.employee_id
                    return True
        return False

    @staticmethod
    def _two_opt_star(ea: ExecutionState, eb: ExecutionState, graph: HotelGraph, limpieza_map: dict[str, int], rooms_idx: dict[str, Room]) -> bool:
        costo_actual = (route_cost(ea.ruta, graph, limpieza_map, ea.staff.pos_inicio) + route_cost(eb.ruta, graph, limpieza_map, eb.staff.pos_inicio))
        for i in range(1, len(ea.ruta)):
            for j in range(1, len(eb.ruta)):
                nueva_a = ea.ruta[:i] + eb.ruta[j:]
                nueva_b = eb.ruta[:j] + ea.ruta[i:]
                if not simular_ruta(ea, nueva_a, limpieza_map, graph):
                    continue
                if not simular_ruta(eb, nueva_b, limpieza_map, graph):
                    continue
                costo_nuevo = (route_cost(nueva_a, graph, limpieza_map, ea.staff.pos_inicio) + route_cost(nueva_b, graph, limpieza_map, eb.staff.pos_inicio))
                if costo_nuevo < costo_actual:
                    sufijo_a_a_eb = ea.ruta[i:]
                    sufijo_eb_a_a = eb.ruta[j:]
                    ea.ruta = nueva_a
                    eb.ruta = nueva_b
                    for hab in sufijo_a_a_eb:
                        room_obj = rooms_idx.get(hab)
                        if room_obj:
                            room_obj.asignada_a = eb.employee_id
                    for hab in sufijo_eb_a_a:
                        room_obj = rooms_idx.get(hab)
                        if room_obj:
                            room_obj.asignada_a = ea.employee_id
                    return True
        return False

    def ejecutar(self, ctx: PlanContext, graph: HotelGraph, limpieza_map: dict[str, int], rooms_idx: dict[str, Room], metrics: MetricsCollector, verbose: bool = True) -> int:
        activos = ctx.activos
        if not activos:
            return 0

        costo_inicial = self.plan_cost(activos, graph, limpieza_map)
        t_inicio      = time.perf_counter()
        iteraciones   = 0
        mejora_global = True

        while mejora_global and (time.perf_counter() - t_inicio) < self.max_vns_sec:
            mejora_global = False
            for e in activos:
                if len(e.ruta) >= 2:
                    if self._or_opt(e, graph, limpieza_map):
                        mejora_global = True

            pares = [(ea, eb) for idx_a, ea in enumerate(activos) for eb in activos[idx_a + 1:] if ea.ruta and eb.ruta]
            for ea, eb in pares:
                if (time.perf_counter() - t_inicio) >= self.max_vns_sec:
                    break
                if self._relocate(ea, eb, graph, limpieza_map, rooms_idx):
                    mejora_global = True
                    continue
                if self._two_opt_star(ea, eb, graph, limpieza_map, rooms_idx):
                    mejora_global = True

            iteraciones += 1

        costo_final = self.plan_cost(activos, graph, limpieza_map)
        ahorro      = costo_inicial - costo_final
        elapsed     = time.perf_counter() - t_inicio

        metrics.registrar('vns_iteraciones',  iteraciones)
        metrics.registrar('vns_ahorro_s',     ahorro)
        metrics.registrar('vns_tiempo_s',     elapsed)
        metrics.registrar('vns_costo_inicial', costo_inicial)
        metrics.registrar('vns_costo_final',   costo_final)

        if verbose:
            gap_pct = (ahorro / costo_inicial * 100) if costo_inicial > 0 else 0
            print(f"  [VNS] {iteraciones} iter | Ahorro {ahorro // 60} min ({gap_pct:.1f}%) | Tiempo {elapsed:.2f}s")
        return ahorro