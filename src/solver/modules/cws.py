# tca_optimization/cws.py
## PARTE 1 ASIGNACION - Clarke Wright Savings

from .entities import Room
from .execution_state import ExecutionState, PlanContext
from .graph import HotelGraph
from .route_utils import _llegada_estimada, simular_ruta

class ClarkeWright:
    @staticmethod
    def _cumple_ventana(room: Room, e: ExecutionState, limpieza_map: dict[str, int], graph: HotelGraph) -> bool:
        if not room.tiene_ventana:
            return True
        llegada = _llegada_estimada(e, room.num_hab, limpieza_map, graph)
        return room.ventana_inicio_s <= llegada <= room.ventana_fin_s

    @staticmethod
    def asignar(rooms_sorted: list[Room], ctx: PlanContext, graph: HotelGraph, limpieza_map: dict[str, int], verbose: bool = True) -> dict[str, list[str]]:
        estados   = ctx.todos
        staff_idx = {e.employee_id: e for e in estados}
        asig:  dict[str, str]       = {}
        rutas: dict[str, list[str]] = {e.employee_id: [] for e in estados}

        for room in rooms_sorted:
            mejor_e   = None
            mejor_tsl = float('inf')
            for e in estados:
                tsl = graph.traslado(e.pos_actual, room.num_hab)
                if not e.puede_asignar(tsl, room.tiempo_limpieza_s):
                    continue
                if not ClarkeWright._cumple_ventana(room, e, limpieza_map, graph):
                    continue
                if tsl < mejor_tsl:
                    mejor_tsl = tsl
                    mejor_e   = e

            if mejor_e is not None:
                rutas[mejor_e.employee_id].append(room.num_hab)
                mejor_e.avanzar_reloj(mejor_tsl, room.tiempo_limpieza_s, room.num_hab)
                mejor_e.pos_actual  = room.num_hab
                asig[room.num_hab]  = mejor_e.employee_id
                room.estado         = 'ASIGNADA'
                room.asignada_a     = mejor_e.employee_id
            else:
                room.estado = 'SIN_ASIGNAR'

        habs_asig = [r.num_hab for r in rooms_sorted if r.estado == 'ASIGNADA']
        rooms_idx_local = {r.num_hab: r for r in rooms_sorted}
        savings   = []
        for idx_i, hi in enumerate(habs_asig):
            eid_i = asig.get(hi)
            if eid_i is None:
                continue
            ei = staff_idx[eid_i]
            for hj in habs_asig[idx_i + 1:]:
                eid_j = asig.get(hj)
                if eid_j is None or eid_j == eid_i:
                    continue
                ej = staff_idx[eid_j]
                sv = (graph.traslado(ei.staff.pos_inicio, hi)
                      + graph.traslado(ej.staff.pos_inicio, hj)
                      - graph.traslado(hi, hj))
                savings.append((sv, hi, hj))

        savings.sort(reverse=True)

        for sv, hi, hj in savings:
            if sv <= 0:
                break
            eid_i = asig.get(hi)
            eid_j = asig.get(hj)
            if eid_i is None or eid_j is None or eid_i == eid_j:
                continue
            ei = staff_idx[eid_i]
            ej = staff_idx[eid_j]

            room_hj = rooms_idx_local.get(hj)
            lim_hj  = limpieza_map.get(hj, 0)
            tsl     = graph.traslado(hi, hj)
            if not ei.puede_asignar(tsl, lim_hj):
                continue
            ruta_candidata = rutas[eid_i] + [hj]
            if not simular_ruta(ei, ruta_candidata, limpieza_map, graph):
                continue
            if room_hj and not ClarkeWright._cumple_ventana(room_hj, ei, limpieza_map, graph):
                continue

            rutas[eid_i].append(hj)
            rutas[eid_j].remove(hj)
            ei.avanzar_reloj(tsl, lim_hj, hj)
            ei.pos_actual = hj
            asig[hj]      = eid_i
            if room_hj:
                room_hj.asignada_a = eid_i

        if verbose:
            total_asig = sum(len(v) for v in rutas.values())
            print(f"  [CWS] {total_asig} habitaciones distribuidas en {len(estados)} rutas iniciales")

        return rutas