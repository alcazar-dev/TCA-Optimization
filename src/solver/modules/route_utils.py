# tca_optimization/route_utils.py
from .graph import HotelGraph
from .execution_state import ExecutionState

def route_cost(route: list[str], graph: HotelGraph, limpieza_map: dict[str, int], pos_inicio: str) -> int:
    if not route:
        return 0
    total = graph.traslado(pos_inicio, route[0]) + limpieza_map.get(route[0], 0)
    for i in range(1, len(route)):
        total += (graph.traslado(route[i - 1], route[i]) + limpieza_map.get(route[i], 0))
    return total

def simular_ruta(ctx: ExecutionState, ruta: list[str], limpieza_map: dict[str, int], graph: HotelGraph) -> bool:
    t   = ctx.staff.turno_inicio_s
    pos = ctx.staff.pos_inicio
    bf  = ctx.staff.break_fin_s
    bi  = ctx.staff.break_inicio_s

    for hab in ruta:
        tsl    = graph.traslado(pos, hab)
        lim    = limpieza_map.get(hab, 0)
        inicio = t + tsl
        if bi <= inicio < bf:
            inicio = bf
        fin = inicio + lim
        if fin > ctx.staff.turno_fin_s:
            return False
        t   = fin
        pos = hab
    return True

def _llegada_estimada(ctx: ExecutionState, num_hab: str, limpieza_map: dict[str, int], graph: HotelGraph) -> int:
    t   = ctx.staff.turno_inicio_s
    pos = ctx.staff.pos_inicio
    bf  = ctx.staff.break_fin_s
    bi  = ctx.staff.break_inicio_s

    for hab in ctx.ruta:
        tsl    = graph.traslado(pos, hab)
        lim    = limpieza_map.get(hab, 0)
        inicio = t + tsl
        if bi <= inicio < bf:
            inicio = bf
        t   = inicio + lim
        pos = hab

    tsl_final = graph.traslado(pos, num_hab)
    llegada   = t + tsl_final
    if bi <= llegada < bf:
        llegada = bf
    return llegada