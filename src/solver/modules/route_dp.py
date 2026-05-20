# tca_optimization/route_dp.py
## PARTE 2 RUTEO - Programacion dinamica

import time
import os
from concurrent.futures import ProcessPoolExecutor, as_completed
from .config import SchedulerConfig
from .execution_state import PlanContext
from .graph import HotelGraph
from .metrics import MetricsCollector
from .route_utils import route_cost

def _dp_worker(args: tuple) -> tuple[str, list[str], int, int]:
    employee_id, ruta, dist, nodes = args
    n = len(ruta)
    INF   = 10**9
    dp    = [[INF] * (n + 1) for _ in range(1 << n)]
    padre = [[-1]  * (n + 1) for _ in range(1 << n)]

    for i in range(1, n + 1):
        dp[1 << (i - 1)][i] = dist[0][i]

    for mask in range(1, 1 << n):
        for u in range(1, n + 1):
            if not (mask & (1 << (u - 1))):
                continue
            if dp[mask][u] == INF:
                continue
            for v in range(1, n + 1):
                if mask & (1 << (v - 1)):
                    continue
                new_mask = mask | (1 << (v - 1))
                new_cost = dp[mask][u] + dist[u][v]
                if new_cost < dp[new_mask][v]:
                    dp[new_mask][v]    = new_cost
                    padre[new_mask][v] = u

    full_mask = (1 << n) - 1
    best_cost = INF
    last_node = -1
    for i in range(1, n + 1):
        if dp[full_mask][i] < best_cost:
            best_cost = dp[full_mask][i]
            last_node = i

    if last_node == -1:
        return employee_id, ruta, INF, INF

    path_nodes = []
    mask = full_mask
    cur  = last_node
    while cur != -1:
        path_nodes.append(cur)
        prev = padre[mask][cur]
        mask = mask ^ (1 << (cur - 1))
        cur  = prev
    path_nodes.reverse()

    ruta_opt       = [nodes[i] for i in path_nodes if i > 0]
    costo_orig_real = sum(dist[i][i + 1] for i in range(n))
    return employee_id, ruta_opt, costo_orig_real, best_cost


class RouteDP:
    def __init__(self, config: SchedulerConfig | None = None):
        self._cfg = config or SchedulerConfig()

    @property
    def dp_max_n(self) -> int:
        return self._cfg.dp_max_n

    def optimizar_todas_las_rutas(self, ctx: PlanContext, graph: HotelGraph, limpieza_map: dict[str, int], metrics: MetricsCollector, verbose: bool = True) -> dict[str, int]:
        estados_con_ruta = [(e, e.ruta[:]) for e in ctx.todos if len(e.ruta) >= 2]
        trabajos_dp   = []
        trabajos_2opt = []

        for e, ruta in estados_con_ruta:
            if len(ruta) > self.dp_max_n:
                trabajos_2opt.append((e, ruta))
                continue
            nodes = [e.staff.pos_inicio] + ruta
            N     = len(nodes)
            dist  = [[0] * N for _ in range(N)]
            for i in range(N):
                for j in range(N):
                    if i != j:
                        dist[i][j] = (graph.traslado(nodes[i], nodes[j])
                                      + limpieza_map.get(nodes[j], 0))
            trabajos_dp.append((e.employee_id, ruta, dist, nodes))

        ahorros: dict[str, int] = {}

        for e, ruta in trabajos_2opt:
            costo_orig = route_cost(ruta, graph, limpieza_map, e.staff.pos_inicio)
            ruta_opt   = self._two_opt(ruta, graph, limpieza_map, e.staff.pos_inicio)
            costo_opt  = route_cost(ruta_opt, graph, limpieza_map, e.staff.pos_inicio)
            ahorro     = costo_orig - costo_opt
            if costo_opt <= costo_orig:
                e.ruta = ruta_opt
            ahorros[e.employee_id] = ahorro
            if verbose:
                print(f"  {e.employee_id} [{e.shift_type}] [2-opt n={len(ruta)}]: {costo_orig//60}min → {costo_opt//60}min | Ahorro {ahorro//60}min")

        if not trabajos_dp:
            return ahorros

        n_workers   = min(len(trabajos_dp), os.cpu_count() or 1)
        estados_idx = {e.employee_id: e for e, _ in estados_con_ruta}
        t_dp = time.perf_counter()

        try:
            with ProcessPoolExecutor(max_workers=n_workers) as pool:
                futuros = {pool.submit(_dp_worker, args): args[0] for args in trabajos_dp}
                for futuro in as_completed(futuros):
                    eid, ruta_opt, costo_orig, costo_opt = futuro.result()
                    e = estados_idx.get(eid)
                    if e is None:
                        continue
                    ahorro = max(0, costo_orig - costo_opt)
                    if costo_opt <= costo_orig and ruta_opt:
                        e.ruta = ruta_opt
                    ahorros[eid] = ahorro
                    if verbose:
                        print(f"  {eid} [{e.shift_type}] [DP n={len(e.ruta)}]: {costo_orig//60}min → {costo_opt//60}min | Ahorro {ahorro//60}min")
        except Exception as exc:
            if verbose:
                print(f"   Pool falló ({exc}), ejecutando DP secuencial")
            for args in trabajos_dp:
                eid, ruta_opt, costo_orig, costo_opt = _dp_worker(args)
                e = estados_idx.get(eid)
                if e and costo_opt <= costo_orig and ruta_opt:
                    e.ruta = ruta_opt
                ahorros[eid] = max(0, costo_orig - costo_opt)

        elapsed_dp = time.perf_counter() - t_dp
        metrics.registrar('dp_tiempo_s', elapsed_dp)

        if verbose:
            print(f"  [DP paralela] {len(trabajos_dp)} rutas en {elapsed_dp:.2f}s ({n_workers} workers)")

        return ahorros

    @staticmethod
    def _two_opt(route: list[str], graph: HotelGraph, limpieza_map: dict[str, int], pos_inicio: str) -> list[str]:
        best      = route[:]
        best_cost = route_cost(best, graph, limpieza_map, pos_inicio)
        improved  = True
        while improved:
            improved = False
            for i in range(len(best) - 1):
                for j in range(i + 2, len(best)):
                    cand = best[:i] + best[i:j + 1][::-1] + best[j + 1:]
                    c    = route_cost(cand, graph, limpieza_map, pos_inicio)
                    if c < best_cost:
                        best, best_cost = cand, c
                        improved = True
        return best