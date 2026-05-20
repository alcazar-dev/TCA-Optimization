# tca_optimization/scheduler.py
## Orquestador

import time
import pandas as pd
from .config import SchedulerConfig, seg_a_hhmm
from .entities import Room, StaffMember
from .graph import HotelGraph
from .execution_state import PlanContext
from .metrics import MetricsCollector
from .prioritizer import Prioritizer
from .route_utils import route_cost, simular_ruta
from .cws import ClarkeWright
from .route_dp import RouteDP
from .vns import VNS

class Scheduler:
    def __init__(self, graph: HotelGraph, config: SchedulerConfig | None = None):
        self.graph   = graph
        self._cfg    = config or SchedulerConfig()
        self._dp     = RouteDP(self._cfg)
        self._vns    = VNS(self._cfg)

    def _insertar_sin_asignar(self, rooms: list[Room], ctx: PlanContext, limpieza_map: dict[str, int], metrics: MetricsCollector, verbose: bool = True) -> int:
        sin_asignar = [r for r in rooms if r.estado in ('SUCIA', 'SIN_ASIGNAR') and not r.es_dnd]
        if not sin_asignar:
            return 0

        estados    = ctx.todos
        reinsertadas = 0

        for room in sin_asignar:
            lim = limpieza_map.get(room.num_hab, room.tiempo_limpieza_s)
            mejor_e        = None
            mejor_costo    = float('inf')
            mejor_pos_ins  = -1

            for e in estados:
                for pos in range(len(e.ruta) + 1):
                    ruta_candidata = e.ruta[:pos] + [room.num_hab] + e.ruta[pos:]
                    if room.tiene_ventana:
                        e_temporal_ruta = e.ruta[:]
                        e.ruta = ruta_candidata
                        cumple = ClarkeWright._cumple_ventana(room, e, limpieza_map, self.graph)
                        e.ruta = e_temporal_ruta
                        if not cumple:
                            continue
                    if not simular_ruta(e, ruta_candidata, limpieza_map, self.graph):
                        continue
                    costo_nuevo = route_cost(ruta_candidata, self.graph, limpieza_map, e.staff.pos_inicio)
                    costo_viejo = route_cost(e.ruta, self.graph, limpieza_map, e.staff.pos_inicio)
                    delta = costo_nuevo - costo_viejo
                    if delta < mejor_costo:
                        mejor_costo   = delta
                        mejor_e       = e
                        mejor_pos_ins = pos

            if mejor_e is not None:
                mejor_e.ruta.insert(mejor_pos_ins, room.num_hab)
                mejor_e.sincronizar_pos_post_opt()
                room.estado     = 'ASIGNADA'
                room.asignada_a = mejor_e.employee_id
                reinsertadas   += 1
                metrics.registrar_reparacion('REINSERCION')
                if verbose:
                    print(f"   [{room.num_hab}] reinsertada en {mejor_e.employee_id} pos={mejor_pos_ins} (delta={mejor_costo//60}min)")

        if verbose and reinsertadas:
            print(f"  [Reinserción] {reinsertadas}/{len(sin_asignar)} habitaciones reincorporadas al plan")
        return reinsertadas

    def planificar(self, rooms: list[Room], staff: list[StaffMember], metodo: str = 'cws_dp', verbose: bool = True) -> tuple[dict[str, list[str]], PlanContext, MetricsCollector]:
        g       = self.graph
        metrics = MetricsCollector(metodo=metodo)
        ctx     = PlanContext(staff)
        t0      = time.perf_counter()

        for r in rooms:
            r.tiempo_limpieza_s = g.limpieza(r.tpo_cama, r.cpo, r.desc)

        rooms_sorted = Prioritizer.rank(rooms, g, self._cfg)
        limpieza_map = {r.num_hab: r.tiempo_limpieza_s for r in rooms}
        rooms_idx    = {r.num_hab: r for r in rooms}

        if verbose:
            self._print_priorizacion(rooms_sorted, g)

        if metodo == 'greedy':
            rutas = self._planificar_greedy(rooms_sorted, ctx, g, limpieza_map, metrics, verbose)
            ctx.sincronizar_rooms(rooms)
            metrics.registrar_plan(rooms, staff)
            metrics.cerrar()
            return rutas, ctx, metrics

        if verbose:
            print("\nETAPA 1-A: Clarke-Wright Savings")

        try:
            t_cws = time.perf_counter()
            rutas = ClarkeWright.asignar(rooms_sorted, ctx, g, limpieza_map, verbose)
            metrics.registrar('cws_tiempo_s', time.perf_counter() - t_cws)

            for e in ctx.todos:
                e.ruta = rutas.get(e.employee_id, [])

            if (time.perf_counter() - t0) > self._cfg.max_plan_sec:
                raise TimeoutError("CWS excedió max_plan_sec")

            if verbose:
                print("\nETAPA 1-B: DP Held-Karp paralela")

            self._dp.optimizar_todas_las_rutas(ctx, g, limpieza_map, metrics, verbose)

            for e in ctx.todos:
                rutas[e.employee_id] = e.ruta
                e.sincronizar_pos_post_opt()

            ctx.sincronizar_rooms(rooms)

            if verbose:
                print("\nETAPA 1-C: VNS refinamiento global")

            self._vns.ejecutar(ctx, g, limpieza_map, rooms_idx, metrics, verbose)

            for e in ctx.todos:
                rutas[e.employee_id] = e.ruta
                e.sincronizar_pos_post_opt()

            ctx.sincronizar_rooms(rooms)
            metrics.registrar_plan(rooms, staff)
            metrics.cerrar()

            if verbose:
                elapsed  = time.perf_counter() - t0
                sin_asig = metrics.como_dict()['rooms_sin_asignar']
                print(f"\nPlan listo en {elapsed:.2f}s | {sum(len(v) for v in rutas.values())} asignadas | {sin_asig} sin asignar")
                print(metrics.resumen())

            return rutas, ctx, metrics

        except Exception as exc:
            if verbose:
                print(f"\n CWS+DP falló ({exc}) - usando Greedy como fallback")
            metrics.registrar('fallback_greedy', True)
            ctx.reset()
            for r in rooms:
                r.estado     = 'SUCIA'
                r.asignada_a = None
            rutas = self._planificar_greedy(rooms_sorted, ctx, g, limpieza_map, metrics, verbose)
            ctx.sincronizar_rooms(rooms)
            metrics.registrar_plan(rooms, staff)
            metrics.cerrar()
            return rutas, ctx, metrics

    def _planificar_greedy(self, rooms_sorted: list[Room], ctx: PlanContext, graph: HotelGraph, limpieza_map: dict[str, int], metrics: MetricsCollector, verbose: bool) -> dict[str, list[str]]:
        if verbose:
            print("\nGREEDY")

        estados = ctx.todos
        asig:   dict[str, list[str]] = {e.employee_id: [] for e in estados}

        for room in rooms_sorted:
            mejor_e   = None
            mejor_tsl = float('inf')
            for e in estados:
                tsl = graph.traslado(e.pos_actual, room.num_hab)
                if not e.puede_asignar(tsl, room.tiempo_limpieza_s):
                    continue
                if room.tiene_ventana:
                    llegada = e.tiempo_actual_s + tsl
                    if not (room.ventana_inicio_s <= llegada <= room.ventana_fin_s):
                        continue
                if tsl < mejor_tsl:
                    mejor_tsl = tsl
                    mejor_e   = e

            if mejor_e is not None:
                tsl = graph.traslado(mejor_e.pos_actual, room.num_hab)
                mejor_e.avanzar_reloj(tsl, room.tiempo_limpieza_s, room.num_hab)
                mejor_e.ruta.append(room.num_hab)
                asig[mejor_e.employee_id].append(room.num_hab)
                mejor_e.pos_actual = room.num_hab
                room.estado        = 'ASIGNADA'
                room.asignada_a    = mejor_e.employee_id
            else:
                room.estado = 'SIN_ASIGNAR'

        if verbose:
            print("  2-opt post-greedy:")
        for e in estados:
            if len(e.ruta) > 2:
                ruta_orig = e.ruta[:]
                e.ruta    = RouteDP._two_opt(e.ruta, graph, limpieza_map, e.staff.pos_inicio)
                asig[e.employee_id] = e.ruta
                if verbose:
                    co = route_cost(ruta_orig, graph, limpieza_map, e.staff.pos_inicio)
                    cn = route_cost(e.ruta,    graph, limpieza_map, e.staff.pos_inicio)
                    print(f"    {e.employee_id}: {len(e.ruta)} habs | Ahorro {(co-cn)//60}min")
            e.sincronizar_pos_post_opt()

        return asig

    def reparar(self, evento: dict, ctx: PlanContext, rooms: list[Room], metrics: MetricsCollector, verbose: bool = True) -> dict[str, list[str]]:
        tipo    = evento.get('tipo', '')
        num_hab = str(evento.get('num_hab', ''))
        g       = self.graph

        room_obj    = next((r for r in rooms if r.num_hab == num_hab), None)
        estado_asig = next((e for e in ctx.todos if num_hab in e.ruta), None)

        limpieza_map = {r.num_hab: r.tiempo_limpieza_s for r in rooms}
        rooms_idx    = {r.num_hab: r for r in rooms}

        metrics.registrar_reparacion(tipo)

        if verbose:
            print(f"\nREPARACIÓN [{tipo}] hab {num_hab}")

        if tipo == 'DND':
            if estado_asig and room_obj:
                estado_asig.ruta.remove(num_hab)
                room_obj.estado              = 'DND'
                room_obj.asignada_a          = None
                room_obj.restriccion_huesped = 'DND'
                if verbose:
                    print(f"  [{num_hab}] removida de {estado_asig.employee_id}")
            self._insertar_sin_asignar(rooms, ctx, limpieza_map, metrics, verbose)

        elif tipo == 'CAMBIO_URGENTE':
            if room_obj:
                mejor_e   = None
                mejor_tsl = float('inf')
                for e in ctx.todos:
                    tsl = g.traslado(e.pos_actual, num_hab)
                    if (e.puede_asignar(tsl, room_obj.tiempo_limpieza_s) and tsl < mejor_tsl):
                        mejor_tsl = tsl
                        mejor_e   = e

                if mejor_e and estado_asig and mejor_e.employee_id != estado_asig.employee_id:
                    estado_asig.ruta.remove(num_hab)
                    mejor_e.ruta.insert(0, num_hab)
                    room_obj.asignada_a = mejor_e.employee_id
                    if verbose:
                        print(f"  [{num_hab}] → {mejor_e.employee_id}")
                elif estado_asig:
                    estado_asig.ruta.remove(num_hab)
                    estado_asig.ruta.insert(0, num_hab)
                    if verbose:
                        print(f"  [{num_hab}] al frente de {estado_asig.employee_id}")
                elif mejor_e:
                    mejor_e.ruta.insert(0, num_hab)
                    room_obj.asignada_a = mejor_e.employee_id
                    room_obj.estado     = 'ASIGNADA'

        elif tipo == 'VENTANA':
            if room_obj:
                room_obj.ventana_inicio_s    = evento.get('ventana_ini', 0)
                room_obj.ventana_fin_s       = evento.get('ventana_fin', 0)
                room_obj.restriccion_huesped = 'VENTANA_SOLICITADA'
                if verbose:
                    print(f"  [{num_hab}] {seg_a_hhmm(room_obj.ventana_inicio_s)}–{seg_a_hhmm(room_obj.ventana_fin_s)}")

        self._vns.ejecutar(ctx, g, limpieza_map, rooms_idx, metrics, verbose)

        for e in ctx.todos:
            e.sincronizar_pos_post_opt()

        ctx.sincronizar_rooms(rooms)
        return ctx.rutas()

    def resumen_detallado(self, ctx: PlanContext, rooms: list[Room]) -> tuple[pd.DataFrame, list[str]]:
        rows = []
        for e in ctx.todos:
            habs_asig   = [r for r in rooms if r.asignada_a == e.employee_id]
            t_limpieza  = sum(r.tiempo_limpieza_s for r in habs_asig)
            utilizacion = (100 * e.tiempo_ocupado_s / e.staff.tiempo_libre_s if e.staff.tiempo_libre_s > 0 else 0)
            rows.append({
                'Empleado':          e.employee_id,
                'Nombre':            e.employee_name,
                'Turno':             e.shift_type,
                'Inicio turno':      seg_a_hhmm(e.staff.turno_inicio_s),
                'Fin turno':         seg_a_hhmm(e.staff.turno_fin_s),
                'Break':             f"{seg_a_hhmm(e.staff.break_inicio_s)} ({e.staff.break_dur_s // 60} min)",
                'Habitaciones':      len(habs_asig),
                'Tiempo limpieza':   f"{t_limpieza // 60} min",
                'Tiempo disponible': f"{e.staff.tiempo_libre_s // 60} min",
                'Utilización':       f"{utilizacion:.0f}%",
                'Ruta (orden)':      ' → '.join(e.ruta),
            })
        sin_asignar = [r.num_hab for r in rooms if r.estado in ('SUCIA', 'SIN_ASIGNAR')]
        return pd.DataFrame(rows), sin_asignar

    def log_por_empleado(self, ctx: PlanContext) -> pd.DataFrame:
        rows = []
        for e in ctx.todos:
            break_entry = {
                'employee_id':     e.employee_id,
                'employee_name':   e.employee_name,
                'num_hab':         '—',
                'tipo':            'BREAK',
                'traslado_inicio': seg_a_hhmm(e.staff.break_inicio_s),
                'tarea_inicio':    seg_a_hhmm(e.staff.break_inicio_s),
                'tarea_fin':       seg_a_hhmm(e.staff.break_fin_s),
                'traslado_s':      0,
                'limpieza_s':      e.staff.break_dur_s,
            }
            for entry in e.log:
                rows.append({'employee_id':   e.employee_id, 'employee_name': e.employee_name, 'tipo':          'LIMPIEZA', **entry})
            rows.append(break_entry)
        df = pd.DataFrame(rows)
        if not df.empty:
            df = df.sort_values(['employee_id', 'tarea_inicio'])
        return df

    @staticmethod
    def _print_priorizacion(rooms_sorted: list[Room], graph: HotelGraph):
        print("\nPRIORIZACIÓN (top 20)")
        print(f"{'Hab':>7}  {'Edif':>4}  {'Piso':>4}  {'Evolución':>12}  {'Tipo':>15}  {'VIP':>3}  {'Score':>7}  {'Limpieza':>9}  {'Restricción':>18}")
        print("─" * 100)
        for r in rooms_sorted[:20]:
            vip = "✓" if graph.vip_multiplier(r.desc) > 1 else ""
            print(f"{r.num_hab:>7}  {r.edificio:>4}  {r.piso:>4}  {r.evolucion:>12}  {r.desc:>15}  {vip:>3}  {r.prioridad:>7.1f}  {r.tiempo_limpieza_s // 60:>6} min  {r.restriccion_huesped:>18}")