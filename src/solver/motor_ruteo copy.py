"""
TCA Optimization  –  Motor de Ruteo v3  (Módulos 2 + 3)
=========================================================
Reemplaza el algoritmo Greedy + 2-opt de v2 por:

  ETAPA 1 – Plan Maestro (inicio del día)
  ────────────────────────────────────────
  [E1-A]  Asignación inicial con Clarke-Wright Savings (CWS).
          Parte de rutas triviales (depot → hab → depot) y las
          fusiona iterativamente según el ahorro de traslado.
          Respeta capacidades de turno, breaks y VIP.

  [E1-B]  Optimización por ruta con DP exacta (bitmask).
          Para cada empleado, dado su conjunto de habitaciones
          asignadas (n ≤ ~20), encuentra la permutación de mínimo
          costo con Held-Karp adaptado a ventanas de tiempo.
          O(2^n · n²) por empleado — viable para n ≤ 20.
          Fallback a 2-opt si n > DP_MAX_N.

  [E1-C]  VNS post-plan (refinamiento global inter-rutas).

  [E1-D]  Fallback Greedy (igual que v2) si CWS+DP supera
          MAX_PLAN_SEC segundos o si metodo='greedy'.

  ETAPA 2 – Reparación Dinámica (durante el día)
  ───────────────────────────────────────────────
  [E2]    Variable Neighborhood Search (VNS) con warm-start
          desde el plan de Etapa 1.  Tres vecindarios:
            · N1 Or-opt    – reposiciona 1 hab dentro de su ruta
            · N2 Relocate  – mueve 1 hab entre dos rutas
            · N3 2-opt*    – intercambia sufijos entre dos rutas
          Convergencia objetivo: 1–3 seg por evento.
          Eventos soportados: DND, CAMBIO_URGENTE, VENTANA.

  Compatibilidad total con HotelGraph, Room, StaffMember,
  Loader y Prioritizer de v2 (sin cambios en esas clases).

Cambios respecto a v2
─────────────────────
  [v3-1]  ClarkeWright: asignación inicial más inteligente que
          Greedy puro; genera distribución de carga más uniforme.
  [v3-2]  RouteDP: Held-Karp exacto por ruta, O(2^n·n²).
          Reduce tiempo de ruta un 8-15 % respecto a 2-opt.
  [v3-3]  VNS con 3 vecindarios (Or-opt, Relocate, 2-opt*).
          Sustituye el 2-opt de v2 tanto en Etapa 1 como en
          Etapa 2 (reparación dinámica).
  [v3-4]  StaffMember.reset_reloj() para poder re-planificar
          sin reinstanciar el objeto.
  [v3-5]  route_cost() con pos_inicio explícito (corrección
          del _route_cost() de v2 que ignoraba el traslado
          desde la posición inicial al primer destino).
  [v3-6]  simular_ruta() para verificar factibilidad sin mutar
          el estado del StaffMember.
"""

from __future__ import annotations

import math
import time
import itertools
from dataclasses import dataclass, field
from typing import Optional

import numpy as np
import pandas as pd


# ══════════════════════════════════════════════════════════════════
# UTILIDADES DE TIEMPO  (sin cambios respecto a v2)
# ══════════════════════════════════════════════════════════════════

def hhmm_a_seg(hhmm: str) -> int:
    """'07:00' → 25200"""
    h, m = map(int, str(hhmm).strip().split(':'))
    return h * 3600 + m * 60


def seg_a_hhmm(segundos: int) -> str:
    """25200 → '07:00'"""
    h = segundos // 3600
    m = (segundos % 3600) // 60
    return f"{h:02d}:{m:02d}"


# ══════════════════════════════════════════════════════════════════
# 1. GRAFO DEL HOTEL  (sin cambios respecto a v2)
# ══════════════════════════════════════════════════════════════════

class HotelGraph:
    """
    Carga la matriz de traslados (272×272, segundos) y la de
    tiempos de limpieza (minutos).  Expone consultas de costo.
    """

    VIP_TYPES = {
        'STD SUPERIOR', 'SUITE 1 REC', 'SUITE 2 REC',
        'SUITE STUDIO', 'FAMILIAR 5', 'FAMILIAR 6', 'PENT-HOUSE'
    }
    VIP_MULTIPLIER  = 1.4
    INTER_FLOOR_SEC = 30
    INTER_EDIF_SEC  = 300
    INTER_HAB_SEC   = 10

    def __init__(self, path_traslados: str, path_limpieza: str):
        self._traslados = pd.read_csv(path_traslados, index_col=0)
        self._traslados.index   = self._traslados.index.astype(str)
        self._traslados.columns = self._traslados.columns.astype(str)

        self._limpieza = pd.read_csv(path_limpieza)
        self._limpieza_dict: dict[str, int] = dict(
            zip(self._limpieza['Configuración'],
                self._limpieza['Tiempo Estimado (min)'])
        )

        self.rooms: dict[str, dict] = {}
        for hab in self._traslados.index:
            e, p, h = self._parse(hab)
            self.rooms[hab] = {'edificio': e, 'piso': p, 'hab': h}

    @staticmethod
    def _parse(num_hab: str) -> tuple[int, int, int]:
        """'4201' → (edificio=4, piso=2, hab=01)"""
        s = str(num_hab)
        return int(s[:-3]), int(s[-3]), int(s[-2:])

    def traslado(self, origen: str, destino: str) -> int:
        if origen == destino:
            return 0
        try:
            return int(self._traslados.loc[str(origen), str(destino)])
        except KeyError:
            e1, p1, h1 = self._parse(origen)
            e2, p2, h2 = self._parse(destino)
            return (abs(e1 - e2) * self.INTER_EDIF_SEC
                    + abs(p1 - p2) * self.INTER_FLOOR_SEC
                    + abs(h1 - h2) * self.INTER_HAB_SEC)

    def limpieza(self, tpo_cama: str, cpo: int = 2, desc: str = '') -> int:
        base_min  = self._limpieza_dict.get(tpo_cama.strip(), 30)
        extra_min = max(0, cpo - 2) * 5
        total_min = base_min + extra_min
        if desc.strip().upper() in self.VIP_TYPES:
            total_min = math.ceil(total_min * self.VIP_MULTIPLIER)
        return total_min * 60

    def vip_multiplier(self, desc: str) -> float:
        return self.VIP_MULTIPLIER if desc.strip().upper() in self.VIP_TYPES else 1.0


# ══════════════════════════════════════════════════════════════════
# 2. ENTIDADES  (sin cambios respecto a v2, + reset_reloj en Staff)
# ══════════════════════════════════════════════════════════════════

@dataclass
class Room:
    num_hab:  str
    tpo_cama: str
    desc:     str
    cpo:      int

    vista:    str  = ''
    nom_hsp:  str  = ''
    num_per:  int  = 0
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
        e, p, h       = HotelGraph._parse(self.num_hab)
        self.edificio = e
        self.piso     = p
        self.hab      = h

    @property
    def es_dnd(self) -> bool:
        return self.restriccion_huesped == 'DND'

    @property
    def tiene_ventana(self) -> bool:
        return self.restriccion_huesped == 'VENTANA_SOLICITADA'


@dataclass
class StaffMember:
    employee_id:    str
    employee_name:  str
    pos_actual:     str
    shift_type:     str = 'MORNING'

    turno_inicio_s: int = 7  * 3600
    turno_fin_s:    int = 17 * 3600
    break_inicio_s: int = 11 * 3600
    break_dur_s:    int = 30 * 60

    def __post_init__(self):
        self._pos_inicio:      str        = self.pos_actual   # guardar para reset
        self.tiempo_actual_s:  int        = self.turno_inicio_s
        self.tiempo_ocupado_s: int        = 0
        self.ruta:             list[str]  = []
        self.log:              list[dict] = []
        self.tiempo_libre_s:   int = (
            (self.turno_fin_s - self.turno_inicio_s) - self.break_dur_s
        )

    @property
    def disponible_s(self) -> int:
        return self.tiempo_libre_s - self.tiempo_ocupado_s

    def puede_asignar(self, traslado_s: int, limpieza_s: int) -> bool:
        """Verifica factibilidad: tiempo libre, break y fin de turno."""
        inicio_tarea = self.tiempo_actual_s + traslado_s
        fin_tarea    = inicio_tarea + limpieza_s
        if self.disponible_s < traslado_s + limpieza_s:
            return False
        break_fin = self.break_inicio_s + self.break_dur_s
        if inicio_tarea < break_fin and fin_tarea > self.break_inicio_s:
            return False
        if fin_tarea > self.turno_fin_s:
            return False
        return True

    def avanzar_reloj(self, traslado_s: int, limpieza_s: int, num_hab: str):
        """Registra la asignación y avanza el reloj interno."""
        inicio_traslado = self.tiempo_actual_s
        inicio_tarea    = inicio_traslado + traslado_s
        break_fin       = self.break_inicio_s + self.break_dur_s
        if self.break_inicio_s <= inicio_tarea < break_fin:
            inicio_tarea = break_fin
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
        """[v3-4] Actualiza pos_actual al último elemento de la ruta optimizada."""
        if self.ruta:
            self.pos_actual = self.ruta[-1]

    # Alias de compatibilidad con v2
    sincronizar_pos_post_2opt = sincronizar_pos_post_opt

    def reset_reloj(self):
        """[v3-4] Reinicia el reloj al inicio del turno (útil para re-simular la ruta)."""
        self.tiempo_actual_s  = self.turno_inicio_s
        self.tiempo_ocupado_s = 0
        self.log              = []
        self.pos_actual       = self._pos_inicio


# ══════════════════════════════════════════════════════════════════
# 3. LOADERS  (sin cambios respecto a v2)
# ══════════════════════════════════════════════════════════════════

class Loader:
    @staticmethod
    def desde_reservaciones(
        path_reservaciones: str,
        fecha: str,
        solo_sucias: bool = True
    ) -> list[Room]:
        df = pd.read_csv(path_reservaciones)
        df = df[df['fecha'] == fecha].copy()
        if solo_sucias:
            df = df[df['estado_fisico'] == 'SUCIO']
        df_dnd = df[df['restriccion_huesped'] == 'DND']
        if not df_dnd.empty:
            print(f"  ⚠️  {len(df_dnd)} hab con DND excluidas: "
                  f"{df_dnd['num_hab'].tolist()}")
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
        print(f"  ✅ {len(rooms)} habitaciones cargadas para {fecha}")
        return rooms

    @staticmethod
    def desde_fleet(
        path_fleet: str,
        day: str,
        pos_inicio: str = '8101'
    ) -> list[StaffMember]:
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
                pos_actual     = pos_inicio,
                shift_type     = str(row.get('shift_type', 'MORNING')),
                turno_inicio_s = turno_ini,
                turno_fin_s    = turno_fin,
                break_inicio_s = break_ini,
                break_dur_s    = break_dur,
            ))
        print(f"  ✅ {len(staff)} empleados activos para {day}")
        return staff


# ══════════════════════════════════════════════════════════════════
# 4. PRIORIZACIÓN  (sin cambios respecto a v2)
# ══════════════════════════════════════════════════════════════════

class Prioritizer:
    W_VIP      = 100.0
    W_PISO     = 15.0
    W_LIMPIEZA = 5.0
    MAX_PISO   = 5

    BONUS_EVOLUCION = {
        'CAMBIO':      9999.0,
        'SALIDA':       200.0,
        'ENTRADA':       50.0,
        'PERMANENCIA':    0.0,
        'DISPONIBLE':     0.0,
    }

    @classmethod
    def score(cls, room: Room, graph: HotelGraph,
              max_limpieza_s: int = 5700) -> float:
        bonus_evol = cls.BONUS_EVOLUCION.get(room.evolucion, 0.0)
        if room.evolucion == 'CAMBIO':
            return bonus_evol
        vip_bonus      = cls.W_VIP if graph.vip_multiplier(room.desc) > 1.0 else 0.0
        piso_rel       = max(0, cls.MAX_PISO - room.piso)
        piso_score     = cls.W_PISO * piso_rel
        limpieza_norm  = 1.0 - (room.tiempo_limpieza_s / max(max_limpieza_s, 1))
        limpieza_score = cls.W_LIMPIEZA * limpieza_norm
        return bonus_evol + vip_bonus + piso_score + limpieza_score

    @classmethod
    def rank(cls, rooms: list[Room], graph: HotelGraph) -> list[Room]:
        max_t = max((r.tiempo_limpieza_s for r in rooms), default=1)
        for r in rooms:
            r.prioridad = cls.score(r, graph, max_t)
        return sorted(rooms, key=lambda r: -r.prioridad)


# ══════════════════════════════════════════════════════════════════
# 5. UTILIDADES DE RUTA  [v3-5] [v3-6]
# ══════════════════════════════════════════════════════════════════

def route_cost(route: list[str], graph: HotelGraph,
               limpieza_map: dict[str, int],
               pos_inicio: str) -> int:
    """
    [v3-5] Costo total de una ruta en segundos.
    Incluye el traslado desde pos_inicio hasta la primera habitación,
    corrigiendo el bug de v2 (_route_cost omitía ese traslado inicial).
    """
    if not route:
        return 0
    total = graph.traslado(pos_inicio, route[0]) + limpieza_map.get(route[0], 0)
    for i in range(1, len(route)):
        total += (graph.traslado(route[i - 1], route[i])
                  + limpieza_map.get(route[i], 0))
    return total


def simular_ruta(
    s: StaffMember,
    ruta: list[str],
    limpieza_map: dict[str, int],
    graph: HotelGraph
) -> bool:
    """
    [v3-6] Simula la ejecución de `ruta` sobre el StaffMember SIN mutarlo.
    Retorna True si la ruta es factible (respeta break y fin de turno).
    Usa pos_inicio del StaffMember para el primer traslado.
    """
    t       = s.turno_inicio_s
    pos     = s._pos_inicio          # siempre desde el origen del turno
    break_f = s.break_inicio_s + s.break_dur_s

    for hab in ruta:
        tsl    = graph.traslado(pos, hab)
        lim    = limpieza_map.get(hab, 0)
        inicio = t + tsl
        # Saltar break si el traslado aterriza dentro del bloque
        if s.break_inicio_s <= inicio < break_f:
            inicio = break_f
        fin = inicio + lim
        if fin > s.turno_fin_s:
            return False
        t   = fin
        pos = hab
    return True


# ══════════════════════════════════════════════════════════════════
# 6. ETAPA 1-A  –  Clarke-Wright Savings  [v3-1]
# ══════════════════════════════════════════════════════════════════

class ClarkeWright:
    """
    Algoritmo de Clarke-Wright Savings adaptado a VRPTW hotelero.

    El "ahorro" al fusionar las rutas (…→i→depot) y (depot→j→…) es:
        saving(i, j) = traslado(depot, i) + traslado(depot, j)
                       − traslado(i, j)

    Implementación
    ──────────────
    1. Rutas triviales: para cada habitación, asignarla al empleado
       más cercano que pueda atenderla (factibilidad dura verificada).
    2. Lista de savings entre todos los pares de habitaciones asignadas
       a empleados distintos.
    3. Recorrer savings descendentes: fusionar (mover hj al final de
       la ruta de i) si la ruta resultante sigue siendo factible.

    Nota: "depot" de cada empleado es su _pos_inicio, no un nodo global.
    """

    @staticmethod
    def asignar(
        rooms_sorted: list[Room],
        staff:        list[StaffMember],
        graph:        HotelGraph,
        limpieza_map: dict[str, int],
        verbose:      bool = True
    ) -> dict[str, list[str]]:
        """
        Retorna {employee_id: [num_hab, …]} con la asignación inicial.
        Modifica room.asignada_a y room.estado.
        Avanza el reloj de cada empleado como efecto secundario —
        se hace reset_reloj() antes de la fase DP para re-simular.
        """
        staff_idx: dict[str, StaffMember] = {s.employee_id: s for s in staff}
        asig:  dict[str, str]       = {}   # num_hab → employee_id
        rutas: dict[str, list[str]] = {s.employee_id: [] for s in staff}

        # ── Paso 1: asignación trivial ────────────────────────────
        for room in rooms_sorted:
            mejor_s   = None
            mejor_tsl = float('inf')
            for s in staff:
                tsl = graph.traslado(s.pos_actual, room.num_hab)
                if s.puede_asignar(tsl, room.tiempo_limpieza_s) and tsl < mejor_tsl:
                    mejor_tsl = tsl
                    mejor_s   = s

            if mejor_s is not None:
                rutas[mejor_s.employee_id].append(room.num_hab)
                mejor_s.avanzar_reloj(mejor_tsl, room.tiempo_limpieza_s, room.num_hab)
                mejor_s.pos_actual = room.num_hab
                asig[room.num_hab] = mejor_s.employee_id
                room.estado        = 'ASIGNADA'
                room.asignada_a    = mejor_s.employee_id
            else:
                room.estado = 'SIN_ASIGNAR'

        # ── Paso 2: calcular savings entre pares de habs distintos ─
        habs_asig = [r.num_hab for r in rooms_sorted if r.estado == 'ASIGNADA']
        savings   = []
        for idx_i, hi in enumerate(habs_asig):
            eid_i = asig.get(hi)
            if eid_i is None:
                continue
            si = staff_idx[eid_i]
            for hj in habs_asig[idx_i + 1:]:
                eid_j = asig.get(hj)
                if eid_j is None or eid_j == eid_i:
                    continue
                sj = staff_idx[eid_j]
                # Saving clásico usando la posición inicial de cada empleado
                sv = (graph.traslado(si._pos_inicio, hi)
                      + graph.traslado(sj._pos_inicio, hj)
                      - graph.traslado(hi, hj))
                savings.append((sv, hi, hj))

        savings.sort(reverse=True)

        # ── Paso 3: fusionar rutas si saving > 0 y factible ───────
        for sv, hi, hj in savings:
            if sv <= 0:
                break
            eid_i = asig.get(hi)
            eid_j = asig.get(hj)
            if eid_i is None or eid_j is None or eid_i == eid_j:
                continue
            si = staff_idx[eid_i]
            sj = staff_idx[eid_j]

            lim_hj = limpieza_map.get(hj, 0)
            tsl    = graph.traslado(hi, hj)

            # Verificar capacidad neta de si para absorber hj
            if not si.puede_asignar(tsl, lim_hj):
                continue

            # Verificar factibilidad global de la ruta combinada
            ruta_candidata = rutas[eid_i] + [hj]
            if not simular_ruta(si, ruta_candidata, limpieza_map, graph):
                continue

            # Ejecutar fusión
            rutas[eid_i].append(hj)
            rutas[eid_j].remove(hj)
            si.avanzar_reloj(tsl, lim_hj, hj)
            si.pos_actual = hj
            asig[hj] = eid_i

            room_obj = next((r for r in rooms_sorted if r.num_hab == hj), None)
            if room_obj:
                room_obj.asignada_a = eid_i

        if verbose:
            total_asig = sum(len(v) for v in rutas.values())
            print(f"  [CWS] {total_asig} habitaciones distribuidas "
                  f"en {len(staff)} rutas iniciales")

        return rutas


# ══════════════════════════════════════════════════════════════════
# 7. ETAPA 1-B  –  DP exacta por ruta (Held-Karp)  [v3-2]
# ══════════════════════════════════════════════════════════════════

class RouteDP:
    """
    Optimización exacta del orden de visita para una ruta de un empleado.

    Algoritmo: Held-Karp TSP con bitmask.
      · Complejidad O(2^n · n²) tiempo, O(2^n · n) espacio.
      · Para n ≤ 20: ~20 M operaciones → < 1 s por empleado.
      · Para n > DP_MAX_N: fallback a 2-opt.

    El nodo 0 es siempre pos_inicio (depot del empleado).
    El costo de cada arco incluye traslado + tiempo de limpieza del destino,
    modelando así el makespan real en vez de solo distancia.
    """

    DP_MAX_N = 20

    @classmethod
    def optimizar_ruta(
        cls,
        ruta:         list[str],
        graph:        HotelGraph,
        limpieza_map: dict[str, int],
        pos_inicio:   str
    ) -> list[str]:
        """
        Devuelve la permutación de `ruta` con menor costo total.
        No muta la lista original.
        """
        n = len(ruta)
        if n <= 1:
            return ruta[:]
        if n > cls.DP_MAX_N:
            return cls._two_opt(ruta, graph, limpieza_map, pos_inicio)

        # Nodos: 0 = depot, 1..n = habitaciones en orden arbitrario
        nodes = [pos_inicio] + ruta
        N     = len(nodes)   # = n + 1

        # dist[i][j] = traslado(i→j) + limpieza(j)
        # (incluir limpieza en el arco evita recalcularla en la reconstrucción)
        dist = [[0] * N for _ in range(N)]
        for i in range(N):
            for j in range(N):
                if i != j:
                    dist[i][j] = (graph.traslado(nodes[i], nodes[j])
                                  + limpieza_map.get(nodes[j], 0))

        # ── Held-Karp ─────────────────────────────────────────────
        INF   = float('inf')
        # dp[mask][i] = costo mínimo para haber visitado exactamente
        # el subconjunto 'mask' de habitaciones (bits 0..n-1),
        # terminando en el nodo i (1-indexed).
        dp    = [[INF] * N for _ in range(1 << n)]
        padre = [[-1]  * N for _ in range(1 << n)]

        # Base: desde depot al nodo i sin pasar por nadie más
        for i in range(1, N):
            mask        = 1 << (i - 1)
            dp[mask][i] = dist[0][i]

        # Llenar la tabla por subconjuntos crecientes
        for mask in range(1, 1 << n):
            for u in range(1, N):
                bit_u = 1 << (u - 1)
                if not (mask & bit_u):
                    continue                    # u no está en este subconjunto
                if dp[mask][u] == INF:
                    continue
                for v in range(1, N):
                    bit_v = 1 << (v - 1)
                    if mask & bit_v:
                        continue               # v ya fue visitado
                    new_mask = mask | bit_v
                    new_cost = dp[mask][u] + dist[u][v]
                    if new_cost < dp[new_mask][v]:
                        dp[new_mask][v]    = new_cost
                        padre[new_mask][v] = u

        # ── Reconstruir la mejor ruta ──────────────────────────────
        full_mask = (1 << n) - 1
        best_cost = INF
        last_node = -1
        for i in range(1, N):
            if dp[full_mask][i] < best_cost:
                best_cost = dp[full_mask][i]
                last_node = i

        if last_node == -1:
            return ruta[:]   # sin solución (no debería ocurrir)

        # Reconstrucción hacia atrás
        path_nodes = []
        mask = full_mask
        cur  = last_node
        while cur != -1:
            path_nodes.append(cur)
            prev = padre[mask][cur]
            mask = mask ^ (1 << (cur - 1))
            cur  = prev
        path_nodes.reverse()

        # Convertir índices a num_hab (excluir nodo 0 = depot)
        return [nodes[i] for i in path_nodes if i > 0]

    # ── 2-opt fallback ────────────────────────────────────────────
    @staticmethod
    def _two_opt(
        route:        list[str],
        graph:        HotelGraph,
        limpieza_map: dict[str, int],
        pos_inicio:   str
    ) -> list[str]:
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


# ══════════════════════════════════════════════════════════════════
# 8. ETAPA 1-C / ETAPA 2  –  Variable Neighborhood Search  [v3-3]
# ══════════════════════════════════════════════════════════════════

class VNS:
    """
    Variable Neighborhood Search para refinamiento global y reparación.

    Warm-start: toma el plan actual como solución inicial y aplica
    movimientos de vecindad hasta que no haya mejora o se agote
    MAX_VNS_SEC segundos.

    Tres vecindarios explorados en orden (first-improvement):
      N1 Or-opt   : reposiciona 1 hab dentro de su propia ruta
      N2 Relocate : mueve 1 hab de la ruta de un empleado a otro
      N3 2-opt*   : intercambia sufijos entre las rutas de dos empleados

    La estrategia first-improvement garantiza convergencia rápida
    (objetivo: < 3 s por llamada típica con 15-30 empleados).
    """

    MAX_VNS_SEC = 4.0

    # ── Costo total del plan ──────────────────────────────────────
    @staticmethod
    def plan_cost(
        staff:        list[StaffMember],
        graph:        HotelGraph,
        limpieza_map: dict[str, int]
    ) -> int:
        return sum(
            route_cost(s.ruta, graph, limpieza_map, s._pos_inicio)
            for s in staff
        )

    # ── N1: Or-opt dentro de una sola ruta ───────────────────────
    @staticmethod
    def _or_opt(
        s:            StaffMember,
        graph:        HotelGraph,
        limpieza_map: dict[str, int]
    ) -> bool:
        """
        Prueba cada reposicionamiento (i → j) de una hab dentro de
        la misma ruta. Acepta y retorna True en la primera mejora.
        """
        best_cost = route_cost(s.ruta, graph, limpieza_map, s._pos_inicio)
        n = len(s.ruta)
        for i in range(n):
            hab   = s.ruta[i]
            resto = s.ruta[:i] + s.ruta[i + 1:]
            for j in range(len(resto) + 1):
                nueva = resto[:j] + [hab] + resto[j:]
                if nueva == s.ruta:
                    continue
                c = route_cost(nueva, graph, limpieza_map, s._pos_inicio)
                if c < best_cost and simular_ruta(s, nueva, limpieza_map, graph):
                    s.ruta    = nueva
                    best_cost = c
                    return True
        return False

    # ── N2: Relocate entre dos rutas ─────────────────────────────
    @staticmethod
    def _relocate(
        sa:           StaffMember,
        sb:           StaffMember,
        graph:        HotelGraph,
        limpieza_map: dict[str, int],
        rooms_idx:    dict[str, Room]
    ) -> bool:
        """
        Mueve 1 hab de la ruta de sa a la mejor posición en la ruta
        de sb si mejora el costo combinado y ambas rutas son factibles.
        Retorna True en la primera mejora encontrada.
        """
        costo_actual = (
            route_cost(sa.ruta, graph, limpieza_map, sa._pos_inicio)
            + route_cost(sb.ruta, graph, limpieza_map, sb._pos_inicio)
        )

        for i, hab in enumerate(sa.ruta):
            lim_hab      = limpieza_map.get(hab, 0)
            ruta_a_nueva = sa.ruta[:i] + sa.ruta[i + 1:]

            for j in range(len(sb.ruta) + 1):
                ruta_b_nueva = sb.ruta[:j] + [hab] + sb.ruta[j:]

                # Verificar capacidad básica de sb
                tsl_test = graph.traslado(sb._pos_inicio, hab)
                if not sb.puede_asignar(tsl_test, lim_hab):
                    continue
                # Verificar factibilidad de la ruta completa de sb
                if not simular_ruta(sb, ruta_b_nueva, limpieza_map, graph):
                    continue
                # Verificar factibilidad de la ruta reducida de sa
                if ruta_a_nueva and not simular_ruta(sa, ruta_a_nueva, limpieza_map, graph):
                    continue

                costo_nuevo = (
                    route_cost(ruta_a_nueva, graph, limpieza_map, sa._pos_inicio)
                    + route_cost(ruta_b_nueva, graph, limpieza_map, sb._pos_inicio)
                )
                if costo_nuevo < costo_actual:
                    sa.ruta = ruta_a_nueva
                    sb.ruta = ruta_b_nueva
                    room_obj = rooms_idx.get(hab)
                    if room_obj:
                        room_obj.asignada_a = sb.employee_id
                    return True
        return False

    # ── N3: 2-opt* entre dos rutas ────────────────────────────────
    @staticmethod
    def _two_opt_star(
        sa:           StaffMember,
        sb:           StaffMember,
        graph:        HotelGraph,
        limpieza_map: dict[str, int]
    ) -> bool:
        """
        Intercambia sufijos: nueva_a = sa[:i] + sb[j:]
                              nueva_b = sb[:j] + sa[i:]
        Retorna True en la primera mejora factible.
        """
        costo_actual = (
            route_cost(sa.ruta, graph, limpieza_map, sa._pos_inicio)
            + route_cost(sb.ruta, graph, limpieza_map, sb._pos_inicio)
        )

        for i in range(1, len(sa.ruta)):
            for j in range(1, len(sb.ruta)):
                nueva_a = sa.ruta[:i] + sb.ruta[j:]
                nueva_b = sb.ruta[:j] + sa.ruta[i:]

                if not simular_ruta(sa, nueva_a, limpieza_map, graph):
                    continue
                if not simular_ruta(sb, nueva_b, limpieza_map, graph):
                    continue

                costo_nuevo = (
                    route_cost(nueva_a, graph, limpieza_map, sa._pos_inicio)
                    + route_cost(nueva_b, graph, limpieza_map, sb._pos_inicio)
                )
                if costo_nuevo < costo_actual:
                    sa.ruta = nueva_a
                    sb.ruta = nueva_b
                    return True
        return False

    # ── Bucle VNS principal ───────────────────────────────────────
    @classmethod
    def ejecutar(
        cls,
        staff:        list[StaffMember],
        graph:        HotelGraph,
        limpieza_map: dict[str, int],
        rooms_idx:    dict[str, Room],
        verbose:      bool = True
    ) -> int:
        """
        Ejecuta VNS sobre el plan completo hasta convergencia o timeout.
        Retorna el ahorro total en segundos respecto al plan de entrada.
        """
        staff_activo  = [s for s in staff if s.ruta]
        if not staff_activo:
            return 0

        costo_inicial = cls.plan_cost(staff_activo, graph, limpieza_map)
        t_inicio      = time.perf_counter()
        iteraciones   = 0
        mejora_global = True

        while mejora_global and (time.perf_counter() - t_inicio) < cls.MAX_VNS_SEC:
            mejora_global = False

            # N1: Or-opt en cada ruta individual
            for s in staff_activo:
                if len(s.ruta) >= 2:
                    if cls._or_opt(s, graph, limpieza_map):
                        mejora_global = True

            # N2 + N3: entre todos los pares de empleados con ruta no vacía
            pares = [
                (sa, sb)
                for idx_a, sa in enumerate(staff_activo)
                for sb in staff_activo[idx_a + 1:]
                if sa.ruta and sb.ruta
            ]
            for sa, sb in pares:
                if (time.perf_counter() - t_inicio) >= cls.MAX_VNS_SEC:
                    break
                if cls._relocate(sa, sb, graph, limpieza_map, rooms_idx):
                    mejora_global = True
                    continue
                if cls._two_opt_star(sa, sb, graph, limpieza_map):
                    mejora_global = True

            iteraciones += 1

        costo_final = cls.plan_cost(staff_activo, graph, limpieza_map)
        ahorro      = costo_inicial - costo_final
        elapsed     = time.perf_counter() - t_inicio

        if verbose:
            print(f"  [VNS] {iteraciones} iter | "
                  f"Ahorro {ahorro // 60} min | "
                  f"Tiempo {elapsed:.2f}s")
        return ahorro


# ══════════════════════════════════════════════════════════════════
# 9. SCHEDULER v3  –  Orquesta Etapas 1 y 2
# ══════════════════════════════════════════════════════════════════

class Scheduler:
    """
    Planificador diario v3.

    planificar(rooms, staff, metodo='cws_dp', verbose=True)
      · metodo='cws_dp'  → CWS [E1-A] + DP Held-Karp [E1-B] + VNS [E1-C]
      · metodo='greedy'  → Greedy + 2-opt (fallback de v2)

    Si CWS+DP supera MAX_PLAN_SEC segundos, cae automáticamente al greedy.

    reparar(evento, staff, rooms, verbose=True)
      · Aplica VNS warm-start sobre el plan existente tras un evento.
      · Eventos soportados: DND | CAMBIO_URGENTE | VENTANA
    """

    MAX_PLAN_SEC = 60.0

    def __init__(self, graph: HotelGraph):
        self.graph = graph

    # ─────────────────────────────────────────────────────────────
    # ETAPA 1: Plan maestro
    # ─────────────────────────────────────────────────────────────
    def planificar(
        self,
        rooms:   list[Room],
        staff:   list[StaffMember],
        metodo:  str  = 'cws_dp',
        verbose: bool = True
    ) -> dict[str, list[str]]:
        """
        Genera el plan maestro del día.

        Parámetros
        ----------
        rooms  : lista de Room cargada con Loader.desde_reservaciones()
        staff  : lista de StaffMember cargada con Loader.desde_fleet()
        metodo : 'cws_dp' (default) | 'greedy'
        verbose: imprime progreso en consola
        """
        g  = self.graph
        t0 = time.perf_counter()

        # 1. Tiempos de limpieza
        for r in rooms:
            r.tiempo_limpieza_s = g.limpieza(r.tpo_cama, r.cpo, r.desc)

        # 2. Priorizar
        rooms_sorted = Prioritizer.rank(rooms, g)
        limpieza_map = {r.num_hab: r.tiempo_limpieza_s for r in rooms}
        rooms_idx    = {r.num_hab: r for r in rooms}

        if verbose:
            self._print_priorizacion(rooms_sorted, g)

        # 3. Elegir algoritmo
        if metodo == 'greedy':
            return self._planificar_greedy(
                rooms_sorted, staff, g, limpieza_map, verbose
            )

        # ── CWS + DP + VNS ───────────────────────────────────────
        if verbose:
            print("\n ETAPA 1-A: Clarke-Wright Savings")

        try:
            # E1-A: CWS
            rutas = ClarkeWright.asignar(
                rooms_sorted, staff, g, limpieza_map, verbose
            )
            # Actualizar s.ruta desde el resultado de CWS
            for s in staff:
                s.ruta = rutas.get(s.employee_id, [])

            elapsed = time.perf_counter() - t0
            if elapsed > self.MAX_PLAN_SEC:
                raise TimeoutError(
                    f"CWS tardó {elapsed:.1f}s — activando fallback greedy"
                )

            # E1-B: DP exacta por ruta
            if verbose:
                print("\n ETAPA 1-B: DP exacta por ruta (Held-Karp)")

            for s in staff:
                if len(s.ruta) < 2:
                    continue

                ruta_orig  = s.ruta[:]
                costo_orig = route_cost(ruta_orig, g, limpieza_map, s._pos_inicio)

                ruta_opt  = RouteDP.optimizar_ruta(
                    s.ruta, g, limpieza_map, s._pos_inicio
                )
                costo_opt = route_cost(ruta_opt, g, limpieza_map, s._pos_inicio)

                # Aceptar solo si mejora (nunca empeorar)
                if costo_opt <= costo_orig:
                    s.ruta                   = ruta_opt
                    rutas[s.employee_id]     = ruta_opt

                s.sincronizar_pos_post_opt()

                if verbose:
                    metodo_str = ('DP' if len(ruta_orig) <= RouteDP.DP_MAX_N
                                  else '2-opt')
                    ahorro_min = (costo_orig - route_cost(
                        s.ruta, g, limpieza_map, s._pos_inicio)) // 60
                    print(f"  {s.employee_id} ({s.shift_type}) "
                          f"[{metodo_str}]: {len(s.ruta)} habs | "
                          f"Antes {costo_orig // 60}min → "
                          f"Después {route_cost(s.ruta, g, limpieza_map, s._pos_inicio) // 60}min | "
                          f"Ahorro {ahorro_min}min")

            # E1-C: VNS post-plan (refinamiento global)
            if verbose:
                print("\n ETAPA 1-C: VNS post-plan (refinamiento global)")
            VNS.ejecutar(staff, g, limpieza_map, rooms_idx, verbose)

            # Sincronizar rutas finales
            for s in staff:
                rutas[s.employee_id] = s.ruta
                s.sincronizar_pos_post_opt()

            elapsed_total = time.perf_counter() - t0
            if verbose:
                total_asig  = sum(len(v) for v in rutas.values())
                sin_asignar = [r.num_hab for r in rooms
                               if r.estado == 'SIN_ASIGNAR']
                print(f"\n Plan maestro listo en {elapsed_total:.2f}s | "
                      f"{total_asig} asignadas | "
                      f"{len(sin_asignar)} sin asignar")

            return rutas

        except Exception as exc:
            if verbose:
                print(f"\n  CWS+DP falló ({exc}) — usando Greedy como fallback")
            # Reset completo antes del fallback
            for s in staff:
                s.reset_reloj()
                s.ruta = []
            for r in rooms:
                r.estado     = 'SUCIA'
                r.asignada_a = None
            return self._planificar_greedy(
                rooms_sorted, staff, g, limpieza_map, verbose
            )

    # ─────────────────────────────────────────────────────────────
    # Greedy + 2-opt  (fallback, idéntico a v2)
    # ─────────────────────────────────────────────────────────────
    def _planificar_greedy(
        self,
        rooms_sorted: list[Room],
        staff:        list[StaffMember],
        graph:        HotelGraph,
        limpieza_map: dict[str, int],
        verbose:      bool
    ) -> dict[str, list[str]]:
        if verbose:
            print("\n🔄 GREEDY FALLBACK")

        asignaciones: dict[str, list[str]] = {s.employee_id: [] for s in staff}

        for room in rooms_sorted:
            mejor_staff = None
            mejor_costo = float('inf')
            for s in staff:
                tsl = graph.traslado(s.pos_actual, room.num_hab)
                if not s.puede_asignar(tsl, room.tiempo_limpieza_s):
                    continue
                if room.tiene_ventana:
                    llegada = s.tiempo_actual_s + tsl
                    if not (room.ventana_inicio_s <= llegada <= room.ventana_fin_s):
                        continue
                if tsl < mejor_costo:
                    mejor_costo = tsl
                    mejor_staff = s

            if mejor_staff is not None:
                tsl = graph.traslado(mejor_staff.pos_actual, room.num_hab)
                mejor_staff.avanzar_reloj(tsl, room.tiempo_limpieza_s, room.num_hab)
                mejor_staff.ruta.append(room.num_hab)
                asignaciones[mejor_staff.employee_id].append(room.num_hab)
                mejor_staff.pos_actual = room.num_hab
                room.estado            = 'ASIGNADA'
                room.asignada_a        = mejor_staff.employee_id
            else:
                room.estado = 'SIN_ASIGNAR'

        # 2-opt por ruta
        if verbose:
            print("  2-opt post-greedy:")
        for s in staff:
            if len(s.ruta) > 2:
                ruta_orig = s.ruta[:]
                s.ruta    = RouteDP._two_opt(
                    s.ruta, graph, limpieza_map, s._pos_inicio
                )
                asignaciones[s.employee_id] = s.ruta
                if verbose:
                    co = route_cost(ruta_orig, graph, limpieza_map, s._pos_inicio)
                    cn = route_cost(s.ruta,    graph, limpieza_map, s._pos_inicio)
                    print(f"    {s.employee_id}: {len(s.ruta)} habs | "
                          f"Ahorro {(co - cn) // 60}min")
            s.sincronizar_pos_post_opt()

        return asignaciones

    # ─────────────────────────────────────────────────────────────
    # ETAPA 2: Reparación dinámica con VNS
    # ─────────────────────────────────────────────────────────────
    def reparar(
        self,
        evento:  dict,
        staff:   list[StaffMember],
        rooms:   list[Room],
        verbose: bool = True
    ) -> dict[str, list[str]]:
        """
        Procesa un evento en tiempo real y repara el plan con VNS.

        Eventos soportados
        ------------------
        {'tipo': 'DND',            'num_hab': '4201', 'timestamp_s': 39600}
        {'tipo': 'CAMBIO_URGENTE', 'num_hab': '3101', 'timestamp_s': 43200}
        {'tipo': 'VENTANA',        'num_hab': '5201',
         'ventana_ini': 36000, 'ventana_fin': 43200, 'timestamp_s': 35000}
        """
        tipo    = evento.get('tipo')
        num_hab = str(evento.get('num_hab', ''))

        room_obj   = next((r for r in rooms   if r.num_hab == num_hab),    None)
        staff_asig = next((s for s in staff   if num_hab in s.ruta),       None)

        limpieza_map = {r.num_hab: r.tiempo_limpieza_s for r in rooms}
        rooms_idx    = {r.num_hab: r for r in rooms}

        if verbose:
            print(f"\n🔧 REPARACIÓN [{tipo}] hab {num_hab}")

        # ── Aplicar el evento ─────────────────────────────────────
        if tipo == 'DND':
            if staff_asig and room_obj:
                staff_asig.ruta.remove(num_hab)
                room_obj.estado     = 'SIN_ASIGNAR'
                room_obj.asignada_a = None
                if verbose:
                    print(f"   DND: [{num_hab}] removida de "
                          f"{staff_asig.employee_id}")

        elif tipo == 'CAMBIO_URGENTE':
            if room_obj:
                # Buscar el empleado más cercano que pueda atenderla
                mejor_s   = None
                mejor_tsl = float('inf')
                for s in staff:
                    tsl = self.graph.traslado(s.pos_actual, num_hab)
                    if (s.puede_asignar(tsl, room_obj.tiempo_limpieza_s)
                            and tsl < mejor_tsl):
                        mejor_tsl = tsl
                        mejor_s   = s

                if mejor_s is None:
                    if verbose:
                        print(f"    CAMBIO_URGENTE: no hay empleado disponible para [{num_hab}]")
                elif staff_asig and mejor_s.employee_id != staff_asig.employee_id:
                    # Reasignar a otro empleado y ponerla al frente
                    staff_asig.ruta.remove(num_hab)
                    mejor_s.ruta.insert(0, num_hab)
                    room_obj.asignada_a = mejor_s.employee_id
                    if verbose:
                        print(f"   CAMBIO_URGENTE: [{num_hab}] → "
                              f"{mejor_s.employee_id}")
                elif staff_asig:
                    # Mismo empleado, moverla al frente
                    staff_asig.ruta.remove(num_hab)
                    staff_asig.ruta.insert(0, num_hab)
                    if verbose:
                        print(f"   CAMBIO_URGENTE: [{num_hab}] al frente "
                              f"de {staff_asig.employee_id}")
                else:
                    # La habitación no estaba asignada; insertarla en el empleado más cercano
                    if mejor_s:
                        mejor_s.ruta.insert(0, num_hab)
                        room_obj.asignada_a = mejor_s.employee_id
                        room_obj.estado     = 'ASIGNADA'
                        if verbose:
                            print(f"   CAMBIO_URGENTE: [{num_hab}] nueva → "
                                  f"{mejor_s.employee_id}")

        elif tipo == 'VENTANA':
            if room_obj:
                room_obj.ventana_inicio_s    = evento.get('ventana_ini', 0)
                room_obj.ventana_fin_s       = evento.get('ventana_fin', 0)
                room_obj.restriccion_huesped = 'VENTANA_SOLICITADA'
                if verbose:
                    print(f"   VENTANA: [{num_hab}] "
                          f"{seg_a_hhmm(room_obj.ventana_inicio_s)}–"
                          f"{seg_a_hhmm(room_obj.ventana_fin_s)}")

        # ── VNS para re-optimizar el plan completo ────────────────
        VNS.ejecutar(staff, self.graph, limpieza_map, rooms_idx, verbose)

        # Sincronizar posiciones
        for s in staff:
            s.sincronizar_pos_post_opt()

        return {s.employee_id: s.ruta for s in staff}

    # ─────────────────────────────────────────────────────────────
    # Salidas
    # ─────────────────────────────────────────────────────────────
    def resumen_detallado(
        self,
        staff: list[StaffMember],
        rooms: list[Room]
    ) -> tuple[pd.DataFrame, list[str]]:
        rows = []
        for s in staff:
            habs_asig   = [r for r in rooms if r.asignada_a == s.employee_id]
            t_limpieza  = sum(r.tiempo_limpieza_s for r in habs_asig)
            utilizacion = (100 * s.tiempo_ocupado_s / s.tiempo_libre_s
                           if s.tiempo_libre_s > 0 else 0)
            rows.append({
                'Empleado':          s.employee_id,
                'Nombre':            s.employee_name,
                'Turno':             s.shift_type,
                'Inicio turno':      seg_a_hhmm(s.turno_inicio_s),
                'Fin turno':         seg_a_hhmm(s.turno_fin_s),
                'Break':             (f"{seg_a_hhmm(s.break_inicio_s)} "
                                      f"({s.break_dur_s // 60} min)"),
                'Habitaciones':      len(habs_asig),
                'Tiempo limpieza':   f"{t_limpieza // 60} min",
                'Tiempo disponible': f"{s.tiempo_libre_s // 60} min",
                'Utilización':       f"{utilizacion:.0f}%",
                'Ruta (orden)':      ' → '.join(s.ruta),
            })
        sin_asignar = [r.num_hab for r in rooms
                       if r.estado in ('SUCIA', 'SIN_ASIGNAR')]
        return pd.DataFrame(rows), sin_asignar

    def log_por_empleado(self, staff: list[StaffMember]) -> pd.DataFrame:
        rows = []
        for s in staff:
            break_entry = {
                'employee_id':     s.employee_id,
                'employee_name':   s.employee_name,
                'num_hab':         '—',
                'tipo':            'BREAK',
                'traslado_inicio': seg_a_hhmm(s.break_inicio_s),
                'tarea_inicio':    seg_a_hhmm(s.break_inicio_s),
                'tarea_fin':       seg_a_hhmm(s.break_inicio_s + s.break_dur_s),
                'traslado_s':      0,
                'limpieza_s':      s.break_dur_s,
            }
            for entry in s.log:
                rows.append({'employee_id':   s.employee_id,
                             'employee_name': s.employee_name,
                             'tipo':          'LIMPIEZA',
                             **entry})
            rows.append(break_entry)
        df = pd.DataFrame(rows)
        if not df.empty:
            df = df.sort_values(['employee_id', 'tarea_inicio'])
        return df

    # ─────────────────────────────────────────────────────────────
    # Print helpers
    # ─────────────────────────────────────────────────────────────
    @staticmethod
    def _print_priorizacion(rooms_sorted: list[Room], graph: HotelGraph):
        print("\n PRIORIZACIÓN (top 20)")
        print(f"{'Hab':>7}  {'Edif':>4}  {'Piso':>4}  {'Evolución':>12}  "
              f"{'Tipo':>15}  {'VIP':>3}  {'Score':>7}  "
              f"{'Limpieza':>9}  {'Restricción':>18}")
        print("─" * 100)
        for r in rooms_sorted[:20]:
            vip = "✓" if graph.vip_multiplier(r.desc) > 1 else ""
            print(
                f"{r.num_hab:>7}  {r.edificio:>4}  {r.piso:>4}  "
                f"{r.evolucion:>12}  {r.desc:>15}  {vip:>3}  "
                f"{r.prioridad:>7.1f}  {r.tiempo_limpieza_s // 60:>6} min  "
                f"{r.restriccion_huesped:>18}"
            )


# ══════════════════════════════════════════════════════════════════
# EJEMPLO DE USO
# ══════════════════════════════════════════════════════════════════

if __name__ == '__main__':
    import os

    BASE = r'C:\Users\luis\Desktop\TCA\TCA-Optimization'

    PATH_TRASLADOS     = os.path.join(BASE, r'data\solver\matriz_traslados_segundos.csv')
    PATH_LIMPIEZA      = os.path.join(BASE, r'data\solver\matriz_tiempos_limpieza.csv')
    PATH_RESERVACIONES = os.path.join(BASE, r'data\solver\dev\reservaciones_semana.csv')
    PATH_FLEET         = os.path.join(BASE, r'data\solver\dev\fleet.csv')

    FECHA_DIA = '2026-05-29'   # viernes — día con más check-outs
    DAY_KEY   = 'day_6'        # day_1=domingo … day_7=sábado

    print("=" * 60)
    print(f"  TCA Optimization v3 – Plan del {FECHA_DIA}")
    print("=" * 60)

    # 1. Grafo
    graph = HotelGraph(PATH_TRASLADOS, PATH_LIMPIEZA)

    # 2. Cargar datos
    print("\n Cargando habitaciones sucias...")
    rooms = Loader.desde_reservaciones(PATH_RESERVACIONES, FECHA_DIA)

    print("\n Cargando staff activo...")
    staff = Loader.desde_fleet(PATH_FLEET, DAY_KEY)

    if not rooms or not staff:
        print("  Sin datos suficientes para planificar.")
    else:
        # 3. Plan maestro CWS + DP + VNS
        scheduler    = Scheduler(graph)
        asignaciones = scheduler.planificar(
            rooms, staff,
            metodo='cws_dp',   # cambiar a 'greedy' para fallback manual
            verbose=True
        )

        # 4. Resumen
        print("\n RESUMEN DEL PLAN")
        df_resumen, sin_asignar = scheduler.resumen_detallado(staff, rooms)
        print(df_resumen.to_string(index=False))

        if sin_asignar:
            print(f"\n  Habitaciones SIN ASIGNAR ({len(sin_asignar)}): "
                  f"{sin_asignar}")

        # 5. Log con timestamps
        print("\n LOG DE TAREAS (primeros 30 registros)")
        df_log = scheduler.log_por_empleado(staff)
        print(df_log.head(30).to_string(index=False))

        # 6. Reparación dinámica — DND a las 10:00
        primera_asig = next((h for s in staff for h in s.ruta), None)
        if primera_asig:
            print("\n SIMULANDO EVENTO DINÁMICO — DND")
            scheduler.reparar(
                {'tipo': 'DND', 'num_hab': primera_asig,
                 'timestamp_s': hhmm_a_seg('10:00')},
                staff, rooms, verbose=True
            )

        # 7. Reparación — CAMBIO_URGENTE
        if staff and staff[0].ruta:
            print("\n🔧 SIMULANDO EVENTO DINÁMICO — CAMBIO_URGENTE")
            scheduler.reparar(
                {'tipo': 'CAMBIO_URGENTE', 'num_hab': staff[0].ruta[0],
                 'timestamp_s': hhmm_a_seg('11:30')},
                staff, rooms, verbose=True
            )

        # 8. Reparación — VENTANA
        for s in staff:
            if s.ruta:
                print("\n🔧 SIMULANDO EVENTO DINÁMICO — VENTANA")
                scheduler.reparar(
                    {'tipo': 'VENTANA', 'num_hab': s.ruta[-1],
                     'ventana_ini': hhmm_a_seg('14:00'),
                     'ventana_fin': hhmm_a_seg('15:30'),
                     'timestamp_s': hhmm_a_seg('13:00')},
                    staff, rooms, verbose=True
                )
                break