"""
TCA Optimization  –  Motor de Ruteo v5  (Módulos 2 + 3)
=========================================================
Mejoras sobre v4:

  [v5-1]  Bug DND corregido: inserción de habitaciones sin asignar.
          Tras remover una habitación por DND (o cualquier causa),
          se llama a _insertar_sin_asignar() antes de VNS para que
          las habitaciones liberadas vuelvan a incorporarse en la
          ruta más barata factible. VNS luego las reubica si mejoran
          el plan global.

  [v5-2]  Ventanas de tiempo verificadas en ClarkeWright.asignar().
          Paso 1 (asignación trivial) y Paso 3 (fusión de savings)
          ahora rechazan asignaciones que violen VENTANA_SOLICITADA,
          igualando el comportamiento del fallback greedy.

  [v5-3]  Sincronización de estado con ctx.sincronizar_rooms(rooms).
          Nuevo método en PlanContext que recalcula room.asignada_a
          y room.estado a partir de ctx.rutas() tras cualquier
          optimización. Se llama automáticamente al final de
          planificar() y reparar(), y puede invocarse manualmente.
          Corrige la desincronización que dejaba _two_opt_star sin
          actualizar los metadatos de las habitaciones.

  [v5-4]  SchedulerConfig dataclass inyectable.
          Todas las constantes que antes estaban dispersas como
          atributos de clase (MAX_PLAN_SEC, DP_MAX_N, MAX_VNS_SEC,
          VIP_MULTIPLIER, pesos del Prioritizer, bonificaciones de
          evolución) están centralizadas en SchedulerConfig.
          Scheduler, RouteDP, VNS y Prioritizer aceptan un config
          opcional; si no se pasa, usan SchedulerConfig() con los
          mismos valores por defecto que v4.

  Sin cambios en: HotelGraph, Room, Loader, _dp_worker,
                  ClarkeWright._savings(), VNS._or_opt(),
                  VNS._two_opt_star(), MetricsCollector.
"""

from __future__ import annotations

import math
import time
import json
import os
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import dataclass, field
from typing import Optional

import numpy as np
import pandas as pd


# ══════════════════════════════════════════════════════════════════
# UTILIDADES DE TIEMPO
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
# [v5-4]  SchedulerConfig – constantes centralizadas
# ══════════════════════════════════════════════════════════════════

@dataclass
class SchedulerConfig:
    """
    [v5-4] Todas las constantes del motor en un único lugar.

    Inyectar en el constructor de Scheduler para sobreescribir
    cualquier valor sin tocar el código:

        cfg = SchedulerConfig(dp_max_n=12, max_vns_sec=6.0)
        scheduler = Scheduler(graph, config=cfg)
    """
    # ── Scheduler ─────────────────────────────────────────────────
    max_plan_sec: float = 60.0          # timeout fase CWS

    # ── RouteDP ───────────────────────────────────────────────────
    dp_max_n: int = 14                  # umbral Held-Karp vs 2-opt

    # ── VNS ───────────────────────────────────────────────────────
    max_vns_sec: float = 4.0            # timeout VNS por llamada

    # ── HotelGraph ────────────────────────────────────────────────
    vip_multiplier: float = 1.4
    inter_floor_sec: int = 30
    inter_edif_sec: int = 300
    inter_hab_sec: int = 10
    vip_types: frozenset = field(default_factory=lambda: frozenset({
        'STD SUPERIOR', 'SUITE 1 REC', 'SUITE 2 REC',
        'SUITE STUDIO', 'FAMILIAR 5', 'FAMILIAR 6', 'PENT-HOUSE'
    }))

    # ── Prioritizer ───────────────────────────────────────────────
    w_vip: float = 100.0
    w_piso: float = 15.0
    w_limpieza: float = 5.0
    max_piso: int = 5
    bonus_evolucion: dict = field(default_factory=lambda: {
        'CAMBIO':      9999.0,
        'SALIDA':       200.0,
        'ENTRADA':       50.0,
        'PERMANENCIA':    0.0,
        'DISPONIBLE':     0.0,
    })


# ══════════════════════════════════════════════════════════════════
# 1. GRAFO DEL HOTEL
# ══════════════════════════════════════════════════════════════════

class HotelGraph:
    """
    Carga la matriz de traslados (272×272, segundos) y la de
    tiempos de limpieza (minutos). Expone consultas de costo.
    Acepta un SchedulerConfig para sus constantes.
    """

    def __init__(
        self,
        path_traslados: str,
        path_limpieza: str,
        config: SchedulerConfig | None = None
    ):
        self._cfg = config or SchedulerConfig()

        self._traslados = pd.read_csv(path_traslados, index_col=0)
        self._traslados.index   = self._traslados.index.astype(str)
        self._traslados.columns = self._traslados.columns.astype(str)

        raw_lim = pd.read_csv(path_limpieza)
        self._limpieza_dict: dict[str, int] = dict(
            zip(raw_lim['Configuración'],
                raw_lim['Tiempo Estimado (min)'])
        )

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
            cfg = self._cfg
            e1, p1, h1 = self._parse(origen)
            e2, p2, h2 = self._parse(destino)
            return (abs(e1 - e2) * cfg.inter_edif_sec
                    + abs(p1 - p2) * cfg.inter_floor_sec
                    + abs(h1 - h2) * cfg.inter_hab_sec)

    def limpieza(self, tpo_cama: str, cpo: int = 2, desc: str = '') -> int:
        base_min  = self._limpieza_dict.get(tpo_cama.strip(), 30)
        extra_min = max(0, cpo - 2) * 5
        total_min = base_min + extra_min
        if desc.strip().upper() in self._cfg.vip_types:
            total_min = math.ceil(total_min * self._cfg.vip_multiplier)
        return total_min * 60

    def vip_multiplier(self, desc: str) -> float:
        return (self._cfg.vip_multiplier
                if desc.strip().upper() in self._cfg.vip_types else 1.0)


# ══════════════════════════════════════════════════════════════════
# 2. ENTIDADES
# ══════════════════════════════════════════════════════════════════

@dataclass
class Room:
    """Habitación sucia pendiente de limpieza."""
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


@dataclass(frozen=True)
class StaffMember:
    """
    Definición inmutable del empleado.
    El estado mutable vive en ExecutionState.
    """
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


# ──────────────────────────────────────────────────────────────────
# ExecutionState
# ──────────────────────────────────────────────────────────────────

class ExecutionState:
    """Estado mutable de un StaffMember durante una planificación."""

    def __init__(self, staff: StaffMember):
        self.staff             = staff
        self.tiempo_actual_s   = staff.turno_inicio_s
        self.tiempo_ocupado_s  = 0
        self.pos_actual:  str        = staff.pos_inicio
        self.ruta:        list[str]  = []
        self.log:         list[dict] = []

    # Alias de compatibilidad
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
    def _pos_inicio(self) -> str:    return self.staff.pos_inicio

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


# ──────────────────────────────────────────────────────────────────
# PlanContext
# ──────────────────────────────────────────────────────────────────

class PlanContext:
    """
    Gestor de estados de ejecución para un día de planificación.

    Métodos principales:
      ctx.estado(employee_id)          → ExecutionState mutable
      ctx.todos                        → lista de todos los estados
      ctx.activos                      → estados con ruta no vacía
      ctx.reset()                      → reinicia todos los estados
      ctx.rutas()                      → snapshot {eid: [habs]}
      ctx.sincronizar_rooms(rooms)     → [v5-3] recalcula room.asignada_a
                                         y room.estado a partir de las rutas
    """

    def __init__(self, staff: list[StaffMember]):
        self._estados: dict[str, ExecutionState] = {
            s.employee_id: ExecutionState(s) for s in staff
        }
        self._staff = staff

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

    # ── [v5-3] ────────────────────────────────────────────────────
    def sincronizar_rooms(self, rooms: list[Room]) -> None:
        """
        [v5-3] Recalcula room.asignada_a y room.estado a partir de
        las rutas actuales de todos los ExecutionState.

        Debe llamarse tras cualquier operación que mute las rutas
        (VNS, DP, reinserción post-DND) para mantener la coherencia
        entre el estado de ctx y los metadatos de las habitaciones.

        Complejidad: O(empleados × habs_por_ruta) — negligible.
        """
        # Índice inverso: num_hab → employee_id
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
                # No tocar habitaciones que ya tenían estado especial
                room.asignada_a = None
                room.estado     = 'SIN_ASIGNAR'


# ══════════════════════════════════════════════════════════════════
# 3. LOADERS
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
            print(f"  {len(df_dnd)} hab con DND excluidas: "
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
        print(f"  {len(rooms)} habitaciones cargadas para {fecha}")
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
                pos_inicio     = pos_inicio,
                shift_type     = str(row.get('shift_type', 'MORNING')),
                turno_inicio_s = turno_ini,
                turno_fin_s    = turno_fin,
                break_inicio_s = break_ini,
                break_dur_s    = break_dur,
            ))
        print(f"  {len(staff)} empleados activos para {day}")
        return staff


# ══════════════════════════════════════════════════════════════════
# 4. PRIORIZACIÓN
# ══════════════════════════════════════════════════════════════════

class Prioritizer:
    """
    Calcula el score de prioridad de cada habitación.
    Acepta un SchedulerConfig para sus pesos y bonificaciones.
    """

    @classmethod
    def score(
        cls,
        room: Room,
        graph: HotelGraph,
        config: SchedulerConfig,
        max_limpieza_s: int = 5700
    ) -> float:
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
    def rank(
        cls,
        rooms: list[Room],
        graph: HotelGraph,
        config: SchedulerConfig
    ) -> list[Room]:
        max_t = max((r.tiempo_limpieza_s for r in rooms), default=1)
        for r in rooms:
            r.prioridad = cls.score(r, graph, config, max_t)
        return sorted(rooms, key=lambda r: -r.prioridad)


# ══════════════════════════════════════════════════════════════════
# 5. UTILIDADES DE RUTA
# ══════════════════════════════════════════════════════════════════

def route_cost(
    route:        list[str],
    graph:        HotelGraph,
    limpieza_map: dict[str, int],
    pos_inicio:   str
) -> int:
    if not route:
        return 0
    total = graph.traslado(pos_inicio, route[0]) + limpieza_map.get(route[0], 0)
    for i in range(1, len(route)):
        total += (graph.traslado(route[i - 1], route[i])
                  + limpieza_map.get(route[i], 0))
    return total


def simular_ruta(
    ctx:          ExecutionState,
    ruta:         list[str],
    limpieza_map: dict[str, int],
    graph:        HotelGraph
) -> bool:
    """
    Simula la ejecución de `ruta` SIN mutar el ExecutionState.
    Retorna True si la ruta es factible (respeta break y fin de turno).
    """
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


def _llegada_estimada(
    ctx:          ExecutionState,
    num_hab:      str,
    limpieza_map: dict[str, int],
    graph:        HotelGraph
) -> int:
    """
    Calcula el tiempo de llegada estimado a num_hab si se insertara
    al final de la ruta actual del empleado, considerando el break.
    """
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

    # Traslado desde la última posición hasta num_hab
    tsl_final = graph.traslado(pos, num_hab)
    llegada   = t + tsl_final
    if bi <= llegada < bf:
        llegada = bf
    return llegada


# ══════════════════════════════════════════════════════════════════
# 6. ETAPA 1-A  –  Clarke-Wright Savings   [v5-2: ventanas]
# ══════════════════════════════════════════════════════════════════

class ClarkeWright:
    """
    Algoritmo de Clarke-Wright Savings adaptado a VRPTW hotelero.

    [v5-2] Verifica VENTANA_SOLICITADA en los pasos 1 y 3 para que
    la ventana de tiempo del huésped se respete desde el plan inicial,
    no solo en el fallback greedy.
    """

    @staticmethod
    def _cumple_ventana(
        room:         Room,
        e:            ExecutionState,
        limpieza_map: dict[str, int],
        graph:        HotelGraph,
        insertar_al_final: bool = True
    ) -> bool:
        """
        [v5-2] Verifica que la llegada estimada a room cumpla su
        ventana de tiempo. Siempre devuelve True si la habitación
        no tiene restricción de ventana.
        """
        if not room.tiene_ventana:
            return True
        llegada = _llegada_estimada(e, room.num_hab, limpieza_map, graph)
        return room.ventana_inicio_s <= llegada <= room.ventana_fin_s

    @staticmethod
    def asignar(
        rooms_sorted: list[Room],
        ctx:          PlanContext,
        graph:        HotelGraph,
        limpieza_map: dict[str, int],
        verbose:      bool = True
    ) -> dict[str, list[str]]:
        estados   = ctx.todos
        staff_idx = {e.employee_id: e for e in estados}
        asig:  dict[str, str]       = {}
        rutas: dict[str, list[str]] = {e.employee_id: [] for e in estados}

        # ── Paso 1: asignación trivial (con ventanas) [v5-2] ──────
        for room in rooms_sorted:
            mejor_e   = None
            mejor_tsl = float('inf')
            for e in estados:
                tsl = graph.traslado(e.pos_actual, room.num_hab)
                if not e.puede_asignar(tsl, room.tiempo_limpieza_s):
                    continue
                # [v5-2] Rechazar si viola ventana de tiempo
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

        # ── Paso 2: calcular savings ───────────────────────────────
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

        # ── Paso 3: fusionar rutas con ventana [v5-2] ─────────────
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
            # [v5-2] No fusionar si hj tiene ventana y ei no la cumple
            if room_hj and not ClarkeWright._cumple_ventana(
                room_hj, ei, limpieza_map, graph
            ):
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
            print(f"  [CWS] {total_asig} habitaciones distribuidas "
                  f"en {len(estados)} rutas iniciales")

        return rutas


# ══════════════════════════════════════════════════════════════════
# 7. ETAPA 1-B  –  DP exacta por ruta (Held-Karp) + paralela
# ══════════════════════════════════════════════════════════════════

def _dp_worker(args: tuple) -> tuple[str, list[str], int, int]:
    """
    Worker ejecutado en un subproceso separado.
    Función top-level para que pickle pueda serializarla.
    """
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
    """
    Optimización de rutas con Held-Karp paralelo.
    Acepta SchedulerConfig para dp_max_n.
    """

    def __init__(self, config: SchedulerConfig | None = None):
        self._cfg = config or SchedulerConfig()

    @property
    def dp_max_n(self) -> int:
        return self._cfg.dp_max_n

    def optimizar_todas_las_rutas(
        self,
        ctx:          PlanContext,
        graph:        HotelGraph,
        limpieza_map: dict[str, int],
        metrics:      'MetricsCollector',
        verbose:      bool = True
    ) -> dict[str, int]:
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
                print(f"  {e.employee_id} [{e.shift_type}] "
                      f"[2-opt n={len(ruta)}]: "
                      f"{costo_orig//60}min → {costo_opt//60}min | "
                      f"Ahorro {ahorro//60}min")

        if not trabajos_dp:
            return ahorros

        n_workers   = min(len(trabajos_dp), os.cpu_count() or 1)
        estados_idx = {e.employee_id: e for e, _ in estados_con_ruta}

        t_dp = time.perf_counter()
        try:
            with ProcessPoolExecutor(max_workers=n_workers) as pool:
                futuros = {
                    pool.submit(_dp_worker, args): args[0]
                    for args in trabajos_dp
                }
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
                        print(f"  {eid} [{e.shift_type}] "
                              f"[DP n={len(e.ruta)}]: "
                              f"{costo_orig//60}min → {costo_opt//60}min | "
                              f"Ahorro {ahorro//60}min")
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
            print(f"  [DP paralela] {len(trabajos_dp)} rutas en {elapsed_dp:.2f}s "
                  f"({n_workers} workers)")

        return ahorros

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
# 8. ETAPA 1-C / ETAPA 2  –  Variable Neighborhood Search
# ══════════════════════════════════════════════════════════════════

class VNS:
    """
    Variable Neighborhood Search (first-improvement).

    Tres vecindarios: N1 Or-opt, N2 Relocate, N3 2-opt*.
    Acepta SchedulerConfig para max_vns_sec.

    [v5-3] _two_opt_star actualiza room.asignada_a tras intercambiar
    sufijos entre rutas de dos empleados distintos.
    """

    def __init__(self, config: SchedulerConfig | None = None):
        self._cfg = config or SchedulerConfig()

    @property
    def max_vns_sec(self) -> float:
        return self._cfg.max_vns_sec

    @staticmethod
    def plan_cost(
        estados:      list[ExecutionState],
        graph:        HotelGraph,
        limpieza_map: dict[str, int]
    ) -> int:
        return sum(
            route_cost(e.ruta, graph, limpieza_map, e.staff.pos_inicio)
            for e in estados
        )

    @staticmethod
    def _or_opt(
        e:            ExecutionState,
        graph:        HotelGraph,
        limpieza_map: dict[str, int]
    ) -> bool:
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
    def _relocate(
        ea:           ExecutionState,
        eb:           ExecutionState,
        graph:        HotelGraph,
        limpieza_map: dict[str, int],
        rooms_idx:    dict[str, Room]
    ) -> bool:
        costo_actual = (
            route_cost(ea.ruta, graph, limpieza_map, ea.staff.pos_inicio)
            + route_cost(eb.ruta, graph, limpieza_map, eb.staff.pos_inicio)
        )
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
                costo_nuevo = (
                    route_cost(ruta_a_nueva, graph, limpieza_map, ea.staff.pos_inicio)
                    + route_cost(ruta_b_nueva, graph, limpieza_map, eb.staff.pos_inicio)
                )
                if costo_nuevo < costo_actual:
                    ea.ruta = ruta_a_nueva
                    eb.ruta = ruta_b_nueva
                    room_obj = rooms_idx.get(hab)
                    if room_obj:
                        room_obj.asignada_a = eb.employee_id
                    return True
        return False

    @staticmethod
    def _two_opt_star(
        ea:           ExecutionState,
        eb:           ExecutionState,
        graph:        HotelGraph,
        limpieza_map: dict[str, int],
        rooms_idx:    dict[str, Room]     # [v5-3] necesario para actualizar metadatos
    ) -> bool:
        """
        [v5-3] Ahora recibe rooms_idx y actualiza room.asignada_a
        para todas las habitaciones que cambian de empleado al
        intercambiar sufijos entre ea y eb.
        """
        costo_actual = (
            route_cost(ea.ruta, graph, limpieza_map, ea.staff.pos_inicio)
            + route_cost(eb.ruta, graph, limpieza_map, eb.staff.pos_inicio)
        )
        for i in range(1, len(ea.ruta)):
            for j in range(1, len(eb.ruta)):
                nueva_a = ea.ruta[:i] + eb.ruta[j:]
                nueva_b = eb.ruta[:j] + ea.ruta[i:]
                if not simular_ruta(ea, nueva_a, limpieza_map, graph):
                    continue
                if not simular_ruta(eb, nueva_b, limpieza_map, graph):
                    continue
                costo_nuevo = (
                    route_cost(nueva_a, graph, limpieza_map, ea.staff.pos_inicio)
                    + route_cost(nueva_b, graph, limpieza_map, eb.staff.pos_inicio)
                )
                if costo_nuevo < costo_actual:
                    # Sufijos que cambian de propietario
                    sufijo_a_a_eb = ea.ruta[i:]   # van de ea → eb
                    sufijo_eb_a_a = eb.ruta[j:]   # van de eb → ea
                    ea.ruta = nueva_a
                    eb.ruta = nueva_b
                    # [v5-3] Actualizar metadatos de rooms
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

    def ejecutar(
        self,
        ctx:          PlanContext,
        graph:        HotelGraph,
        limpieza_map: dict[str, int],
        rooms_idx:    dict[str, Room],
        metrics:      'MetricsCollector',
        verbose:      bool = True
    ) -> int:
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

            pares = [
                (ea, eb)
                for idx_a, ea in enumerate(activos)
                for eb in activos[idx_a + 1:]
                if ea.ruta and eb.ruta
            ]
            for ea, eb in pares:
                if (time.perf_counter() - t_inicio) >= self.max_vns_sec:
                    break
                if self._relocate(ea, eb, graph, limpieza_map, rooms_idx):
                    mejora_global = True
                    continue
                # [v5-3] Pasar rooms_idx a _two_opt_star
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
            print(f"  [VNS] {iteraciones} iter | "
                  f"Ahorro {ahorro // 60} min ({gap_pct:.1f}%) | "
                  f"Tiempo {elapsed:.2f}s")
        return ahorro


# ══════════════════════════════════════════════════════════════════
# MetricsCollector
# ══════════════════════════════════════════════════════════════════

class MetricsCollector:
    """
    Registra métricas de cada planificación y reparación.
    Sin cambios respecto a v4 salvo que reparaciones_REINSERCION
    se agrega como contador nuevo.
    """

    def __init__(self, fecha: str = '', metodo: str = 'cws_dp'):
        self._datos: dict = {
            'fecha':                fecha,
            'metodo':               metodo,
            'ts_inicio':            time.time(),
            'ts_fin':               None,
            'duracion_total_s':     None,
            'cws_tiempo_s':         None,
            'dp_tiempo_s':          None,
            'vns_tiempo_s':         None,
            'vns_iteraciones':      None,
            'vns_ahorro_s':         None,
            'vns_costo_inicial':    None,
            'vns_costo_final':      None,
            'vns_gap_pct':          None,
            'total_rooms':          None,
            'rooms_asignadas':      None,
            'rooms_sin_asignar':    None,
            'total_staff':          None,
            'reparaciones_total':          0,
            'reparaciones_DND':            0,
            'reparaciones_CAMBIO_URGENTE': 0,
            'reparaciones_VENTANA':        0,
            'reparaciones_REINSERCION':    0,   # [v5-1]
            'fallback_greedy':      False,
        }

    def registrar(self, clave: str, valor):
        if clave in self._datos:
            self._datos[clave] = valor

    def registrar_reparacion(self, tipo: str):
        self._datos['reparaciones_total'] += 1
        key = f'reparaciones_{tipo}'
        if key in self._datos:
            self._datos[key] += 1

    def registrar_plan(self, rooms: list[Room], staff: list[StaffMember]):
        sin_asig = sum(1 for r in rooms if r.estado in ('SUCIA', 'SIN_ASIGNAR'))
        self._datos['total_rooms']       = len(rooms)
        self._datos['rooms_asignadas']   = len(rooms) - sin_asig
        self._datos['rooms_sin_asignar'] = sin_asig
        self._datos['total_staff']       = len(staff)

    def cerrar(self):
        self._datos['ts_fin']           = time.time()
        self._datos['duracion_total_s'] = (
            self._datos['ts_fin'] - self._datos['ts_inicio']
        )
        if self._datos['vns_costo_inicial']:
            ci = self._datos['vns_costo_inicial']
            cf = self._datos['vns_costo_final'] or ci
            self._datos['vns_gap_pct'] = (ci - cf) / ci * 100 if ci > 0 else 0

    def resumen(self) -> str:
        d = self._datos
        lines = [
            "─" * 55,
            f"  MÉTRICAS  {d['fecha']}  [{d['metodo']}]",
            "─" * 55,
        ]
        if d['duracion_total_s'] is not None:
            lines.append(f"  Duración total:   {d['duracion_total_s']:.2f}s")
        for etapa, key in [('CWS', 'cws_tiempo_s'),
                           ('DP ', 'dp_tiempo_s'),
                           ('VNS', 'vns_tiempo_s')]:
            val = d[key]
            if val is not None:
                lines.append(f"  Tiempo {etapa}:       {val:.2f}s")
        if d['vns_ahorro_s'] is not None:
            lines.append(
                f"  VNS gap:          {d['vns_ahorro_s']//60}min "
                f"({d.get('vns_gap_pct', 0):.1f}%) "
                f"en {d['vns_iteraciones']} iter"
            )
        if d['total_rooms'] is not None:
            lines.append(
                f"  Rooms:            {d['rooms_asignadas']}/{d['total_rooms']} "
                f"asignadas  |  {d['rooms_sin_asignar']} sin asignar"
            )
        if d['reparaciones_total'] > 0:
            lines.append(
                f"  Reparaciones:     {d['reparaciones_total']} total  "
                f"(DND:{d['reparaciones_DND']}  "
                f"URGENTE:{d['reparaciones_CAMBIO_URGENTE']}  "
                f"VENTANA:{d['reparaciones_VENTANA']}  "
                f"REINSERC:{d['reparaciones_REINSERCION']})"
            )
        if d['fallback_greedy']:
            lines.append("   Fallback greedy activado")
        lines.append("─" * 55)
        return "\n".join(lines)

    def exportar_json(self, path: str):
        datos_export = {
            k: v for k, v in self._datos.items()
            if k not in ('ts_inicio', 'ts_fin')
        }
        with open(path, 'w', encoding='utf-8') as f:
            json.dump(datos_export, f, ensure_ascii=False, indent=2)
        print(f"  📄 Métricas exportadas a {path}")

    def como_dataframe(self) -> pd.DataFrame:
        return pd.DataFrame([self._datos])

    def como_dict(self) -> dict:
        return dict(self._datos)


# ══════════════════════════════════════════════════════════════════
# 9. SCHEDULER v5  –  Orquesta Etapas 1 y 2
# ══════════════════════════════════════════════════════════════════

class Scheduler:
    """
    Planificador diario v5.

    Cambios respecto a v4:
      · Acepta SchedulerConfig en el constructor [v5-4].
        Instancia RouteDP y VNS con ese mismo config.
      · _insertar_sin_asignar(): fase de reinserción post-DND [v5-1].
      · Llama ctx.sincronizar_rooms(rooms) tras cada optimización [v5-3].
      · ClarkeWright.asignar() verifica ventanas en pasos 1 y 3 [v5-2].
    """

    def __init__(
        self,
        graph:  HotelGraph,
        config: SchedulerConfig | None = None
    ):
        self.graph   = graph
        self._cfg    = config or SchedulerConfig()
        self._dp     = RouteDP(self._cfg)
        self._vns    = VNS(self._cfg)

    # ─────────────────────────────────────────────────────────────
    # [v5-1]  Reinserción de habitaciones sin asignar
    # ─────────────────────────────────────────────────────────────
    def _insertar_sin_asignar(
        self,
        rooms:        list[Room],
        ctx:          PlanContext,
        limpieza_map: dict[str, int],
        metrics:      MetricsCollector,
        verbose:      bool = True
    ) -> int:
        """
        [v5-1] Intenta insertar cada habitación SIN_ASIGNAR en la
        ruta del empleado cuyo coste incremental sea mínimo y factible.

        Se ejecuta antes de VNS en reparar() para garantizar que las
        habitaciones liberadas por un evento DND vuelvan al plan.
        VNS luego las reubica si mejoran el plan global.

        Retorna el número de habitaciones reinsertadas.
        """
        sin_asignar = [
            r for r in rooms
            if r.estado in ('SUCIA', 'SIN_ASIGNAR') and not r.es_dnd
        ]
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
                # Probar inserción en cada posición de la ruta
                for pos in range(len(e.ruta) + 1):
                    ruta_candidata = e.ruta[:pos] + [room.num_hab] + e.ruta[pos:]
                    # Verificar ventana si aplica
                    if room.tiene_ventana:
                        # Reconstruir llegada con la ruta candidata
                        e_temporal_ruta = e.ruta[:]
                        e.ruta = ruta_candidata
                        cumple = ClarkeWright._cumple_ventana(
                            room, e, limpieza_map, self.graph
                        )
                        e.ruta = e_temporal_ruta
                        if not cumple:
                            continue
                    if not simular_ruta(e, ruta_candidata, limpieza_map, self.graph):
                        continue
                    costo_nuevo = route_cost(
                        ruta_candidata, self.graph, limpieza_map, e.staff.pos_inicio
                    )
                    costo_viejo = route_cost(
                        e.ruta, self.graph, limpieza_map, e.staff.pos_inicio
                    )
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
                    print(f"   [{room.num_hab}] reinsertada en "
                          f"{mejor_e.employee_id} pos={mejor_pos_ins} "
                          f"(delta={mejor_costo//60}min)")

        if verbose and reinsertadas:
            print(f"  [Reinserción] {reinsertadas}/{len(sin_asignar)} "
                  f"habitaciones reincorporadas al plan")
        return reinsertadas

    # ─────────────────────────────────────────────────────────────
    # ETAPA 1: Plan maestro
    # ─────────────────────────────────────────────────────────────
    def planificar(
        self,
        rooms:   list[Room],
        staff:   list[StaffMember],
        metodo:  str  = 'cws_dp',
        verbose: bool = True
    ) -> tuple[dict[str, list[str]], PlanContext, MetricsCollector]:
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
            rutas = self._planificar_greedy(
                rooms_sorted, ctx, g, limpieza_map, metrics, verbose
            )
            ctx.sincronizar_rooms(rooms)   # [v5-3]
            metrics.registrar_plan(rooms, staff)
            metrics.cerrar()
            return rutas, ctx, metrics

        # ── CWS + DP + VNS ───────────────────────────────────────
        if verbose:
            print("\nETAPA 1-A: Clarke-Wright Savings")

        try:
            t_cws = time.perf_counter()
            rutas = ClarkeWright.asignar(
                rooms_sorted, ctx, g, limpieza_map, verbose
            )
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

            ctx.sincronizar_rooms(rooms)   # [v5-3]

            if verbose:
                print("\nETAPA 1-C: VNS refinamiento global")

            self._vns.ejecutar(ctx, g, limpieza_map, rooms_idx, metrics, verbose)

            for e in ctx.todos:
                rutas[e.employee_id] = e.ruta
                e.sincronizar_pos_post_opt()

            ctx.sincronizar_rooms(rooms)   # [v5-3]

            metrics.registrar_plan(rooms, staff)
            metrics.cerrar()

            if verbose:
                elapsed  = time.perf_counter() - t0
                sin_asig = metrics.como_dict()['rooms_sin_asignar']
                print(f"\nPlan listo en {elapsed:.2f}s | "
                      f"{sum(len(v) for v in rutas.values())} asignadas | "
                      f"{sin_asig} sin asignar")
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
            rutas = self._planificar_greedy(
                rooms_sorted, ctx, g, limpieza_map, metrics, verbose
            )
            ctx.sincronizar_rooms(rooms)   # [v5-3]
            metrics.registrar_plan(rooms, staff)
            metrics.cerrar()
            return rutas, ctx, metrics

    # ── Greedy fallback ───────────────────────────────────────────
    def _planificar_greedy(
        self,
        rooms_sorted: list[Room],
        ctx:          PlanContext,
        graph:        HotelGraph,
        limpieza_map: dict[str, int],
        metrics:      MetricsCollector,
        verbose:      bool
    ) -> dict[str, list[str]]:
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
                e.ruta    = RouteDP._two_opt(
                    e.ruta, graph, limpieza_map, e.staff.pos_inicio
                )
                asig[e.employee_id] = e.ruta
                if verbose:
                    co = route_cost(ruta_orig, graph, limpieza_map, e.staff.pos_inicio)
                    cn = route_cost(e.ruta,    graph, limpieza_map, e.staff.pos_inicio)
                    print(f"    {e.employee_id}: {len(e.ruta)} habs | "
                          f"Ahorro {(co-cn)//60}min")
            e.sincronizar_pos_post_opt()

        return asig

    # ─────────────────────────────────────────────────────────────
    # ETAPA 2: Reparación dinámica
    # ─────────────────────────────────────────────────────────────
    def reparar(
        self,
        evento:  dict,
        ctx:     PlanContext,
        rooms:   list[Room],
        metrics: MetricsCollector,
        verbose: bool = True
    ) -> dict[str, list[str]]:
        """
        Procesa un evento en tiempo real y repara el plan.

        [v5-1] Para DND: tras remover la habitación, llama a
        _insertar_sin_asignar() para que las rooms liberadas
        (y cualquier otra pendiente) vuelvan al plan antes de VNS.

        [v5-3] Llama ctx.sincronizar_rooms(rooms) al finalizar
        para garantizar coherencia entre rutas y metadatos.

        Eventos reconocidos:
          {'tipo': 'DND',            'num_hab': '4201'}
          {'tipo': 'CAMBIO_URGENTE', 'num_hab': '3101'}
          {'tipo': 'VENTANA',        'num_hab': '5201',
           'ventana_ini': 36000, 'ventana_fin': 43200}
        """
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
                room_obj.estado              = 'DND'   # estado especial, no reinsertar
                room_obj.asignada_a          = None
                room_obj.restriccion_huesped = 'DND'
                if verbose:
                    print(f"  [{num_hab}] removida de {estado_asig.employee_id}")

            # [v5-1] Reinsertar habitaciones sin asignar antes de VNS
            self._insertar_sin_asignar(rooms, ctx, limpieza_map, metrics, verbose)

        elif tipo == 'CAMBIO_URGENTE':
            if room_obj:
                mejor_e   = None
                mejor_tsl = float('inf')
                for e in ctx.todos:
                    tsl = g.traslado(e.pos_actual, num_hab)
                    if (e.puede_asignar(tsl, room_obj.tiempo_limpieza_s)
                            and tsl < mejor_tsl):
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
                    print(f"  [{num_hab}] "
                          f"{seg_a_hhmm(room_obj.ventana_inicio_s)}–"
                          f"{seg_a_hhmm(room_obj.ventana_fin_s)}")

        self._vns.ejecutar(ctx, g, limpieza_map, rooms_idx, metrics, verbose)

        for e in ctx.todos:
            e.sincronizar_pos_post_opt()

        ctx.sincronizar_rooms(rooms)   # [v5-3]

        return ctx.rutas()

    # ─────────────────────────────────────────────────────────────
    # Salidas
    # ─────────────────────────────────────────────────────────────
    def resumen_detallado(
        self,
        ctx:   PlanContext,
        rooms: list[Room]
    ) -> tuple[pd.DataFrame, list[str]]:
        rows = []
        for e in ctx.todos:
            habs_asig   = [r for r in rooms if r.asignada_a == e.employee_id]
            t_limpieza  = sum(r.tiempo_limpieza_s for r in habs_asig)
            utilizacion = (100 * e.tiempo_ocupado_s / e.staff.tiempo_libre_s
                           if e.staff.tiempo_libre_s > 0 else 0)
            rows.append({
                'Empleado':          e.employee_id,
                'Nombre':            e.employee_name,
                'Turno':             e.shift_type,
                'Inicio turno':      seg_a_hhmm(e.staff.turno_inicio_s),
                'Fin turno':         seg_a_hhmm(e.staff.turno_fin_s),
                'Break':             (f"{seg_a_hhmm(e.staff.break_inicio_s)} "
                                      f"({e.staff.break_dur_s // 60} min)"),
                'Habitaciones':      len(habs_asig),
                'Tiempo limpieza':   f"{t_limpieza // 60} min",
                'Tiempo disponible': f"{e.staff.tiempo_libre_s // 60} min",
                'Utilización':       f"{utilizacion:.0f}%",
                'Ruta (orden)':      ' → '.join(e.ruta),
            })
        sin_asignar = [r.num_hab for r in rooms
                       if r.estado in ('SUCIA', 'SIN_ASIGNAR')]
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
                rows.append({'employee_id':   e.employee_id,
                             'employee_name': e.employee_name,
                             'tipo':          'LIMPIEZA',
                             **entry})
            rows.append(break_entry)
        df = pd.DataFrame(rows)
        if not df.empty:
            df = df.sort_values(['employee_id', 'tarea_inicio'])
        return df

    @staticmethod
    def _print_priorizacion(rooms_sorted: list[Room], graph: HotelGraph):
        print("\n📋 PRIORIZACIÓN (top 20)")
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
    BASE = r'C:\Users\luis\Desktop\TCA\TCA-Optimization'

    PATH_TRASLADOS     = os.path.join(BASE, r'data\solver\matriz_traslados_segundos.csv')
    PATH_LIMPIEZA      = os.path.join(BASE, r'data\solver\matriz_tiempos_limpieza.csv')
    PATH_RESERVACIONES = os.path.join(BASE, r'data\solver\dev\reservaciones_semana.csv')
    PATH_FLEET         = os.path.join(BASE, r'data\solver\dev\fleet.csv')

    FECHA_DIA = '2026-05-29'
    DAY_KEY   = 'day_6'

    print("=" * 60)
    print(f"  TCA Optimization v5 – Plan del {FECHA_DIA}")
    print("=" * 60)

    # [v5-4] Config personalizable; los defaults son iguales a v4
    cfg = SchedulerConfig(
        dp_max_n    = 14,
        max_vns_sec = 4.0,
    )

    graph     = HotelGraph(PATH_TRASLADOS, PATH_LIMPIEZA, config=cfg)
    scheduler = Scheduler(graph, config=cfg)

    print("\nCargando habitaciones sucias...")
    rooms = Loader.desde_reservaciones(PATH_RESERVACIONES, FECHA_DIA)

    print("\nCargando staff activo...")
    staff = Loader.desde_fleet(PATH_FLEET, DAY_KEY)

    if not rooms or not staff:
        print("Sin datos suficientes para planificar.")
    else:
        rutas, ctx, metrics = scheduler.planificar(
            rooms, staff, metodo='cws_dp', verbose=True
        )

        print("\nRESUMEN DEL PLAN")
        df_resumen, sin_asignar = scheduler.resumen_detallado(ctx, rooms)
        print(df_resumen.to_string(index=False))

        if sin_asignar:
            print(f"\nSin asignar ({len(sin_asignar)}): {sin_asignar}")

        print("\nLOG (primeros 30 registros)")
        df_log = scheduler.log_por_empleado(ctx)
        print(df_log.head(30).to_string(index=False))

        metrics.exportar_json(
            os.path.join(BASE, f'data/solver/dev/metrics_{FECHA_DIA}.json')
        )

        # Simular DND – [v5-1] la habitación liberada se reinserta
        primera_asig = next((h for e in ctx.todos for h in e.ruta), None)
        if primera_asig:
            print("\nSIMULANDO DND")
            rutas = scheduler.reparar(
                {'tipo': 'DND', 'num_hab': primera_asig},
                ctx, rooms, metrics, verbose=True
            )

        if ctx.activos and ctx.activos[0].ruta:
            print("\nSIMULANDO CAMBIO_URGENTE")
            rutas = scheduler.reparar(
                {'tipo': 'CAMBIO_URGENTE', 'num_hab': ctx.activos[0].ruta[0]},
                ctx, rooms, metrics, verbose=True
            )

        metrics.cerrar()
        print(metrics.resumen())
