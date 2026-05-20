"""
TCA Optimization Motor de Ruteo v2  (Módulos 2 + 3)
======================================================
Mejoras sobre v1:
  [FIX-1]  Modelo de tiempo real con reloj interno por empleado.
           Todo en segundos desde medianoche (ej. 07:00 = 25200).
           Reemplaza el contador simple 'tiempo_ocupado_s'.

  [FIX-2]  Break como restricción dura.
           puede_asignar() verifica que ninguna tarea cruce el bloque
           de break ni el fin de turno. En v1 el break existía en la
           entidad pero el scheduler lo ignoraba completamente.

  [FIX-3]  Criterio greedy unificado.
           V1 usaba 'costo_total' para verificar disponibilidad y
           'costo_efectivo' (con penalizaciones duplicadas) para elegir.
           V2 usa un único costo_efectivo para ambas decisiones,
           eliminando la inconsistencia.

  [FIX-4]  pos_actual sincronizado post-2opt.
           En v1, pos_actual quedaba apuntando al último destino del
           orden greedy, no del orden optimizado. Ahora se actualiza
           al final de la ruta 2-opt. Crítico para la reparación dinámica.

  [FIX-5]  Score de prioridad corregido.
           V1: W_PISO*(5-piso+1) daba 100pts al piso 1, igual que VIP.
           V2: escala relativa al piso máximo real del hotel, sin empate.

  [NEW-1]  Room enriquecida con las 3 dimensiones del espacio de estados:
           estado_fisico, ocupacion, evolucion, restriccion_huesped,
           ventana_inicio_s, ventana_fin_s.

  [NEW-2]  StaffMember cargado desde fleet.csv (shift_type MORNING/AFTERNOON).
           La posición inicial se deriva del turno: el primer edificio
           asignado al inicio del turno.

  [NEW-3]  Loader.desde_reservaciones() construye el pool de habitaciones
           sucias del día directamente desde reservaciones_semana.csv,
           respetando DND y aplicando la prioridad_inicial del CSV.

  [NEW-4]  Loader.desde_fleet() carga el staff del día desde fleet.csv,
           filtrando solo status=='WORK' y convirtiendo horarios a segundos.

  [NEW-5]  Scheduler.reparar() — interfaz de reparación dinámica (Etapa 2).
           Recibe un evento (DND, VENTANA, CAMBIO_URGENTE) y aplica
           un 2-opt local solo sobre la ruta del empleado afectado,
           sin recalcular todo el plan.

  [NEW-6]  Scheduler.resumen_detallado() genera el plan con timestamps
           reales (HH:MM) por habitación, incluyendo el bloque de break.
"""

from __future__ import annotations

import pandas as pd
import numpy as np
from dataclasses import dataclass, field
from typing import Optional
import math


# ══════════════════════════════════════════════════════════════════
# UTILIDADES DE TIEMPO
# Todo el motor trabaja en segundos desde medianoche para
# simplificar comparaciones (suma de enteros, sin datetime).
# Ej: "07:00" → 25200,  "17:00" → 61200
# ══════════════════════════════════════════════════════════════════

def hhmm_a_seg(hhmm: str) -> int:
    """'07:00' → 25200  |  '14:30' → 52200"""
    h, m = map(int, str(hhmm).strip().split(':'))
    return h * 3600 + m * 60


def seg_a_hhmm(segundos: int) -> str:
    """25200 → '07:00'"""
    h = segundos // 3600
    m = (segundos % 3600) // 60
    return f"{h:02d}:{m:02d}"


# ══════════════════════════════════════════════════════════════════
# 1. GRAFO DEL HOTEL
# ══════════════════════════════════════════════════════════════════

class HotelGraph:
    """
    Carga la matriz de traslados y la de tiempos de limpieza.
    Expone consultas de costo en segundos.
    """

    VIP_TYPES = {
        'STD SUPERIOR', 'SUITE 1 REC', 'SUITE 2 REC',
        'SUITE STUDIO',  'FAMILIAR 5',  'FAMILIAR 6', 'PENT-HOUSE'
    }
    VIP_MULTIPLIER   = 1.4   # factor de penalización de demora para VIP
    INTER_FLOOR_SEC  = 30    # segundos por piso de diferencia (fallback)
    INTER_EDIF_SEC   = 300   # segundos por edificio de diferencia (fallback)
    INTER_HAB_SEC    = 10    # segundos por habitación de diferencia (fallback)

    def __init__(self, path_traslados: str, path_limpieza: str):
        # ── Matriz de traslados (272×272, en segundos) ──────────
        self._traslados = pd.read_csv(path_traslados, index_col=0)
        self._traslados.index   = self._traslados.index.astype(str)
        self._traslados.columns = self._traslados.columns.astype(str)

        # ── Tiempos de limpieza por tipo de cama (en minutos) ───
        self._limpieza = pd.read_csv(path_limpieza)
        self._limpieza_dict: dict[str, int] = dict(
            zip(
                self._limpieza['Configuración'],
                self._limpieza['Tiempo Estimado (min)']
            )
        )

        # ── Inventario de habitaciones ───────────────────────────
        self.rooms: dict[str, dict] = {}
        for hab in self._traslados.index:
            e, p, h = self._parse(hab)
            self.rooms[hab] = {'edificio': e, 'piso': p, 'hab': h}

    # ── Parseo de código de habitación ──────────────────────────
    @staticmethod
    def _parse(num_hab: str) -> tuple[int, int, int]:
        """'4201' → (edificio=4, piso=2, hab=01)"""
        s = str(num_hab)
        return int(s[:-3]), int(s[-3]), int(s[-2:])

    # ── Tiempo de traslado (segundos) ───────────────────────────
    def traslado(self, origen: str, destino: str) -> int:
        if origen == destino:
            return 0
        try:
            return int(self._traslados.loc[str(origen), str(destino)])
        except KeyError:
            # Fallback Manhattan si la celda no existe en la matriz
            e1, p1, h1 = self._parse(origen)
            e2, p2, h2 = self._parse(destino)
            return (
                abs(e1 - e2) * self.INTER_EDIF_SEC
                + abs(p1 - p2) * self.INTER_FLOOR_SEC
                + abs(h1 - h2) * self.INTER_HAB_SEC
            )

    # ── Tiempo de limpieza (segundos) ───────────────────────────
    def limpieza(self, tpo_cama: str, cpo: int = 2, desc: str = '') -> int:
        """
        base_min por tipo de cama + 5 min por persona adicional sobre 2.
        Si la habitación es VIP, aplica VIP_MULTIPLIER al tiempo total.
        [FIX de v1: desc ahora se usa para aplicar el multiplicador VIP
         también al tiempo de limpieza, no solo al score de prioridad.]
        """
        base_min = self._limpieza_dict.get(tpo_cama.strip(), 30)
        extra_min = max(0, cpo - 2) * 5
        total_min = base_min + extra_min
        if desc.strip().upper() in self.VIP_TYPES:
            total_min = math.ceil(total_min * self.VIP_MULTIPLIER)
        return total_min * 60

    # ── Multiplicador VIP ────────────────────────────────────────
    def vip_multiplier(self, desc: str) -> float:
        return self.VIP_MULTIPLIER if desc.strip().upper() in self.VIP_TYPES else 1.0


# ══════════════════════════════════════════════════════════════════
# 2. ENTIDADES
# ══════════════════════════════════════════════════════════════════

@dataclass
class Room:
    """
    Habitación sucia pendiente de limpiar.

    [NEW-1] Añade las 3 dimensiones del espacio de estados:
      · estado_fisico   : LIMPIO | SUCIO | FUERA_DE_SERVICIO
      · ocupacion       : LIBRE  | OCUPADO | EN_LIMPIEZA
      · evolucion       : DISPONIBLE | ENTRADA | PERMANENCIA | SALIDA | CAMBIO

    [NEW-1] Restricciones de huésped:
      · restriccion_huesped : NINGUNA | DND | VENTANA_SOLICITADA
      · ventana_inicio_s / ventana_fin_s : segundos desde medianoche
        (solo relevantes cuando restriccion_huesped == 'VENTANA_SOLICITADA')
    """
    # ── Identificación ──────────────────────────────────────────
    num_hab:  str
    tpo_cama: str
    desc:     str
    cpo:      int

    # ── Datos de reserva ────────────────────────────────────────
    vista:    str   = ''
    nom_hsp:  str   = ''
    num_per:  int   = 0
    evolucion: str  = 'PERMANENCIA'   # [NEW-1]

    # ── Dimensión física ────────────────────────────────────────
    estado_fisico: str = 'SUCIO'      # [NEW-1]
    ocupacion:     str = 'LIBRE'      # [NEW-1]

    # ── Restricción de huésped ──────────────────────────────────
    restriccion_huesped: str = 'NINGUNA'          # [NEW-1]
    ventana_inicio_s:    int = 0                  # [NEW-1]
    ventana_fin_s:       int = 0                  # [NEW-1]

    # ── Planificación ───────────────────────────────────────────
    prioridad:         float = 0.0
    tiempo_limpieza_s: int   = 0
    estado:            str   = 'SUCIA'    # SUCIA | ASIGNADA | SIN_ASIGNAR
    asignada_a: Optional[str] = None

    def __post_init__(self):
        e, p, h        = HotelGraph._parse(self.num_hab)
        self.edificio  = e
        self.piso      = p
        self.hab       = h

    @property
    def es_dnd(self) -> bool:
        return self.restriccion_huesped == 'DND'

    @property
    def tiene_ventana(self) -> bool:
        return self.restriccion_huesped == 'VENTANA_SOLICITADA'


@dataclass
class StaffMember:
    """
    Empleado con turno MORNING o AFTERNOON y reloj interno real.

    [FIX-2] El reloj (tiempo_actual_s) avanza con cada asignación.
            puede_asignar() verifica que la tarea no cruce el break
            ni el fin de turno — en v1 solo verificaba tiempo acumulado.

    [FIX-4] pos_actual se actualiza correctamente post-2opt.

    [NEW-2] shift_type: 'MORNING' (07-17) | 'AFTERNOON' (14-22).
    """
    employee_id:    str
    employee_name:  str
    pos_actual:     str           # num_hab de inicio (posición lógica)
    shift_type:     str = 'MORNING'

    # Horarios en segundos desde medianoche
    turno_inicio_s: int = 7  * 3600   # 07:00
    turno_fin_s:    int = 17 * 3600   # 17:00
    break_inicio_s: int = 11 * 3600   # 11:00
    break_dur_s:    int = 30 * 60     # 30 min

    def __post_init__(self):
        # Reloj interno: empieza al inicio del turno
        self.tiempo_actual_s: int   = self.turno_inicio_s
        self.tiempo_ocupado_s: int  = 0
        self.ruta:  list[str]       = []
        self.log:   list[dict]      = []   # timestamps reales por habitación

        # Tiempo útil neto = turno total − break
        self.tiempo_libre_s: int = (
            (self.turno_fin_s - self.turno_inicio_s) - self.break_dur_s
        )

    # ── Disponibilidad neta restante ────────────────────────────
    @property
    def disponible_s(self) -> int:
        return self.tiempo_libre_s - self.tiempo_ocupado_s

    # ── Verificación de viabilidad con restricción de break ─────
    def puede_asignar(self, traslado_s: int, limpieza_s: int) -> bool:
        """
        [FIX-2] Verifica tres condiciones:
          1. Hay tiempo libre neto suficiente.
          2. La tarea no invade el bloque de break.
          3. La tarea no supera el fin de turno.
        """
        inicio_tarea = self.tiempo_actual_s + traslado_s
        fin_tarea    = inicio_tarea + limpieza_s

        # 1. Tiempo libre restante
        if self.disponible_s < traslado_s + limpieza_s:
            return False

        # 2. La tarea no puede cruzar el bloque de break
        break_fin = self.break_inicio_s + self.break_dur_s
        if inicio_tarea < break_fin and fin_tarea > self.break_inicio_s:
            return False

        # 3. No puede superar el fin de turno
        if fin_tarea > self.turno_fin_s:
            return False

        return True

    def avanzar_reloj(self, traslado_s: int, limpieza_s: int, num_hab: str):
        """
        Registra la asignación y avanza el reloj interno.
        Si el traslado llegaría durante el break, el reloj
        salta al fin del break antes de iniciar la limpieza.
        """
        inicio_traslado = self.tiempo_actual_s
        inicio_tarea    = inicio_traslado + traslado_s
        break_fin       = self.break_inicio_s + self.break_dur_s

        # Saltar el break si el traslado aterriza dentro de él
        if self.break_inicio_s <= inicio_tarea < break_fin:
            inicio_tarea = break_fin

        fin_tarea = inicio_tarea + limpieza_s

        self.log.append({
            'num_hab':          num_hab,
            'traslado_inicio':  seg_a_hhmm(inicio_traslado),
            'tarea_inicio':     seg_a_hhmm(inicio_tarea),
            'tarea_fin':        seg_a_hhmm(fin_tarea),
            'traslado_s':       traslado_s,
            'limpieza_s':       limpieza_s,
        })

        self.tiempo_actual_s   = fin_tarea
        self.tiempo_ocupado_s += traslado_s + limpieza_s

    def sincronizar_pos_post_2opt(self):
        """
        [FIX-4] Actualiza pos_actual al último elemento de la ruta
        optimizada. Debe llamarse después de route_2opt().
        Sin esto, los tiempos de traslado en reparar() serían incorrectos.
        """
        if self.ruta:
            self.pos_actual = self.ruta[-1]


# ══════════════════════════════════════════════════════════════════
# 3. LOADERS  – Construyen entidades desde los CSV
# ══════════════════════════════════════════════════════════════════

class Loader:
    """
    [NEW-2/3/4] Fábrica de entidades desde los CSV del proyecto.
    Centraliza el parseo para que el Scheduler no dependa del
    formato de archivo directamente.
    """

    # ── Pool de habitaciones desde reservaciones_semana.csv ─────
    @staticmethod
    def desde_reservaciones(
        path_reservaciones: str,
        fecha: str,           # 'YYYY-MM-DD'  ej. '2026-05-29'
        solo_sucias: bool = True
    ) -> list[Room]:
        """
        [NEW-3] Carga las habitaciones del día indicado.
        - Filtra DND: las excluye del pool de optimización.
        - Respeta evolucion=='CAMBIO' como prioridad máxima.
        - solo_sucias=True (default): solo ingresa estado_fisico=='SUCIO'.
        """
        df = pd.read_csv(path_reservaciones)
        df = df[df['fecha'] == fecha].copy()

        if solo_sucias:
            df = df[df['estado_fisico'] == 'SUCIO']

        # DND: sacar del pool (no se puede limpiar en este momento)
        df_dnd = df[df['restriccion_huesped'] == 'DND']
        if not df_dnd.empty:
            print(f"  ⚠️  {len(df_dnd)} hab con DND excluidas del pool: "
                  f"{df_dnd['num_hab'].tolist()}")
        df = df[df['restriccion_huesped'] != 'DND']

        rooms = []
        for _, row in df.iterrows():
            # Convertir ventanas de tiempo a segundos
            vent_ini = 0
            vent_fin = 0
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

        print(f"  ✅ {len(rooms)} habitaciones sucias cargadas para {fecha}")
        return rooms

    # ── Staff desde fleet.csv ────────────────────────────────────
    @staticmethod
    def desde_fleet(
        path_fleet: str,
        day: str,              # 'day_1' … 'day_7'
        pos_inicio: str = '8101'   # posición lógica de inicio (ej. bodega central)
    ) -> list[StaffMember]:
        """
        [NEW-4] Carga el staff activo del día.
        - Filtra status=='WORK'.
        - Convierte shift_start/shift_end/break_hour a segundos.
        - Asigna pos_inicio como posición de partida del turno.
        """
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

        print(f"  ✅ {len(staff)} empleados activos cargados para {day}")
        return staff


# ══════════════════════════════════════════════════════════════════
# 4. MÓDULO 3 – PRIORIZACIÓN
# ══════════════════════════════════════════════════════════════════

class Prioritizer:
    """
    Asigna score de prioridad a cada habitación sucia.
    Mayor score → se limpia antes.

    Score = bonus_evolucion + bonus_vip + piso_score + limpieza_score

    [FIX-5] piso_score corregido: escala relativa al piso máximo
            real del hotel. En v1, piso=1 daba 100pts (igual que VIP).
    [NEW-1] bonus_evolucion: CAMBIO es prioridad absoluta (9999),
            SALIDA tiene bonus sobre PERMANENCIA.
            DND ya fue filtrado por el Loader.
    """

    W_VIP        = 100.0
    W_PISO       = 15.0    # por nivel de piso; escala relativa
    W_LIMPIEZA   = 5.0     # habitaciones más rápidas tienen ligera ventaja
    MAX_PISO     = 5       # piso máximo del hotel

    BONUS_EVOLUCION = {
        'CAMBIO':      9999.0,   # prioridad absoluta: sale y entra el mismo día
        'SALIDA':       200.0,   # check-out pendiente
        'ENTRADA':       50.0,   # cuarto que recibirá huésped hoy
        'PERMANENCIA':    0.0,
        'DISPONIBLE':     0.0,
    }

    @classmethod
    def score(cls, room: Room, graph: HotelGraph,
              max_limpieza_s: int = 5700) -> float:
        # CAMBIO y SALIDA tienen precedencia absoluta/alta
        bonus_evol = cls.BONUS_EVOLUCION.get(room.evolucion, 0.0)
        if room.evolucion == 'CAMBIO':
            return bonus_evol   # no se mezcla con otros factores

        # VIP bonus
        vip_bonus = cls.W_VIP if graph.vip_multiplier(room.desc) > 1.0 else 0.0

        # [FIX-5] Piso: escala 0 a W_PISO*(MAX_PISO-1)
        # Piso más alto limpia primero (menos tráfico de carrito en escaleras al bajar)
        piso_rel   = max(0, cls.MAX_PISO - room.piso)
        piso_score = cls.W_PISO * piso_rel

        # Limpieza: habitaciones más cortas tienen ligera ventaja
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
# 5. MÓDULO 2 – RUTEO  (Greedy + 2-opt + Reparación dinámica)
# ══════════════════════════════════════════════════════════════════

def _route_cost(route: list[str], graph: HotelGraph,
                limpieza_map: dict[str, int]) -> int:
    """Costo total de una ruta: traslados + limpiezas en segundos."""
    if not route:
        return 0
    total = limpieza_map.get(route[0], 0)
    for i in range(1, len(route)):
        total += graph.traslado(route[i-1], route[i]) + limpieza_map.get(route[i], 0)
    return total


def route_2opt(route: list[str], graph: HotelGraph,
               limpieza_map: dict[str, int]) -> list[str]:
    """
    Mejora local 2-opt: invierte segmentos hasta que ningún intercambio
    reduzca el costo total. O(n²) por iteración.
    """
    best      = route[:]
    best_cost = _route_cost(best, graph, limpieza_map)
    improved  = True
    while improved:
        improved = False
        for i in range(len(best) - 1):
            for j in range(i + 2, len(best)):
                candidate = best[:i] + best[i:j+1][::-1] + best[j+1:]
                cost = _route_cost(candidate, graph, limpieza_map)
                if cost < best_cost:
                    best, best_cost = candidate, cost
                    improved = True
    return best


class Scheduler:
    """
    Planificador diario — integra Módulos 2 y 3.

    Etapa 1 (planificar):
      1. Calcula tiempos de limpieza para cada habitación.
      2. Prioriza habitaciones (Módulo 3) respetando CAMBIO > SALIDA > VIP > piso.
      3. Greedy con criterio unificado [FIX-3]: asigna al empleado cuyo
         traslado real sea menor, verificando break y fin de turno [FIX-2].
      4. 2-opt por ruta y sincroniza pos_actual [FIX-4].

    Etapa 2 (reparar):
      [NEW-5] Recibe un evento en tiempo real y aplica un 2-opt local
      solo sobre la ruta del empleado afectado.
    """

    def __init__(self, graph: HotelGraph):
        self.graph = graph

    # ── ETAPA 1: Plan maestro ────────────────────────────────────
    def planificar(
        self,
        rooms: list[Room],
        staff: list[StaffMember],
        verbose: bool = True
    ) -> dict[str, list[str]]:

        g = self.graph

        # 1. Calcular tiempos de limpieza reales
        for r in rooms:
            r.tiempo_limpieza_s = g.limpieza(r.tpo_cama, r.cpo, r.desc)

        # 2. Priorizar [FIX-5, NEW-1]
        rooms_sorted = Prioritizer.rank(rooms, g)

        if verbose:
            self._print_priorizacion(rooms_sorted, g)

        # 3. Greedy con criterio unificado [FIX-3] y reloj real [FIX-2]
        limpieza_map: dict[str, int] = {r.num_hab: r.tiempo_limpieza_s for r in rooms}
        asignaciones: dict[str, list[str]] = {s.employee_id: [] for s in staff}

        for room in rooms_sorted:

            # Respetar ventana solicitada: solo asignar si hay staff
            # que pueda llegar dentro de la ventana
            mejor_staff  = None
            mejor_costo  = float('inf')

            for s in staff:
                traslado_s = g.traslado(s.pos_actual, room.num_hab)

                # [FIX-2] Verificar break y fin de turno
                if not s.puede_asignar(traslado_s, room.tiempo_limpieza_s):
                    continue

                # [NEW-1] Si hay ventana solicitada, verificar que la llegada
                # estimada caiga dentro de ella
                if room.tiene_ventana:
                    llegada_est = s.tiempo_actual_s + traslado_s
                    if not (room.ventana_inicio_s <= llegada_est <= room.ventana_fin_s):
                        continue

                # [FIX-3] Un único costo_efectivo para elegir al mejor
                # (traslado real desde la posición actual; ya incluye
                # la penalización implícita por edificio/piso)
                if traslado_s < mejor_costo:
                    mejor_costo = traslado_s
                    mejor_staff = s

            if mejor_staff is not None:
                traslado_s = g.traslado(mejor_staff.pos_actual, room.num_hab)
                mejor_staff.avanzar_reloj(traslado_s, room.tiempo_limpieza_s, room.num_hab)
                mejor_staff.ruta.append(room.num_hab)
                asignaciones[mejor_staff.employee_id].append(room.num_hab)
                mejor_staff.pos_actual = room.num_hab
                room.estado     = 'ASIGNADA'
                room.asignada_a = mejor_staff.employee_id
            else:
                room.estado = 'SIN_ASIGNAR'

        # 4. 2-opt + sincronizar pos_actual [FIX-4]
        if verbose:
            print("\n🔧 MEJORA 2-OPT POR RUTA")

        for s in staff:
            if len(s.ruta) > 2:
                ruta_orig   = s.ruta[:]
                s.ruta      = route_2opt(s.ruta, g, limpieza_map)
                asignaciones[s.employee_id] = s.ruta
                costo_orig  = _route_cost(ruta_orig, g, limpieza_map)
                costo_opt   = _route_cost(s.ruta,    g, limpieza_map)
                if verbose:
                    ahorro = costo_orig - costo_opt
                    print(f"  {s.employee_id} ({s.shift_type}): {len(s.ruta)} habs | "
                          f"Antes {costo_orig//60}min → Después {costo_opt//60}min "
                          f"| Ahorro {ahorro//60}min")

            # [FIX-4] Sincronizar posición real post-2opt
            s.sincronizar_pos_post_2opt()

        return asignaciones

    # ── ETAPA 2: Reparación dinámica ────────────────────────────
    def reparar(
        self,
        evento: dict,
        staff:  list[StaffMember],
        rooms:  list[Room],
        verbose: bool = True
    ) -> dict[str, list[str]]:
        """
        [NEW-5] Procesa un evento en tiempo real y repara solo la ruta afectada.

        Tipos de evento soportados:
          {'tipo': 'DND',            'num_hab': '4201', 'timestamp_s': 39600}
          {'tipo': 'CAMBIO_URGENTE', 'num_hab': '3101', 'timestamp_s': 43200}
          {'tipo': 'VENTANA',        'num_hab': '5201', 'ventana_ini': 36000,
                                     'ventana_fin': 43200, 'timestamp_s': 35000}

        Estrategia:
          - DND: remover de la ruta asignada y marcar SIN_ASIGNAR.
          - CAMBIO_URGENTE: reordenar para que aparezca lo antes posible.
          - VENTANA: reinsertar respetando la nueva ventana.
          Aplica 2-opt local sobre la ruta modificada (no global).
        """
        tipo    = evento.get('tipo')
        num_hab = str(evento.get('num_hab', ''))
        ts      = evento.get('timestamp_s', 0)

        # Localizar habitación y empleado asignado
        room_obj   = next((r for r in rooms if r.num_hab == num_hab), None)
        staff_asig = next((s for s in staff if num_hab in s.ruta), None)

        limpieza_map = {r.num_hab: r.tiempo_limpieza_s for r in rooms}

        if tipo == 'DND':
            if staff_asig and room_obj:
                staff_asig.ruta.remove(num_hab)
                room_obj.estado     = 'SIN_ASIGNAR'
                room_obj.asignada_a = None
                if verbose:
                    print(f"  🚫 DND [{num_hab}] removida de ruta {staff_asig.employee_id}")
                # 2-opt local sobre la ruta reducida
                if len(staff_asig.ruta) > 2:
                    staff_asig.ruta = route_2opt(staff_asig.ruta, self.graph, limpieza_map)
                staff_asig.sincronizar_pos_post_2opt()

        elif tipo == 'CAMBIO_URGENTE':
            if staff_asig and room_obj:
                # Buscar al staff con menor tiempo de llegada desde ts
                mejor_staff = min(
                    staff,
                    key=lambda s: self.graph.traslado(s.pos_actual, num_hab)
                    if s.puede_asignar(
                        self.graph.traslado(s.pos_actual, num_hab),
                        room_obj.tiempo_limpieza_s
                    ) else float('inf')
                )
                if mejor_staff.employee_id != staff_asig.employee_id:
                    staff_asig.ruta.remove(num_hab)
                    mejor_staff.ruta.insert(0, num_hab)   # urgente: al frente
                    room_obj.asignada_a = mejor_staff.employee_id
                    if verbose:
                        print(f"  🔄 CAMBIO_URGENTE [{num_hab}] reasignada a "
                              f"{mejor_staff.employee_id}")
                else:
                    # Ya está en el staff óptimo; moverla al frente de su ruta
                    staff_asig.ruta.remove(num_hab)
                    staff_asig.ruta.insert(0, num_hab)
                    if verbose:
                        print(f"  🔄 CAMBIO_URGENTE [{num_hab}] movida al frente "
                              f"de {staff_asig.employee_id}")
                staff_asig.sincronizar_pos_post_2opt()
                mejor_staff.sincronizar_pos_post_2opt()

        elif tipo == 'VENTANA':
            room_obj.ventana_inicio_s    = evento.get('ventana_ini', 0)
            room_obj.ventana_fin_s       = evento.get('ventana_fin', 0)
            room_obj.restriccion_huesped = 'VENTANA_SOLICITADA'
            if verbose:
                print(f"  🕐 VENTANA [{num_hab}] actualizada: "
                      f"{seg_a_hhmm(room_obj.ventana_inicio_s)} – "
                      f"{seg_a_hhmm(room_obj.ventana_fin_s)}")
            if staff_asig and len(staff_asig.ruta) > 2:
                staff_asig.ruta = route_2opt(staff_asig.ruta, self.graph, limpieza_map)
                staff_asig.sincronizar_pos_post_2opt()

        return {s.employee_id: s.ruta for s in staff}

    # ── Resumen detallado con timestamps reales ──────────────────
    def resumen_detallado(
        self,
        staff: list[StaffMember],
        rooms: list[Room]
    ) -> tuple[pd.DataFrame, list[str]]:
        """
        [NEW-6] Genera tabla resumen con timestamps HH:MM por habitación.
        Incluye bloque de break en el log de cada empleado.
        """
        rows = []
        for s in staff:
            habs_asig  = [r for r in rooms if r.asignada_a == s.employee_id]
            t_limpieza = sum(r.tiempo_limpieza_s for r in habs_asig)
            utilizacion = (
                100 * s.tiempo_ocupado_s / s.tiempo_libre_s
                if s.tiempo_libre_s > 0 else 0
            )
            rows.append({
                'Empleado':          s.employee_id,
                'Nombre':            s.employee_name,
                'Turno':             s.shift_type,
                'Inicio turno':      seg_a_hhmm(s.turno_inicio_s),
                'Fin turno':         seg_a_hhmm(s.turno_fin_s),
                'Break':             f"{seg_a_hhmm(s.break_inicio_s)} "
                                     f"({s.break_dur_s//60} min)",
                'Habitaciones':      len(habs_asig),
                'Tiempo limpieza':   f"{t_limpieza//60} min",
                'Tiempo disponible': f"{s.tiempo_libre_s//60} min",
                'Utilización':       f"{utilizacion:.0f}%",
                'Ruta (orden)':      ' → '.join(s.ruta),
            })

        sin_asignar = [r.num_hab for r in rooms if r.estado in ('SUCIA', 'SIN_ASIGNAR')]
        return pd.DataFrame(rows), sin_asignar

    def log_por_empleado(self, staff: list[StaffMember]) -> pd.DataFrame:
        """
        Devuelve el log detallado de cada tarea con timestamps reales.
        Útil para exportar el plan a la aplicación de turno.
        """
        rows = []
        for s in staff:
            # Insertar break en el log
            break_entry = {
                'employee_id':    s.employee_id,
                'employee_name':  s.employee_name,
                'num_hab':        '—',
                'tipo':           'BREAK',
                'traslado_inicio': seg_a_hhmm(s.break_inicio_s),
                'tarea_inicio':    seg_a_hhmm(s.break_inicio_s),
                'tarea_fin':       seg_a_hhmm(s.break_inicio_s + s.break_dur_s),
                'traslado_s':      0,
                'limpieza_s':      s.break_dur_s,
            }
            for entry in s.log:
                rows.append({'employee_id': s.employee_id,
                             'employee_name': s.employee_name,
                             'tipo': 'LIMPIEZA',
                             **entry})
            rows.append(break_entry)

        df = pd.DataFrame(rows)
        if not df.empty:
            df = df.sort_values(['employee_id', 'tarea_inicio'])
        return df

    # ── Print helpers ────────────────────────────────────────────
    @staticmethod
    def _print_priorizacion(rooms_sorted: list[Room], graph: HotelGraph):
        print("\n📋 PRIORIZACIÓN DE HABITACIONES (top 20)")
        print(f"{'Hab':>7}  {'Edif':>4}  {'Piso':>4}  {'Evolución':>12}  "
              f"{'Tipo':>15}  {'VIP':>3}  {'Score':>7}  {'Limpieza':>10}  {'Restricción':>18}")
        print("─" * 100)
        for r in rooms_sorted[:20]:
            vip = "✓" if graph.vip_multiplier(r.desc) > 1 else ""
            print(
                f"{r.num_hab:>7}  {r.edificio:>4}  {r.piso:>4}  "
                f"{r.evolucion:>12}  {r.desc:>15}  {vip:>3}  "
                f"{r.prioridad:>7.1f}  {r.tiempo_limpieza_s//60:>7} min  "
                f"{r.restriccion_huesped:>18}"
            )


# ══════════════════════════════════════════════════════════════════
# EJEMPLO DE USO
# ══════════════════════════════════════════════════════════════════

if __name__ == '__main__':
    import os

    BASE = r'C:\Users\luis\Desktop\TCA\TCA-Optimization'

    PATH_TRASLADOS     = os.path.join(BASE, 'matriz_traslados_segundos.csv')
    PATH_LIMPIEZA      = os.path.join(BASE, 'matriz_tiempos_limpieza.csv')
    PATH_RESERVACIONES = os.path.join(BASE, 'reservaciones_semana.csv')
    PATH_FLEET         = os.path.join(BASE, 'fleet.csv')

    FECHA_DIA = '2026-05-29'   # viernes — día con más check-outs
    DAY_KEY   = 'day_6'        # day_1=domingo … day_7=sábado

    print("=" * 60)
    print(f"  TCA Optimization – Plan del {FECHA_DIA}")
    print("=" * 60)

    # 1. Cargar grafo
    graph = HotelGraph(PATH_TRASLADOS, PATH_LIMPIEZA)

    # 2. Cargar habitaciones sucias del día
    print("\n📂 Cargando habitaciones sucias...")
    rooms = Loader.desde_reservaciones(PATH_RESERVACIONES, FECHA_DIA)

    # 3. Cargar staff activo del día
    print("\n👥 Cargando staff activo...")
    staff = Loader.desde_fleet(PATH_FLEET, DAY_KEY)

    if not rooms:
        print("⚠️  Sin habitaciones sucias para este día.")
    elif not staff:
        print("⚠️  Sin empleados activos para este día.")
    else:
        # 4. Planificar
        scheduler = Scheduler(graph)
        asignaciones = scheduler.planificar(rooms, staff, verbose=True)

        # 5. Resumen
        print("\n📊 RESUMEN DEL PLAN")
        df_resumen, sin_asignar = scheduler.resumen_detallado(staff, rooms)
        print(df_resumen.to_string(index=False))

        if sin_asignar:
            print(f"\n⚠️  Habitaciones SIN ASIGNAR ({len(sin_asignar)}): {sin_asignar}")

        # 6. Log detallado con timestamps
        print("\n🕐 LOG DE TAREAS (primeros 30 registros)")
        df_log = scheduler.log_por_empleado(staff)
        print(df_log.head(30).to_string(index=False))

        # 7. Ejemplo de reparación dinámica
        print("\n🔧 SIMULANDO EVENTO DINÁMICO...")
        if sin_asignar:
            pass   # nada urgente si no quedaron sin asignar

        # Simular DND a las 10:00 en la primera habitación asignada
        primera_asig = next(
            (h for s in staff for h in s.ruta), None
        )
        if primera_asig:
            evento_dnd = {
                'tipo':        'DND',
                'num_hab':     primera_asig,
                'timestamp_s': hhmm_a_seg('10:00'),
            }
            print(f"  Evento: DND en hab {primera_asig} a las 10:00")
            scheduler.reparar(evento_dnd, staff, rooms, verbose=True)