# Motor de Ruteo v3 — TCA Optimization

> Planificador diario de limpieza hotelera con asignación Clarke-Wright Savings, optimización exacta Held-Karp por ruta (DP bitmask), refinamiento global VNS con 3 vecindarios y reparación dinámica warm-start ante eventos en tiempo real.

**Versión:** v3.0 · **Idioma:** Python · **Dependencias:** pandas, numpy, dataclasses, math, time, typing

---

## Tabla de Contenidos

- [Visión General](#visión-general)
- [Changelog v3](#changelog-v3)
- [Arquitectura del Sistema](#arquitectura-del-sistema)
- [Módulos](#módulos)
  - [HotelGraph](#hotelgraph)
  - [Entidades](#entidades)
  - [Loader](#loader)
  - [Prioritizer](#prioritizer)
  - [Scheduler](#scheduler)
- [Archivos CSV](#archivos-csv)
- [Ejemplo de Uso](#ejemplo-de-uso)
- [Constantes Clave](#constantes-clave)

---

## Visión General

El **Motor de Ruteo v3** resuelve el problema de asignación y secuenciación de tareas de limpieza para una flota de empleados en un hotel multi-edificio (VRPTW). Opera en dos etapas:

- **Etapa 1:** Genera el plan maestro del día mediante Clarke-Wright Savings + DP exacta Held-Karp + VNS global.
- **Etapa 2:** Repara el plan completo con VNS warm-start ante eventos dinámicos (DND, ventanas, cambios urgentes).

Todo el motor trabaja en **segundos desde medianoche** para operar únicamente con aritmética entera y evitar el manejo de objetos `datetime`. Por ejemplo, `07:00 → 25200`, `17:00 → 61200`.

> **Nota:** No requiere solver externo. La DP exacta (Held-Karp) es óptima por ruta; CWS y VNS son heurísticas de alta calidad.

---

## Changelog v3

| Versión | Tipo | Descripción |
|---------|------|-------------|
| **v3-1** | ✨ Nuevo | **Clarke-Wright Savings (E1-A):** Reemplaza el greedy puro. Genera rutas triviales y las fusiona por saving descendente. Produce una distribución de carga más uniforme entre empleados. |
| **v3-2** | ✨ Nuevo | **RouteDP — Held-Karp exacto (E1-B):** DP bitmask O(2ⁿ·n²) para optimizar el orden dentro de cada ruta. Para n ≤ 20 es exacto; para n > 20 cae a 2-opt. Reduce ~8–15 % el tiempo de ruta vs 2-opt. |
| **v3-3** | ✨ Nuevo | **VNS con 3 vecindarios (E1-C / E2):** Variable Neighborhood Search con Or-opt, Relocate y 2-opt*. Sustituye el 2-opt de v2 tanto en refinamiento post-plan como en reparación dinámica. |
| **v3-4** | 🔧 Fix | **`StaffMember.reset_reloj()`:** Restaura el reloj y `pos_actual` al estado inicial del turno. Permite re-planificar sin reinstanciar el objeto, necesario para el fallback greedy. |
| **v3-5** | 🔧 Fix | **`route_cost()` con pos_inicio explícito:** Corrige `_route_cost()` de v2 que omitía el traslado desde la posición inicial al primer destino. Ahora el costo refleja el makespan real completo. |
| **v3-6** | 🔧 Fix | **`simular_ruta()` sin mutación:** Función auxiliar que verifica factibilidad de una ruta sobre un `StaffMember` sin modificar su estado interno. Usa `_pos_inicio` fijo para consistencia. |

---

## Arquitectura del Sistema

El flujo principal sigue 6 etapas bien definidas:

```
① HotelGraph  →  ② Loader  →  ③ Prioritizer  →  ④ E1-A CWS  →  ⑤ E1-B DP  →  ⑥ E1-C VNS  →  ⑦ Outputs
     ↓                ↓              ↓                  ↓              ↓             ↓
 Carga matrices   Construye      Ordena por        Asignación     Held-Karp    Refinamiento
 de traslado y    Rooms y        score              Clarke-Wright  exacto por   global 3
 limpieza         StaffMembers                      Savings        ruta         vecindarios
```

> **Etapa 2 — Reparación dinámica:** cualquier evento en tiempo real dispara `Scheduler.reparar()`, que aplica VNS warm-start sobre el plan completo (no solo la ruta afectada), convergiendo en 1–3 s.

---

## Módulos

### HotelGraph

**Tipo:** `class`

Carga la matriz de traslados (272×272, en segundos) y la de tiempos de limpieza (en minutos). Expone consultas de costo en segundos. Parsea códigos de habitación del formato `EPPH` donde **E**=edificio, **P**=piso, **HH**=número.

#### Constantes

| Constante | Valor | Descripción |
|-----------|-------|-------------|
| `VIP_MULTIPLIER` | `1.4×` | Factor de penalización de tiempo para tipos VIP |
| `INTER_FLOOR_SEC` | `30 s` | Fallback por piso de diferencia (si no está en matriz) |
| `INTER_EDIF_SEC` | `300 s` | Fallback por edificio de diferencia |
| `INTER_HAB_SEC` | `10 s` | Fallback por habitación de diferencia |

#### Tipos VIP

```
STD SUPERIOR · SUITE 1 REC · SUITE 2 REC · SUITE STUDIO · FAMILIAR 5 · FAMILIAR 6 · PENT-HOUSE
```

#### Métodos

| Método | Retorno | Descripción |
|--------|---------|-------------|
| `traslado(origen, destino)` | `int` | Tiempo de traslado en segundos entre dos habitaciones. Usa la matriz CSV; si la celda no existe aplica fallback Manhattan por edificio/piso/habitación. |
| `limpieza(tpo_cama, cpo=2, desc='')` | `int` | Tiempo de limpieza en segundos: base por tipo + 5 min/persona adicional sobre 2. Si la habitación es VIP aplica `VIP_MULTIPLIER` al total. |
| `vip_multiplier(desc)` | `float` | Retorna 1.4 si el tipo de descripción está en el conjunto VIP, 1.0 en caso contrario. |

---

### Entidades

#### `Room` — @dataclass

Habitación sucia pendiente de limpiar. Incluye las 3 dimensiones del espacio de estados del hotel: física, de ocupación y de evolución. Soporta restricciones de huésped con ventanas de tiempo.

##### Campos principales

| Campo | Tipo | Default | Descripción |
|-------|------|---------|-------------|
| `num_hab` | `str` | — | Clave de habitación (ej. `'4201'`) |
| `tpo_cama` | `str` | — | Tipo de cama para calcular tiempo de limpieza |
| `desc` | `str` | — | Descripción del cuarto (determina si es VIP) |
| `cpo` | `int` | — | Capacidad de personas (afecta tiempo de limpieza) |
| `evolucion` | `str` | `'PERMANENCIA'` | `CAMBIO \| SALIDA \| ENTRADA \| PERMANENCIA \| DISPONIBLE` |
| `estado_fisico` | `str` | `'SUCIO'` | `LIMPIO \| SUCIO \| FUERA_DE_SERVICIO` |
| `ocupacion` | `str` | `'LIBRE'` | `LIBRE \| OCUPADO \| EN_LIMPIEZA` |
| `restriccion_huesped` | `str` | `'NINGUNA'` | `NINGUNA \| DND \| VENTANA_SOLICITADA` |
| `ventana_inicio_s` | `int` | `0` | Inicio de ventana en seg desde medianoche |
| `ventana_fin_s` | `int` | `0` | Fin de ventana en seg desde medianoche |
| `prioridad` | `float` | `0.0` | Score asignado por `Prioritizer.rank()` |
| `tiempo_limpieza_s` | `int` | `0` | Calculado por `Scheduler.planificar()` |
| `estado` | `str` | `'SUCIA'` | `SUCIA \| ASIGNADA \| SIN_ASIGNAR` |
| `asignada_a` | `Optional[str]` | `None` | `employee_id` del empleado asignado |

##### Propiedades calculadas

- `@property es_dnd → bool`: True si `restriccion_huesped == 'DND'`.
- `@property tiene_ventana → bool`: True si `restriccion_huesped == 'VENTANA_SOLICITADA'`.

---

#### `StaffMember` — @dataclass

Empleado con turno `MORNING` o `AFTERNOON` y reloj interno real. El reloj (`tiempo_actual_s`) avanza con cada asignación y salta automáticamente el bloque de break si una tarea aterrizaría durante él.

##### Campos de turno

| Campo | Tipo | Default | Descripción |
|-------|------|---------|-------------|
| `employee_id` | `str` | — | Identificador único |
| `employee_name` | `str` | — | Nombre completo |
| `pos_actual` | `str` | — | `num_hab` de posición lógica actual |
| `shift_type` | `str` | `'MORNING'` | `MORNING` (07-17) \| `AFTERNOON` (14-22) |
| `turno_inicio_s` | `int` | `25200` | 07:00 en segundos |
| `turno_fin_s` | `int` | `61200` | 17:00 en segundos |
| `break_inicio_s` | `int` | `39600` | 11:00 en segundos |
| `break_dur_s` | `int` | `1800` | 30 minutos en segundos |

##### Métodos

| Método | Descripción |
|--------|-------------|
| `puede_asignar(traslado_s, limpieza_s) → bool` | Verifica 3 condiciones: (1) tiempo libre neto suficiente, (2) la tarea no invade el break, (3) la tarea no supera el fin de turno. |
| `avanzar_reloj(traslado_s, limpieza_s, num_hab)` | Registra la asignación en el log con timestamps reales y avanza el reloj interno. Si el traslado aterrizaría durante el break, salta al fin del break antes de iniciar la limpieza. |
| `sincronizar_pos_post_opt()` | Actualiza `pos_actual` al último elemento de la ruta optimizada. Alias `sincronizar_pos_post_2opt` mantenido para compatibilidad con v2. |
| `reset_reloj()` | Restaura el reloj al inicio del turno (`tiempo_actual_s`, `tiempo_ocupado_s`, `log`) y `pos_actual` a `_pos_inicio`. Necesario para el fallback greedy sin reinstanciar el objeto. |

---

### Loader

**Tipo:** `class` (métodos estáticos)

Fábrica de entidades. Centraliza el parseo de CSV para que el Scheduler no dependa del formato de archivo directamente.

| Método | Descripción |
|--------|-------------|
| `desde_reservaciones(path, fecha, solo_sucias=True) → list[Room]` | Filtra el CSV por fecha y `estado_fisico=='SUCIO'`. Excluye habitaciones DND del pool (las imprime como advertencia). Convierte ventanas de tiempo a segundos. Las habitaciones con `evolucion=='CAMBIO'` recibirán prioridad máxima. |
| `desde_fleet(path, day, pos_inicio='8101') → list[StaffMember]` | Filtra por `day` (ej. `'day_6'`) y `status=='WORK'`. Convierte `shift_start`/`shift_end`/`break_hour` a segundos. Todos los empleados parten de `pos_inicio`. |

> **Convención de días:** `day_1` = domingo, `day_7` = sábado. El campo `DAY_KEY` en el ejemplo de uso corresponde al día de la semana de `FECHA_DIA`.

---

### Prioritizer

**Tipo:** `class` (métodos de clase)

Asigna un score de prioridad a cada habitación sucia. Mayor score → se limpia antes.

**Fórmula:** `score = bonus_evolucion + bonus_vip + piso_score + limpieza_score`

#### Pesos configurables

| Constante | Valor | Descripción |
|-----------|-------|-------------|
| `W_VIP` | `100` | Bonus si tipo ∈ VIP_TYPES |
| `W_PISO` | `15` | Por nivel de piso (escala relativa) |
| `W_LIMPIEZA` | `5` | Habitaciones más rápidas, ligera ventaja |
| `MAX_PISO` | `5` | Piso máximo del hotel |

#### Bonus por evolución

| Prioridad | Evolución | Bonus |
|-----------|-----------|-------|
| #1 | `CAMBIO` | **9 999 pts** |
| #2 | `SALIDA` | 200 pts |
| #3 | `ENTRADA` | 50 pts |
| #4 | `PERMANENCIA` | 0 pts |

> **Nota:** `CAMBIO` devuelve el bonus directamente sin mezclar con VIP, piso, ni limpieza — tiene precedencia absoluta sobre cualquier otro criterio.

---

### Scheduler

**Tipo:** `class`

Planificador diario que integra los Módulos 2 y 3. Recibe el grafo en su constructor y expone dos etapas: `planificar()` para el plan maestro y `reparar()` para ajustes dinámicos.

#### Etapa 1 — `planificar(rooms, staff, metodo='cws_dp', verbose=True)`

Pipeline de 5 pasos:
1. Calcular tiempos de limpieza
2. Priorizar con `Prioritizer`
3. **[E1-A]** Clarke-Wright Savings (asignación inicial)
4. **[E1-B]** Held-Karp exacto por ruta
5. **[E1-C]** VNS post-plan (refinamiento global)

Si `metodo='greedy'` o CWS+DP supera `MAX_PLAN_SEC=60s`, cae al greedy + 2-opt de v2.

#### Etapa 2 — `reparar(evento, staff, rooms, verbose=True)`

Procesa un evento en tiempo real: aplica el cambio de estado correspondiente y luego ejecuta VNS warm-start sobre el **plan completo** (no solo la ruta afectada), convergiendo en 1–3 s.

| Tipo de Evento | Badge | Descripción |
|----------------|-------|-------------|
| **DND** | 🔴 | Remueve la habitación de la ruta del empleado asignado, la marca `SIN_ASIGNAR`, y lanza VNS global para redistribuir la capacidad liberada. |
| **CAMBIO_URGENTE** | 🟠 | Busca al empleado con menor traslado que pueda atender la habitación. Reasigna si es necesario y mueve la habitación al frente de la ruta. VNS reoptimiza el plan global resultante. |
| **VENTANA** | 🔵 | Actualiza `ventana_inicio_s`, `ventana_fin_s` y `restriccion_huesped='VENTANA_SOLICITADA'`. VNS reordena el plan completo respetando la nueva restricción temporal. |

#### Salidas

| Método | Descripción |
|--------|-------------|
| `resumen_detallado(staff, rooms) → (DataFrame, list[str])` | Tabla resumen por empleado con inicio/fin de turno, break, habitaciones, tiempo de limpieza, utilización y ruta en orden. Retorna también la lista de habitaciones sin asignar. |
| `log_por_empleado(staff) → DataFrame` | Log detallado de cada tarea con timestamps reales (traslado_inicio, tarea_inicio, tarea_fin), incluyendo el bloque de break como entrada especial. Ordenado por empleado y hora de inicio. |

---

## Archivos CSV

### `fleet.csv`
`data\solver\dev\fleet.csv`

| Columna | Tipo | Descripción |
|---------|------|-------------|
| `employee_id` | `str` | Identificador único del empleado |
| `employee_name` | `str` | Nombre completo |
| `day` | `str` | `day_1` (dom) … `day_7` (sáb) |
| `status` | `str` | `WORK \| OFF \| VACATION` (solo se cargan WORK) |
| `shift_type` | `str` | `MORNING \| AFTERNOON` |
| `shift_start` | `str` | Hora inicio turno HH:MM (ej. 07:00) |
| `shift_end` | `str` | Hora fin turno HH:MM (ej. 17:00) |
| `break_hour` | `int` | Hora entera de inicio de break (ej. 11 → 11:00) |
| `break_duration_minutes` | `int` | Duración del break en minutos (ej. 30) |

---

### `reservaciones_semana.csv`
`data\solver\dev\reservaciones_semana.csv`

| Columna | Tipo | Descripción |
|---------|------|-------------|
| `fecha` | `str` | YYYY-MM-DD — filtro principal del día |
| `num_hab` | `int` | Código de habitación (ej. 4201) |
| `tpo_cama` | `str` | Tipo de cama para calcular tiempo de limpieza |
| `desc` | `str` | Descripción del cuarto (determina VIP) |
| `cpo` | `int` | Capacidad máxima de personas |
| `vista` | `str` | Vista de la habitación |
| `nom_hsp` | `str` | Nombre del huésped |
| `num_per` | `int` | Personas actualmente en la habitación |
| `evolucion` | `str` | `CAMBIO \| SALIDA \| ENTRADA \| PERMANENCIA \| DISPONIBLE` |
| `estado_fisico` | `str` | `LIMPIO \| SUCIO \| FUERA_DE_SERVICIO` |
| `ocupacion` | `str` | `LIBRE \| OCUPADO \| EN_LIMPIEZA` |
| `restriccion_huesped` | `str` | `NINGUNA \| DND \| VENTANA_SOLICITADA` |
| `ventana_inicio` | `str` | HH:MM — inicio ventana (solo si VENTANA_SOLICITADA) |
| `ventana_fin` | `str` | HH:MM — fin ventana (solo si VENTANA_SOLICITADA) |

---

### `matriz_tiempos_limpieza.csv`
`data\solver\matriz_tiempos_limpieza.csv`

| Columna | Tipo | Descripción |
|---------|------|-------------|
| `Configuración` | `str` | Clave de tipo de cama (coincide con `tpo_cama`) |
| `Tiempo Estimado (min)` | `int` | Minutos base de limpieza para ese tipo |

---

### `matriz_traslados_segundos.csv`
`data\solver\matriz_traslados_segundos.csv`

- **Formato:** Matriz cuadrada 272×272
- **Índice (col 0):** `num_hab` origen (ej. `'4201'`) — cargado como `index_col=0`
- **Columnas:** `num_hab` destino — hasta 272 columnas
- **Valores:** Segundos de traslado entre origen y destino
- **Fallback:** Si la celda no existe, `HotelGraph.traslado()` aplica el fallback Manhattan.

---

## Ejemplo de Uso

```python
import os

BASE = r'C:\Users\luis\Desktop\TCA\TCA-Optimization'

PATH_TRASLADOS     = os.path.join(BASE, 'data\solver\matriz_traslados_segundos.csv')
PATH_LIMPIEZA      = os.path.join(BASE, 'data\solver\matriz_tiempos_limpieza.csv')
PATH_RESERVACIONES = os.path.join(BASE, 'data\solver\dev\reservaciones_semana.csv')
PATH_FLEET         = os.path.join(BASE, 'data\solver\dev\fleet.csv')

FECHA_DIA = '2026-05-29'   # viernes — día con más check-outs
DAY_KEY   = 'day_6'        # day_1=domingo … day_7=sábado

# 1. Cargar grafo
graph = HotelGraph(PATH_TRASLADOS, PATH_LIMPIEZA)

# 2. Cargar habitaciones sucias del día
rooms = Loader.desde_reservaciones(PATH_RESERVACIONES, FECHA_DIA)

# 3. Cargar staff activo del día
staff = Loader.desde_fleet(PATH_FLEET, DAY_KEY)

# 4. Plan maestro CWS + DP + VNS (metodo='greedy' para fallback)
scheduler = Scheduler(graph)
asignaciones = scheduler.planificar(rooms, staff,
                                   metodo='cws_dp',
                                   verbose=True)

# 5. Resumen con timestamps
df_resumen, sin_asignar = scheduler.resumen_detallado(staff, rooms)
df_log = scheduler.log_por_empleado(staff)

# 6. Reparación dinámica — DND (Etapa 2, VNS warm-start)
scheduler.reparar({'tipo': 'DND', 'num_hab': '4201',
                   'timestamp_s': hhmm_a_seg('10:00')},
                  staff, rooms, verbose=True)

# 7. Reparación — CAMBIO_URGENTE
scheduler.reparar({'tipo': 'CAMBIO_URGENTE', 'num_hab': '3101',
                   'timestamp_s': hhmm_a_seg('11:30')},
                  staff, rooms, verbose=True)

# 8. Reparación — VENTANA
scheduler.reparar({'tipo': 'VENTANA', 'num_hab': '5201',
                   'ventana_ini': hhmm_a_seg('14:00'),
                   'ventana_fin': hhmm_a_seg('15:30'),
                   'timestamp_s': hhmm_a_seg('13:00')},
                  staff, rooms, verbose=True)
```

> **Salida esperada:** tabla de priorización (top 20), progreso de CWS + DP (ahorro por empleado) + VNS (ahorro global e iteraciones), DataFrame resumen y log de tareas con timestamps HH:MM.

---

## Constantes Clave

| Constante | Valor | Ubicación | Descripción |
|-----------|-------|-----------|-------------|
| `MAX_PLAN_SEC` | `60 s` | Scheduler | Timeout máximo para la Etapa 1 |
| `DP_MAX_N` | `20` | RouteDP | Umbral para activar Held-Karp exacto (por encima, usa 2-opt) |
| `MAX_VNS_SEC` | `4 s` | VNS | Tiempo máximo de refinamiento VNS en reparación dinámica |

---

<p align="center">
  <strong>TCA Optimization</strong> · Motor de Ruteo v3 · Módulos 2 + 3 · mayo 2026
</p>
