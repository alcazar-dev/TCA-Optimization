  Motor de Ruteo v2 – TCA Optimization   :root { --tca-red: #D34836; --tca-bg: #F5F5F5; --tca-dark: #242424; --tca-muted: #888; --tca-line: rgba(0,0,0,0.07); --green: #16a34a; --blue: #2563eb; --amber: #b45309; --purple: #7c3aed; --sidebar-w: 260px; --radius: 6px; --mono: 'DM Mono', monospace; --serif: 'Fraunces', Georgia, serif; --sans: 'DM Sans', sans-serif; } \*, \*::before, \*::after { box-sizing: border-box; margin: 0; padding: 0; } html { scroll-behavior: smooth; } body { background: var(--tca-bg); color: var(--tca-dark); font-family: var(--sans); font-size: 15px; line-height: 1.7; display: flex; } /\* ── SIDEBAR ────────────────────────────────────────────── \*/ .sidebar { position: fixed; top: 0; left: 0; width: var(--sidebar-w); height: 100vh; background: var(--tca-dark); color: #fff; display: flex; flex-direction: column; overflow-y: auto; z-index: 100; } .sidebar-header { padding: 28px 24px 20px; border-bottom: 1px solid rgba(255,255,255,0.08); } .sidebar-logo { font-family: var(--serif); font-size: 22px; font-weight: 700; letter-spacing: -0.5px; color: var(--tca-red); } .sidebar-sub { font-size: 11px; text-transform: uppercase; letter-spacing: 1.4px; color: rgba(255,255,255,0.35); margin-top: 4px; } .sidebar-version { display: inline-block; margin-top: 10px; background: var(--tca-red); color: #fff; font-family: var(--mono); font-size: 10px; padding: 2px 8px; border-radius: 3px; letter-spacing: 0.5px; } nav { padding: 20px 0 40px; } .nav-group { margin-bottom: 8px; } .nav-label { font-size: 10px; text-transform: uppercase; letter-spacing: 1.5px; color: rgba(255,255,255,0.25); padding: 6px 24px; } nav a { display: flex; align-items: center; gap: 8px; padding: 7px 24px; color: rgba(255,255,255,0.6); text-decoration: none; font-size: 13.5px; border-left: 2px solid transparent; transition: color 0.15s, border-color 0.15s, background 0.15s; } nav a:hover { color: #fff; background: rgba(255,255,255,0.05); border-left-color: var(--tca-red); } nav a .dot { width: 6px; height: 6px; border-radius: 50%; background: currentColor; opacity: 0.5; flex-shrink: 0; } /\* ── MAIN ───────────────────────────────────────────────── \*/ .main { margin-left: var(--sidebar-w); flex: 1; min-height: 100vh; padding: 0 0 80px; } /\* ── HERO ───────────────────────────────────────────────── \*/ .hero { background: var(--tca-dark); color: #fff; padding: 60px 64px 56px; position: relative; overflow: hidden; } .hero::before { content: ''; position: absolute; top: -80px; right: -80px; width: 380px; height: 380px; border-radius: 50%; background: radial-gradient(circle, rgba(211,72,54,0.18) 0%, transparent 70%); pointer-events: none; } .hero-eyebrow { font-family: var(--mono); font-size: 11px; letter-spacing: 2px; text-transform: uppercase; color: var(--tca-red); margin-bottom: 16px; } .hero h1 { font-family: var(--serif); font-size: 48px; font-weight: 700; line-height: 1.1; letter-spacing: -1.5px; margin-bottom: 14px; } .hero h1 span { color: var(--tca-red); } .hero-desc { font-size: 16px; color: rgba(255,255,255,0.55); max-width: 580px; line-height: 1.6; } .hero-meta { margin-top: 32px; display: flex; gap: 32px; flex-wrap: wrap; } .hero-stat { display: flex; flex-direction: column; gap: 2px; } .hero-stat-val { font-family: var(--mono); font-size: 22px; color: #fff; } .hero-stat-lbl { font-size: 11px; text-transform: uppercase; letter-spacing: 1px; color: rgba(255,255,255,0.3); } /\* ── CONTENT ────────────────────────────────────────────── \*/ .content { padding: 0 64px; } /\* ── SECTION ────────────────────────────────────────────── \*/ .section { padding: 56px 0 0; } .section-anchor { display: block; position: relative; top: -80px; } .section-header { display: flex; align-items: baseline; gap: 16px; margin-bottom: 28px; padding-bottom: 14px; border-bottom: 1px solid var(--tca-line); } .section-num { font-family: var(--mono); font-size: 11px; color: var(--tca-red); opacity: 0.7; } h2 { font-family: var(--serif); font-size: 30px; font-weight: 700; letter-spacing: -0.8px; color: var(--tca-dark); } h3 { font-family: var(--sans); font-size: 15px; font-weight: 600; color: var(--tca-dark); margin: 24px 0 8px; display: flex; align-items: center; gap: 8px; } h3 .tag { font-family: var(--mono); font-size: 10px; padding: 2px 7px; border-radius: 3px; font-weight: 400; } p { margin-bottom: 14px; color: #3a3a3a; } /\* ── CHANGELOG GRID ─────────────────────────────────────── \*/ .changelog-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 12px; margin-bottom: 8px; } .changelog-card { background: #fff; border: 1px solid var(--tca-line); border-radius: var(--radius); padding: 16px 18px; position: relative; overflow: hidden; } .changelog-card::before { content: ''; position: absolute; top: 0; left: 0; width: 3px; height: 100%; } .changelog-card.fix::before { background: var(--blue); } .changelog-card.new::before { background: var(--green); } .changelog-badge { display: inline-block; font-family: var(--mono); font-size: 10px; padding: 2px 7px; border-radius: 3px; margin-bottom: 8px; font-weight: 500; } .badge-fix { background: #eff6ff; color: var(--blue); } .badge-new { background: #f0fdf4; color: var(--green); } .changelog-card h4 { font-size: 13px; font-weight: 600; margin-bottom: 5px; color: var(--tca-dark); } .changelog-card p { font-size: 12.5px; color: var(--tca-muted); margin: 0; line-height: 1.55; } /\* ── CLASS BOX ──────────────────────────────────────────── \*/ .class-box { background: #fff; border: 1px solid var(--tca-line); border-radius: var(--radius); margin-bottom: 20px; overflow: hidden; } .class-box-header { display: flex; align-items: center; justify-content: space-between; padding: 14px 20px; border-bottom: 1px solid var(--tca-line); background: #fafafa; } .class-name { font-family: var(--mono); font-size: 15px; font-weight: 500; color: var(--tca-dark); } .class-type { font-family: var(--mono); font-size: 11px; padding: 3px 9px; border-radius: 3px; } .type-class { background: #f3f0ff; color: var(--purple); } .type-dataclass{ background: #fff7ed; color: var(--amber); } .type-func { background: #f0fdf4; color: var(--green); } .class-box-body { padding: 18px 20px; } .class-desc { font-size: 13.5px; color: #4a4a4a; margin-bottom: 16px; line-height: 1.6; } /\* ── ATTR TABLE ─────────────────────────────────────────── \*/ .attr-table { width: 100%; border-collapse: collapse; font-size: 13px; } .attr-table th { text-align: left; font-size: 10px; font-weight: 600; text-transform: uppercase; letter-spacing: 1px; color: var(--tca-muted); padding: 6px 10px; border-bottom: 1px solid var(--tca-line); background: #fafafa; } .attr-table td { padding: 7px 10px; border-bottom: 1px solid var(--tca-line); vertical-align: top; } .attr-table tr:last-child td { border-bottom: none; } .attr-table tr:hover td { background: #fafafa; } .col-name { font-family: var(--mono); font-size: 12.5px; color: var(--tca-dark); white-space: nowrap; } .col-type { font-family: var(--mono); font-size: 11px; color: var(--purple); white-space: nowrap; } .col-default { font-family: var(--mono); font-size: 11px; color: var(--amber); } .col-desc { color: #5a5a5a; font-size: 12.5px; line-height: 1.5; } /\* ── METHOD LIST ────────────────────────────────────────── \*/ .method-list { display: flex; flex-direction: column; gap: 8px; margin-top: 12px; } .method-item { background: #fafafa; border: 1px solid var(--tca-line); border-radius: 5px; padding: 12px 16px; } .method-sig { font-family: var(--mono); font-size: 12.5px; color: var(--blue); margin-bottom: 5px; } .method-desc { font-size: 12.5px; color: #5a5a5a; margin: 0; line-height: 1.5; } /\* ── CSV TABLE ──────────────────────────────────────────── \*/ .csv-block { background: #fff; border: 1px solid var(--tca-line); border-radius: var(--radius); margin-bottom: 20px; overflow: hidden; } .csv-header { display: flex; align-items: center; gap: 10px; padding: 12px 18px; border-bottom: 1px solid var(--tca-line); background: #fafafa; } .csv-icon { width: 28px; height: 28px; background: var(--green); border-radius: 4px; display: flex; align-items: center; justify-content: center; color: #fff; font-size: 12px; font-family: var(--mono); font-weight: 500; } .csv-filename { font-family: var(--mono); font-size: 13px; font-weight: 500; color: var(--tca-dark); } .csv-path { font-family: var(--mono); font-size: 11px; color: var(--tca-muted); background: #f0f0f0; border-radius: 3px; padding: 2px 7px; margin-left: auto; } .csv-body { padding: 0; overflow-x: auto; } /\* ── FLOW DIAGRAM ───────────────────────────────────────── \*/ .flow { display: flex; align-items: flex-start; gap: 0; margin: 24px 0; overflow-x: auto; padding: 4px 0 8px; } .flow-step { display: flex; flex-direction: column; align-items: center; flex: 1; min-width: 110px; } .flow-circle { width: 44px; height: 44px; border-radius: 50%; display: flex; align-items: center; justify-content: center; font-family: var(--mono); font-size: 16px; font-weight: 500; color: #fff; margin-bottom: 10px; flex-shrink: 0; } .flow-label { font-size: 12px; text-align: center; color: var(--tca-dark); font-weight: 500; line-height: 1.3; max-width: 100px; } .flow-sub { font-size: 10.5px; text-align: center; color: var(--tca-muted); line-height: 1.3; max-width: 100px; margin-top: 3px; } .flow-arrow { flex-shrink: 0; align-self: center; margin-bottom: 34px; color: var(--tca-muted); font-size: 18px; padding: 0 4px; } /\* ── CODE BLOCK ─────────────────────────────────────────── \*/ .code-block { background: var(--tca-dark); border-radius: var(--radius); padding: 18px 22px; margin: 16px 0; overflow-x: auto; position: relative; } .code-block-label { position: absolute; top: 10px; right: 14px; font-family: var(--mono); font-size: 10px; color: rgba(255,255,255,0.25); letter-spacing: 1px; text-transform: uppercase; } pre { font-family: var(--mono); font-size: 12.5px; color: #e2e8f0; line-height: 1.7; white-space: pre; } .kw { color: #f472b6; } .fn { color: #60a5fa; } .st { color: #86efac; } .cm { color: rgba(255,255,255,0.3); font-style: italic; } .nb { color: #fbbf24; } .op { color: rgba(255,255,255,0.5); } /\* ── CALLOUT ────────────────────────────────────────────── \*/ .callout { border-left: 3px solid; border-radius: 0 5px 5px 0; padding: 12px 16px; margin: 16px 0; font-size: 13.5px; } .callout-info { border-color: var(--blue); background: #eff6ff; color: #1d4ed8; } .callout-warn { border-color: var(--amber); background: #fffbeb; color: #92400e; } .callout-ok { border-color: var(--green); background: #f0fdf4; color: #166534; } .callout strong { font-weight: 600; } /\* ── PRIORITY TABLE ─────────────────────────────────────── \*/ .priority-row { display: flex; align-items: center; gap: 12px; padding: 10px 0; border-bottom: 1px solid var(--tca-line); } .priority-row:last-child { border-bottom: none; } .p-rank { font-family: var(--mono); font-size: 13px; width: 28px; color: var(--tca-muted); } .p-bar-wrap { flex: 1; height: 6px; background: #eee; border-radius: 3px; } .p-bar { height: 100%; border-radius: 3px; } .p-label { font-size: 13px; font-weight: 500; width: 140px; color: var(--tca-dark); } .p-value { font-family: var(--mono); font-size: 12px; color: var(--tca-muted); width: 60px; text-align: right; } /\* ── CONSTANTS BOX ──────────────────────────────────────── \*/ .constants-grid { display: grid; grid-template-columns: repeat(3, 1fr); gap: 10px; margin: 14px 0; } .const-card { background: #fff; border: 1px solid var(--tca-line); border-radius: 5px; padding: 12px 14px; } .const-name { font-family: var(--mono); font-size: 11.5px; color: var(--purple); margin-bottom: 4px; } .const-val { font-family: var(--mono); font-size: 18px; font-weight: 500; color: var(--tca-dark); } .const-desc { font-size: 11px; color: var(--tca-muted); margin-top: 3px; line-height: 1.4; } /\* ── EVENT TYPES ────────────────────────────────────────── \*/ .event-list { display: flex; flex-direction: column; gap: 10px; } .event-card { background: #fff; border: 1px solid var(--tca-line); border-radius: 5px; padding: 14px 18px; display: flex; gap: 16px; align-items: flex-start; } .event-badge { font-family: var(--mono); font-size: 11px; padding: 3px 9px; border-radius: 3px; font-weight: 500; flex-shrink: 0; margin-top: 2px; } .badge-dnd { background: #fef2f2; color: var(--tca-red); } .badge-urgente { background: #fff7ed; color: var(--amber); } .badge-ventana { background: #eff6ff; color: var(--blue); } .event-body h4 { font-size: 13.5px; font-weight: 600; margin-bottom: 4px; } .event-body p { font-size: 12.5px; color: var(--tca-muted); margin: 0; line-height: 1.5; } /\* ── FOOTER ─────────────────────────────────────────────── \*/ .doc-footer { margin-top: 60px; padding: 28px 64px; border-top: 1px solid var(--tca-line); display: flex; align-items: center; justify-content: space-between; font-size: 12px; color: var(--tca-muted); } .footer-brand { font-family: var(--serif); font-size: 16px; color: var(--tca-dark); } /\* ── UTILS ──────────────────────────────────────────────── \*/ .divider { height: 1px; background: var(--tca-line); margin: 32px 0; } .mt8 { margin-top: 8px; } .mt16 { margin-top: 16px; } @media (max-width: 900px) { .sidebar { display: none; } .main { margin-left: 0; } .hero, .content, .doc-footer { padding-left: 24px; padding-right: 24px; } .changelog-grid { grid-template-columns: 1fr; } .constants-grid { grid-template-columns: 1fr 1fr; } }

TCA

Motor de Ruteo

v2.0

Inicio

[Visión General](#overview) [Changelog v2](#changelog) [Arquitectura](#arquitectura)

Módulos

[HotelGraph](#hotel-graph) [Entidades](#entidades) [Loader](#loader) [Prioritizer](#prioritizer) [Scheduler](#scheduler) [Ruteo 2-opt](#2opt)

Datos

[Archivos CSV](#csvs) [Ejemplo de Uso](#uso)

TCA Optimization · Documentación Técnica

Motor de Ruteo  
v2 (Mód. 2 + 3)
================================

Planificador diario de limpieza hotelera con reloj real por empleado, restricciones de break como regla dura, criterio greedy unificado, optimización 2-opt y reparación dinámica ante eventos en tiempo real.

6 FIX Correcciones vs v1

6 NEW Nuevas funciones

272 Habitaciones soportadas

3 Tipos de evento dinámico

01

Visión General
--------------

El **Motor de Ruteo v2** resuelve el problema de asignación y secuenciación de tareas de limpieza para una flota de empleados en un hotel multi-edificio. Opera en dos etapas: **Etapa 1** genera el plan maestro del día mediante un algoritmo greedy + 2-opt, y **Etapa 2** repara rutas individuales en respuesta a eventos dinámicos (DND, ventanas solicitadas, cambios urgentes).

Todo el motor trabaja en **segundos desde medianoche** para operar únicamente con aritmética entera y evitar el manejo de objetos `datetime`. Por ejemplo, `07:00 → 25200`, `17:00 → 61200`.

**Dependencias:** pandas, numpy, dataclasses, math, typing. No requiere solver externo; la optimización es heurística.

02

Changelog v2
------------

FIX-1

#### Reloj interno real por empleado

Reemplaza el contador simple `tiempo_ocupado_s`. Cada empleado tiene un reloj que avanza con traslados y limpiezas, saltando automáticamente el break.

FIX-2

#### Break como restricción dura

`puede_asignar()` verifica que ninguna tarea cruce el bloque de break ni el fin de turno. En v1 el break existía en la entidad pero el scheduler lo ignoraba.

FIX-3

#### Criterio greedy unificado

v1 usaba `costo_total` para verificar y `costo_efectivo` con penalizaciones duplicadas para elegir. v2 usa un único `costo_efectivo` en ambas decisiones.

FIX-4

#### pos\_actual sincronizado post-2opt

En v1, `pos_actual` quedaba apuntando al último destino greedy, no al optimizado. Ahora se actualiza al final de la ruta 2-opt. Crítico para reparación dinámica.

FIX-5

#### Score de prioridad corregido

v1: `W_PISO*(5-piso+1)` daba 100 pts al piso 1, igual que VIP. v2 usa escala relativa al piso máximo real del hotel, eliminando empates incorrectos.

NEW-1

#### Room con espacio de estados

Añade `estado_fisico`, `ocupacion`, `evolucion`, `restriccion_huesped`, `ventana_inicio_s`, `ventana_fin_s`.

NEW-2

#### StaffMember desde fleet.csv

Cargado con turno MORNING/AFTERNOON. Posición inicial derivada del turno; primer edificio asignado al inicio de jornada.

NEW-3

#### Loader.desde\_reservaciones()

Construye el pool de habitaciones sucias del día directamente desde el CSV, respetando DND y aplicando la `prioridad_inicial`.

NEW-4

#### Loader.desde\_fleet()

Carga staff activo filtrando `status=='WORK'` y convirtiendo horarios a segundos automáticamente.

NEW-5

#### Scheduler.reparar()

Interfaz de reparación dinámica. Recibe un evento (DND, VENTANA, CAMBIO\_URGENTE) y aplica 2-opt local solo sobre la ruta del empleado afectado.

NEW-6

#### resumen\_detallado() con timestamps

Genera el plan con tiempos reales (HH:MM) por habitación, incluyendo el bloque de break en el log de cada empleado.

03

Arquitectura del Sistema
------------------------

El flujo principal del motor sigue 4 etapas bien definidas:

①

HotelGraph

Carga matrices de traslado y limpieza

→

②

Loader

Construye Rooms y StaffMembers desde CSV

→

③

Prioritizer

Ordena habitaciones por score

→

④

Scheduler

Greedy + 2-opt + Reparación

→

⑤

Outputs

Plan con timestamps + log por empleado

**Etapa 2 – Reparación dinámica:** cualquier evento en tiempo real puede disparar `Scheduler.reparar()` para actualizar solo la ruta afectada sin recalcular el plan completo.

04

HotelGraph
----------

HotelGraph class

Carga la matriz de traslados (272×272, en segundos) y la de tiempos de limpieza (en minutos). Expone consultas de costo en segundos. Parsea códigos de habitación del formato `EPPH` donde **E**\=edificio, **P**\=piso, **HH**\=número.

### Constantes

VIP\_MULTIPLIER

1.4×

Factor de penalización de tiempo para tipos VIP

INTER\_FLOOR\_SEC

30 s

Fallback por piso de diferencia (si no está en matriz)

INTER\_EDIF\_SEC

300 s

Fallback por edificio de diferencia

INTER\_HAB\_SEC

10 s

Fallback por habitación de diferencia

### Tipos VIP

STD SUPERIOR · SUITE 1 REC · SUITE 2 REC · SUITE STUDIO · FAMILIAR 5 · FAMILIAR 6 · PENT-HOUSE

### Métodos

traslado(origen: str, destino: str) → int

Tiempo de traslado en segundos entre dos habitaciones. Usa la matriz CSV; si la celda no existe aplica fallback Manhattan por edificio/piso/habitación.

limpieza(tpo\_cama: str, cpo: int = 2, desc: str = '') → int

Tiempo de limpieza en segundos: base por tipo + 5 min/persona adicional sobre 2. Si la habitación es VIP aplica `VIP_MULTIPLIER` al total.

vip\_multiplier(desc: str) → float

Retorna 1.4 si el tipo de descripción está en el conjunto VIP, 1.0 en caso contrario.

05

Entidades
---------

Room @dataclass

\[NEW-1\] Habitación sucia pendiente de limpiar. Incluye las 3 dimensiones del espacio de estados del hotel: física, de ocupación y de evolución. Soporta restricciones de huésped con ventanas de tiempo.

### Campos principales

Campo

Tipo

Default

Descripción

num\_hab

str

—

Clave de habitación (ej. `'4201'`)

tpo\_cama

str

—

Tipo de cama para calcular tiempo de limpieza

desc

str

—

Descripción del cuarto (determina si es VIP)

cpo

int

—

Capacidad de personas (afecta tiempo de limpieza)

evolucion

str

'PERMANENCIA'

CAMBIO | SALIDA | ENTRADA | PERMANENCIA | DISPONIBLE

estado\_fisico

str

'SUCIO'

LIMPIO | SUCIO | FUERA\_DE\_SERVICIO

ocupacion

str

'LIBRE'

LIBRE | OCUPADO | EN\_LIMPIEZA

restriccion\_huesped

str

'NINGUNA'

NINGUNA | DND | VENTANA\_SOLICITADA

ventana\_inicio\_s

int

0

Inicio de ventana en seg desde medianoche

ventana\_fin\_s

int

0

Fin de ventana en seg desde medianoche

prioridad

float

0.0

Score asignado por Prioritizer.rank()

tiempo\_limpieza\_s

int

0

Calculado por Scheduler.planificar()

estado

str

'SUCIA'

SUCIA | ASIGNADA | SIN\_ASIGNAR

asignada\_a

Optional\[str\]

None

employee\_id del empleado asignado

### Propiedades calculadas

@property es\_dnd → bool

True si `restriccion_huesped == 'DND'`.

@property tiene\_ventana → bool

True si `restriccion_huesped == 'VENTANA_SOLICITADA'`.

StaffMember @dataclass

\[FIX-1/2, NEW-2\] Empleado con turno MORNING o AFTERNOON y reloj interno real. El reloj (`tiempo_actual_s`) avanza con cada asignación y salta automáticamente el bloque de break si una tarea aterrizaría durante él.

### Campos de turno

Campo

Tipo

Default

Descripción

employee\_id

str

—

Identificador único del empleado

employee\_name

str

—

Nombre completo

pos\_actual

str

—

num\_hab de posición lógica actual

shift\_type

str

'MORNING'

MORNING (07-17) | AFTERNOON (14-22)

turno\_inicio\_s

int

25200

07:00 en segundos

turno\_fin\_s

int

61200

17:00 en segundos

break\_inicio\_s

int

39600

11:00 en segundos

break\_dur\_s

int

1800

30 minutos en segundos

### Métodos

puede\_asignar(traslado\_s: int, limpieza\_s: int) → bool

\[FIX-2\] Verifica 3 condiciones: (1) tiempo libre neto suficiente, (2) la tarea no invade el break, (3) la tarea no supera el fin de turno.

avanzar\_reloj(traslado\_s: int, limpieza\_s: int, num\_hab: str)

Registra la asignación en el log con timestamps reales y avanza el reloj interno. Si el traslado aterrizaría durante el break, salta al fin del break antes de iniciar la limpieza.

sincronizar\_pos\_post\_2opt()

\[FIX-4\] Actualiza `pos_actual` al último elemento de la ruta optimizada. Debe llamarse después de `route_2opt()`.

06

Loader
------

Loader class (métodos estáticos)

\[NEW-3/4\] Fábrica de entidades. Centraliza el parseo de CSV para que el Scheduler no dependa del formato de archivo directamente.

@staticmethod desde\_reservaciones(path, fecha, solo\_sucias=True) → list\[Room\]

Filtra el CSV por fecha y `estado_fisico=='SUCIO'`. Excluye habitaciones DND del pool (las imprime como advertencia). Convierte ventanas de tiempo a segundos. Las habitaciones con `evolucion=='CAMBIO'` recibirán prioridad máxima.

@staticmethod desde\_fleet(path, day, pos\_inicio='8101') → list\[StaffMember\]

Filtra por `day` (ej. `'day_6'`) y `status=='WORK'`. Convierte `shift_start`/`shift_end`/`break_hour` a segundos. Todos los empleados parten de `pos_inicio`.

**Convención de días:** `day_1` = domingo, `day_7` = sábado. El campo `DAY_KEY` en el ejemplo de uso corresponde al día de la semana de `FECHA_DIA`.

07

Prioritizer
-----------

Prioritizer class (métodos de clase)

\[FIX-5, NEW-1\] Asigna un score de prioridad a cada habitación sucia. Mayor score → se limpia antes. El cálculo es: `score = bonus_evolucion + bonus_vip + piso_score + limpieza_score`

### Pesos configurables

W\_VIP

100

Bonus si tipo ∈ VIP\_TYPES

W\_PISO

15

Por nivel de piso (escala relativa)

W\_LIMPIEZA

5

Habitaciones más rápidas, ligera ventaja

MAX\_PISO

5

Piso máximo del hotel

### Bonus por evolución

#1 CAMBIO

9 999 pts

#2 SALIDA

200 pts

#3 ENTRADA

50 pts

#4 PERMANENCIA

0 pts

**CAMBIO** devuelve el bonus directamente sin mezclar con VIP, piso, ni limpieza — tiene precedencia absoluta sobre cualquier otro criterio.

08

Scheduler
---------

Scheduler class

Planificador diario que integra los Módulos 2 y 3. Recibe el grafo en su constructor y expone dos etapas: `planificar()` para el plan maestro y `reparar()` para ajustes dinámicos.

### Etapa 1 — planificar(rooms, staff, verbose=True)

Ejecuta el pipeline de 4 pasos: calcular tiempos de limpieza → priorizar con Prioritizer → greedy con criterio unificado \[FIX-3\] y verificación de break \[FIX-2\] → 2-opt por ruta + sincronización \[FIX-4\].

Python

\# Estructura del greedy
for room in rooms\_sorted:
    mejor\_staff = None
    mejor\_costo = float('inf')

    for s in staff:
        traslado\_s = g.traslado(s.pos\_actual, room.num\_hab)

        \# \[FIX-2\] Verificar break y fin de turno
        if not s.puede\_asignar(traslado\_s, room.tiempo\_limpieza\_s):
            continue

        \# \[NEW-1\] Verificar ventana solicitada
        if room.tiene\_ventana:
            llegada\_est = s.tiempo\_actual\_s + traslado\_s
            if not (room.ventana\_inicio\_s <= llegada\_est <= room.ventana\_fin\_s):
                continue

        \# \[FIX-3\] Un único costo\_efectivo
        if traslado\_s < mejor\_costo:
            mejor\_costo = traslado\_s
            mejor\_staff = s

### Etapa 2 — reparar(evento, staff, rooms, verbose=True)

\[NEW-5\] Procesa un evento en tiempo real y repara solo la ruta afectada. Soporta 3 tipos:

DND

#### Do Not Disturb

Remueve la habitación de la ruta del empleado asignado, la marca `SIN_ASIGNAR`, y aplica 2-opt local sobre la ruta reducida.

CAMBIO\_URGENTE

#### Cambio Urgente

Busca al empleado con menor tiempo de llegada. Si es diferente al asignado, reasigna. En ambos casos mueve la habitación al frente de la ruta y sincroniza `pos_actual`.

VENTANA

#### Ventana Solicitada

Actualiza `ventana_inicio_s`, `ventana_fin_s` y `restriccion_huesped`. Si la ruta tiene más de 2 habitaciones, aplica 2-opt local para reordenar respetando la nueva ventana.

### Salidas

resumen\_detallado(staff, rooms) → (DataFrame, list\[str\])

Tabla resumen por empleado con inicio/fin de turno, break, habitaciones, tiempo de limpieza, utilización y ruta en orden. Retorna también la lista de habitaciones sin asignar.

log\_por\_empleado(staff) → DataFrame

Log detallado de cada tarea con timestamps reales (traslado\_inicio, tarea\_inicio, tarea\_fin), incluyendo el bloque de break como entrada especial. Ordenado por empleado y hora de inicio.

09

Ruteo 2-opt
-----------

route\_2opt · \_route\_cost funciones

Mejora local 2-opt: invierte segmentos de la ruta hasta que ningún intercambio reduzca el costo total. Complejidad O(n²) por iteración. Se usa tanto en el plan maestro (por cada empleado) como en la reparación dinámica (solo sobre la ruta afectada).

\_route\_cost(route, graph, limpieza\_map) → int

Costo total de una ruta en segundos: suma traslados entre habitaciones consecutivas + tiempos de limpieza. El primer elemento solo suma su limpieza (no hay traslado previo).

route\_2opt(route, graph, limpieza\_map) → list\[str\]

Recibe la ruta greedy y retorna la ruta optimizada. Prueba todas las inversiones de subarray \[i:j+1\] y aplica la que reduzca el costo. Itera hasta convergencia.

Algoritmo

\# Inversión de segmento \[i, j\]
candidate = best\[:i\] + best\[i:j+1\]\[::-1\] + best\[j+1:\]
cost = \_route\_cost(candidate, graph, limpieza\_map)
if cost < best\_cost:
    best, best\_cost = candidate, cost
    improved = True

**Impacto:** el resumen detallado muestra el ahorro en minutos por empleado al comparar el costo greedy vs el costo 2-opt.

10

Archivos CSV
------------

FL

fleet.csv data\\solver\\dev\\fleet.csv

Columna

Tipo

Descripción

employee\_id

str

Identificador único del empleado

employee\_name

str

Nombre completo

day

str

day\_1 (dom) … day\_7 (sáb)

status

str

WORK | OFF | VACATION (solo se cargan WORK)

shift\_type

str

MORNING | AFTERNOON

shift\_start

str

Hora inicio turno HH:MM (ej. 07:00)

shift\_end

str

Hora fin turno HH:MM (ej. 17:00)

break\_hour

int

Hora entera de inicio de break (ej. 11 → 11:00)

break\_duration\_minutes

int

Duración del break en minutos (ej. 30)

RV

reservaciones\_semana.csv data\\solver\\dev\\reservaciones\_semana.csv

Columna

Tipo

Descripción

fecha

str

YYYY-MM-DD — filtro principal del día

num\_hab

int

Código de habitación (ej. 4201)

tpo\_cama

str

Tipo de cama para calcular tiempo de limpieza

desc

str

Descripción del cuarto (determina VIP)

cpo

int

Capacidad máxima de personas

vista

str

Vista de la habitación

nom\_hsp

str

Nombre del huésped

num\_per

int

Personas actualmente en la habitación

evolucion

str

CAMBIO | SALIDA | ENTRADA | PERMANENCIA | DISPONIBLE

estado\_fisico

str

LIMPIO | SUCIO | FUERA\_DE\_SERVICIO

ocupacion

str

LIBRE | OCUPADO | EN\_LIMPIEZA

restriccion\_huesped

str

NINGUNA | DND | VENTANA\_SOLICITADA

ventana\_inicio

str

HH:MM — inicio ventana (solo si VENTANA\_SOLICITADA)

ventana\_fin

str

HH:MM — fin ventana (solo si VENTANA\_SOLICITADA)

TL

matriz\_tiempos\_limpieza.csv data\\solver\\matriz\_tiempos\_limpieza.csv

Columna

Tipo

Descripción

Configuración

str

Clave de tipo de cama (coincide con `tpo_cama`)

Tiempo Estimado (min)

int

Minutos base de limpieza para ese tipo

TR

matriz\_traslados\_segundos.csv data\\solver\\matriz\_traslados\_segundos.csv

Estructura

Tipo

Descripción

index (col 0)

str

num\_hab origen (ej. '4201') — cargado como index\_col=0

columnas

str

num\_hab destino — hasta 272 columnas

valores

int

Segundos de traslado entre origen y destino

Formato: matriz cuadrada 272×272. El índice y las columnas son strings del num\_hab. Si la celda no existe, `HotelGraph.traslado()` aplica el fallback Manhattan.

11

Ejemplo de Uso
--------------

Python — main

import os

BASE = r'C:\\Users\\luis\\Desktop\\TCA\\TCA-Optimization'

PATH\_TRASLADOS     = os.path.join(BASE, 'data\\solver\\matriz\_traslados\_segundos.csv')
PATH\_LIMPIEZA      = os.path.join(BASE, 'data\\solver\\matriz\_tiempos\_limpieza.csv')
PATH\_RESERVACIONES = os.path.join(BASE, 'data\\solver\\dev\\reservaciones\_semana.csv')
PATH\_FLEET         = os.path.join(BASE, 'data\\solver\\dev\\fleet.csv')

FECHA\_DIA = '2026-05-29'   \# viernes — día con más check-outs
DAY\_KEY   = 'day\_6'        \# day\_1=domingo … day\_7=sábado

\# 1. Cargar grafo
graph = HotelGraph(PATH\_TRASLADOS, PATH\_LIMPIEZA)

\# 2. Cargar habitaciones sucias del día
rooms = Loader.desde\_reservaciones(PATH\_RESERVACIONES, FECHA\_DIA)

\# 3. Cargar staff activo del día
staff = Loader.desde\_fleet(PATH\_FLEET, DAY\_KEY)

\# 4. Planificar (Etapa 1)
scheduler = Scheduler(graph)
asignaciones = scheduler.planificar(rooms, staff, verbose=True)

\# 5. Resumen con timestamps
df\_resumen, sin\_asignar = scheduler.resumen\_detallado(staff, rooms)
df\_log = scheduler.log\_por\_empleado(staff)

\# 6. Reparación dinámica (Etapa 2)
evento\_dnd = {
    'tipo':        'DND',
    'num\_hab':     '4201',
    'timestamp\_s': hhmm\_a\_seg('10:00'),  \# 36000
}
scheduler.reparar(evento\_dnd, staff, rooms, verbose=True)

**Salida esperada:** tabla de priorización (top 20), resumen de 2-opt con ahorro en minutos por empleado, DataFrame resumen y log de tareas con timestamps HH:MM.

TCA Optimization Motor de Ruteo v2 · Módulos 2 + 3 · mayo 2026
