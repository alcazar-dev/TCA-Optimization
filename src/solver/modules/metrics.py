# tca_optimization/metrics.py
## Metricas y actualizacion continua

import time
import json
import pandas as pd
from .entities import Room, StaffMember

class MetricsCollector:
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
            'reparaciones_REINSERCION':    0,
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
        self._datos['duracion_total_s'] = self._datos['ts_fin'] - self._datos['ts_inicio']
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
        for etapa, key in [('CWS', 'cws_tiempo_s'), ('DP ', 'dp_tiempo_s'), ('VNS', 'vns_tiempo_s')]:
            val = d[key]
            if val is not None:
                lines.append(f"  Tiempo {etapa}:       {val:.2f}s")
        if d['vns_ahorro_s'] is not None:
            lines.append(f"  VNS gap:          {d['vns_ahorro_s']//60}min ({d.get('vns_gap_pct', 0):.1f}%) en {d['vns_iteraciones']} iter")
        if d['total_rooms'] is not None:
            lines.append(f"  Rooms:            {d['rooms_asignadas']}/{d['total_rooms']} asignadas  |  {d['rooms_sin_asignar']} sin asignar")
        if d['reparaciones_total'] > 0:
            lines.append(f"  Reparaciones:     {d['reparaciones_total']} total  (DND:{d['reparaciones_DND']}  URGENTE:{d['reparaciones_CAMBIO_URGENTE']}  VENTANA:{d['reparaciones_VENTANA']}  REINSERC:{d['reparaciones_REINSERCION']})")
        if d['fallback_greedy']:
            lines.append("   Fallback greedy activado")
        lines.append("─" * 55)
        return "\n".join(lines)

    def exportar_json(self, path: str):
        datos_export = {k: v for k, v in self._datos.items() if k not in ('ts_inicio', 'ts_fin')}
        with open(path, 'w', encoding='utf-8') as f:
            json.dump(datos_export, f, ensure_ascii=False, indent=2)
        print(f"  Métricas exportadas a {path}")

    def como_dataframe(self) -> pd.DataFrame:
        return pd.DataFrame([self._datos])

    def como_dict(self) -> dict:
        return dict(self._datos)