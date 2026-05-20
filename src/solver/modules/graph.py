# tca_optimization/graph.py
## Grafos y costos

import math
import pandas as pd
from .config import SchedulerConfig, parse_num_hab

class HotelGraph:
    def __init__(self, path_traslados: str, path_limpieza: str, config: SchedulerConfig | None = None):
        self._cfg = config or SchedulerConfig()
        self._traslados = pd.read_csv(path_traslados, index_col=0)
        self._traslados.index   = self._traslados.index.astype(str)
        self._traslados.columns = self._traslados.columns.astype(str)
        raw_lim = pd.read_csv(path_limpieza)
        self._limpieza_dict: dict[str, int] = dict(
            zip(raw_lim['Configuración'], raw_lim['Tiempo Estimado (min)'])
        )

    def traslado(self, origen: str, destino: str) -> int:
        if origen == destino:
            return 0
        try:
            return int(self._traslados.loc[str(origen), str(destino)])
        except KeyError:
            cfg = self._cfg
            e1, p1, h1 = parse_num_hab(origen)
            e2, p2, h2 = parse_num_hab(destino)
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