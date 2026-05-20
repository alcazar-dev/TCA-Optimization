# tca_optimization/config.py
## Funciones de utilidad

from dataclasses import dataclass, field

def hhmm_a_seg(hhmm: str) -> int:
    """'07:00' → 25200"""
    h, m = map(int, str(hhmm).strip().split(':'))
    return h * 3600 + m * 60

def seg_a_hhmm(segundos: int) -> str:
    """25200 → '07:00'"""
    h = segundos // 3600
    m = (segundos % 3600) // 60
    return f"{h:02d}:{m:02d}"

def parse_num_hab(num_hab: str) -> tuple[int, int, int]:
    """'4201' → (edificio=4, piso=2, hab=01)"""
    s = str(num_hab)
    return int(s[:-3]), int(s[-3]), int(s[-2:])

@dataclass
class SchedulerConfig:
    max_plan_sec: float = 60.0          
    dp_max_n: int = 14                  
    max_vns_sec: float = 4.0            
    vip_multiplier: float = 1.4
    inter_floor_sec: int = 30
    inter_edif_sec: int = 300
    inter_hab_sec: int = 10
    vip_types: frozenset = field(default_factory=lambda: frozenset({
        'STD SUPERIOR', 'SUITE 1 REC', 'SUITE 2 REC',
        'SUITE STUDIO', 'FAMILIAR 5', 'FAMILIAR 6', 'PENT-HOUSE'
    }))
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