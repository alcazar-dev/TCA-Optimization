# Pruebas Algoritmo e implementacion

https://docs.google.com/document/d/1E1Vs5BlPRqs8J_mdl9tlNrOgOuYn4ytSERCwHO45RKg/edit?usp=sharing


---

## Diccionario de Datos: Vista Maestra (`v_maestra_hotel`)
(obtenido a partir de `dict_gen.py`
Esta vista representa la **Capa Silver** de datos, consolidando información de múltiples fuentes para alimentar el algoritmo de optimización de rutas (VRPTW).

### 1. Información de la Reservación (HSP)
Contiene los detalles de la estancia y el perfil del huésped.

| COLUMNA_FINAL | ORIGEN_LOGICO | TIPO | DESCRIPCION |
| :--- | :--- | :--- | :--- |
| `hsp_num_hab` | `hothsp.h_num_hab` | **DIRECTA** | NÚMERO DE HABITACIÓN |
| `hsp_tpo_hab` | `hothsp.h_tpo_hab` | **DIRECTA** | TIPO DE LAS HABITACIONES |
| `hsp_status` | `hothsp.h_status` | **DIRECTA** | Estatus de la reserva (10/50) |
| `hsp_fec_llegada` | `hothsp.h_fec_lld` | **DIRECTA** | FECHA DE LLEGADA |
| `hsp_hra_llegada` | `hothsp.h_hra_lld` | **DIRECTA** | HORA DE LLEGADA |
| `hsp_fec_salida` | `hothsp.h_fec_sda` | **DIRECTA** | FECHA DE SALIDA |
| `hsp_hra_salida` | `hothsp.h_hra_sda` | **DIRECTA** | HORA DE SALIDA |
| `hsp_num_personas` | `hothsp.h_num_per` | **DIRECTA** | NÚMERO DE PERSONAS |
| `hsp_num_adultos` | `hothsp.h_num_adu` | **DIRECTA** | NÚMERO DE ADULTOS |
| `hsp_num_menores` | `hothsp.h_num_men` | **DIRECTA** | NÚMERO DE MENORES |
| `hsp_num_juniors` | `hothsp.h_num_jun` | **DIRECTA** | Número de menores junior |
| `hsp_segmento_mercado` | `hothsp.h_seg_mer` | **DIRECTA** | SEGMENTO DE MERCADO |
| `hsp_paquete` | `hothsp.h_paquete` | **DIRECTA** | TIPO DE PAQUETE |
| `hsp_programa` | `hothsp.h_programa` | **DIRECTA** | PROG COMERCIAL AL QUE PERTENECE |
| `hsp_cupo` | `hothsp.h_cupo` | **DIRECTA** | Número de cupo 0 al 9 |
| `hsp_tarifa_renta` | `hothsp.h_tfa_renta` | **DIRECTA** | Tarifa para Renta |
| `hsp_tarifa_impuestos` | `hothsp.h_tfa_impuestos` | **DIRECTA** | Tarifa con Impuestos |
| `hsp_canal_reservacion` | `hothsp.h_can_res` | **DIRECTA** | CANAL DE RESERVACIÓN |
| `hsp_clave_reservacion` | `hothsp.h_res_cve` | **DIRECTA** | Clave única de reserva |
| `hsp_fec_reservacion` | `hothsp.h_res_fec` | **DIRECTA** | FECHA DE RESERVACIÓN |
| `hsp_hra_ultimo_cambio` | `hothsp.h_ult_cam_hra` | **DIRECTA** | HORA DE ÚLTIMO CAMBIO AL REGISTRO |
| `hsp_tipo_brazalete` | `hothsp.h_tipo_bra` | **DIRECTA** | Catálogo de tipo de brazalete |

---

### 2. Estado Físico y Operativo (HAB / STP / CTH)
Define la ubicación física y las condiciones de limpieza de la unidad.

| COLUMNA_FINAL | ORIGEN_LOGICO | TIPO | DESCRIPCION |
| :--- | :--- | :--- | :--- |
| `hab_status_recepcion` | `hothab.c_sta_rec` | **DIRECTA** | STATUS DE RECEPCIÓN |
| `hab_status_ama` | `hothab.c_sta_ama` | **DIRECTA** | STATUS DE AMA DE LLAVES |
| `hab_edificio` | `hothab.c_edif` | **DIRECTA** | EDIFICIO EN DONDE SE ENCUENTRA LA HAB. |
| `hab_piso` | `hothab.c_piso` | **DIRECTA** | PISO |
| `hab_factor_ama` | `hothab.c_factor_ama` | **DIRECTA** | VALOR EN UNIDADES PARA CAMARISTA |
| `hab_tiene_cuna` | `hothab.c_cuna` | **DIRECTA** | INDICA SI TIENE CUNA |
| `hab_tipo_cama` | `hothab.c_tpo_cma` | **DIRECTA** | TIPO DE CAMA(S) |
| `hab_num_per_ama` | `hothab.c_num_per` | **DIRECTA** | NUM DE PERSONAS REPORTADAS POR AMA |
| `hab_noches_ocupadas` | `hothab.c_num_noc` | **DIRECTA** | NOCHES OCUPADAS |
| `hab_noches_desde_cambio_colchon` | `hothab.c_num_noc_col` | **DIRECTA** | NOCHES DESDE CAMBIO DE COLCHÓN |
| `hab_fec_cambio_blancos` | `hothab.c_fec_cam_bla` | **DIRECTA** | FECHA DE CAMBIO DE BLANCOS |
| `stp_descripcion_estado` | `hotstp.o_des_corto` | **DIRECTA** | Descripción corta del semáforo |
| `stp_cod_corto` | `hotstp.o_cod_corto` | **DIRECTA** | Letra código corto |
| `stp_origen` | `hotstp.o_inr` | **DIRECTA** | Origen del estatus (Ama o Recepción) |
| `cth_descripcion` | `hotcth.descrip` | **DIRECTA** | Descripción del tipo de habitación |
| `cth_factor_ama_base` | `hotcth.fac_ama` | **DIRECTA** | Factor base de limpieza por tipo |
| `cth_cupo_teorico` | `hotcth.cupo` | **DIRECTA** | Cupo Teórico (1=Ind, 2=Dob, etc.) |
| `cth_es_compuesta` | `hotcth.tpo_comp` | **DIRECTA** | Indica si la habitación es compuesta (S/N) |

---

### 3. Logística y Agregaciones (AGG)
Variables calculadas que resumen la complejidad y urgencia del servicio.

| COLUMNA_FINAL | ORIGEN_LOGICO | TIPO | DESCRIPCION |
| :--- | :--- | :--- | :--- |
| `fbl_prioridad_recepcion` | `fbl_agg` | **AGREGACION** | Resumen de prioridad de bloqueos (Count/Max) |
| `fbl_ultimo_bloqueo` | `fbl_agg` | **AGREGACION** | Fecha del último bloqueo registrado |
| `req_total` | `req_agg` | **AGREGACION** | Total de requerimientos especiales pendientes |
| `req_tipo_r` | `req_agg` | **AGREGACION** | Conteo de requerimientos tipo R (Recamarista) |
| `req_tipo_b` | `req_agg` | **AGREGACION** | Conteo de requerimientos tipo B (Brazalete) |
| `req_tipo_e` | `req_agg` | **AGREGACION** | Conteo de requerimientos tipo E (Especial) |
| `req_articulos_total` | `req_agg` | **AGREGACION** | Suma total de artículos solicitados |
| `req_proxima_ejecucion` | `req_agg` | **AGREGACION** | Fecha mínima de ejecución de requerimientos |
| `tra_num_transacciones` | `tra_agg` | **AGREGACION** | Volumen de transacciones en la reserva |
| `tra_renta_total` | `tra_agg` | **AGREGACION** | Suma de renta total (monetario) |
| `tra_ultima_transaccion` | `tra_agg` | **AGREGACION** | Fecha de la última actividad financiera |

---

### 4. Estadísticas y Predicción (VTA / EST)
Datos históricos y métricas de ocupación para validación.

| COLUMNA_FINAL | ORIGEN_LOGICO | TIPO | DESCRIPCION |
| :--- | :--- | :--- | :--- |
| `vta_cuartos_noche` | `hotvta.cto_noc` | **DIRECTA** | HABITACIONES VENDIDAS POR DÍA |
| `vta_cuartos_llegada` | `hotvta.cto_lld` | **DIRECTA** | CTOS ENTRANDO EN EL DÍA |
| `vta_cuartos_salida` | `hotvta.cto_sda` | **DIRECTA** | CUARTOS SALIDA |
| `vta_ingreso_hab` | `hotvta.cto_ing` | **DIRECTA** | INGRESOS POR HABITACIÓN |
| `vta_ingreso_dls` | `hotvta.cto_ing_dls` | **DIRECTA** | Ingresos en Dólares |
| `vta_segmento_mercado` | `hotvta.seg_mer` | **DIRECTA** | SEGMENTO DE MERCADO (Venta) |
| `est_cuartos_total` | `hotest.cto_tot` | **DIRECTA** | TOTAL DE CUARTOS EN EL HOTEL |
| `est_fuera_servicio` | `hotest.cto_fs` | **DIRECTA** | FUERA DE SERVICIO / OCUP. OTRO HOTEL |
| `est_cancelados` | `hotest.cto_cancel` | **DIRECTA** | CUARTOS CANCELADOS DEL DÍA |

---

### 5. Variables Clave del Solver
Lógica de negocio derivada para el algoritmo de optimización.

| COLUMNA_FINAL | ORIGEN_LOGICO | TIPO | DESCRIPCION |
| :--- | :--- | :--- | :--- |
| **`factor_limpieza_efectivo`** | `hab + cth` | **DERIVADA** | **Variable crítica:** Prioridad y peso de limpieza calculada mediante `COALESCE` entre el factor específico de la habitación y el base por tipo. |

---

Espero que este formato te sea de gran utilidad para documentar tu proyecto. ¿Te gustaría que añadiera una breve explicación técnica sobre cómo la columna `factor_limpieza_efectivo` influye en el cálculo del VRPTW?
