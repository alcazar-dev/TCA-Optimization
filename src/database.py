import duckdb
import os

# --- CONFIGURACIÓN DE RUTAS ---
DATA_PATH = r"D:\TCA_App\Optimization\TCA-Optimization\data\charts"
DB_FILE   = r"D:\TCA_App\Optimization\TCA-Optimization\housekeeping.db"

def get_connection():
    """Establece conexión persistente con DuckDB."""
    return duckdb.connect(DB_FILE)

def init_db():
    """
    Inicializa la base de datos:
    1. Registra vistas individuales de cada Parquet (Capa Bronze).
    2. Crea la Vista Maestra Unificada (Capa Silver) para el Solver.
    """
    if not os.path.exists(DATA_PATH):
        print(f"Error: No se encontró la carpeta de datos en {DATA_PATH}")
        return

    con = get_connection()

    # --- PASO 1: REGISTRO DE VISTAS INDIVIDUALES (Capa Bronze) ---
    files = [f for f in os.listdir(DATA_PATH) if f.endswith('.parquet')]
    if not files:
        print(f"Error: No se encontraron archivos .parquet en {DATA_PATH}")
        con.close()
        return

    for file in files:
        table_name = file.replace('.parquet', '')
        file_path  = os.path.join(DATA_PATH, file).replace("\\", "/")
        con.execute(f"CREATE OR REPLACE VIEW {table_name} AS SELECT * FROM read_parquet('{file_path}');")
        print(f"Vista Bronze registrada: {table_name}")

    # --- PASO 2: CREACIÓN DE LA VISTA MAESTRA (Capa Silver) ---
    def p(name):
        return os.path.join(DATA_PATH, f"{name}.parquet").replace("\\", "/")

    master_query = f"""
    CREATE OR REPLACE VIEW v_maestra_hotel AS
    WITH
    hsp AS (
        SELECT
            h_num_hab               AS hsp_num_hab,
            h_tpo_hab               AS hsp_tpo_hab,
            h_status                AS hsp_status,
            h_fec_lld               AS hsp_fec_llegada,
            h_fec_sda               AS hsp_fec_salida,
            h_hra_lld               AS hsp_hra_llegada,
            h_hra_sda               AS hsp_hra_salida,
            h_num_per               AS hsp_num_personas,
            h_num_adu               AS hsp_num_adultos,
            h_num_men               AS hsp_num_menores,
            h_num_jun               AS hsp_num_juniors,
            h_seg_mer               AS hsp_segmento_mercado,
            h_paquete               AS hsp_paquete,
            h_programa              AS hsp_programa,
            h_cupo                  AS hsp_cupo,
            h_tfa_renta             AS hsp_tarifa_renta,
            h_tfa_impuestos         AS hsp_tarifa_impuestos,
            h_can_res               AS hsp_canal_reservacion,
            h_res_cve               AS hsp_clave_reservacion,
            h_res_fec               AS hsp_fec_reservacion,
            h_ult_cam_hra           AS hsp_hra_ultimo_cambio,
            h_tipo_bra              AS hsp_tipo_brazalete
        FROM read_parquet('{p("hothsp")}')
        WHERE h_status IN ('10', '50')
    ),
    hab AS (
        SELECT
            c_num_hab               AS hab_num_hab,
            c_tpo_hab               AS hab_tpo_hab,
            c_sta_rec               AS hab_status_recepcion,
            c_sta_ama               AS hab_status_ama,
            c_edif                  AS hab_edificio,
            c_piso                  AS hab_piso,
            c_factor_ama            AS hab_factor_ama,
            c_cuna                  AS hab_tiene_cuna,
            c_tpo_cma               AS hab_tipo_cama,
            c_num_per               AS hab_num_per_ama,
            c_num_noc               AS hab_noches_ocupadas,
            c_num_noc_col           AS hab_noches_desde_cambio_colchon,
            c_fec_ini_blo           AS hab_fec_inicio_bloqueo,
            c_fec_fin_blo           AS hab_fec_fin_bloqueo,
            c_cve_res               AS hab_clave_reservacion,
            c_des_cta               AS hab_descripcion_corta,
            c_cod_corto             AS hab_codigo_corto,
            c_fec_cam_bla           AS hab_fec_cambio_blancos
        FROM read_parquet('{p("hothab")}')
    ),
    stp AS (
        SELECT
            o_cod                   AS stp_cod_estado,
            o_cod_corto             AS stp_cod_corto,
            o_des_corto             AS stp_descripcion_estado,
            o_inr                   AS stp_origen
        FROM read_parquet('{p("hotstp")}')
    ),
    cth AS (
        SELECT
            tpo_hab                 AS cth_tpo_hab,
            descrip                 AS cth_descripcion,
            fac_ama                 AS cth_factor_ama_base,
            cupo                    AS cth_cupo_teorico,
            tpo_comp                AS cth_es_compuesta
        FROM read_parquet('{p("hotcth")}')
    ),
    fbl AS (
        SELECT
            num_hab                 AS fbl_num_hab,
            fecha_ini_blo           AS fbl_fec_inicio,
            cod_blo                 AS fbl_cod_bloqueo
        FROM read_parquet('{p("hotfbl")}')
    ),
    req AS (
        SELECT
            res_cve                 AS req_clave_reservacion,
            cve_req                 AS req_clave,
            desc_req                AS req_descripcion,
            tipo_req                AS req_tipo,
            fecha_ejec              AS req_fecha_ejecucion,
            num_art                 AS req_num_articulos
        FROM read_parquet('{p("hotreq")}')
        WHERE pendiente = 'S'
    ),
    tra AS (
        SELECT
            t_cve_res               AS tra_clave_reservacion,
            t_fecha                 AS tra_fecha,
            t_renta                 AS tra_renta,
            t_noches                AS tra_noches,
            t_num_per               AS tra_num_personas
        FROM read_parquet('{p("hottra")}')
    ),
    vta AS (
        SELECT
            fec_vta                 AS vta_fecha,
            tpo_hab                 AS vta_tpo_hab,
            cto_noc                 AS vta_cuartos_noche,
            cto_lld                 AS vta_cuartos_llegada,
            cto_sda                 AS vta_cuartos_salida,
            cto_ing                 AS vta_ingreso_hab,
            cto_ing_dls             AS vta_ingreso_dls,
            seg_mer                 AS vta_segmento_mercado
        FROM read_parquet('{p("hotvta")}')
    ),
    est AS (
        SELECT
            est_fec                 AS est_fecha,
            tpo_hab                 AS est_tpo_hab,
            cto_tot                 AS est_cuartos_total,
            cto_fs                  AS est_fuera_servicio,
            cto_cancel              AS est_cancelados
        FROM read_parquet('{p("hotest")}')
    ),
    -- Pre-agregaciones para evitar explosión de filas en JOINs 1-a-muchos
    req_agg AS (
        SELECT
            req_clave_reservacion,
            COUNT(*)                                            AS req_total,
            COUNT(*) FILTER (WHERE req_tipo = 'R')             AS req_tipo_r,
            COUNT(*) FILTER (WHERE req_tipo = 'B')             AS req_tipo_b,
            COUNT(*) FILTER (WHERE req_tipo = 'E')             AS req_tipo_e,
            SUM(req_num_articulos)                             AS req_articulos_total,
            MIN(req_fecha_ejecucion)                           AS req_proxima_ejecucion
        FROM req
        GROUP BY req_clave_reservacion
    ),
    tra_agg AS (
        SELECT
            tra_clave_reservacion,
            COUNT(*)                                            AS tra_num_transacciones,
            SUM(TRY_CAST(tra_renta AS DOUBLE))                 AS tra_renta_total,
            MAX(tra_fecha)                                     AS tra_ultima_transaccion
        FROM tra
        GROUP BY tra_clave_reservacion
    ),
    fbl_agg AS (
        SELECT
            fbl_num_hab,
            MAX(CASE WHEN fbl_cod_bloqueo = '01' THEN 1 ELSE 0 END) AS fbl_prioridad_recepcion,
            MAX(fbl_fec_inicio)                                AS fbl_ultimo_bloqueo
        FROM fbl
        GROUP BY fbl_num_hab
    )

    SELECT
        -- Reservación base
        hsp.hsp_num_hab,
        hsp.hsp_tpo_hab,
        hsp.hsp_status,
        hsp.hsp_fec_llegada,
        hsp.hsp_hra_llegada,
        hsp.hsp_fec_salida,
        hsp.hsp_hra_salida,
        hsp.hsp_num_personas,
        hsp.hsp_num_adultos,
        hsp.hsp_num_menores,
        hsp.hsp_num_juniors,
        hsp.hsp_segmento_mercado,
        hsp.hsp_paquete,
        hsp.hsp_programa,
        hsp.hsp_cupo,
        hsp.hsp_tarifa_renta,
        hsp.hsp_tarifa_impuestos,
        hsp.hsp_canal_reservacion,
        hsp.hsp_clave_reservacion,
        hsp.hsp_fec_reservacion,
        hsp.hsp_hra_ultimo_cambio,
        hsp.hsp_tipo_brazalete,
        -- Estado operativo
        hab.hab_status_recepcion,
        hab.hab_status_ama,
        hab.hab_edificio,
        hab.hab_piso,
        hab.hab_factor_ama,
        hab.hab_tiene_cuna,
        hab.hab_tipo_cama,
        hab.hab_num_per_ama,
        hab.hab_noches_ocupadas,
        hab.hab_noches_desde_cambio_colchon,
        hab.hab_fec_cambio_blancos,
        -- Semáforo
        stp.stp_descripcion_estado,
        stp.stp_cod_corto,
        stp.stp_origen,
        -- Complejidad por tipo
        cth.cth_descripcion,
        cth.cth_factor_ama_base,
        cth.cth_cupo_teorico,
        cth.cth_es_compuesta,
        -- Bloqueos
        fbl_agg.fbl_prioridad_recepcion,
        fbl_agg.fbl_ultimo_bloqueo,
        -- Requerimientos
        req_agg.req_total,
        req_agg.req_tipo_r,
        req_agg.req_tipo_b,
        req_agg.req_tipo_e,
        req_agg.req_articulos_total,
        req_agg.req_proxima_ejecucion,
        -- Transacciones
        tra_agg.tra_num_transacciones,
        tra_agg.tra_renta_total,
        tra_agg.tra_ultima_transaccion,
        -- Predicción
        vta.vta_cuartos_noche,
        vta.vta_cuartos_llegada,
        vta.vta_cuartos_salida,
        vta.vta_ingreso_hab,
        vta.vta_ingreso_dls,
        vta.vta_segmento_mercado,
        -- Ground truth
        est.est_cuartos_total,
        est.est_fuera_servicio,
        est.est_cancelados,
        -- Columna derivada clave para el Solver
        COALESCE(hab.hab_factor_ama, cth.cth_factor_ama_base) AS factor_limpieza_efectivo

    FROM hsp
    LEFT JOIN hab     ON hsp.hsp_num_hab        = hab.hab_num_hab
    LEFT JOIN stp     ON hab.hab_status_recepcion = stp.stp_cod_estado
    LEFT JOIN cth     ON hsp.hsp_tpo_hab         = cth.cth_tpo_hab
    LEFT JOIN fbl_agg ON hsp.hsp_num_hab         = fbl_agg.fbl_num_hab
    LEFT JOIN req_agg ON hsp.hsp_clave_reservacion = req_agg.req_clave_reservacion
    LEFT JOIN tra_agg ON hsp.hsp_clave_reservacion = tra_agg.tra_clave_reservacion
    LEFT JOIN vta     ON hsp.hsp_fec_llegada     = vta.vta_fecha
                      AND hsp.hsp_tpo_hab        = vta.vta_tpo_hab
    LEFT JOIN est     ON hsp.hsp_fec_llegada     = est.est_fecha
                      AND hsp.hsp_tpo_hab        = est.est_tpo_hab
    """

    try:
        con.execute(master_query)
        print("Vista Maestra 'v_maestra_hotel' creada con éxito.")
    except Exception as e:
        print(f"Error al crear la Vista Maestra: {e}")
        raise  # re-lanza para ver el traceback completo
    finally:
        con.close()


def get_solver_data():
    """Retorna el universo de trabajo consolidado en un DataFrame."""
    con = get_connection()
    try:
        df = con.execute("SELECT * FROM v_maestra_hotel").df()
        return df
    finally:
        con.close()


if __name__ == "__main__":
    init_db()
    data = get_solver_data()
    print(f"\n Datos listos para el Solver: {data.shape[0]} habitaciones identificadas.")
    print(data[[
        'hsp_num_hab', 'hab_edificio', 'hab_piso',
        'factor_limpieza_efectivo', 'fbl_prioridad_recepcion',
        'req_total', 'stp_descripcion_estado'
    ]].head(10))