import streamlit as st
import pandas as pd
import numpy as np
import io
from sqlalchemy import create_engine, text
from datetime import datetime, date

# ============================================================
# CONFIGURACIÓN
# ============================================================
st.set_page_config(
    page_title="MRP Gastronómico",
    page_icon="🍽️",
    layout="wide",
    initial_sidebar_state="expanded"
)

st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=DM+Serif+Display&family=DM+Sans:wght@300;400;500;600&display=swap');

    html, body, [class*="css"] {
        font-family: 'DM Sans', sans-serif;
    }

    .main { background-color: #0f0f0f; color: #f0ede8; }
    .stApp { background-color: #0f0f0f; }

    section[data-testid="stSidebar"] {
        background-color: #1a1a1a;
        border-right: 1px solid #2a2a2a;
    }

    .block-container { padding-top: 2rem; }

    h1, h2, h3 {
        font-family: 'DM Serif Display', serif;
        color: #f0ede8;
        letter-spacing: -0.02em;
    }

    .metric-card {
        background: #1a1a1a;
        border: 1px solid #2a2a2a;
        border-radius: 12px;
        padding: 1.2rem 1.5rem;
        margin-bottom: 0.5rem;
    }
    .metric-card .label {
        font-size: 0.75rem;
        text-transform: uppercase;
        letter-spacing: 0.1em;
        color: #888;
        margin-bottom: 0.3rem;
    }
    .metric-card .value {
        font-family: 'DM Serif Display', serif;
        font-size: 1.8rem;
        color: #d4a853;
    }

    .tag-green  { background:#1a3a2a; color:#4caf7d; padding:3px 10px; border-radius:20px; font-size:0.8rem; }
    .tag-orange { background:#3a2a1a; color:#e89c45; padding:3px 10px; border-radius:20px; font-size:0.8rem; }
    .tag-red    { background:#3a1a1a; color:#e84545; padding:3px 10px; border-radius:20px; font-size:0.8rem; }

    .stDataFrame { border-radius: 10px; overflow: hidden; }

    div[data-testid="stMetric"] {
        background: #1a1a1a;
        border: 1px solid #2a2a2a;
        border-radius: 12px;
        padding: 1rem 1.2rem;
    }
    div[data-testid="stMetric"] label { color: #888 !important; font-size: 0.8rem; }
    div[data-testid="stMetric"] div[data-testid="stMetricValue"] { color: #d4a853 !important; }

    .stButton>button {
        background: #d4a853;
        color: #0f0f0f;
        font-weight: 600;
        border: none;
        border-radius: 8px;
        padding: 0.5rem 1.5rem;
        transition: all 0.2s;
    }
    .stButton>button:hover { background: #e8c07a; transform: translateY(-1px); }

    .stSelectbox>div>div, .stDateInput>div>div>input {
        background: #1a1a1a !important;
        border-color: #2a2a2a !important;
        color: #f0ede8 !important;
    }

    .section-title {
        font-family: 'DM Serif Display', serif;
        font-size: 1.6rem;
        color: #f0ede8;
        border-bottom: 1px solid #2a2a2a;
        padding-bottom: 0.5rem;
        margin-bottom: 1.5rem;
    }

    .info-box {
        background: #1a1f2e;
        border-left: 3px solid #d4a853;
        border-radius: 0 8px 8px 0;
        padding: 0.8rem 1rem;
        margin: 0.5rem 0 1rem 0;
        font-size: 0.88rem;
        color: #aaa;
    }
</style>
""", unsafe_allow_html=True)


# ============================================================
# BASE DE DATOS
# ============================================================
@st.cache_resource
def get_engine():
    try:
        db = st.secrets["connections"]["supabase"]
        conn_str = (
            f"postgresql+psycopg2://{db['user']}:{db['password']}"
            f"@{db['host']}:{db['port']}/{db['database']}?sslmode=require"
        )
        return create_engine(
            conn_str,
            pool_pre_ping=True,
            pool_recycle=300,
            connect_args={"options": "-c statement_timeout=30000"}
        )
    except Exception as e:
        st.error(f"❌ Error de conexión: {e}")
        return None


def run_query(sql, params=None):
    engine = get_engine()
    if engine is None:
        return pd.DataFrame()
    try:
        with engine.connect() as conn:
            return pd.read_sql(text(sql), conn, params=params or {})
    except Exception as e:
        st.error(f"Error en consulta: {e}")
        return pd.DataFrame()


# ============================================================
# LÓGICA MRP (código 1 preservado íntegramente)
# ============================================================
def process_bom(df_v, df_d, df_p):
    df_v.columns = df_v.columns.str.strip()
    df_d.columns = df_d.columns.str.strip()
    df_p.columns = df_p.columns.str.strip()

    df_v = df_v.rename(columns={'SKU': 'SKU_VENTA', 'Cantidad': 'CANT_VENTA'})
    skus_vendidos = set(df_v['SKU_VENTA'].astype(str).str.strip().str.upper())

    def validar_opcion(row):
        es_op = str(row['EsOpcion']).strip()
        if pd.isna(row['EsOpcion']) or es_op in ["", "0", "4"]:
            return True
        return str(row['SKU']).strip().upper() in skus_vendidos

    df_d_ready = df_d[df_d.apply(validar_opcion, axis=1)].copy()
    m1 = pd.merge(df_v, df_d_ready, left_on='SKU_VENTA', right_on='CODIGO VENTA', how='inner')

    es_proc = m1['SKU'].str.startswith('PRO-', na=False)
    df_insumos_directos = m1[~es_proc].copy()
    df_procesados_a_explotar = m1[es_proc].copy()

    if not df_procesados_a_explotar.empty:
        rendimientos = df_p.groupby('Codigo Venta')['CantReceta'].sum().reset_index()
        rendimientos = rendimientos.rename(columns={'CantReceta': 'TOTAL_RECETA_AUTO'})

        df_p_clean = df_p.rename(columns={
            'Codigo Venta': 'COD_P',
            'Ingrediente': 'NOM_P',
            'CantEfic': 'CE_P',
            'CantReceta': 'CR_P',
            'Porcion': 'MARK_P',
            'UM Salida': 'UM_P',
            'SKU Ingrediente': 'SKU_P'
        })
        df_p_final = pd.merge(df_p_clean, rendimientos, left_on='COD_P', right_on='Codigo Venta', how='left')
        m2 = pd.merge(df_procesados_a_explotar, df_p_final, left_on='SKU', right_on='COD_P', how='left')

        def calcular_m2(row):
            if row['MARK_P'] == 1:
                return row['CANT_VENTA'] * row['CantReal'] * row['CE_P']
            else:
                divisor = row['TOTAL_RECETA_AUTO'] if row['TOTAL_RECETA_AUTO'] > 0 else 1
                return row['CANT_VENTA'] * row['CantReal'] * (row['CE_P'] / divisor)

        m2['CANT_OUT'] = m2.apply(calcular_m2, axis=1)
        exp_f = m2[['SKU_P', 'NOM_P', 'CANT_OUT', 'UM_P']].rename(
            columns={'SKU_P': 'SKU_FIN', 'NOM_P': 'ING_FIN', 'UM_P': 'UM_FIN'})
    else:
        exp_f = pd.DataFrame()

    df_insumos_directos['CANT_OUT'] = (
        df_insumos_directos['CANT_VENTA'] * df_insumos_directos['CantReal']
    )
    dir_out = df_insumos_directos[['SKU', 'Ingrediente', 'CANT_OUT', 'UM']].rename(
        columns={'SKU': 'SKU_FIN', 'Ingrediente': 'ING_FIN', 'UM': 'UM_FIN'})

    consolidado = pd.concat([dir_out, exp_f], ignore_index=True)
    resumen = consolidado.groupby(['SKU_FIN', 'UM_FIN'], as_index=False).agg(
        {'CANT_OUT': 'sum', 'ING_FIN': 'first'})

    def formatear(row):
        um = str(row['UM_FIN']).upper()
        if um in ['G', 'ML', 'CC']:
            return row['CANT_OUT'] / 1000
        return row['CANT_OUT']

    resumen['TOTAL'] = resumen.apply(formatear, axis=1)
    return resumen[['SKU_FIN', 'ING_FIN', 'UM_FIN', 'TOTAL']].rename(
        columns={'SKU_FIN': 'SKU', 'ING_FIN': 'Insumo', 'UM_FIN': 'UM', 'TOTAL': 'Total Kg/L/Un'})


# ============================================================
# CÁLCULO DE COSTO TEÓRICO POR PLATO (Informe 1)
# Directos: CantReal × MUC
# Procesados: CantEfic × MUC  (usando último precio por SKU)
# ============================================================
def calcular_costo_platos(engine, fecha_i, fecha_f, local):
    """
    Devuelve DataFrame con costo teórico por código de venta (plato).
    Precio unitario = monto_real / cant_conv (último registro por SKU).
    Aplica factor_um para convertir unidades del recetario a unidades de compra.
    """
    # Precio unitario real = monto_real / cant_conv (último registro por SKU)
    precio_sql = """
        SELECT DISTINCT ON (sku) sku,
               monto_real / NULLIF(cant_conv, 0) as precio_unitario
        FROM compras
        WHERE cant_conv > 0
        ORDER BY sku, fecha_dte DESC
    """
    df_precio = run_query(precio_sql)
    if df_precio.empty:
        return pd.DataFrame()

    # Factor conversión unidades: G/CC/ML → /1000, resto → 1
    def factor_um(um):
        if pd.isna(um): return 1
        um = str(um).strip().upper()
        if um in ['G', 'CC', 'ML']: return 1/1000
        return 1

    # Recetario completo
    df_rec = run_query("SELECT * FROM recetas")
    if df_rec.empty:
        return pd.DataFrame()

    df_dir  = df_rec[df_rec['es_procesado'] == False].copy()
    df_proc = df_rec[df_rec['es_procesado'] == True].copy()

    # ---- DIRECTOS: cant_real × factor_um × precio_unitario ----
    dir_m = pd.merge(df_dir, df_precio, left_on='sku_ingrediente', right_on='sku', how='left')
    dir_m['cant_real']      = pd.to_numeric(dir_m['cant_real'], errors='coerce').fillna(0)
    dir_m['precio_unitario']= pd.to_numeric(dir_m['precio_unitario'], errors='coerce').fillna(0)
    dir_m['factor']         = dir_m['um_salida'].apply(factor_um)
    dir_m['costo_parcial']  = dir_m['cant_real'] * dir_m['factor'] * dir_m['precio_unitario']
    costo_dir = dir_m.groupby('codigo_venta')['costo_parcial'].sum().reset_index()

    # ---- PROCESADOS: cant_efic × factor_um × precio_unitario ----
    proc_m = pd.merge(df_proc, df_precio, left_on='sku_ingrediente', right_on='sku', how='left')
    proc_m['cant_efic']      = pd.to_numeric(proc_m['cant_efic'], errors='coerce').fillna(0)
    proc_m['precio_unitario']= pd.to_numeric(proc_m['precio_unitario'], errors='coerce').fillna(0)
    proc_m['factor']         = proc_m['um_salida'].apply(factor_um)
    proc_m['costo_parcial']  = proc_m['cant_efic'] * proc_m['factor'] * proc_m['precio_unitario']
    costo_proc = proc_m.groupby('codigo_venta')['costo_parcial'].sum().reset_index()

    # ---- Combinar ----
    costo_total  = pd.concat([costo_dir, costo_proc], ignore_index=True)
    costo_platos = costo_total.groupby('codigo_venta')['costo_parcial'].sum().reset_index()
    costo_platos.columns = ['sku_producto', 'costo_unitario_teorico']

    return costo_platos


# ============================================================
# INFORME 1: RENTABILIDAD POR PRODUCTO / CATEGORÍA
# ============================================================
def informe_rentabilidad(fecha_i, fecha_f, local):
    engine = get_engine()
    if engine is None:
        return pd.DataFrame()

    filtro_local_r = "AND UPPER(local) = UPPER(:l)" if local != "Todos" else ""
    params = {"i": str(fecha_i), "f": str(fecha_f)}
    if local != "Todos":
        params["l"] = local

    q_v = f"""
        SELECT sku_producto, nombre_producto, categoria_menu,
               SUM(cantidad_vendida) as cant,
               SUM(monto_venta_real) as venta
        FROM ventas
        WHERE fecha_venta BETWEEN :i AND :f
        {filtro_local_r}
        GROUP BY 1, 2, 3
    """

    df_v = run_query(q_v, params)
    if df_v.empty:
        st.warning("No hay ventas para el período/local seleccionado.")
        return pd.DataFrame()

    costo_platos = calcular_costo_platos(engine, fecha_i, fecha_f, local)
    if costo_platos.empty:
        st.warning("No se pudo calcular el costo teórico. Verifica recetario y MUC en compras.")
        return pd.DataFrame()

    df = pd.merge(df_v, costo_platos, on='sku_producto', how='left')
    df['costo_unitario_teorico'] = df['costo_unitario_teorico'].fillna(0)
    df['costo_total'] = df['cant'] * df['costo_unitario_teorico']
    df['venta'] = df['venta'].fillna(0)
    df['rentabilidad'] = df['venta'] - df['costo_total']
    df['margen_pct'] = df.apply(
        lambda x: (x['rentabilidad'] / x['venta'] * 100) if x['venta'] > 0 else 0, axis=1)

    return df.sort_values('venta', ascending=False)


# ============================================================
# INFORME 2: DESVIACIÓN REAL VS TEÓRICO
# ============================================================
def informe_desviacion(fecha_i, fecha_f, local):
    engine = get_engine()
    if engine is None:
        return pd.DataFrame()

    # Ventas del período — casteamos fechas a string para evitar problemas de tipo con SQLAlchemy
    filtro_local_v = "AND UPPER(local) = UPPER(:l)" if local != "Todos" else ""
    params = {"i": str(fecha_i), "f": str(fecha_f)}
    if local != "Todos":
        params["l"] = local

    q_v = f"""
        SELECT sku_producto, SUM(cantidad_vendida) as cant_vendida
        FROM ventas
        WHERE fecha_venta BETWEEN :i AND :f
        {filtro_local_v}
        GROUP BY 1
    """
    df_v = run_query(q_v, params)

    # Recetario completo
    df_rec = run_query("SELECT * FROM recetas")
    if df_rec.empty or df_v.empty:
        return pd.DataFrame()

    # Filtrar opcionales — NULL se trata como 0 (siempre va en el plato)
    df_rec['es_opcion'] = pd.to_numeric(df_rec['es_opcion'], errors='coerce').fillna(0)
    df_rec['cant_real'] = pd.to_numeric(df_rec['cant_real'], errors='coerce').fillna(0)
    df_rec['cant_efic'] = pd.to_numeric(df_rec['cant_efic'], errors='coerce').fillna(0)
    df_rec = df_rec[df_rec['es_opcion'] == 0].copy()  # solo fijos (null→0 por fillna, opciones 1/2/3/5/6 excluidas)

    # Separar directos y procesados
    df_dir  = df_rec[df_rec['es_procesado'] == False].copy()
    df_proc = df_rec[df_rec['es_procesado'] == True].copy()

    # Factor de conversión según um_salida: G/CC/ML → /1000, UN/KG/LT → 1
    def factor_um(um):
        if pd.isna(um): return 1
        um = str(um).strip().upper()
        if um in ['G', 'CC', 'ML']: return 1/1000
        return 1

    # ---- DIRECTOS que no son PRO- ----
    dir_no_pro = df_dir[~df_dir['sku_ingrediente'].str.startswith('PRO-', na=False)].copy()
    dir_no_pro['factor_um'] = dir_no_pro['um_salida'].apply(factor_um)
    merge_dir = pd.merge(df_v, dir_no_pro, left_on='sku_producto', right_on='codigo_venta', how='inner')
    merge_dir['consumo_parcial'] = merge_dir['cant_vendida'] * merge_dir['cant_real'] * merge_dir['factor_um']
    dir_out = merge_dir[['sku_ingrediente', 'nombre_ingrediente', 'consumo_parcial']]



    # ---- EXPLOSIÓN PROCESADOS ----
    # Paso 1: platos que usan un PRO- como ingrediente
    dir_pro = df_dir[df_dir['sku_ingrediente'].str.startswith('PRO-', na=False)].copy()
    merge_pro = pd.merge(df_v, dir_pro, left_on='sku_producto', right_on='codigo_venta', how='inner')

    exp_out = pd.DataFrame()
    if not merge_pro.empty and not df_proc.empty:
        # Paso 2: rendimiento total y porcion de cada procesado
        # Usa MAX(rendimiento) si está definido (>1), sino SUM(cant_real)
        rend_exp = df_proc.groupby('codigo_venta').agg(
            rendimiento_explicito=('rendimiento', 'max'),
            rendimiento_suma=('cant_real', 'sum'),
            porcion=('porcion', 'first')
        ).reset_index()
        rend_exp['rendimiento_total'] = rend_exp.apply(
            lambda r: r['rendimiento_explicito'] if pd.notna(r['rendimiento_explicito']) and r['rendimiento_explicito'] > 1
            else r['rendimiento_suma'], axis=1
        )
        rend = rend_exp[['codigo_venta', 'rendimiento_total', 'porcion']]

        # Agregar ventas por codigo_venta + sku_ingrediente (PRO-XX) para evitar iterar fila por fila
        merge_pro_agg = merge_pro.groupby(['codigo_venta', 'sku_ingrediente']).agg(
            cant_vendida=('cant_vendida', 'sum'),
            cant_real=('cant_real', 'first'),
            um_salida=('um_salida', 'first')
        ).reset_index()

        rows = []
        for _, plato_row in merge_pro_agg.iterrows():
            pro_sku    = plato_row['sku_ingrediente']  # PRO-XX
            cant_plato = pd.to_numeric(plato_row['cant_real'], errors='coerce') or 0
            ventas     = pd.to_numeric(plato_row['cant_vendida'], errors='coerce') or 0

            base_rows = df_proc[df_proc['codigo_venta'] == pro_sku]
            if base_rows.empty:
                continue

            rend_row   = rend[rend['codigo_venta'] == pro_sku]
            rend_total = float(rend_row['rendimiento_total'].values[0]) if not rend_row.empty else 1
            porcion    = int(rend_row['porcion'].values[0]) if not rend_row.empty else 0
            if rend_total == 0:
                rend_total = 1

            um_plato = factor_um(plato_row['um_salida'] if 'um_salida' in plato_row.index else '')
            cant_plato_conv = cant_plato * um_plato

            for _, base in base_rows.iterrows():
                cant_base = pd.to_numeric(base['cant_real'], errors='coerce') or 0
                um_base   = factor_um(base['um_salida'] if pd.notna(base.get('um_salida')) else '')
                if porcion == 1:
                    consumo = ventas * cant_plato_conv * cant_base * um_base
                else:
                    consumo = ventas * (cant_plato_conv / rend_total) * cant_base * um_base
                rows.append({
                    'sku_ingrediente':    base['sku_ingrediente'],
                    'nombre_ingrediente': base['nombre_ingrediente'],
                    'consumo_parcial':    consumo
                })

        if rows:
            exp_out = pd.DataFrame(rows)



    # ---- CONSOLIDAR ----
    todo = pd.concat([df for df in [dir_out, exp_out] if not df.empty], ignore_index=True)
    cons_teo = todo.groupby('sku_ingrediente').agg(
        consumo_teorico=('consumo_parcial', 'sum'),
        nombre_ingrediente=('nombre_ingrediente', 'first')
    ).reset_index()




    # Compras reales del período — fecha_dte es timestamp, cant_conv ya está en unidades
    filtro_local_c  = "AND UPPER(local) = UPPER(:l)" if local != "Todos" else ""
    filtro_local_c2 = "AND UPPER(c.local) = UPPER(:l)" if local != "Todos" else ""
    params_c = {"i": fecha_i, "f": fecha_f}
    if local != "Todos":
        params_c["l"] = local

    q_c = f"""
        SELECT
            COALESCE(e.sku_receta, c.sku) as sku,
            SUM(c.cant_conv) AS cant_real_comprada,
            AVG(c.muc) AS muc_promedio
        FROM compras c
        LEFT JOIN sku_equivalencias e ON c.sku = e.sku_compra
        WHERE c.fecha_dte::date BETWEEN :i AND :f
          AND c.subcat = 'Directo'
        {filtro_local_c2}
        GROUP BY 1
    """
    df_c = run_query(q_c, params_c)

    # Fallback: si el período no tiene compras, mostrar histórico completo
    if df_c.empty:
        q_c2 = f"""
            SELECT
                COALESCE(e.sku_receta, c.sku) as sku,
                SUM(c.cant_conv) AS cant_real_comprada,
                AVG(c.muc) AS muc_promedio
            FROM compras c
            LEFT JOIN sku_equivalencias e ON c.sku = e.sku_compra
            WHERE c.subcat = 'Directo'
            {filtro_local_c2}
            GROUP BY 1
        """
        df_c = run_query(q_c2, params_c)
        if not df_c.empty:
            st.warning("⚠️ Sin compras en el período seleccionado — mostrando totales históricos.")

    # Equivalencias ya aplicadas en SQL — no necesita remapeo en Python

    # Nombre canónico desde compras — incluir equivalencias para SKUs que solo existen como destino
    q_nom = """
        SELECT sku, MIN(nombre_producto) as nombre_compra
        FROM compras
        WHERE subcat IN ('Directo', 'Indirecto')
        GROUP BY sku
    """
    nombres_compras = run_query(q_nom)
    dict_nombres = dict(zip(nombres_compras['sku'], nombres_compras['nombre_compra'])) if not nombres_compras.empty else {}

    # Fallback de nombres via equivalencias
    df_equiv = run_query("SELECT sku_compra, sku_receta FROM sku_equivalencias")
    if not df_equiv.empty:
        for _, row in df_equiv.iterrows():
            sku_dest = row['sku_receta']
            sku_orig = row['sku_compra']
            if sku_dest not in dict_nombres and sku_orig in dict_nombres:
                dict_nombres[sku_dest] = dict_nombres[sku_orig]

    # Subcat por SKU (para categorización en informe)
    df_subcat = run_query("""
        SELECT sku, MIN(subcat) as subcat
        FROM compras
        WHERE subcat IN ('Directo','Indirecto')
        GROUP BY sku
    """)
    dict_subcat = dict(zip(df_subcat['sku'], df_subcat['subcat'])) if not df_subcat.empty else {}

    informe = pd.merge(
        cons_teo, df_c,
        left_on='sku_ingrediente', right_on='sku', how='outer'
    )
    informe = informe.fillna(0)

    # Eliminar filas que son SKUs originales ya consolidados via equivalencias
    # (aparecen solo en compras con consumo_teorico=0 porque ya fueron mapeados a su sku_receta)
    skus_compra_equiv = set(df_equiv['sku_compra'].tolist()) if not df_equiv.empty else set()
    informe = informe[~(
        (informe['consumo_teorico'] == 0) &
        (informe['sku'].isin(skus_compra_equiv))
    )]
    informe['subcat'] = informe.apply(
        lambda r: dict_subcat.get(str(r['sku_ingrediente']), dict_subcat.get(str(r['sku']), '')), axis=1
    )

    # SKU final: unificar sku_ingrediente y sku en una sola columna
    informe['sku_final'] = informe.apply(
        lambda r: r['sku_ingrediente'] if r['sku_ingrediente'] not in [0, '', None]
        else r['sku'], axis=1
    )

    # Nombre final: recetario primero, compras como fallback para ingredientes sin receta
    informe['nombre_final'] = informe.apply(
        lambda r: r['nombre_ingrediente']
        if (r['nombre_ingrediente'] not in [0, '', None] and str(r['nombre_ingrediente']).strip() != '')
        else dict_nombres.get(str(r['sku_final']), str(r['sku_final'])), axis=1
    )

    informe['desviacion_cant']   = informe['cant_real_comprada'] - informe['consumo_teorico']
    informe['desviacion_dinero'] = informe['desviacion_cant'] * informe['muc_promedio']

    # Renombrar para consistencia con el resto del informe
    informe['sku_ingrediente']   = informe['sku_final']
    informe['nombre_ingrediente']= informe['nombre_final']

    return informe.sort_values('desviacion_dinero', ascending=False)


# ============================================================
# PERSISTENCIA
# ============================================================
def save_recetario(df_directos, df_procesados):
    engine = get_engine()
    if engine is None:
        return

    df_dir = df_directos.copy()
    df_dir.columns = df_dir.columns.str.strip()
    df_dir = df_dir.rename(columns={
        'CODIGO VENTA': 'codigo_venta', 'Plato': 'nombre_plato',
        'SKU': 'sku_ingrediente', 'Ingrediente': 'nombre_ingrediente',
        'CantReal': 'cant_real', 'Eficiencia': 'rendimiento',
        'UM': 'um_salida', 'EsOpcion': 'es_opcion'
    })
    df_dir['es_procesado'] = False
    df_dir['cant_efic'] = None
    df_dir['porcion'] = 0

    df_proc = df_procesados.copy()
    df_proc.columns = df_proc.columns.str.strip()
    # Detectar columna porcion con cualquier variación de nombre o espacios
    col_porcion = next((c for c in df_proc.columns if c.strip().lower() == 'porcion'), None)
    rename_map = {
        'Codigo Venta': 'codigo_venta', 'Ingrediente Proc': 'nombre_plato',
        'SKU Ingrediente': 'sku_ingrediente', 'Ingrediente': 'nombre_ingrediente',
        'CantReceta': 'cant_real', 'CantEfic': 'cant_efic',
        'UM Salida': 'um_salida', 'Eficiencia': 'rendimiento'
    }
    if col_porcion:
        rename_map[col_porcion] = 'porcion'
    df_proc = df_proc.rename(columns=rename_map)
    df_proc['es_procesado'] = True
    df_proc['es_opcion'] = 0
    if 'porcion' not in df_proc.columns:
        df_proc['porcion'] = 0

    cols_base = ['codigo_venta', 'nombre_plato', 'sku_ingrediente', 'nombre_ingrediente',
                 'cant_real', 'cant_efic', 'rendimiento', 'um_salida', 'es_procesado', 'es_opcion', 'porcion']

    df_final = pd.concat([df_dir, df_proc], ignore_index=True)
    cols = [c for c in cols_base if c in df_final.columns]
    df_final['rendimiento'] = pd.to_numeric(df_final['rendimiento'], errors='coerce').fillna(1)
    df_final['cant_real'] = pd.to_numeric(df_final['cant_real'], errors='coerce').fillna(0)
    df_final['cant_efic'] = pd.to_numeric(df_final['cant_efic'], errors='coerce').fillna(0)

    # Consolidar duplicados: mismo ingrediente en el mismo plato → sumar cantidades
    df_final = df_final[cols].copy()
    agg_dict = {
        'nombre_plato':      'first',
        'nombre_ingrediente':'first',
        'cant_real':         'sum',
        'cant_efic':         'sum',
        'rendimiento':       'first',
        'um_salida':         'first',
        'es_opcion':         'first'
    }
    if 'porcion' in df_final.columns:
        agg_dict['porcion'] = 'first'

    df_agg = df_final.groupby(
        ['codigo_venta', 'sku_ingrediente', 'es_procesado'],
        as_index=False
    ).agg(agg_dict)

    # cols final solo con columnas que existen en df_agg
    cols = [c for c in cols_base if c in df_agg.columns]

    duplicados = len(df_final) - len(df_agg)
    if duplicados > 0:
        st.warning(f"⚠️ Se consolidaron {duplicados} filas duplicadas (mismo SKU en mismo plato).")

    try:
        with engine.connect() as conn:
            conn.execute(text("DROP VIEW IF EXISTS vista_costo_recetas CASCADE"))
            conn.commit()
        df_agg[cols].to_sql('recetas', engine, if_exists='replace', index=False)
        st.success(f"✅ Recetario sincronizado — {len(df_agg)} filas únicas cargadas.")
    except Exception as e:
        st.error(f"Error al guardar recetario: {e}")


# ============================================================
# PROCESADO DE COMPRAS
# ============================================================

TASAS_IMP_ADIC = {
    '271': 0.18,
    '27':  0.10,
    '26':  0.21,
    '25':  0.21,
    '24':  0.3155,
    '19':  0.12,
    '18':  0.05,
}

# Columnas mínimas que debe traer el archivo fuente
COLS_REQUERIDAS = [
    'local', 'fecha_dte', 'rut_proveedor', 'nombre_proveedor',
    'tipo_dte', 'folio', 'nombre_producto',
    'cantidad', 'total_item', 'codigo_impuesto', 'iva',
    'descuento_global', 'recargo_global', 'total',
    'sku', 'subcat', 'conversion', 'formato', 'categoria_producto',
]

def _normalizar_columnas(df: pd.DataFrame) -> pd.DataFrame:
    """Limpia y normaliza los nombres de columna del Excel fuente."""
    df = df.copy()
    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(r'[\s]+', '_', regex=True)
        .str.replace(r'[áàä]', 'a', regex=True)
        .str.replace(r'[éèë]', 'e', regex=True)
        .str.replace(r'[íìï]', 'i', regex=True)
        .str.replace(r'[óòö]', 'o', regex=True)
        .str.replace(r'[úùü]', 'u', regex=True)
        .str.replace(r'[^a-z0-9_]', '_', regex=True)
    )
    # Alias frecuentes
    aliases = {
        'categoria_producto': ['categoria_producto', 'categoria producto', 'categoria'],
        'recargo_global':     ['recargo_global', 'recargo global'],
        'descuento_global':   ['descuento_global', 'descuento global'],
        'codigo_impuesto':    ['codigo_impuesto', 'codigo impuesto', 'cod_impuesto'],
    }
    for canonical, variants in aliases.items():
        for v in variants:
            v_norm = v.replace(' ', '_')
            if v_norm in df.columns and canonical not in df.columns:
                df = df.rename(columns={v_norm: canonical})
    return df


def procesar_compras(df_raw: pd.DataFrame) -> tuple[pd.DataFrame, list[str]]:
    """
    Recibe el DataFrame crudo del Excel de compras y devuelve
    (df_procesado, lista_de_advertencias).

    Columnas calculadas:
        cant_conv       = cantidad × conversion
        monto_real      = total_item  (negativo si tipo_dte == 61)
        recargo2        = (Recargo_Global - Descuento_Global) × participación línea en folio
        total_neto2     = monto_real + recargo2
        imp_adic        = monto_real × tasa según codigo_impuesto
        IVA_2           = total_neto2 × 0.19  (0 si IVA del folio == 0)
        tootal2         = total_neto2 + imp_adic + IVA_2
        costo_realfinal = tootal2 + despacho_distribuido + ajuste_redondeo  (0 en líneas de despacho)
        MUC             = costo_realfinal / (cant_conv × formato)
                          si formato == 1 → MUC = costo_realfinal / cant_conv
    """
    warnings = []
    df = _normalizar_columnas(df_raw)

    # ── Verificar columnas mínimas ──────────────────────────────────────────
    faltantes = [c for c in COLS_REQUERIDAS if c not in df.columns]
    if faltantes:
        warnings.append(
            f"⚠️ Columnas no encontradas tras normalizar nombres: **{', '.join(faltantes)}**\n"
            f"Columnas recibidas: {', '.join(df.columns.tolist())}"
        )
    # Columnas críticas para el cálculo — si faltan el resultado será incorrecto
    criticas = {
        'total_item':       'monto_real será 0',
        'recargo_global':   'recargo2 será 0 (no se distribuye recargo)',
        'descuento_global': 'descuento no se aplicará',
        'iva':              'IVA_2 será 0 en todos los folios',
        'total':            'no se podrá ajustar redondeo ni distribuir despacho',
        'conversion':       'cant_conv = cantidad (sin conversión)',
        'formato':          'MUC calculado como por unidad en todos los casos',
    }
    for col, impacto in criticas.items():
        if col not in df.columns:
            warnings.append(f"🔴 Columna crítica **'{col}'** no encontrada → {impacto}")

    # ── Tipos básicos ────────────────────────────────────────────────────────
    df['tipo_dte']        = pd.to_numeric(df.get('tipo_dte', 33), errors='coerce').fillna(33).astype(int)
    df['total_item']      = pd.to_numeric(df.get('total_item', 0), errors='coerce').fillna(0)
    df['cantidad']        = pd.to_numeric(df.get('cantidad', 1), errors='coerce').fillna(1)
    df['conversion']      = pd.to_numeric(df.get('conversion', 1), errors='coerce').fillna(1)
    df['formato']         = pd.to_numeric(df.get('formato', 1), errors='coerce').fillna(1)
    df['recargo_global']  = pd.to_numeric(df.get('recargo_global', 0), errors='coerce').fillna(0)
    df['descuento_global']= pd.to_numeric(df.get('descuento_global', 0), errors='coerce').fillna(0)
    df['iva']             = pd.to_numeric(df.get('iva', 0), errors='coerce').fillna(0)
    df['total']           = pd.to_numeric(df.get('total', 0), errors='coerce').fillna(0)

    # ── PASO 1: cant_conv ────────────────────────────────────────────────────
    df['cant_conv'] = df['cantidad'] * df['conversion']

    # ── PASO 2: monto_real ───────────────────────────────────────────────────
    df['monto_real'] = np.where(df['tipo_dte'] == 61, -df['total_item'], df['total_item'])

    # ── PASO 3: recargo2  (distribución proporcional por folio) ─────────────
    # participación = monto_real_línea / suma_monto_real_folio
    df['_tot_folio'] = df.groupby('folio')['monto_real'].transform('sum')
    df['_recargo_neto'] = df['recargo_global'] - df['descuento_global']
    df['_part'] = np.where(df['_tot_folio'] != 0, df['monto_real'] / df['_tot_folio'], 0)
    df['recargo2'] = df['_part'] * df['_recargo_neto']
    df['total_neto2'] = df['monto_real'] + df['recargo2']

    # ── PASO 4: imp_adic ─────────────────────────────────────────────────────
    cod_str = (
        df.get('codigo_impuesto', pd.Series([''] * len(df)))
        .fillna('')
        .astype(str)
        .str.strip()
        .str.replace(r'\.0$', '', regex=True)
        .str.replace(r'^nan$', '', regex=True)
    )
    tasa = cod_str.map(TASAS_IMP_ADIC).fillna(0)
    df['imp_adic'] = df['monto_real'] * tasa

    # ── PASO 5: IVA_2  (por folio: si el folio tiene IVA registrado > 0) ────
    df['_tiene_iva'] = df.groupby('folio')['iva'].transform('max') != 0
    df['iva_2'] = np.where(df['_tiene_iva'], df['total_neto2'] * 0.19, 0)

    # ── PASO 6: tootal2 ──────────────────────────────────────────────────────
    df['tootal2'] = df['total_neto2'] + df['imp_adic'] + df['iva_2']

    # ── PASO 7: identificar líneas de despacho ───────────────────────────────
    nombre_lower = df['nombre_producto'].str.lower().fillna('')
    df['_es_despacho'] = (
        nombre_lower.str.contains('despacho', na=False) |
        nombre_lower.str.contains('flete',    na=False) |
        nombre_lower.str.contains('distribucion', na=False)
    )

    # ── PASO 8: Desp_Folio = suma(monto_real de líneas despacho) × 1.19 ─────
    df['_desp_linea'] = np.where(df['_es_despacho'], df['monto_real'] * 1.19, 0)
    df['_desp_folio'] = df.groupby('folio')['_desp_linea'].transform('sum')

    # ── PASO 9: ajuste redondeo = Total_factura - suma(tootal2) del folio ────
    df['_suma_tootal2_folio'] = df.groupby('folio')['tootal2'].transform('sum')
    df['_total_factura']      = df.groupby('folio')['total'].transform('max')
    df['_diferencia']         = df['_total_factura'] - df['_suma_tootal2_folio']

    # desp+red2 por folio = Desp_Folio + diferencia
    df['_desp_red2'] = df['_desp_folio'] + df['_diferencia']

    # ── PASO 10: Part_Item (excluye despachos del denominador) ───────────────
    df['_monto_limpio'] = np.where(df['_es_despacho'], 0, df['monto_real'].abs())
    df['_tot_limpio_folio'] = df.groupby('folio')['_monto_limpio'].transform('sum')
    df['_part_item'] = np.where(
        df['_tot_limpio_folio'] != 0,
        df['_monto_limpio'] / df['_tot_limpio_folio'],
        0
    )

    # ── PASO 11: dist_desp = part_item × desp_red2  (redondeado a entero) ───
    df['_dist_desp'] = (df['_part_item'] * df['_desp_red2']).round(0)

    # ── PASO 12: costo_realfinal ─────────────────────────────────────────────
    df['costo_realfinal'] = np.where(
        df['_es_despacho'],
        0,
        df['tootal2'] + df['_dist_desp']
    )

    # ── PASO 13: MUC ─────────────────────────────────────────────────────────
    denominador = np.where(
        df['formato'] == 1,
        df['cant_conv'],
        df['cant_conv'] * df['formato']
    )
    df['muc'] = np.where(
        (denominador != 0) & (~df['_es_despacho']),
        df['costo_realfinal'] / denominador,
        0
    )

    # ── Limpiar columnas temporales ──────────────────────────────────────────
    cols_temp = [c for c in df.columns if c.startswith('_')]
    df = df.drop(columns=cols_temp)

    # ── Renombrar IVA_2 para consistencia con BD ─────────────────────────────
    df = df.rename(columns={'iva_2': 'iva_2'})  # ya en minúsculas

    # ── Advertencias sobre datos ─────────────────────────────────────────────
    sin_sku = df['sku'].isna().sum() if 'sku' in df.columns else 0
    if sin_sku > 0:
        warnings.append(f"⚠️ {sin_sku} líneas sin SKU asignado.")
    sin_conv = (df['conversion'] == 0).sum()
    if sin_conv > 0:
        warnings.append(f"⚠️ {sin_conv} líneas con Conversion = 0.")

    return df, warnings


def save_compras(df: pd.DataFrame):
    """Guarda el DataFrame ya procesado en la tabla compras de Supabase."""
    engine = get_engine()
    if engine is None:
        return
    cols_req = [
        'local', 'fecha_dte', 'rut_proveedor', 'nombre_proveedor', 'tipo_dte',
        'folio', 'nombre_producto', 'sku', 'subcat', 'codigo_impuesto',
        'cantidad', 'conversion', 'formato', 'categoria_producto',
        'cant_conv', 'monto_real', 'recargo2', 'total_neto2',
        'imp_adic', 'iva_2', 'tootal2', 'costo_realfinal', 'muc'
    ]
    # Sólo guardar columnas que existen en el df
    cols_ok = [c for c in cols_req if c in df.columns]
    try:
        df[cols_ok].to_sql('compras', engine, if_exists='append', index=False)
        st.success(f"✅ {len(df)} registros de compras guardados en la base de datos.")
    except Exception as e:
        st.error(f"Error al guardar compras: {e}")


def save_ventas(df):
    engine = get_engine()
    if engine is None:
        return
    df.columns = df.columns.str.strip().str.lower()
    df = df.rename(columns={
        'fecha_pura': 'fecha_venta', 'cat_menu': 'categoria_menu',
        'nombre': 'nombre_producto', 'id_producto': 'sku_producto',
        'cantidad': 'cantidad_vendida', 'venta_real': 'monto_venta_real'
    })
    df['fecha_venta'] = pd.to_datetime(df['fecha_venta'], dayfirst=True, errors='coerce').dt.date
    df = df.dropna(subset=['fecha_venta'])
    try:
        df.to_sql('ventas', engine, if_exists='append', index=False, method='multi')
        st.success(f"✅ {len(df)} registros de ventas cargados.")
    except Exception as e:
        st.error(f"Error al guardar ventas: {e}")


# ============================================================
# HELPERS UI
# ============================================================
def semaforo_margen(val):
    if val >= 65:
        return 'background-color: #1a3a2a; color: #4caf7d'
    elif val >= 50:
        return 'background-color: #3a2a1a; color: #e89c45'
    else:
        return 'background-color: #3a1a1a; color: #e84545'


def get_locales():
    df = run_query("SELECT DISTINCT local FROM ventas WHERE local IS NOT NULL ORDER BY 1")
    return ["Todos"] + df['local'].tolist() if not df.empty else ["Todos"]


# ============================================================
# SIDEBAR
# ============================================================
with st.sidebar:
    st.markdown("""
    <div style='padding: 1rem 0 0.5rem 0;'>
        <span style='font-family: DM Serif Display, serif; font-size: 1.4rem; color: #d4a853;'>
            🍽️ MRP Gastro
        </span><br>
        <span style='font-size: 0.75rem; color: #666; letter-spacing: 0.05em;'>
            SISTEMA DE COSTEOS
        </span>
    </div>
    """, unsafe_allow_html=True)

    st.divider()

    # Menú en cascada elegante
    menu_items = {
        "📦 Gestión de Datos": ["Recetario", "Compras", "Ventas", "Equivalencias SKU"],
        "🧮 Explosión MRP":    [],
        "📊 Informes":         ["Rentabilidad", "Desviación", "Variación Precio Compras"],
    }

    if 'menu_abierto' not in st.session_state:
        st.session_state['menu_abierto'] = None
    if 'modulo' not in st.session_state:
        st.session_state['modulo'] = "📦 Gestión de Datos"

    # CSS menú
    st.markdown("""
    <style>
    section[data-testid="stSidebar"] button {
        background: transparent !important;
        border: none !important;
        border-radius: 6px !important;
        color: #c8c4be !important;
        font-size: 0.88rem !important;
        font-weight: 500 !important;
        text-align: left !important;
        padding: 8px 12px !important;
        transition: background 0.15s, color 0.15s !important;
        letter-spacing: 0.02em !important;
    }
    section[data-testid="stSidebar"] button:hover {
        background: #1f1f1f !important;
        color: #d4a853 !important;
    }
    section[data-testid="stSidebar"] button p {
        text-align: left !important;
    }
    </style>
    """, unsafe_allow_html=True)

    for item, subitems in menu_items.items():
        es_activo = st.session_state['modulo'].startswith(item[:3])
        label = f"**{item}**" if es_activo else item

        if st.sidebar.button(label, key=f"menu_{item}", use_container_width=True):
            if st.session_state['menu_abierto'] == item and not subitems:
                pass
            elif st.session_state['menu_abierto'] == item:
                st.session_state['menu_abierto'] = None
            else:
                st.session_state['menu_abierto'] = item
            st.session_state['modulo'] = item

        # Subitems
        if subitems and st.session_state['menu_abierto'] == item:
            for sub in subitems:
                sub_key = f"{item} — {sub}"
                es_sub  = st.session_state['modulo'] == sub_key
                prefix  = "▸ " if es_sub else "  · "
                sub_label = f"**{prefix}{sub}**" if es_sub else f"{prefix}{sub}"
                if st.sidebar.button(sub_label, key=f"sub_{sub_key}", use_container_width=True):
                    st.session_state['modulo'] = sub_key
                    st.session_state['menu_abierto'] = item

    modulo = st.session_state['modulo']

    st.divider()
    st.markdown("<div style='font-size:0.75rem; color:#666; text-transform:uppercase; letter-spacing:0.08em;'>Filtros globales</div>", unsafe_allow_html=True)

    f_inicio = st.date_input("Desde", value=date(datetime.now().year, datetime.now().month, 1))
    f_fin    = st.date_input("Hasta", value=date.today())
    locales  = get_locales()
    f_local  = st.selectbox("Local", locales)


# ============================================================
# MÓDULO: GESTIÓN DE DATOS
# ============================================================
if modulo.startswith("📦"):
    st.markdown(f"""
    <div style="margin-bottom:1.5rem">
        <div style="font-size:0.72rem;text-transform:uppercase;letter-spacing:0.12em;color:#555;margin-bottom:4px">Módulo</div>
        <div style="font-family:'DM Serif Display',serif;font-size:2rem;color:#f0ede8;letter-spacing:-0.02em;line-height:1.1">
            📦 Gestión de Datos
        </div>
        <div style="width:40px;height:2px;background:#d4a853;margin-top:8px;border-radius:2px"></div>
    </div>
    """, unsafe_allow_html=True)

    tab1, tab2, tab3, tab4 = st.tabs(["📖 Recetario", "🛒 Compras", "📈 Ventas", "🔀 Equivalencias SKU"])

    with tab1:
        st.markdown("<div class='info-box'>Carga las hojas <b>Directos</b> y <b>Procesados</b> de tu recetario. Esto reemplaza el recetario actual.</div>", unsafe_allow_html=True)
        c1, c2 = st.columns(2)
        with c1:
            f_dir  = st.file_uploader("Hoja Directos (.xlsx)", type="xlsx", key="dir")
        with c2:
            f_proc = st.file_uploader("Hoja Procesados (.xlsx)", type="xlsx", key="proc")
        if f_dir and f_proc:
            if st.button("🔄 Sincronizar Recetario"):
                save_recetario(pd.read_excel(f_dir), pd.read_excel(f_proc))

        st.markdown("---")
        df_rec_view = run_query("SELECT * FROM recetas LIMIT 200")
        if not df_rec_view.empty:
            st.caption(f"Vista previa recetario — {len(df_rec_view)} filas (máx 200)")
            st.dataframe(df_rec_view, use_container_width=True, hide_index=True)

    with tab2:
        st.markdown("""
        <div style="margin-bottom:1.5rem">
            <div style="font-size:0.72rem;text-transform:uppercase;letter-spacing:0.12em;color:#555;margin-bottom:4px">Gestión de Datos</div>
            <div style="font-family:'DM Serif Display',serif;font-size:2rem;color:#f0ede8;letter-spacing:-0.02em;line-height:1.1">
                🧾 Procesado de Compras
            </div>
            <div style="font-size:0.8rem;color:#888;margin-top:4px">Carga · Procesa · Valida · Guarda</div>
            <div style="width:40px;height:2px;background:#d4a853;margin-top:8px;border-radius:2px"></div>
        </div>
        """, unsafe_allow_html=True)
        st.markdown("""
        <div class='info-box'>
        Carga el Excel de facturas del período. El sistema calcula automáticamente
        <strong>cant_conv, monto_real, recargo2, imp_adic, IVA_2, tootal2, costo_realfinal y MUC</strong>,
        distribuye despachos y ajusta redondeos antes de guardar en la base de datos.
        </div>
        """, unsafe_allow_html=True)

        f_comp = st.file_uploader("📂 Excel de Compras fuente (.xlsx)", type="xlsx", key="comp")

        if f_comp:
            # ── Leer archivo ─────────────────────────────────────────────
            if 'df_compras_procesado' not in st.session_state or \
               st.session_state.get('comp_filename') != f_comp.name:
                with st.spinner("Procesando archivo..."):
                    df_raw = pd.read_excel(f_comp)
                    df_proc, warns = procesar_compras(df_raw)
                    st.session_state['df_compras_procesado'] = df_proc
                    st.session_state['comp_warnings'] = warns
                    st.session_state['comp_filename'] = f_comp.name

            df_proc = st.session_state['df_compras_procesado']
            warns   = st.session_state.get('comp_warnings', [])

            # ── Advertencias ─────────────────────────────────────────────
            for w in warns:
                st.warning(w)

            # ── Métricas resumen ─────────────────────────────────────────
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric("Líneas procesadas", f"{len(df_proc):,}")
            with col2:
                n_folios = df_proc['folio'].nunique() if 'folio' in df_proc.columns else 0
                st.metric("Folios únicos", f"{n_folios:,}")
            with col3:
                tot = df_proc['costo_realfinal'].sum() if 'costo_realfinal' in df_proc.columns else 0
                st.metric("Costo total procesado", f"${tot:,.0f}")
            with col4:
                n_desp = df_proc['nombre_producto'].str.lower().str.contains(
                    'despacho|flete|distribucion', na=False).sum()
                st.metric("Líneas despacho", f"{n_desp:,}")

            st.markdown("---")

            # ── Validador: comparar costo_realfinal vs Total factura ──────────
            with st.expander("🔍 Validación por folio — Diferencias vs Total declarado", expanded=False):
                if 'total' in df_proc.columns and 'folio' in df_proc.columns:
                    subcat_col = next((c for c in df_proc.columns if c == 'subcat'), None)

                    if subcat_col:
                        # Solo folios donde TODAS las líneas son Directo o Indirecto
                        # (excluir folios mixtos donde el Total de factura incluye otras subcats)
                        subcat_por_folio = df_proc.groupby('folio')[subcat_col].apply(
                            lambda s: s.isin(['Directo','Indirecto']).all()
                        )
                        folios_puros = subcat_por_folio[subcat_por_folio].index
                        df_val = df_proc[df_proc['folio'].isin(folios_puros)]
                        n_mixtos = df_proc['folio'].nunique() - len(folios_puros)
                    else:
                        df_val = df_proc
                        n_mixtos = 0

                    val = df_val.groupby('folio').agg(
                        total_declarado=('total', 'max'),
                        costo_calculado=('costo_realfinal', 'sum')
                    ).reset_index()
                    val['diferencia'] = val['total_declarado'] - val['costo_calculado']
                    val['dif_abs'] = val['diferencia'].abs()
                    val_issues = val[val['dif_abs'] > 1].sort_values('dif_abs', ascending=False)

                    c1v, c2v, c3v = st.columns(3)
                    c1v.metric("Folios validados", f"{len(val):,}")
                    c2v.metric("Folios mixtos (excluidos)", f"{n_mixtos:,}",
                               help="Folios con Directo/Indirecto + otras subcats — el Total de factura no es comparable con solo las líneas MRP")
                    c3v.metric("Folios con diferencia > $1", f"{len(val_issues):,}")

                    if val_issues.empty:
                        st.success("✅ Todos los folios cuadran con el total declarado.")
                    else:
                        st.warning(f"⚠️ {len(val_issues)} folio(s) con diferencia > $1 — revisar")
                        st.dataframe(
                            val_issues[['folio','total_declarado','costo_calculado','diferencia']],
                            use_container_width=True, hide_index=True
                        )
                    st.caption("ℹ️ Se validan solo folios donde el 100% de líneas son Directo o Indirecto. Los folios mixtos tienen un Total de factura que incluye otras categorías.")
                else:
                    st.info("No se encontró columna 'total' para validar.")

            # ── Vista previa del resultado ────────────────────────────────
            cols_preview = [
                'local', 'fecha_dte', 'folio', 'nombre_producto', 'sku', 'subcat',
                'cantidad', 'conversion', 'cant_conv',
                'monto_real', 'recargo2', 'total_neto2',
                'imp_adic', 'iva_2', 'tootal2', 'costo_realfinal', 'muc'
            ]
            cols_preview = [c for c in cols_preview if c in df_proc.columns]

            st.markdown("#### Vista previa")
            filtro_local_c = st.selectbox(
                "Filtrar por local",
                ["Todos"] + sorted(df_proc['local'].dropna().unique().tolist()) if 'local' in df_proc.columns else ["Todos"],
                key="comp_filtro_local"
            )
            df_vista = df_proc if filtro_local_c == "Todos" else df_proc[df_proc['local'] == filtro_local_c]
            st.caption(f"{len(df_vista):,} líneas")
            st.dataframe(df_vista[cols_preview].head(500), use_container_width=True, hide_index=True)

            st.markdown("---")

            # ── Descargar resultado procesado ────────────────────────────
            buf = io.BytesIO()
            df_proc.to_excel(buf, index=False)
            buf.seek(0)
            st.download_button(
                label="⬇️ Descargar Excel procesado",
                data=buf,
                file_name=f"compras_procesadas_{f_comp.name}",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )

            # ── Guardar en base de datos ─────────────────────────────────
            st.markdown("#### Guardar en base de datos")
            st.markdown(
                "<div class='info-box'>Al guardar se hace <strong>append</strong> — "
                "asegúrate de no cargar el mismo período dos veces.</div>",
                unsafe_allow_html=True
            )
            if st.button("💾 Guardar en base de datos", type="primary"):
                save_compras(df_proc)
        else:
            st.info("Carga el archivo Excel fuente para comenzar el procesado.")

    with tab3:
        st.markdown("<div class='info-box'>Carga el historial de ventas exportado desde tu POS. Se añade al historial existente (append).</div>", unsafe_allow_html=True)
        f_ven = st.file_uploader("Excel de Ventas (.xlsx)", type="xlsx", key="ven")
        if f_ven and st.button("💾 Cargar Ventas"):
            save_ventas(pd.read_excel(f_ven))

    with tab4:
        st.markdown("<div class='info-box'>Mapea SKUs de compras sin código de venta hacia SKUs equivalentes que sí tienen receta.<br>Ejemplo: Erdinger Trigo (BA-CA-078) → Erdinger Weissbier (BA-CA-066)</div>", unsafe_allow_html=True)

        df_eq = run_query("SELECT sku_compra, sku_receta, descripcion FROM sku_equivalencias ORDER BY sku_compra")
        if not df_eq.empty:
            st.caption(f"{len(df_eq)} equivalencias registradas")
            st.dataframe(df_eq, use_container_width=True, hide_index=True)
        else:
            st.caption("No hay equivalencias registradas aún.")

        st.markdown("#### Agregar equivalencia")
        c1, c2, c3 = st.columns(3)
        with c1: sku_compra_in = st.text_input("SKU Compras (origen)", placeholder="BA-CA-078")
        with c2: sku_receta_in = st.text_input("SKU Receta (destino)", placeholder="BA-CA-066")
        with c3: desc_in = st.text_input("Descripción", placeholder="Erdinger Trigo -> Weissbier")

        if st.button("Agregar Equivalencia"):
            if sku_compra_in and sku_receta_in:
                engine = get_engine()
                try:
                    with engine.connect() as conn:
                        conn.execute(text(
                            "INSERT INTO sku_equivalencias (sku_compra, sku_receta, descripcion) "
                            "VALUES (:c, :r, :d) "
                            "ON CONFLICT (sku_compra) DO UPDATE SET sku_receta = :r, descripcion = :d"
                        ), {"c": sku_compra_in.strip(), "r": sku_receta_in.strip(), "d": desc_in.strip()})
                        conn.commit()
                    st.success(f"Equivalencia guardada: {sku_compra_in} -> {sku_receta_in}")
                    st.rerun()
                except Exception as e:
                    st.error(f"Error: {e}")
            else:
                st.warning("Completa SKU origen y destino.")

        if not df_eq.empty:
            st.markdown("#### Eliminar equivalencia")
            sku_del = st.selectbox("Seleccionar SKU a eliminar", df_eq['sku_compra'].tolist())
            if st.button("Eliminar"):
                engine = get_engine()
                try:
                    with engine.connect() as conn:
                        conn.execute(text("DELETE FROM sku_equivalencias WHERE sku_compra = :c"), {"c": sku_del})
                        conn.commit()
                    st.success(f"Eliminada equivalencia para {sku_del}")
                    st.rerun()
                except Exception as e:
                    st.error(f"Error: {e}")


# ============================================================
# MÓDULO: EXPLOSIÓN MRP
# ============================================================
elif modulo.startswith("🧮"):
    st.markdown(f"""
    <div style="margin-bottom:1.5rem">
        <div style="font-size:0.72rem;text-transform:uppercase;letter-spacing:0.12em;color:#555;margin-bottom:4px">Módulo</div>
        <div style="font-family:'DM Serif Display',serif;font-size:2rem;color:#f0ede8;letter-spacing:-0.02em;line-height:1.1">
            🧮 Explosión MRP
        </div>
        <div style="width:40px;height:2px;background:#d4a853;margin-top:8px;border-radius:2px"></div>
    </div>
    """, unsafe_allow_html=True)
    st.markdown("<div class='info-box'>Sube el Excel con las hojas <b>Ventas</b>, <b>Directos</b> y <b>Procesados</b>. La lógica de cálculo es la versión validada.</div>", unsafe_allow_html=True)

    file_mrp = st.file_uploader("Archivo Excel MRP (.xlsx)", type="xlsx")

    if file_mrp:
        try:
            xls = pd.ExcelFile(file_mrp)
            res = process_bom(
                pd.read_excel(xls, 'Ventas'),
                pd.read_excel(xls, 'Directos'),
                pd.read_excel(xls, 'Procesados')
            )

            col_a, col_b, col_c = st.columns(3)
            col_a.metric("Insumos únicos", len(res))
            col_b.metric("Registros explotados", len(res))

            st.markdown("#### 📋 Resultado de la explosión")
            st.dataframe(
                res.style.format({"Total Kg/L/Un": "{:,.3f}"}),
                use_container_width=True,
                hide_index=True
            )

            buf = io.BytesIO()
            with pd.ExcelWriter(buf, engine='openpyxl') as w:
                res.to_excel(w, index=False)
            st.download_button(
                "📥 Descargar MRP (.xlsx)",
                buf.getvalue(),
                "MRP_Explosion.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
        except Exception as e:
            st.error(f"Error al procesar: {e}")


# ============================================================
# MÓDULO: INFORMES
# ============================================================
elif modulo.startswith("📊"):

    # Derivar informe activo desde subitem del menú
    if "Rentabilidad" in modulo:
        informe_sel = "Informe 1"
    elif "Desviación" in modulo:
        informe_sel = "Informe 2"
    elif "Variación Precio Compras" in modulo:
        informe_sel = "Informe 3"
    else:
        informe_sel = "Informe 1"  # default

    # Título elegante según informe
    titulos = {
        "Informe 1": ("💰", "Rentabilidad por Producto"),
        "Informe 2": ("📉", "Desviación Real vs Teórico"),
        "Informe 3": ("🔀", "Variación Precio Compras"),
    }
    icono, titulo_txt = titulos.get(informe_sel, ("📊", "Informes"))
    st.markdown(f"""
    <div style="margin-bottom:1.5rem">
        <div style="font-size:0.72rem;text-transform:uppercase;letter-spacing:0.12em;color:#555;margin-bottom:4px">Informes</div>
        <div style="font-family:'DM Serif Display',serif;font-size:2rem;color:#f0ede8;letter-spacing:-0.02em;line-height:1.1">
            {icono} {titulo_txt}
        </div>
        <div style="width:40px;height:2px;background:#d4a853;margin-top:8px;border-radius:2px"></div>
    </div>
    """, unsafe_allow_html=True)

    # ----------------------------------------------------------
    # INFORME 1
    # ----------------------------------------------------------
    if "Informe 1" in informe_sel:
        st.markdown("### 💰 Rentabilidad por Producto / Categoría")
        st.markdown(f"<div class='info-box'>Período: <b>{f_inicio}</b> → <b>{f_fin}</b> · Local: <b>{f_local}</b><br>Costo unitario = directos × MUC(CantReal) + procesados × MUC(CantEfic) usando último precio por SKU.</div>", unsafe_allow_html=True)

        if st.button("▶ Generar Informe 1"):
            with st.spinner("Calculando rentabilidad..."):
                df_inf1 = informe_rentabilidad(f_inicio, f_fin, f_local)

            if not df_inf1.empty:
                venta_total = df_inf1['venta'].sum()
                costo_total = df_inf1['costo_total'].sum()
                rent_total  = df_inf1['rentabilidad'].sum()
                margen_gral = (rent_total / venta_total * 100) if venta_total > 0 else 0

                m1, m2, m3, m4 = st.columns(4)
                m1.metric("💰 Venta Total",        f"${venta_total:,.0f}")
                m2.metric("📦 Costo Teórico",       f"${costo_total:,.0f}")
                m3.metric("📈 Rentabilidad Bruta",  f"${rent_total:,.0f}")
                m4.metric("🎯 Margen General",      f"{margen_gral:.1f}%")

                st.markdown("<br>", unsafe_allow_html=True)

                # --- Helpers badge ---
                def badge_margen(val):
                    if pd.isna(val): return '<span style="color:#555">—</span>'
                    if val >= 60:
                        return f'<span style="background:#1a3a2a;color:#4caf7d;padding:2px 8px;border-radius:12px;font-size:0.78rem;font-weight:600">{val:.1f}%</span>'
                    elif val >= 40:
                        return f'<span style="background:#3a2a1a;color:#e89c45;padding:2px 8px;border-radius:12px;font-size:0.78rem;font-weight:600">{val:.1f}%</span>'
                    return f'<span style="background:#3a1a1a;color:#e84545;padding:2px 8px;border-radius:12px;font-size:0.78rem;font-weight:600">{val:.1f}%</span>'

                def fmt_rent(val):
                    if val >= 0:
                        return f'<span style="color:#4caf7d;font-weight:600">${val:,.0f}</span>'
                    return f'<span style="color:#e84545;font-weight:600">${val:,.0f}</span>'

                # --- Tabla detalle por producto ---
                rows_html = ''
                cols_show = ['sku_producto', 'categoria_menu', 'nombre_producto',
                             'cant', 'venta', 'costo_total', 'rentabilidad', 'margen_pct']
                for _, r in df_inf1[cols_show].iterrows():
                    margen = r.get('margen_pct', 0)
                    bg = '#121e14' if margen >= 60 else '#1e1a12' if margen >= 40 else '#1e1212'
                    rows_html += (
                        f'<tr style="border-bottom:1px solid #1e1e1e;background:{bg}">'
                        f'<td style="padding:10px 14px;color:#666;font-size:0.76rem;font-family:monospace">{r.get("sku_producto","")}</td>'
                        f'<td style="padding:10px 14px;color:#555;font-size:0.8rem">{r.get("categoria_menu","")}</td>'
                        f'<td style="padding:10px 14px;font-weight:500;color:#e8e4de">{r.get("nombre_producto","")}</td>'
                        f'<td style="padding:10px 14px;text-align:right;color:#aaa;font-variant-numeric:tabular-nums">{r.get("cant",0):,.0f}</td>'
                        f'<td style="padding:10px 14px;text-align:right;color:#ccc;font-variant-numeric:tabular-nums">${r.get("venta",0):,.0f}</td>'
                        f'<td style="padding:10px 14px;text-align:right;color:#777;font-variant-numeric:tabular-nums">${r.get("costo_total",0):,.0f}</td>'
                        f'<td style="padding:10px 14px;text-align:right;font-variant-numeric:tabular-nums">{fmt_rent(r.get("rentabilidad",0))}</td>'
                        f'<td style="padding:10px 14px;text-align:center">{badge_margen(margen)}</td>'
                        f'</tr>'
                    )

                hs = 'padding:11px 14px;font-size:0.7rem;text-transform:uppercase;letter-spacing:0.09em;font-weight:600;color:#444;border-bottom:1px solid #2a2a2a'
                tabla_html = (
                    '<div style="overflow-x:auto;border-radius:14px;border:1px solid #1e1e1e;margin-top:0.5rem;background:#0d0d0d">'
                    '<table style="width:100%;border-collapse:collapse;font-family:DM Sans,sans-serif;font-size:0.84rem">'
                    '<thead><tr style="background:#111">'
                    f'<th style="{hs};text-align:left">SKU</th>'
                    f'<th style="{hs};text-align:left">Categoría</th>'
                    f'<th style="{hs};text-align:left">Producto</th>'
                    f'<th style="{hs};text-align:right">Cant.</th>'
                    f'<th style="{hs};text-align:right">Venta</th>'
                    f'<th style="{hs};text-align:right">Costo</th>'
                    f'<th style="{hs};text-align:right">Rentabilidad</th>'
                    f'<th style="{hs};text-align:center">Margen</th>'
                    f'</tr></thead><tbody>{rows_html}</tbody></table></div>'
                )
                st.markdown("#### Detalle por Producto")
                st.markdown(tabla_html, unsafe_allow_html=True)

                # --- Resumen por Categoría ---
                st.markdown("---")
                st.markdown("#### Resumen por Categoría")
                cat = df_inf1.groupby('categoria_menu').agg(
                    venta=('venta','sum'),
                    costo=('costo_total','sum'),
                    rentabilidad=('rentabilidad','sum'),
                    productos=('sku_producto','count')
                ).reset_index()
                cat['margen_pct'] = cat.apply(
                    lambda r: r['rentabilidad']/r['venta']*100 if r['venta']>0 else 0, axis=1
                ).round(1)
                cat = cat.sort_values('rentabilidad', ascending=False)

                cat_rows = ''
                for _, r in cat.iterrows():
                    cat_rows += (
                        f'<tr style="border-bottom:1px solid #1e1e1e">'
                        f'<td style="padding:10px 14px;font-weight:500;color:#e8e4de">{r["categoria_menu"]}</td>'
                        f'<td style="padding:10px 14px;text-align:right;color:#aaa">{r["productos"]:,.0f}</td>'
                        f'<td style="padding:10px 14px;text-align:right;color:#ccc;font-variant-numeric:tabular-nums">${r["venta"]:,.0f}</td>'
                        f'<td style="padding:10px 14px;text-align:right;color:#777;font-variant-numeric:tabular-nums">${r["costo"]:,.0f}</td>'
                        f'<td style="padding:10px 14px;text-align:right;font-variant-numeric:tabular-nums">{fmt_rent(r["rentabilidad"])}</td>'
                        f'<td style="padding:10px 14px;text-align:center">{badge_margen(r["margen_pct"])}</td>'
                        f'</tr>'
                    )

                cat_html = (
                    '<div style="overflow-x:auto;border-radius:14px;border:1px solid #1e1e1e;margin-top:0.5rem;background:#0d0d0d">'
                    '<table style="width:100%;border-collapse:collapse;font-family:DM Sans,sans-serif;font-size:0.84rem">'
                    '<thead><tr style="background:#111">'
                    f'<th style="{hs};text-align:left">Categoría</th>'
                    f'<th style="{hs};text-align:right">Productos</th>'
                    f'<th style="{hs};text-align:right">Venta</th>'
                    f'<th style="{hs};text-align:right">Costo</th>'
                    f'<th style="{hs};text-align:right">Rentabilidad</th>'
                    f'<th style="{hs};text-align:center">Margen</th>'
                    f'</tr></thead><tbody>{cat_rows}</tbody></table></div>'
                )
                st.markdown(cat_html, unsafe_allow_html=True)

                # Descarga
                buf2 = io.BytesIO()
                with pd.ExcelWriter(buf2, engine='openpyxl') as w:
                    df_inf1[cols_show].to_excel(w, sheet_name='Rentabilidad', index=False)
                    cat.to_excel(w, sheet_name='Por Categoria', index=False)
                st.download_button("📥 Descargar Informe 1", buf2.getvalue(), "Informe1_Rentabilidad.xlsx")

    # ----------------------------------------------------------
    # INFORME 2
    # ----------------------------------------------------------
    elif "Informe 2" in informe_sel:
        st.markdown("### 📉 Informe de Desviación")
        st.markdown(f"<div class='info-box'>Período: <b>{f_inicio}</b> → <b>{f_fin}</b> · Local: <b>{f_local}</b><br>Consumo teórico = ventas × CantReal. Comprado real = cant_conv de facturas. Variación % = (Comprado - Teórico) / Teórico × 100.</div>", unsafe_allow_html=True)

        if st.button("▶ Generar Informe 2"):
            with st.spinner("Calculando desviaciones..."):
                df_inf2 = informe_desviacion(f_inicio, f_fin, f_local)

            if not df_inf2.empty:
                # Calcular variación %
                df_inf2['variacion_pct'] = df_inf2.apply(
                    lambda r: ((r['cant_real_comprada'] - r['consumo_teorico']) / r['consumo_teorico'] * 100)
                    if r['consumo_teorico'] > 0 else None, axis=1
                )

                perdida_total  = df_inf2[df_inf2['desviacion_dinero'] > 0]['desviacion_dinero'].sum()
                ahorro_total   = df_inf2[df_inf2['desviacion_dinero'] < 0]['desviacion_dinero'].sum()
                items_exceso   = (df_inf2['desviacion_dinero'] > 0).sum()
                items_ok       = (df_inf2['desviacion_dinero'] <= 0).sum()

                # Métricas superiores
                m1, m2, m3, m4 = st.columns(4)
                m1.metric("🔴 Exceso comprado", f"${perdida_total:,.0f}", f"{items_exceso} ítems")
                m2.metric("🟢 Bajo lo teórico",  f"${abs(ahorro_total):,.0f}", f"{items_ok} ítems")
                m3.metric("📦 Total ítems", f"{len(df_inf2)}")
                desv_neta = perdida_total + ahorro_total
                m4.metric("⚖️ Desviación neta", f"${desv_neta:,.0f}")

                st.markdown("<br>", unsafe_allow_html=True)

                # Semáforos
                def semaforo_desv(val):
                    if pd.isna(val): return ''
                    if val > 0:   return 'background-color: #3a1a1a; color: #e84545'
                    elif val < 0: return 'background-color: #1a3a2a; color: #4caf7d'
                    return ''

                def semaforo_pct(val):
                    if pd.isna(val): return 'color: #555'
                    if val > 20:    return 'background-color: #3a1a1a; color: #e84545; font-weight:600'
                    elif val > 5:   return 'background-color: #3a2a1a; color: #e89c45; font-weight:600'
                    elif val < -5:  return 'background-color: #1a3a2a; color: #4caf7d; font-weight:600'
                    return 'color: #aaa'

                cols_show2 = ['sku_ingrediente', 'nombre_ingrediente', 'subcat',
                              'consumo_teorico', 'cant_real_comprada',
                              'desviacion_cant', 'variacion_pct', 'desviacion_dinero']
                existing_cols = [c for c in cols_show2 if c in df_inf2.columns]

                def badge_pct(val):
                    if val is None or (isinstance(val, float) and pd.isna(val)):
                        return '<span style="color:#555">—</span>'
                    if val > 20:
                        return f'<span style="background:#3a1a1a;color:#e84545;padding:2px 8px;border-radius:12px;font-size:0.78rem;font-weight:600">{val:+.1f}%</span>'
                    elif val > 5:
                        return f'<span style="background:#3a2a1a;color:#e89c45;padding:2px 8px;border-radius:12px;font-size:0.78rem;font-weight:600">{val:+.1f}%</span>'
                    elif val < -5:
                        return f'<span style="background:#1a3a2a;color:#4caf7d;padding:2px 8px;border-radius:12px;font-size:0.78rem;font-weight:600">{val:+.1f}%</span>'
                    return f'<span style="color:#aaa;font-size:0.78rem">{val:+.1f}%</span>'

                def fmt_dinero_html(val):
                    if val > 0:
                        return f'<span style="color:#e84545;font-weight:600">${val:,.0f}</span>'
                    elif val < 0:
                        return f'<span style="color:#4caf7d;font-weight:600">${val:,.0f}</span>'
                    return f'<span style="color:#aaa">${val:,.0f}</span>'

                rows_html = ''
                for _, r in df_inf2.iterrows():
                    pct    = r.get('variacion_pct', None)
                    dinero = r.get('desviacion_dinero', 0)
                    bg     = '#1e1212' if dinero > 0 else '#121e14' if dinero < 0 else ''
                    rows_html += (
                        f'<tr style="border-bottom:1px solid #1e1e1e;background:{bg};transition:background 0.15s">'
                        f'<td style="padding:10px 14px;color:#666;font-size:0.76rem;font-family:monospace;white-space:nowrap">{r.get("sku_ingrediente","")}</td>'
                        f'<td style="padding:10px 14px;font-weight:500;color:#e8e4de">{r.get("nombre_ingrediente","")}</td>'
                        f'<td style="padding:10px 14px;color:#555;font-size:0.8rem">{r.get("subcat","")}</td>'
                        f'<td style="padding:10px 14px;text-align:right;color:#777;font-variant-numeric:tabular-nums">{r.get("consumo_teorico",0):,.2f}</td>'
                        f'<td style="padding:10px 14px;text-align:right;color:#ccc;font-variant-numeric:tabular-nums;font-weight:500">{r.get("cant_real_comprada",0):,.2f}</td>'
                        f'<td style="padding:10px 14px;text-align:right;color:#777;font-variant-numeric:tabular-nums">{r.get("desviacion_cant",0):,.2f}</td>'
                        f'<td style="padding:10px 14px;text-align:center">{badge_pct(pct)}</td>'
                        f'<td style="padding:10px 14px;text-align:right;font-variant-numeric:tabular-nums">{fmt_dinero_html(dinero)}</td>'
                        f'</tr>'
                    )

                hs = 'padding:11px 14px;font-size:0.7rem;text-transform:uppercase;letter-spacing:0.09em;font-weight:600;color:#444;border-bottom:1px solid #2a2a2a'
                tabla_html = (
                    '<div style="overflow-x:auto;border-radius:14px;border:1px solid #1e1e1e;margin-top:0.5rem;background:#0d0d0d">'
                    '<table style="width:100%;border-collapse:collapse;font-family:DM Sans,sans-serif;font-size:0.84rem">'
                    '<thead><tr style="background:#111">'
                    f'<th style="{hs};text-align:left">SKU</th>'
                    f'<th style="{hs};text-align:left">Ingrediente</th>'
                    f'<th style="{hs};text-align:left">Cat.</th>'
                    f'<th style="{hs};text-align:right">Teórico</th>'
                    f'<th style="{hs};text-align:right">Comprado</th>'
                    f'<th style="{hs};text-align:right">Δ Cant.</th>'
                    f'<th style="{hs};text-align:center">Δ %</th>'
                    f'<th style="{hs};text-align:right">Δ $</th>'
                    f'</tr></thead><tbody>{rows_html}</tbody></table></div>'
                )
                st.markdown(tabla_html, unsafe_allow_html=True)

                # Resumen por subcategoría
                if 'subcat' in df_inf2.columns:
                    st.markdown("---")
                    st.markdown("#### Resumen por Categoría")
                    sub = df_inf2.groupby('subcat').agg(
                        Teórico=('consumo_teorico','sum'),
                        Comprado=('cant_real_comprada','sum'),
                        Δ_dinero=('desviacion_dinero','sum'),
                        Items=('sku_ingrediente','count')
                    ).reset_index().sort_values('Δ_dinero', ascending=False)
                    sub['Δ %'] = ((sub['Comprado'] - sub['Teórico']) / sub['Teórico'].replace(0,1) * 100).round(1)
                    sub = sub.rename(columns={'subcat':'Categoría','Δ_dinero':'Δ $'})
                    st.dataframe(
                        sub.style
                            .applymap(semaforo_desv, subset=['Δ $'])
                            .applymap(semaforo_pct,  subset=['Δ %'])
                            .format({'Teórico':'{:,.2f}','Comprado':'{:,.2f}','Δ $':'${:,.0f}','Δ %':'{:+.1f}%'}),
                        use_container_width=True, hide_index=True
                    )

                st.markdown("<br>", unsafe_allow_html=True)
                buf3 = io.BytesIO()
                export_cols = existing_cols
                with pd.ExcelWriter(buf3, engine='openpyxl') as w:
                    df_inf2[export_cols].to_excel(w, sheet_name='Desviacion', index=False)
                st.download_button("📥 Descargar Excel", buf3.getvalue(), "Informe2_Desviacion.xlsx")

    # ----------------------------------------------------------
    # INFORME 3 — IMPACTO DE PRECIOS SOBRE CANASTA DE INGREDIENTES
    # ----------------------------------------------------------
    elif "Informe 3" in informe_sel:

        # Selectores de mes
        meses_disp3 = run_query("""
            SELECT DISTINCT DATE_TRUNC('month', fecha_dte::timestamp)::date as mes
            FROM compras WHERE subcat IN ('Directo','Indirecto') ORDER BY 1
        """)

        if meses_disp3.empty:
            st.warning("No hay datos de compras disponibles.")
        else:
            meses_list3 = pd.to_datetime(meses_disp3['mes']).tolist()
            meses_fmt3  = [m.strftime('%B %Y').capitalize() for m in meses_list3]

            mc1, mc2, mc3 = st.columns([2, 2, 2])
            with mc1:
                mes_base_idx3 = st.selectbox("Mes muestra (canasta)", range(len(meses_fmt3)),
                                             format_func=lambda i: meses_fmt3[i],
                                             index=0, key='inf3_base')
            with mc2:
                mes_comp_idx3 = st.selectbox("Mes comparación (precios)", range(len(meses_fmt3)),
                                             format_func=lambda i: meses_fmt3[i],
                                             index=len(meses_list3)-1, key='inf3_comp')
            with mc3:
                cat3_q = run_query("SELECT DISTINCT categoria_producto FROM compras WHERE categoria_producto IS NOT NULL AND subcat IN ('Directo','Indirecto') ORDER BY 1")
                cats3  = ['Todos'] + cat3_q['categoria_producto'].tolist() if not cat3_q.empty else ['Todos']
                cat3_sel = st.selectbox("Categoría", cats3, key='inf3_cat')

            mes_base3     = meses_list3[mes_base_idx3]
            mes_comp3     = meses_list3[mes_comp_idx3]
            mes_base3_str = mes_base3.strftime('%B %Y').capitalize()
            mes_comp3_str = mes_comp3.strftime('%B %Y').capitalize()

            # Ordenamiento fuera del botón
            ord3_col, ord3_dir_col = st.columns([3, 1])
            with ord3_col:
                ord3_col_sel = st.selectbox("Ordenar por", [
                    'Ingrediente', f'Cant. {mes_base3_str}',
                    f'Costo {mes_base3_str}', f'Costo {mes_comp3_str}',
                    'Δ$ Precio', 'Δ% Precio'
                ], key='ord3_col')
            with ord3_dir_col:
                ord3_dir = st.selectbox("Dir.", ['↓', '↑'], key='ord3_dir')

            if st.button("▶ Generar Informe 3"):
                base_i = mes_base3.strftime('%Y-%m-01')
                base_f = (mes_base3 + pd.offsets.MonthEnd(1)).strftime('%Y-%m-%d')
                comp_i = mes_comp3.strftime('%Y-%m-01')
                comp_f = (mes_comp3 + pd.offsets.MonthEnd(1)).strftime('%Y-%m-%d')
                filtro_cat3 = f"AND categoria_producto = '{cat3_sel}'" if cat3_sel != 'Todos' else ""

                q_ing = f"""
                    WITH equiv AS (
                        SELECT sku_compra, sku_receta FROM sku_equivalencias
                    ),
                    base AS (
                        SELECT
                            COALESCE(e.sku_receta, c.sku) as sku,
                            MIN(c.nombre_producto) as nombre,
                            MIN(c.subcat) as subcat,
                            MIN(c.categoria_producto) as categoria,
                            SUM(c.cant_conv) as cant_base,
                            SUM(c.costo_realfinal) / NULLIF(SUM(c.cant_conv), 0) as precio_base
                        FROM compras c
                        LEFT JOIN equiv e ON c.sku = e.sku_compra
                        WHERE c.fecha_dte::date BETWEEN '{base_i}' AND '{base_f}'
                          AND c.subcat IN ('Directo','Indirecto')
                          AND c.costo_realfinal > 0
                          {filtro_cat3}
                        GROUP BY 1
                    ),
                    comp AS (
                        SELECT
                            COALESCE(e.sku_receta, c.sku) as sku,
                            SUM(c.costo_realfinal) / NULLIF(SUM(c.cant_conv), 0) as precio_comp
                        FROM compras c
                        LEFT JOIN equiv e ON c.sku = e.sku_compra
                        WHERE c.fecha_dte::date BETWEEN '{comp_i}' AND '{comp_f}'
                          AND c.subcat IN ('Directo','Indirecto')
                          AND c.costo_realfinal > 0
                        GROUP BY 1
                    )
                    SELECT
                        b.sku, b.nombre, b.subcat, b.categoria,
                        b.cant_base, b.precio_base,
                        c.precio_comp,
                        b.cant_base * b.precio_base as impacto_base,
                        b.cant_base * COALESCE(c.precio_comp, b.precio_base) as impacto_comp
                    FROM base b
                    LEFT JOIN comp c ON b.sku = c.sku
                    ORDER BY b.sku
                """
                df3 = run_query(q_ing)

                if df3.empty:
                    st.warning("Sin datos para el mes seleccionado.")
                else:
                    df3['precio_base']  = pd.to_numeric(df3['precio_base'],  errors='coerce').fillna(0)
                    df3['precio_comp']  = pd.to_numeric(df3['precio_comp'],  errors='coerce').fillna(df3['precio_base'])
                    df3['cant_base']    = pd.to_numeric(df3['cant_base'],    errors='coerce').fillna(0)
                    df3['impacto_base'] = pd.to_numeric(df3['impacto_base'], errors='coerce').fillna(0)
                    df3['impacto_comp'] = df3['cant_base'] * df3['precio_comp']
                    df3['delta_dinero'] = df3['impacto_comp'] - df3['impacto_base']
                    df3['delta_pct']    = df3.apply(
                        lambda r: (r['delta_dinero'] / r['impacto_base'] * 100) if r['impacto_base'] > 0 else None, axis=1
                    )
                    df3['sin_precio_comp'] = df3['precio_comp'] == df3['precio_base']
                    st.session_state['inf3_df']     = df3
                    st.session_state['inf3_labels'] = (mes_base3_str, mes_comp3_str)

            if 'inf3_df' in st.session_state:
                df3 = st.session_state['inf3_df'].copy()
                mes_base3_str, mes_comp3_str = st.session_state['inf3_labels']

                # Ordenar
                asc3 = ord3_dir == '↑'
                sort_map3 = {
                    'Ingrediente':              ('nombre',       asc3),
                    f'Cant. {mes_base3_str}':   ('cant_base',    asc3),
                    f'Costo {mes_base3_str}':   ('impacto_base', asc3),
                    f'Costo {mes_comp3_str}':   ('impacto_comp', asc3),
                    'Δ$ Precio':               ('delta_dinero', asc3),
                    'Δ% Precio':               ('delta_pct',    asc3),
                }
                if ord3_col_sel in sort_map3:
                    col_s, asc_s = sort_map3[ord3_col_sel]
                    df3 = df3.sort_values(col_s, ascending=asc_s, na_position='last')

                # Métricas
                tot_base = df3['impacto_base'].sum()
                tot_comp = df3['impacto_comp'].sum()
                tot_delta = tot_comp - tot_base
                tot_pct   = (tot_delta / tot_base * 100) if tot_base > 0 else 0
                sin_precio = df3['sin_precio_comp'].sum()

                mm1, mm2, mm3, mm4 = st.columns(4)
                mm1.metric(f"Canasta {mes_base3_str}",     f"${tot_base:,.0f}")
                mm2.metric(f"Canasta a precios {mes_comp3_str}", f"${tot_comp:,.0f}")
                mm3.metric("Δ$ impacto precio",            f"${tot_delta:,.0f}")
                mm4.metric("Δ% total",                     f"{tot_pct:+.1f}%")
                if sin_precio > 0:
                    st.info(f"ℹ️ {int(sin_precio)} ingrediente(s) sin precio en mes de comparación — se usó precio del mes muestra.")

                st.markdown("<br>", unsafe_allow_html=True)

                def badge3(val):
                    if val is None or (isinstance(val, float) and pd.isna(val)):
                        return '<span style="color:#444">—</span>'
                    if val > 10:
                        return f'<span style="background:#3a1a1a;color:#e84545;padding:2px 8px;border-radius:12px;font-size:0.78rem;font-weight:600">{val:+.1f}%</span>'
                    elif val > 3:
                        return f'<span style="background:#3a2a1a;color:#e89c45;padding:2px 8px;border-radius:12px;font-size:0.78rem;font-weight:600">{val:+.1f}%</span>'
                    elif val < -3:
                        return f'<span style="background:#1a3a2a;color:#4caf7d;padding:2px 8px;border-radius:12px;font-size:0.78rem;font-weight:600">{val:+.1f}%</span>'
                    return f'<span style="color:#aaa;font-size:0.75rem">{val:+.1f}%</span>'

                def fmt_d3(val):
                    if val > 0: return f'<span style="color:#e84545;font-weight:600">${val:,.0f}</span>'
                    if val < 0: return f'<span style="color:#4caf7d;font-weight:600">${val:,.0f}</span>'
                    return f'<span style="color:#aaa">${val:,.0f}</span>'

                rows3 = ''
                for _, r in df3.iterrows():
                    bg = '#1e1212' if (r['delta_dinero'] or 0) > 0 else '#121e14' if (r['delta_dinero'] or 0) < 0 else ''
                    sin_p = r.get('sin_precio_comp', False)
                    row_bg = bg if bg else ('rgba(13,30,60,0.6)' if sin_p else '')
                    icono_cell = '<span style="color:#4a9eda;font-size:0.75rem">ℹ️ </span>' if sin_p else ''
                    precio_comp_color = '#4a9eda' if sin_p else '#ccc'
                    rows3 += (
                        f'<tr style="border-bottom:1px solid #1e1e1e;background:{row_bg}">'
                        f'<td style="padding:10px 14px;color:#666;font-family:monospace;font-size:0.76rem">{r.get("sku","")}</td>'
                        f'<td style="padding:10px 14px;font-weight:500;color:{"#4a9eda" if sin_p else "#e8e4de"}">{icono_cell}{r.get("nombre","")}</td>'
                        f'<td style="padding:10px 14px;color:#555;font-size:0.8rem">{r.get("categoria","")}</td>'
                        f'<td style="padding:10px 14px;color:#444;font-size:0.78rem">{r.get("subcat","")}</td>'
                        f'<td style="padding:10px 14px;text-align:right;color:#aaa;font-variant-numeric:tabular-nums">{r.get("cant_base",0):,.2f}</td>'
                        f'<td style="padding:10px 14px;text-align:right;color:#888;font-variant-numeric:tabular-nums">${r.get("precio_base",0):,.2f}</td>'
                        f'<td style="padding:10px 14px;text-align:right;color:{precio_comp_color};font-variant-numeric:tabular-nums">${r.get("precio_comp",0):,.2f}</td>'
                        f'<td style="padding:10px 14px;text-align:right;color:#777;font-variant-numeric:tabular-nums">${r.get("impacto_base",0):,.0f}</td>'
                        f'<td style="padding:10px 14px;text-align:right;color:#e8e4de;font-variant-numeric:tabular-nums">${r.get("impacto_comp",0):,.0f}</td>'
                        f'<td style="padding:10px 14px;text-align:right">{fmt_d3(r.get("delta_dinero",0))}</td>'
                        f'<td style="padding:10px 14px;text-align:center">{badge3(r.get("delta_pct",None))}</td>'
                        f'</tr>'
                    )

                hs3 = 'padding:11px 14px;font-size:0.7rem;text-transform:uppercase;letter-spacing:0.09em;font-weight:600;color:#444;border-bottom:1px solid #2a2a2a'
                hdrs3 = ['SKU','Ingrediente','Categoría','Tipo',
                          f'Cant. {mes_base3_str}',
                          f'P. Unit {mes_base3_str}', f'P. Unit {mes_comp3_str}',
                          f'Total {mes_base3_str}', f'Total {mes_comp3_str}',
                          'Δ$','Δ%']
                tabla3 = (
                    '<div style="overflow-x:auto;border-radius:14px;border:1px solid #1e1e1e;margin-top:0.5rem;background:#0d0d0d">'
                    '<table style="width:100%;border-collapse:collapse;font-family:DM Sans,sans-serif;font-size:0.84rem">'
                    '<thead><tr style="background:#111">'
                    + ''.join([f'<th style="{hs3};text-align:{'left' if i<4 else 'right'}">{h}</th>' for i, h in enumerate(hdrs3)])
                    + f'</tr></thead><tbody>{rows3}</tbody></table></div>'
                )
                st.markdown(tabla3, unsafe_allow_html=True)

                st.markdown("<br>", unsafe_allow_html=True)
                buf_inf3 = io.BytesIO()
                with pd.ExcelWriter(buf_inf3, engine='openpyxl') as w:
                    df3[['sku','nombre','categoria','subcat','cant_base',
                          'precio_base','precio_comp','impacto_base',
                          'impacto_comp','delta_dinero','delta_pct']].to_excel(w, sheet_name='Canasta', index=False)
                st.download_button("📥 Descargar Excel", buf_inf3.getvalue(), "Informe3_Canasta.xlsx")
