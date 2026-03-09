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
        "🍹 Tendencias Bar":   [],
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

    tab1, tab2, tab3, tab4, tab5 = st.tabs(["📖 Recetario", "🛒 Compras", "📈 Ventas", "🔀 Equivalencias SKU", "🔍 Auditoría Compras"])

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

    # ================================================================
    # TAB 5 — AUDITORÍA DE COMPRAS
    # ================================================================
    with tab5:
        st.markdown("<div class='info-box'>Detecta inconsistencias en <b>conversion</b> y <b>formato</b> comparando el MUC de cada registro contra la mediana histórica del SKU. Un MUC muy alejado de la mediana indica que el precio, conversion o formato están mal configurados.</div>", unsafe_allow_html=True)

        # Controles
        ac1, ac2 = st.columns([2, 2])
        with ac1:
            umbral_audit = st.slider("Umbral de alerta (× esperado)", min_value=2.0, max_value=20.0, value=5.0, step=0.5,
                                     key='audit_umbral')
        with ac2:
            cat_audit_q = run_query("SELECT DISTINCT categoria_producto FROM compras WHERE categoria_producto IS NOT NULL ORDER BY 1")
            cats_raw = cat_audit_q['categoria_producto'].tolist() if not cat_audit_q.empty else []
            cats_colacion = [c for c in cats_raw if 'colacion' in c.lower().replace('ó','o').replace('ô','o') or c.upper() == 'COLACION']
            cats_normales = [c for c in cats_raw if c not in cats_colacion]
            cats_audit = ['Todas (sin Colación)'] + cats_normales + (['── Colación ──'] if cats_colacion else []) + cats_colacion
            cat_audit_sel = st.selectbox("Categoría", cats_audit, key='audit_cat')

        def _run_audit_query():
            cat_sel = st.session_state.get('audit_cat', 'Todas (sin Colación)')
            if cat_sel in ('Todas (sin Colación)', '── Colación ──'):
                fcat   = "AND UPPER(categoria_producto) NOT LIKE '%COLACION%' AND UPPER(categoria_producto) NOT LIKE '%COLACIÓN%'"
                fcat_c = "AND UPPER(g.categoria) NOT LIKE '%COLACION%' AND UPPER(g.categoria) NOT LIKE '%COLACIÓN%'"
            else:
                fcat   = f"AND categoria_producto = '{cat_sel}'"
                fcat_c = f"AND g.categoria = '{cat_sel}'"
            umbral = st.session_state.get('audit_umbral', 5.0)
            return fcat, fcat_c, umbral

        if st.button("▶ Ejecutar Auditoría"):
            st.session_state.pop('audit_df', None)

        if 'audit_df' not in st.session_state:
            cat_sel = st.session_state.get('audit_cat', 'Todas (sin Colación)')
            umbral_audit = st.session_state.get('audit_umbral', 5.0)
            if cat_sel in ('Todas (sin Colación)', '── Colación ──'):
                filtro_cat_audit   = "AND UPPER(categoria_producto) NOT LIKE '%COLACION%' AND UPPER(categoria_producto) NOT LIKE '%COLACIÓN%'"
                filtro_cat_audit_c = "AND UPPER(g.categoria) NOT LIKE '%COLACION%' AND UPPER(g.categoria) NOT LIKE '%COLACIÓN%'"
            else:
                filtro_cat_audit   = f"AND categoria_producto = '{cat_sel}'"
                filtro_cat_audit_c = f"AND g.categoria = '{cat_sel}'"
            q_audit = f"""
                WITH grupos AS (
                    SELECT
                        sku,
                        ROUND(muc::numeric, 1)                                          AS muc_grupo,
                        COUNT(*)                                                        AS n_registros,
                        ARRAY_AGG(id)                                                   AS ids,
                        MODE() WITHIN GROUP (ORDER BY nombre_producto)                                            AS nombre_producto,
                        MAX(categoria_producto)                                         AS categoria,
                        MAX(conversion)                                                 AS conversion,
                        MAX(formato)                                                    AS formato,
                        ROUND(AVG(monto_real / NULLIF(cant_conv, 0))::numeric, 2)       AS precio_factura
                    FROM compras
                    WHERE muc > 0
                      AND costo_realfinal > 0
                      AND UPPER(sku) != 'COLACION'
                      AND UPPER(sku) NOT IN ('N. CREDITO', 'NCR')
                      {filtro_cat_audit}
                    GROUP BY sku, ROUND(muc::numeric, 1)
                    HAVING ROUND(muc::numeric, 1) > 0
                ),
                dispersos AS (
                    SELECT
                        sku,
                        MAX(muc_grupo)                                                  AS muc_max,
                        MIN(muc_grupo)                                                  AS muc_min,
                        ROUND((MAX(muc_grupo) / NULLIF(MIN(muc_grupo), 0))::numeric, 1) AS dispersion,
                        COUNT(*)                                                        AS n_grupos
                    FROM grupos
                    WHERE muc_grupo > 0
                    GROUP BY sku
                    HAVING COUNT(*) >= 2
                       AND MAX(muc_grupo) / NULLIF(MIN(muc_grupo), 0) > {umbral_audit}
                )
                SELECT
                    g.sku,
                    g.nombre_producto,
                    g.categoria,
                    g.conversion,
                    g.formato,
                    g.muc_grupo                                                         AS muc,
                    g.n_registros,
                    g.ids,
                    g.precio_factura,
                    d.muc_min,
                    d.muc_max,
                    d.dispersion
                FROM grupos g
                JOIN dispersos d ON g.sku = d.sku
                  {filtro_cat_audit_c.replace('c.sku', 'g.sku').replace('c.categoria_producto', 'g.categoria')}
                ORDER BY d.dispersion DESC, g.sku, g.muc_grupo
                LIMIT 500
            """
            df_audit = run_query(q_audit)
            st.session_state['audit_df'] = df_audit

        if 'audit_df' in st.session_state:
            df_audit = st.session_state['audit_df'].copy()

            if df_audit.empty:
                st.success("✅ Sin inconsistencias detectadas con el umbral configurado.")
            else:
                # Marcar revisados en session_state
                if 'audit_revisados' not in st.session_state:
                    st.session_state['audit_revisados'] = set()

                n_total = len(df_audit)
                n_rev   = len(st.session_state['audit_revisados'])
                am1, am2, am3 = st.columns(3)
                am1.metric("⚠️ Inconsistencias", n_total)
                am2.metric("✅ Revisadas", n_rev)
                am3.metric("⏳ Pendientes", n_total - n_rev)

                st.markdown("<br>", unsafe_allow_html=True)

                # ── Construir grupos ──────────────────────────────────────
                ids_disp    = df_audit['id'].astype(str).tolist() if 'id' in df_audit.columns else []
                nombres_disp = (df_audit['sku'] + ' — ' + df_audit['nombre_producto']).tolist()

                # Cada fila ya es un grupo SKU+MUC — construir labels para el buscador
                if not df_audit.empty:
                    df_audit['_label'] = df_audit.apply(
                        lambda r: f"{r['sku']} — {r['nombre_producto'][:40]} | MUC {float(r['muc']):.4f} ({int(r['n_registros'])} reg. | {float(r['dispersion']):.0f}×)",
                        axis=1
                    )
                    # Para el selector de SKU (agrupa todas las filas del mismo SKU)
                    grupos = df_audit.groupby('sku').agg(
                        nombre     = ('nombre_producto', 'first'),
                        dispersion = ('dispersion', 'first'),
                        n_filas    = ('n_registros', 'sum'),
                    ).reset_index()
                    grupos = grupos.sort_values('dispersion', ascending=False).reset_index(drop=True)
                    grupos['label'] = grupos.apply(
                        lambda r: f"{r['sku']} — {r['nombre'][:40]} ({int(r['n_filas'])} reg. | {float(r['dispersion']):.0f}×)",
                        axis=1
                    )
                else:
                    grupos = pd.DataFrame()

                # Buscador: selectbox con búsqueda nativa — una opción por SKU+MUC
                if not df_audit.empty:
                    opciones_muc = df_audit.apply(
                        lambda r: f"{r['sku']} — {r['nombre_producto'][:40]} | MUC {float(r['muc']):.1f} ({int(r['n_registros'])} reg.)",
                        axis=1
                    ).tolist()
                else:
                    opciones_muc = []

                label_sel_muc = st.selectbox("🔍 Buscar SKU / producto / MUC",
                                             [None] + opciones_muc,
                                             format_func=lambda x: "— Todos —" if x is None else x,
                                             key='audit_grupo_sel')

                # Extraer SKU seleccionado del label
                if label_sel_muc:
                    sku_activo = label_sel_muc.split(" — ")[0].strip()
                    label_sel = grupos[grupos['sku'] == sku_activo]['label'].iloc[0] if not grupos.empty and sku_activo in grupos['sku'].values else None
                else:
                    label_sel = None

                # ── Inspector libre de SKU ────────────────────────────────
                with st.expander("🔎 Inspeccionar cualquier SKU", expanded=False):
                    sku_inspect = st.text_input("SKU exacto", key='audit_inspect_sku', placeholder="ej: AL-AF-095")
                    if sku_inspect:
                        q_inspect = f"""
                            SELECT
                                ROUND(muc::numeric, 1)                                    AS muc,
                                COUNT(*)                                                  AS n_registros,
                                ARRAY_AGG(id)                                             AS ids,
                                ROUND(AVG(monto_real / NULLIF(cant_conv,0))::numeric, 2) AS precio_factura,
                                MAX(conversion)                                           AS conversion,
                                MAX(formato)                                              AS formato,
                                MODE() WITHIN GROUP (ORDER BY nombre_producto)                                      AS nombre_producto,
                                MAX(categoria_producto)                                   AS categoria
                            FROM compras
                            WHERE UPPER(sku) = UPPER('{sku_inspect}')
                              AND muc > 0 AND costo_realfinal > 0
                              AND ROUND(muc::numeric, 1) > 0
                            GROUP BY ROUND(muc::numeric, 1)
                            ORDER BY ROUND(muc::numeric, 1)
                        """
                        df_inspect = run_query(q_inspect)
                        if df_inspect.empty:
                            st.warning(f"SKU '{sku_inspect}' no encontrado o sin registros.")
                        else:
                            nombre_insp = df_inspect['nombre_producto'].iloc[0]
                            cat_insp    = df_inspect['categoria'].iloc[0]
                            muc_min_i   = float(df_inspect['muc'].min())
                            muc_max_i   = float(df_inspect['muc'].max())
                            disp_i      = round(muc_max_i / muc_min_i, 1) if muc_min_i > 0 else 0
                            if disp_i > 8:
                                badge = f'🔴 {disp_i:.0f}×'
                            elif disp_i > 2:
                                badge = f'🟡 {disp_i:.1f}×'
                            else:
                                badge = f'⚪ {disp_i:.1f}×'
                            st.caption(f"**{nombre_insp}** | {cat_insp} | {len(df_inspect)} grupos MUC | dispersión {badge}")

                            # Tabla igual al informe
                            hs_i = 'padding:9px 12px;font-size:0.68rem;text-transform:uppercase;letter-spacing:0.09em;font-weight:600;color:#444;border-bottom:1px solid #2a2a2a'
                            rows_i = ''
                            for _, r in df_inspect.iterrows():
                                muc_i  = float(r['muc'])
                                es_min = abs(muc_i - muc_min_i) < 0.0001
                                es_max = abs(muc_i - muc_max_i) < 0.0001
                                es_out = es_min or es_max
                                mc     = '#e84545' if es_out else '#aaa'
                                precio_i = float(r['precio_factura'] or 0)
                                if disp_i > 8:
                                    sc = '#e84545'; sl = f'🔴 {disp_i:.0f}×'
                                elif disp_i > 2:
                                    sc = '#e89c45'; sl = f'🟡 {disp_i:.1f}×'
                                else:
                                    sc = '#aaa';    sl = f'⚪ {disp_i:.1f}×'
                                rows_i += (
                                    f'<tr style="border-bottom:1px solid #1e1e1e">'
                                    f'<td style="padding:9px 12px;color:#666;font-family:monospace;font-size:0.72rem">{sku_inspect.upper()}</td>'
                                    f'<td style="padding:9px 12px;font-weight:500;color:#e8e4de;font-size:0.8rem">{r.get("nombre_producto","")}</td>'
                                    f'<td style="padding:9px 12px;color:#666;font-size:0.75rem">{r.get("categoria","")}</td>'
                                    f'<td style="padding:9px 12px;text-align:right;color:#888">{r["conversion"]}</td>'
                                    f'<td style="padding:9px 12px;text-align:right;color:#888">{r["formato"]}</td>'
                                    f'<td style="padding:9px 12px;text-align:right;color:#aaa;font-variant-numeric:tabular-nums">${precio_i:,.2f}</td>'
                                    f'<td style="padding:9px 12px;text-align:right;color:{mc};font-weight:{"700" if es_out else "400"};font-variant-numeric:tabular-nums">{muc_i:.4f}</td>'
                                    f'<td style="padding:9px 12px;text-align:right;color:#666">{int(r["n_registros"])}</td>'
                                    f'<td style="padding:9px 12px;text-align:center;color:{sc};font-weight:600">{sl}</td>'
                                    f'</tr>'
                                )
                            hdrs_i = ['SKU','Producto','Categoría','Conv.','Formato','Neto Fact/u','MUC','# Reg.','Dispersión']
                            st.markdown(
                                '<div style="overflow-x:auto;border-radius:14px;border:1px solid #1e1e1e;margin-top:0.5rem;background:#0d0d0d">'
                                '<table style="width:100%;border-collapse:collapse;font-family:DM Sans,sans-serif;font-size:0.82rem">'
                                '<thead><tr style="background:#111">'
                                + ''.join([f'<th style="{hs_i};text-align:{"left" if i<3 else "right" if i<8 else "center"}">{h}</th>' for i, h in enumerate(hdrs_i)])
                                + f'</tr></thead><tbody>{rows_i}</tbody></table></div>',
                                unsafe_allow_html=True
                            )

                            # Acciones de corrección
                            st.markdown("<br>", unsafe_allow_html=True)
                            st.markdown("**Corregir grupo**")
                            mucs_i = sorted(df_inspect['muc'].dropna().unique().tolist())
                            muc_fix = st.selectbox(
                                "MUC a corregir",
                                mucs_i,
                                format_func=lambda m: f"{float(m):.4f}  ({int(df_inspect[df_inspect['muc']==m]['n_registros'].sum())} reg.)",
                                key='inspect_muc_fix'
                            )
                            fila_fix = df_inspect[df_inspect['muc'] == muc_fix].iloc[0]
                            ic1, ic2 = st.columns(2)
                            with ic1:
                                conv_fix = st.number_input("Nueva conversion", value=float(fila_fix['conversion'] or 1),
                                                           min_value=0.001, step=0.1, key='inspect_conv_fix')
                            with ic2:
                                fmt_fix  = st.number_input("Nuevo formato", value=float(fila_fix['formato'] or 1),
                                                           min_value=0.001, step=1.0, key='inspect_fmt_fix')
                            import ast as _ast2
                            raw_ids_i = fila_fix['ids']
                            if isinstance(raw_ids_i, str):
                                raw_ids_i = _ast2.literal_eval(raw_ids_i)
                            ids_fix = [int(i) for i in raw_ids_i]
                            st.caption(f"Afecta **{len(ids_fix)}** registros")
                            if st.button("💾 Aplicar corrección", key='inspect_apply'):
                                engine = get_engine()
                                try:
                                    with engine.connect() as conn:
                                        conn.execute(text("""
                                            UPDATE compras SET
                                                conversion = :conv, formato = :fmt,
                                                cant_conv  = cantidad * :conv,
                                                muc        = CASE WHEN :fmt = 1
                                                             THEN costo_realfinal / NULLIF(cantidad * :conv, 0)
                                                             ELSE costo_realfinal / NULLIF(cantidad * :conv * :fmt, 0) END
                                            WHERE id = ANY(:ids)
                                        """), {"conv": conv_fix, "fmt": fmt_fix, "ids": ids_fix})
                                        conn.commit()
                                    st.success(f"✅ {len(ids_fix)} registros corregidos")
                                    st.session_state.pop('audit_df', None)
                                    st.rerun()
                                except Exception as e:
                                    st.error(f"Error: {e}")



                # ── Corrección informe (arriba de la tabla) ─────────────────
                if not df_audit.empty and label_sel_muc:
                    sku_sel_inf = label_sel_muc.split(' — ')[0].strip()
                    filas_sku = df_audit[df_audit['sku'] == sku_sel_inf]
                    if not filas_sku.empty:
                        st.markdown(f'**⚙️ Corregir — {sku_sel_inf}**')
                        ca1, ca2, ca3, ca4 = st.columns([2, 2, 2, 2])
                        mucs_disp = sorted(filas_sku['muc'].dropna().unique().tolist())
                        with ca1:
                            muc_sel_lote = st.selectbox(
                                'MUC a corregir', mucs_disp,
                                format_func=lambda m: f'{float(m):.4f} ({int(filas_sku[filas_sku["muc"]==m]["n_registros"].sum())} reg.)',
                                key='audit_muc_lote'
                            )
                        fila_sel = filas_sku[filas_sku['muc'] == muc_sel_lote].iloc[0]
                        with ca2:
                            nuevo_conv_lote = st.number_input('Nueva conversion',
                                value=float(fila_sel['conversion'] or 1),
                                min_value=0.001, step=0.1, key='audit_conv_lote')
                        with ca3:
                            nuevo_fmt_lote = st.number_input('Nuevo formato',
                                value=float(fila_sel['formato'] or 1),
                                min_value=0.001, step=1.0, key='audit_fmt_lote')
                        import ast as _ast
                        raw_ids = fila_sel['ids']
                        if isinstance(raw_ids, str):
                            raw_ids = _ast.literal_eval(raw_ids)
                        ids_lote = [int(i) for i in raw_ids]
                        with ca4:
                            st.caption(f'Afecta **{len(ids_lote)}** registros')
                            cb1, cb2 = st.columns(2)
                            with cb1:
                                if st.button('💾 Aplicar', key='audit_apply'):
                                    engine = get_engine()
                                    try:
                                        with engine.connect() as conn:
                                            conn.execute(text(
                                                'UPDATE compras SET conversion=:conv,formato=:fmt,'
                                                'cant_conv=cantidad*:conv,'
                                                'muc=CASE WHEN :fmt=1 THEN costo_realfinal/NULLIF(cantidad*:conv,0)'
                                                ' ELSE costo_realfinal/NULLIF(cantidad*:conv*:fmt,0) END'
                                                ' WHERE id=ANY(:ids)'
                            ), {'conv': nuevo_conv_lote, 'fmt': nuevo_fmt_lote, 'ids': ids_lote})
                                            conn.commit()
                                        st.success(f'✅ {len(ids_lote)} registros corregidos')
                                        st.session_state.pop('audit_df', None)
                                        st.session_state.pop('audit_grupo_sel', None)
                                        st.rerun()
                                    except Exception as e:
                                        st.error(f'Error: {e}')
                            with cb2:
                                if st.button('✅ Revisado', key='audit_mark'):
                                    st.session_state['audit_revisados'].update([str(i) for i in ids_lote])
                                    st.rerun()

                # ── Exportar y limpiar revisados ──────────────────────────
                ex1, ex2 = st.columns([1, 1])
                with ex1:
                    buf_audit = io.BytesIO()
                    export_cols = ['sku','nombre_producto','categoria','conversion','formato',
                                   'precio_factura','muc','muc_min','muc_max','dispersion','n_registros']
                    export_cols_exist = [c for c in export_cols if c in df_audit.columns]
                    with pd.ExcelWriter(buf_audit, engine='openpyxl') as w:
                        df_audit[export_cols_exist].to_excel(w, sheet_name='Inconsistencias', index=False)
                    st.download_button('📥 Exportar Excel', buf_audit.getvalue(),
                                       'Auditoria_Compras.xlsx',
                                       mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
                with ex2:
                    st.caption(f"{len(st.session_state['audit_revisados'])} marcados revisados")
                    if st.button('🔄 Limpiar revisados'):
                        st.session_state['audit_revisados'] = set()
                        st.rerun()

                # ── Tabla — una fila por SKU+MUC ─────────────────────────
                if label_sel and not grupos.empty:
                    grupo_activo_row = grupos[grupos['label'] == label_sel]
                    if not grupo_activo_row.empty:
                        sku_activo = grupo_activo_row.iloc[0]['sku']
                        df_tabla = df_audit[df_audit['sku'] == sku_activo]
                        st.caption(f"Mostrando {len(df_tabla)} grupos MUC del SKU {sku_activo}")
                    else:
                        df_tabla = df_audit
                else:
                    df_tabla = df_audit

                hs_a = 'padding:9px 12px;font-size:0.68rem;text-transform:uppercase;letter-spacing:0.09em;font-weight:600;color:#444;border-bottom:1px solid #2a2a2a'
                rows_a = ''
                for _, r in df_tabla.iterrows():
                    muc        = float(r.get('muc', 0) or 0)
                    muc_min    = float(r.get('muc_min', 0) or 0)
                    muc_max    = float(r.get('muc_max', 0) or 0)
                    dispersion = float(r.get('dispersion', 1) or 1)
                    n_reg      = int(r.get('n_registros', 1) or 1)
                    es_outlier = muc_min > 0 and (abs(muc - muc_min) < 0.0001 or abs(muc - muc_max) < 0.0001)
                    if dispersion > 8:
                        sev_color = '#e84545'; sev_label = f'🔴 {dispersion:.0f}×'
                    elif dispersion > 2:
                        sev_color = '#e89c45'; sev_label = f'🟡 {dispersion:.1f}×'
                    else:
                        sev_color = '#aaa';    sev_label = f'⚪ {dispersion:.1f}×'
                    muc_color = '#e84545' if es_outlier else '#aaa'
                    precio    = float(r.get('precio_factura', 0) or 0)
                    rows_a += (
                        f'<tr style="border-bottom:1px solid #1e1e1e">'
                        f'<td style="padding:9px 12px;color:#666;font-family:monospace;font-size:0.72rem">{r.get("sku","")}</td>'
                        f'<td style="padding:9px 12px;font-weight:500;color:#e8e4de;font-size:0.8rem">{r.get("nombre_producto","")}</td>'
                        f'<td style="padding:9px 12px;color:#666;font-size:0.75rem">{r.get("categoria","")}</td>'
                        f'<td style="padding:9px 12px;text-align:right;color:#888;font-variant-numeric:tabular-nums">{r.get("conversion","")}</td>'
                        f'<td style="padding:9px 12px;text-align:right;color:#888;font-variant-numeric:tabular-nums">{r.get("formato","")}</td>'
                        f'<td style="padding:9px 12px;text-align:right;color:#aaa;font-variant-numeric:tabular-nums">${precio:,.2f}</td>'
                        f'<td style="padding:9px 12px;text-align:right;color:{muc_color};font-weight:{"700" if es_outlier else "400"};font-variant-numeric:tabular-nums">{muc:,.4f}</td>'
                        f'<td style="padding:9px 12px;text-align:right;color:#666;font-variant-numeric:tabular-nums">{n_reg}</td>'
                        f'<td style="padding:9px 12px;text-align:center;color:{sev_color};font-weight:600">{sev_label}</td>'
                        f'</tr>'
                    )

                hdrs_a = ['SKU','Producto','Categoría','Conv.','Formato','Neto Fact/u','MUC','# Reg.','Dispersión']
                tabla_a = (
                    '<div style="overflow-x:auto;border-radius:14px;border:1px solid #1e1e1e;margin-top:0.5rem;background:#0d0d0d">'
                    '<table style="width:100%;border-collapse:collapse;font-family:DM Sans,sans-serif;font-size:0.82rem">'
                    '<thead><tr style="background:#111">'
                    + ''.join([f'<th style="{hs_a};text-align:{"left" if i<3 else "right" if i<8 else "center"}">{h}</th>' for i, h in enumerate(hdrs_a)])
                    + f'</tr></thead><tbody>{rows_a}</tbody></table></div>'
                )
                st.markdown(tabla_a, unsafe_allow_html=True)


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

            # Filtro texto + ordenamiento
            fc1, fc2, fc3 = st.columns([3, 2, 1])
            with fc1:
                filtro_texto3 = st.text_input("🔍 Buscar SKU o producto",
                                              placeholder="Ej: AL-AF-276 o papas...",
                                              key='inf3_buscar')
            with fc2:
                ord3_col_sel = st.selectbox("Ordenar por", [
                    'Producto', f'Cant. {mes_base3_str}',
                    f'Costo {mes_base3_str}', f'Costo {mes_comp3_str}',
                    'Δ$ Precio', 'Δ% Precio'
                ], key='ord3_col')
            with fc3:
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
                            COALESCE(e.sku_receta, c.sku)                                              AS sku,
                            MIN(c.nombre_producto)                                                     AS nombre,
                            MIN(c.nombre_proveedor)                                                    AS proveedor,
                            MIN(c.categoria_producto)                                                  AS categoria,
                            SUM(c.cant_conv)                                                           AS cant_base,
                            SUM(c.costo_realfinal) / NULLIF(SUM(c.costo_realfinal / NULLIF(c.muc,0)),0) AS precio_base
                        FROM compras c
                        LEFT JOIN equiv e ON c.sku = e.sku_compra
                        WHERE c.fecha_dte::date BETWEEN '{base_i}' AND '{base_f}'
                          AND c.subcat IN ('Directo','Indirecto')
                          AND c.costo_realfinal > 0
                          AND c.muc > 0
                          {filtro_cat3}
                        GROUP BY 1
                    ),
                    comp AS (
                        SELECT
                            COALESCE(e.sku_receta, c.sku)                                              AS sku,
                            SUM(c.costo_realfinal) / NULLIF(SUM(c.costo_realfinal / NULLIF(c.muc,0)),0) AS precio_comp
                        FROM compras c
                        LEFT JOIN equiv e ON c.sku = e.sku_compra
                        WHERE c.fecha_dte::date BETWEEN '{comp_i}' AND '{comp_f}'
                          AND c.subcat IN ('Directo','Indirecto')
                          AND c.costo_realfinal > 0
                          AND c.muc > 0
                        GROUP BY 1
                    )
                    SELECT
                        b.sku, b.nombre, b.proveedor, b.categoria,
                        b.cant_base, b.precio_base,
                        c.precio_comp,
                        b.cant_base * b.precio_base                          AS impacto_base,
                        b.cant_base * COALESCE(c.precio_comp, b.precio_base) AS impacto_comp
                    FROM base b
                    LEFT JOIN comp c ON b.sku = c.sku
                    ORDER BY b.nombre
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
                    'Producto':                 ('nombre',       asc3),
                    f'Cant. {mes_base3_str}':   ('cant_base',    asc3),
                    f'Costo {mes_base3_str}':   ('impacto_base', asc3),
                    f'Costo {mes_comp3_str}':   ('impacto_comp', asc3),
                    'Δ$ Precio':               ('delta_dinero', asc3),
                    'Δ% Precio':               ('delta_pct',    asc3),
                }
                if ord3_col_sel in sort_map3:
                    col_s, asc_s = sort_map3[ord3_col_sel]
                    df3 = df3.sort_values(col_s, ascending=asc_s, na_position='last')

                # Filtro de texto
                if filtro_texto3:
                    mask3 = (
                        df3['sku'].str.contains(filtro_texto3, case=False, na=False) |
                        df3['nombre'].str.contains(filtro_texto3, case=False, na=False)
                    )
                    df3 = df3[mask3]

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
                        f'<td style="padding:10px 14px;color:#666;font-size:0.78rem">{r.get("proveedor","")}</td>'
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
                hdrs3 = ['SKU', 'Producto', 'Categoría', 'Proveedor',
                          f'Cant. {mes_base3_str}',
                          f'P. Unit {mes_base3_str}', f'P. Unit {mes_comp3_str}',
                          f'Total {mes_base3_str}', f'Total {mes_comp3_str}',
                          'Δ$', 'Δ%']
                tabla3 = (
                    '<div style="overflow-x:auto;border-radius:14px;border:1px solid #1e1e1e;margin-top:0.5rem;background:#0d0d0d">'
                    '<table style="width:100%;border-collapse:collapse;font-family:DM Sans,sans-serif;font-size:0.84rem">'
                    '<thead><tr style="background:#111">'
                    + ''.join([f'<th style="{hs3};text-align:{"left" if i<4 else "right"}">{h}</th>' for i, h in enumerate(hdrs3)])
                    + f'</tr></thead><tbody>{rows3}</tbody></table></div>'
                )
                st.markdown(tabla3, unsafe_allow_html=True)

                st.markdown("<br>", unsafe_allow_html=True)
                buf_inf3 = io.BytesIO()
                with pd.ExcelWriter(buf_inf3, engine='openpyxl') as w:
                    df3[['sku','nombre','categoria','proveedor','cant_base',
                          'precio_base','precio_comp','impacto_base',
                          'impacto_comp','delta_dinero','delta_pct']].to_excel(w, sheet_name='Canasta', index=False)
                st.download_button("📥 Descargar Excel", buf_inf3.getvalue(), "Informe3_Canasta.xlsx")


# ============================================================
# MÓDULO: TENDENCIAS BAR
# ============================================================
# ============================================================
# MÓDULO: TENDENCIAS BAR
# ============================================================
if modulo.startswith("🍹"):
    st.markdown("""
    <div style="margin-bottom:1.5rem">
        <div style="font-size:0.72rem;text-transform:uppercase;letter-spacing:0.12em;color:#555;margin-bottom:4px">Módulo</div>
        <div style="font-family:'DM Serif Display',serif;font-size:2rem;color:#f0ede8;letter-spacing:-0.02em;line-height:1.1">
            🍹 Tendencias Bar
        </div>
        <div style="font-size:0.82rem;color:#666;margin-top:4px">Análisis completo de compras · Todos los productos · Resumen + Detalle</div>
    </div>
    """, unsafe_allow_html=True)

    # ── Helpers ───────────────────────────────────────────────
    def _bar_local_filter(local):
        if local and local != "Todos":
            return f"AND UPPER(local) = UPPER('{local}')"
        return ""

    def _mes_lbl(y, m):
        meses = ["Ene","Feb","Mar","Abr","May","Jun","Jul","Ago","Sep","Oct","Nov","Dic"]
        return f"{meses[m-1]} {str(y)[-2:]}"

    # ── Controles globales del módulo ─────────────────────────
    ca, cb, cc = st.columns([2, 4, 2])
    with ca:
        bar_local = st.selectbox("Local", ["Todos"] + [l for l in get_locales() if l != "Todos"], key="bar_local")
    with cb:
        bar_texto = st.text_input("Filtrar SKU / Producto", key="bar_texto", placeholder="Ej: RON, VODKA, CERVEZA…")
    with cc:
        st.markdown("<div style='height:1.6rem'></div>", unsafe_allow_html=True)
        if st.button("▶ Cargar análisis Bar", key="bar_run", use_container_width=True):
            for k in ['bar_resumen_df','bar_vol_df','bar_gasto_df','bar_freq_df']:
                st.session_state.pop(k, None)

    filtro_local_bar = _bar_local_filter(bar_local)

    # ════════════════════════════════════════════════════════════
    # QUERY RESUMEN — Una fila por SKU con todos los indicadores
    # ════════════════════════════════════════════════════════════
    sql_resumen = f"""
        WITH nombres AS (
            SELECT sku,
                   MODE() WITHIN GROUP (ORDER BY nombre_producto) AS nombre,
                   MODE() WITHIN GROUP (ORDER BY nombre_proveedor) AS proveedor
            FROM compras
            WHERE UPPER(categoria_producto) LIKE '%BAR%'
              AND tipo_dte != '61'
            GROUP BY sku
        ),
        base AS (
            SELECT
                sku,
                DATE_TRUNC('month', fecha_dte::timestamp)::date           AS mes,
                fecha_dte::timestamp::date                                AS fecha_compra,
                SUM(cant_conv)                                            AS vol_mes,
                SUM(costo_realfinal)                                      AS gasto_mes,
                AVG(muc)                                                  AS muc_mes
            FROM compras
            WHERE UPPER(categoria_producto) LIKE '%BAR%'
              AND cant_conv > 0
              AND costo_realfinal > 0
              AND tipo_dte != '61'
              {filtro_local_bar}
            GROUP BY sku, DATE_TRUNC('month', fecha_dte::timestamp), fecha_dte::timestamp::date
        ),
        por_mes AS (
            SELECT sku,
                   mes,
                   SUM(vol_mes)   AS vol_total_mes,
                   SUM(gasto_mes) AS gasto_total_mes,
                   AVG(muc_mes)   AS muc_promedio_mes
            FROM base
            GROUP BY sku, mes
        ),
        stats AS (
            SELECT
                sku,
                COUNT(DISTINCT mes)                                        AS n_meses,
                ROUND(AVG(vol_total_mes)::numeric, 2)                     AS vol_prom_mes,
                ROUND(STDDEV(vol_total_mes)::numeric, 2)                  AS vol_std,
                ROUND(SUM(vol_total_mes)::numeric, 2)                     AS vol_total,
                ROUND(AVG(gasto_total_mes)::numeric, 0)                   AS gasto_prom_mes,
                ROUND(SUM(gasto_total_mes)::numeric, 0)                   AS gasto_total,
                ROUND(AVG(muc_promedio_mes)::numeric, 4)                  AS muc_actual,
                -- Tendencia: promedio últimos 2 meses vs 2 anteriores
                ROUND(AVG(CASE WHEN mes >= (SELECT MAX(mes) - INTERVAL '1 month' FROM por_mes p2 WHERE p2.sku = por_mes.sku)
                               THEN vol_total_mes END)::numeric, 2)       AS vol_ult2m,
                ROUND(AVG(CASE WHEN mes < (SELECT MAX(mes) - INTERVAL '1 month' FROM por_mes p2 WHERE p2.sku = por_mes.sku)
                               AND mes >= (SELECT MAX(mes) - INTERVAL '3 months' FROM por_mes p2 WHERE p2.sku = por_mes.sku)
                               THEN vol_total_mes END)::numeric, 2)       AS vol_ant2m
            FROM por_mes
            GROUP BY sku
        ),
        ultima_compra AS (
            SELECT sku,
                   MAX(fecha_compra) AS ultima_compra,
                   (CURRENT_DATE - MAX(fecha_compra))  AS dias_sin_comprar
            FROM base
            GROUP BY sku
        ),
        ciclo AS (
            SELECT sku,
                   CASE WHEN COUNT(DISTINCT fecha_compra) > 1
                        THEN ROUND((MAX(fecha_compra) - MIN(fecha_compra))::numeric
                             / NULLIF(COUNT(DISTINCT fecha_compra) - 1, 0), 1)
                        ELSE NULL END AS dias_entre_compras,
                   COUNT(DISTINCT fecha_compra) AS n_compras
            FROM base
            GROUP BY sku
        )
        SELECT
            s.sku, n.nombre, n.proveedor,
            s.n_meses, s.vol_prom_mes, s.vol_std, s.vol_total,
            s.gasto_prom_mes, s.gasto_total, s.muc_actual,
            s.vol_ult2m, s.vol_ant2m,
            CASE WHEN s.vol_ant2m > 0
                 THEN ROUND(((s.vol_ult2m - s.vol_ant2m) / s.vol_ant2m * 100)::numeric, 1)
                 ELSE NULL END                                             AS tendencia_pct,
            CASE WHEN s.vol_prom_mes > 0
                 THEN ROUND((s.vol_std / s.vol_prom_mes * 100)::numeric, 1)
                 ELSE NULL END                                             AS cv_pct,
            CASE WHEN c.dias_entre_compras IS NOT NULL AND s.vol_prom_mes > 0
                 THEN ROUND(((s.vol_prom_mes + 1.65 * COALESCE(s.vol_std, 0))
                              / 30.0 * c.dias_entre_compras)::numeric, 2)
                 ELSE NULL END                                             AS stock_seg_sugerido,
            c.dias_entre_compras,
            c.n_compras,
            u.ultima_compra,
            u.dias_sin_comprar
        FROM stats s
        JOIN nombres n      ON s.sku = n.sku
        JOIN ultima_compra u ON s.sku = u.sku
        JOIN ciclo c         ON s.sku = c.sku
        ORDER BY s.gasto_total DESC
    """

    # ── QUERY VOLUMEN PIVOT ────────────────────────────────────
    sql_vol = f"""
        SELECT
            sku,
            MODE() WITHIN GROUP (ORDER BY nombre_producto)                              AS nombre,
            DATE_TRUNC('month', fecha_dte::timestamp)::date   AS mes,
            ROUND(SUM(cant_conv)::numeric, 2)                 AS vol_total
        FROM compras
        WHERE UPPER(categoria_producto) LIKE '%BAR%'
          AND cant_conv > 0
          AND tipo_dte != '61'
          {filtro_local_bar}
        GROUP BY sku, DATE_TRUNC('month', fecha_dte::timestamp)
        ORDER BY mes, sku
    """

    # ── QUERY GASTO PIVOT ─────────────────────────────────────
    sql_gasto = f"""
        SELECT
            sku,
            MODE() WITHIN GROUP (ORDER BY nombre_producto)                              AS nombre,
            DATE_TRUNC('month', fecha_dte::timestamp)::date   AS mes,
            ROUND(SUM(costo_realfinal)::numeric, 0)           AS gasto_total
        FROM compras
        WHERE UPPER(categoria_producto) LIKE '%BAR%'
          AND costo_realfinal > 0
          AND tipo_dte != '61'
          {filtro_local_bar}
        GROUP BY sku, DATE_TRUNC('month', fecha_dte::timestamp)
        ORDER BY mes, sku
    """

    # ── QUERY FRECUENCIA ──────────────────────────────────────
    sql_freq = f"""
        WITH fechas AS (
            SELECT sku, MODE() WITHIN GROUP (ORDER BY nombre_producto) AS nombre, local,
                   fecha_dte::timestamp::date AS fecha_compra,
                   SUM(cant_conv)             AS vol_dia,
                   SUM(costo_realfinal)       AS gasto_dia
            FROM compras
            WHERE UPPER(categoria_producto) LIKE '%BAR%'
              AND cant_conv > 0
              AND tipo_dte != '61'
              {filtro_local_bar}
            GROUP BY sku, local, fecha_dte::timestamp::date
        )
        SELECT
            sku, MAX(nombre) AS nombre, local,
            COUNT(*)                                                          AS n_compras,
            MIN(fecha_compra)                                                 AS primera_compra,
            MAX(fecha_compra)                                                 AS ultima_compra,
            (CURRENT_DATE - MAX(fecha_compra))                                AS dias_sin_comprar,
            CASE WHEN COUNT(*) > 1
                 THEN ROUND((MAX(fecha_compra) - MIN(fecha_compra))::numeric
                      / NULLIF(COUNT(*) - 1, 0), 1)
                 ELSE NULL END                                                AS dias_entre_compras,
            ROUND(AVG(vol_dia)::numeric, 2)                                  AS vol_promedio,
            ROUND(AVG(gasto_dia)::numeric, 0)                                AS gasto_promedio,
            ROUND(STDDEV(vol_dia)::numeric, 2)                               AS vol_std
        FROM fechas
        GROUP BY sku, local
        ORDER BY dias_entre_compras ASC NULLS LAST, sku
    """

    # ── Carga automática ──────────────────────────────────────
    if 'bar_resumen_df' not in st.session_state:
        with st.spinner("Cargando análisis completo del bar…"):
            try:
                st.session_state['bar_resumen_df'] = run_query(sql_resumen)
                st.session_state['bar_vol_df']     = run_query(sql_vol)
                st.session_state['bar_gasto_df']   = run_query(sql_gasto)
                st.session_state['bar_freq_df']    = run_query(sql_freq)
            except Exception as e:
                st.error(f"Error cargando datos bar: {e}")
                st.stop()

    df_res  = st.session_state.get('bar_resumen_df', pd.DataFrame())
    df_vol  = st.session_state.get('bar_vol_df',     pd.DataFrame())
    df_gasto= st.session_state.get('bar_gasto_df',   pd.DataFrame())
    df_freq = st.session_state.get('bar_freq_df',    pd.DataFrame())

    # ── Filtro texto (aplica a todos los tabs) ────────────────
    def _filtrar_texto(df, texto, cols=('sku','nombre')):
        if texto and not df.empty:
            mask = pd.Series([False]*len(df), index=df.index)
            for c in cols:
                if c in df.columns:
                    mask |= df[c].astype(str).str.contains(texto, case=False, na=False)
            return df[mask]
        return df

    df_res_f   = _filtrar_texto(df_res,   bar_texto)
    df_vol_f   = _filtrar_texto(df_vol,   bar_texto)
    df_gasto_f = _filtrar_texto(df_gasto, bar_texto)
    df_freq_f  = _filtrar_texto(df_freq,  bar_texto)

    # ════════════════════════════════════════════════════════════
    # TABS
    # ════════════════════════════════════════════════════════════
    tb_res, tb_vol, tb_gasto, tb_freq = st.tabs([
        "📋 Resumen Ejecutivo",
        "📦 Volumen por Mes",
        "💰 Gasto por Mes",
        "🔄 Frecuencia de Compra"
    ])

    # ════════════════════════════════════════════════════════════
    # TAB RESUMEN EJECUTIVO
    # ════════════════════════════════════════════════════════════
    with tb_res:
        st.markdown("<div style='height:0.5rem'></div>", unsafe_allow_html=True)

        if df_res_f is not None and not df_res_f.empty:
            # KPIs globales
            k1, k2, k3, k4, k5 = st.columns(5)
            k1.metric("SKUs Bar", str(len(df_res_f)))
            k2.metric("Gasto Total", f"${df_res_f['gasto_total'].sum():,.0f}")
            k3.metric("Vol. Total (kg/lt)", f"{df_res_f['vol_total'].sum():,.1f}")
            k4.metric("Prom. días entre compras",
                      f"{df_res_f['dias_entre_compras'].median():.0f}d"
                      if not df_res_f['dias_entre_compras'].isna().all() else "—")
            n_urgente = (df_res_f['dias_sin_comprar'] >= df_res_f['dias_entre_compras'].fillna(999)).sum()
            k5.metric("⚠️ Próximos a pedir", str(int(n_urgente)))

            st.markdown("<div style='height:0.75rem'></div>", unsafe_allow_html=True)

            # Helpers visuales
            def tend_badge(pct):
                if pd.isna(pct):
                    return '<span style="color:#444">—</span>'
                if pct > 10:
                    return f'<span style="color:#4cdd8a;font-weight:600">▲ {pct:.1f}%</span>'
                elif pct < -10:
                    return f'<span style="color:#e84545;font-weight:600">▼ {pct:.1f}%</span>'
                else:
                    return f'<span style="color:#888">→ {pct:.1f}%</span>'

            def cv_badge(cv):
                if pd.isna(cv):
                    return '<span style="color:#444">—</span>'
                if cv <= 20:
                    return f'<span style="background:#1a3a2a;color:#4cdd8a;padding:2px 8px;border-radius:10px;font-size:0.74rem">{cv:.0f}%</span>'
                elif cv <= 50:
                    return f'<span style="background:#2a2a1a;color:#e8c14a;padding:2px 8px;border-radius:10px;font-size:0.74rem">{cv:.0f}%</span>'
                else:
                    return f'<span style="background:#2a1a1a;color:#e84545;padding:2px 8px;border-radius:10px;font-size:0.74rem">{cv:.0f}%</span>'

            def urgencia_cell(dias_sin, ciclo):
                if pd.isna(ciclo) or pd.isna(dias_sin):
                    return f'<span style="color:#555;font-size:0.78rem">{int(dias_sin) if not pd.isna(dias_sin) else "—"}d</span>'
                ratio = dias_sin / ciclo
                if ratio >= 0.9:
                    return f'<span style="background:#3a1a1a;color:#ff6b6b;padding:2px 8px;border-radius:10px;font-size:0.74rem;font-weight:700">🔴 {int(dias_sin)}d</span>'
                elif ratio >= 0.6:
                    return f'<span style="background:#2a2a1a;color:#e8c14a;padding:2px 8px;border-radius:10px;font-size:0.74rem">🟡 {int(dias_sin)}d</span>'
                else:
                    return f'<span style="color:#555;font-size:0.78rem">{int(dias_sin)}d</span>'

            hs = 'padding:10px 12px;font-size:0.67rem;text-transform:uppercase;letter-spacing:0.09em;font-weight:600;color:#444;border-bottom:1px solid #1e1e1e;white-space:nowrap'
            hdrs_r = ['SKU','Producto','Proveedor','Meses','Vol.Prom/mes','CV%','Tendencia','MUC actual','Ciclo(d)','Sin comprar','Stock Seg.','Gasto Prom/mes']
            thead_r = '<tr style="background:#111">' + ''.join(
                [f'<th style="{hs};text-align:left">{h}</th>' if i < 3
                 else f'<th style="{hs};text-align:right">{h}</th>'
                 for i, h in enumerate(hdrs_r)]
            ) + '</tr>'

            rows_r = ''
            for _, r in df_res_f.iterrows():
                rows_r += (
                    f'<tr style="border-bottom:1px solid #161616">'
                    f'<td style="padding:9px 12px;color:#666;font-family:monospace;font-size:0.74rem;white-space:nowrap">{r["sku"]}</td>'
                    f'<td style="padding:9px 12px;color:#ccc;max-width:200px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="{r["nombre"]}">{str(r["nombre"])[:38]}</td>'
                    f'<td style="padding:9px 12px;color:#777;font-size:0.78rem;white-space:nowrap;max-width:140px;overflow:hidden;text-overflow:ellipsis">{str(r["proveedor"])[:22]}</td>'
                    f'<td style="padding:9px 12px;text-align:right;color:#aaa">{int(r["n_meses"])}</td>'
                    f'<td style="padding:9px 12px;text-align:right;color:#7ab8e8;font-variant-numeric:tabular-nums">{r["vol_prom_mes"]:,.2f}</td>'
                    f'<td style="padding:9px 12px;text-align:right">{cv_badge(r["cv_pct"])}</td>'
                    f'<td style="padding:9px 12px;text-align:right">{tend_badge(r["tendencia_pct"])}</td>'
                    f'<td style="padding:9px 12px;text-align:right;color:#d4a853;font-variant-numeric:tabular-nums">{r["muc_actual"]:.4f}</td>'
                    f'<td style="padding:9px 12px;text-align:right;color:#888">'
                    + (f'{r["dias_entre_compras"]:.0f}d' if not pd.isna(r["dias_entre_compras"]) else '—') +
                    f'</td>'
                    f'<td style="padding:9px 12px;text-align:right">{urgencia_cell(r["dias_sin_comprar"], r["dias_entre_compras"])}</td>'
                    f'<td style="padding:9px 12px;text-align:right;color:#b8e8c0;font-variant-numeric:tabular-nums">'
                    + (f'{r["stock_seg_sugerido"]:,.2f}' if not pd.isna(r["stock_seg_sugerido"]) else '—') +
                    f'</td>'
                    f'<td style="padding:9px 12px;text-align:right;color:#d4a853;font-variant-numeric:tabular-nums">${r["gasto_prom_mes"]:,.0f}</td>'
                    f'</tr>'
                )

            tabla_r = (
                '<div style="overflow-x:auto;border-radius:14px;border:1px solid #1e1e1e;background:#0d0d0d">'
                '<table style="width:100%;border-collapse:collapse;font-family:DM Sans,sans-serif;font-size:0.83rem">'
                f'<thead>{thead_r}</thead><tbody>{rows_r}</tbody></table></div>'
            )
            st.markdown(tabla_r, unsafe_allow_html=True)

            st.markdown("""
            <div style='margin-top:0.75rem;font-size:0.75rem;color:#555'>
            <b style='color:#666'>CV%</b>: Coeficiente de variación mensual — 
            <span style='color:#4cdd8a'>≤20% estable</span> · 
            <span style='color:#e8c14a'>21-50% variable</span> · 
            <span style='color:#e84545'>>50% errático</span> &nbsp;|&nbsp;
            <b style='color:#666'>Tendencia</b>: últimos 2 meses vs 2 anteriores &nbsp;|&nbsp;
            <b style='color:#666'>Stock Seg.</b>: kg/lt recomendados al momento de pedir (95% nivel servicio)
            </div>
            """, unsafe_allow_html=True)

            st.markdown("<div style='height:0.75rem'></div>", unsafe_allow_html=True)
            buf_r = io.BytesIO()
            export_r = df_res_f[['sku','nombre','proveedor','n_meses','vol_prom_mes','cv_pct',
                                  'tendencia_pct','muc_actual','dias_entre_compras',
                                  'dias_sin_comprar','stock_seg_sugerido','gasto_prom_mes','gasto_total']].copy()
            export_r.columns = ['SKU','Producto','Proveedor','Meses','Vol.Prom/mes','CV%',
                                 'Tendencia%','MUC actual','Ciclo(días)',
                                 'Días sin comprar','Stock Seg.','Gasto Prom/mes','Gasto Total']
            with pd.ExcelWriter(buf_r, engine='openpyxl') as w:
                export_r.to_excel(w, sheet_name='Resumen_Bar', index=False)
            st.download_button("📥 Exportar Resumen", buf_r.getvalue(), "Bar_Resumen.xlsx")
        else:
            st.info("Presiona ▶ Cargar análisis Bar para ejecutar.")

    # ════════════════════════════════════════════════════════════
    # TAB VOLUMEN POR MES
    # ════════════════════════════════════════════════════════════
    with tb_vol:
        st.markdown("<div style='height:0.5rem'></div>", unsafe_allow_html=True)
        if df_vol_f is not None and not df_vol_f.empty:
            df_vol_f['mes_lbl'] = df_vol_f['mes'].apply(lambda x: _mes_lbl(x.year, x.month))
            pivot_v = df_vol_f.pivot_table(index=['sku','nombre'], columns='mes_lbl',
                                            values='vol_total', aggfunc='sum').reset_index()
            meses_ord = sorted(df_vol_f['mes'].unique())
            col_m = [_mes_lbl(m.year, m.month) for m in meses_ord]
            col_m = [c for c in col_m if c in pivot_v.columns]
            pivot_v['_prom'] = pivot_v[col_m].mean(axis=1, skipna=True)
            pivot_v['_total'] = pivot_v[col_m].sum(axis=1, skipna=True)
            pivot_v = pivot_v.sort_values('_total', ascending=False)

            mv1, mv2, mv3 = st.columns(3)
            mv1.metric("SKUs", str(len(pivot_v)))
            mv2.metric("Vol. Total (kg/lt)", f"{pivot_v['_total'].sum():,.1f}")
            mv3.metric("Meses", str(len(col_m)))
            st.markdown("<div style='height:0.5rem'></div>", unsafe_allow_html=True)

            def hm(val, rmax, rmin):
                if pd.isna(val) or rmax == rmin:
                    return '<td style="padding:9px 12px;text-align:right;color:#333">—</td>'
                ratio = (val - rmin) / (rmax - rmin)
                if ratio > 0.75:   bg,fg = '#1a3a2a','#4cdd8a'
                elif ratio > 0.4:  bg,fg = '#1a2a3a','#4aaded'
                elif ratio > 0.1:  bg,fg = '#2a2a1a','#c8b94a'
                else:              bg,fg = '#1a1a1a','#555'
                return f'<td style="padding:9px 12px;text-align:right;background:{bg};color:{fg};font-weight:600;font-variant-numeric:tabular-nums">{val:,.2f}</td>'

            hs = 'padding:10px 12px;font-size:0.67rem;text-transform:uppercase;letter-spacing:0.09em;font-weight:600;color:#444;border-bottom:1px solid #1e1e1e;white-space:nowrap'
            hdrs_v = ['SKU','Producto'] + col_m + ['Prom/mes','Total']
            thead_v = '<tr style="background:#111">' + ''.join(
                [f'<th style="{hs};text-align:left">{h}</th>' if i<2 else f'<th style="{hs};text-align:right">{h}</th>'
                 for i,h in enumerate(hdrs_v)]) + '</tr>'
            rows_v = ''
            for _, r in pivot_v.iterrows():
                vals = [r.get(c, float('nan')) for c in col_m]
                vnum = [v for v in vals if not pd.isna(v)]
                rmx,rmn = (max(vnum),min(vnum)) if vnum else (1,0)
                rows_v += (
                    f'<tr style="border-bottom:1px solid #161616">'
                    f'<td style="padding:9px 12px;color:#666;font-family:monospace;font-size:0.74rem">{r["sku"]}</td>'
                    f'<td style="padding:9px 12px;color:#ccc;max-width:220px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">{str(r["nombre"])[:44]}</td>'
                    + ''.join([hm(v,rmx,rmn) for v in vals])
                    + f'<td style="padding:9px 12px;text-align:right;color:#7ab8e8;font-variant-numeric:tabular-nums">{r["_prom"]:,.2f}</td>'
                    + f'<td style="padding:9px 12px;text-align:right;color:#d4a853;font-weight:700;font-variant-numeric:tabular-nums">{r["_total"]:,.1f}</td>'
                    f'</tr>'
                )
            st.markdown(
                '<div style="overflow-x:auto;border-radius:14px;border:1px solid #1e1e1e;background:#0d0d0d">'
                '<table style="width:100%;border-collapse:collapse;font-family:DM Sans,sans-serif;font-size:0.83rem">'
                f'<thead>{thead_v}</thead><tbody>{rows_v}</tbody></table></div>',
                unsafe_allow_html=True)
            st.markdown("<div style='height:0.75rem'></div>", unsafe_allow_html=True)
            buf_v = io.BytesIO()
            exp_v = pivot_v[['sku','nombre']+col_m+['_prom','_total']].copy()
            exp_v.columns = ['SKU','Producto']+col_m+['Prom/mes','Total']
            with pd.ExcelWriter(buf_v, engine='openpyxl') as w:
                exp_v.to_excel(w, sheet_name='Volumen_Bar', index=False)
            st.download_button("📥 Exportar Excel", buf_v.getvalue(), "Bar_Volumen.xlsx")
        else:
            st.info("Presiona ▶ Cargar análisis Bar para ejecutar.")

    # ════════════════════════════════════════════════════════════
    # TAB GASTO POR MES
    # ════════════════════════════════════════════════════════════
    with tb_gasto:
        st.markdown("<div style='height:0.5rem'></div>", unsafe_allow_html=True)
        if df_gasto_f is not None and not df_gasto_f.empty:
            df_gasto_f['mes_lbl'] = df_gasto_f['mes'].apply(lambda x: _mes_lbl(x.year, x.month))
            pivot_g = df_gasto_f.pivot_table(index=['sku','nombre'], columns='mes_lbl',
                                              values='gasto_total', aggfunc='sum').reset_index()
            meses_ord_g = sorted(df_gasto_f['mes'].unique())
            col_mg = [_mes_lbl(m.year, m.month) for m in meses_ord_g]
            col_mg = [c for c in col_mg if c in pivot_g.columns]
            pivot_g['_prom'] = pivot_g[col_mg].mean(axis=1, skipna=True)
            pivot_g['_total'] = pivot_g[col_mg].sum(axis=1, skipna=True)
            pivot_g = pivot_g.sort_values('_total', ascending=False)

            mg1, mg2, mg3 = st.columns(3)
            mg1.metric("SKUs", str(len(pivot_g)))
            mg2.metric("Gasto Total", f"${pivot_g['_total'].sum():,.0f}")
            mg3.metric("Meses", str(len(col_mg)))
            st.markdown("<div style='height:0.5rem'></div>", unsafe_allow_html=True)

            def hmg(val, rmax, rmin):
                if pd.isna(val) or rmax == rmin:
                    return '<td style="padding:9px 12px;text-align:right;color:#333">—</td>'
                ratio = (val - rmin) / (rmax - rmin)
                if ratio > 0.75:   bg,fg = '#3a1a1a','#ff8c8c'
                elif ratio > 0.4:  bg,fg = '#2a1a2a','#c87ae8'
                elif ratio > 0.1:  bg,fg = '#1a2a2a','#4ae8c8'
                else:              bg,fg = '#1a1a1a','#555'
                return f'<td style="padding:9px 12px;text-align:right;background:{bg};color:{fg};font-weight:600;font-variant-numeric:tabular-nums">${val:,.0f}</td>'

            hs = 'padding:10px 12px;font-size:0.67rem;text-transform:uppercase;letter-spacing:0.09em;font-weight:600;color:#444;border-bottom:1px solid #1e1e1e;white-space:nowrap'
            hdrs_g = ['SKU','Producto'] + col_mg + ['Prom/mes','Total']
            thead_g = '<tr style="background:#111">' + ''.join(
                [f'<th style="{hs};text-align:left">{h}</th>' if i<2 else f'<th style="{hs};text-align:right">{h}</th>'
                 for i,h in enumerate(hdrs_g)]) + '</tr>'
            rows_g = ''
            for _, r in pivot_g.iterrows():
                vals = [r.get(c, float('nan')) for c in col_mg]
                vnum = [v for v in vals if not pd.isna(v)]
                rmx,rmn = (max(vnum),min(vnum)) if vnum else (1,0)
                rows_g += (
                    f'<tr style="border-bottom:1px solid #161616">'
                    f'<td style="padding:9px 12px;color:#666;font-family:monospace;font-size:0.74rem">{r["sku"]}</td>'
                    f'<td style="padding:9px 12px;color:#ccc;max-width:220px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">{str(r["nombre"])[:44]}</td>'
                    + ''.join([hmg(v,rmx,rmn) for v in vals])
                    + f'<td style="padding:9px 12px;text-align:right;color:#c87ae8;font-variant-numeric:tabular-nums">${r["_prom"]:,.0f}</td>'
                    + f'<td style="padding:9px 12px;text-align:right;color:#d4a853;font-weight:700;font-variant-numeric:tabular-nums">${r["_total"]:,.0f}</td>'
                    f'</tr>'
                )
            st.markdown(
                '<div style="overflow-x:auto;border-radius:14px;border:1px solid #1e1e1e;background:#0d0d0d">'
                '<table style="width:100%;border-collapse:collapse;font-family:DM Sans,sans-serif;font-size:0.83rem">'
                f'<thead>{thead_g}</thead><tbody>{rows_g}</tbody></table></div>',
                unsafe_allow_html=True)
            st.markdown("<div style='height:0.75rem'></div>", unsafe_allow_html=True)
            buf_g = io.BytesIO()
            exp_g = pivot_g[['sku','nombre']+col_mg+['_prom','_total']].copy()
            exp_g.columns = ['SKU','Producto']+col_mg+['Prom/mes','Total']
            with pd.ExcelWriter(buf_g, engine='openpyxl') as w:
                exp_g.to_excel(w, sheet_name='Gasto_Bar', index=False)
            st.download_button("📥 Exportar Excel", buf_g.getvalue(), "Bar_Gasto.xlsx")
        else:
            st.info("Presiona ▶ Cargar análisis Bar para ejecutar.")

    # ════════════════════════════════════════════════════════════
    # TAB FRECUENCIA DE COMPRA
    # ════════════════════════════════════════════════════════════
    with tb_freq:
        st.markdown("<div style='height:0.5rem'></div>", unsafe_allow_html=True)
        if df_freq_f is not None and not df_freq_f.empty:
            mf1, mf2, mf3, mf4 = st.columns(4)
            med_dias = df_freq_f['dias_entre_compras'].median()
            mf1.metric("Mediana ciclo", f"{med_dias:.0f}d" if not pd.isna(med_dias) else "—")
            mf2.metric("🟢 ≤7 días", str(int((df_freq_f['dias_entre_compras']<=7).sum())))
            mf3.metric("🟡 8–15 días", str(int(((df_freq_f['dias_entre_compras']>7)&(df_freq_f['dias_entre_compras']<=15)).sum())))
            mf4.metric("🔴 >15 días", str(int((df_freq_f['dias_entre_compras']>15).sum())))
            st.markdown("<div style='height:0.5rem'></div>", unsafe_allow_html=True)

            def badge_c(dias):
                if pd.isna(dias):
                    return '<span style="color:#444;font-size:0.75rem">1 compra</span>'
                if dias<=7:
                    return f'<span style="background:#1a3a2a;color:#4cdd8a;padding:2px 9px;border-radius:12px;font-size:0.75rem;font-weight:600">cada {dias:.0f}d</span>'
                elif dias<=15:
                    return f'<span style="background:#2a2a1a;color:#e8c14a;padding:2px 9px;border-radius:12px;font-size:0.75rem;font-weight:600">cada {dias:.0f}d</span>'
                else:
                    return f'<span style="background:#2a1a1a;color:#e84545;padding:2px 9px;border-radius:12px;font-size:0.75rem;font-weight:600">cada {dias:.0f}d</span>'

            def urg_b(dias_sin, ciclo):
                if pd.isna(ciclo) or pd.isna(dias_sin): return f'<span style="color:#555">{int(dias_sin) if not pd.isna(dias_sin) else "—"}d</span>'
                ratio = dias_sin/ciclo
                if ratio>=0.9: return f'<span style="background:#3a1a1a;color:#ff6b6b;padding:2px 8px;border-radius:10px;font-size:0.74rem;font-weight:700">🔴 {int(dias_sin)}d</span>'
                elif ratio>=0.6: return f'<span style="background:#2a2a1a;color:#e8c14a;padding:2px 8px;border-radius:10px;font-size:0.74rem">🟡 {int(dias_sin)}d</span>'
                return f'<span style="color:#555;font-size:0.78rem">{int(dias_sin)}d</span>'

            hs = 'padding:10px 12px;font-size:0.67rem;text-transform:uppercase;letter-spacing:0.09em;font-weight:600;color:#444;border-bottom:1px solid #1e1e1e;white-space:nowrap'
            hdrs_f = ['SKU','Producto','Local','# Compras','Ciclo','Sin comprar','Vol.Prom(kg/lt)','Gasto Prom $','Primera','Última']
            thead_f = '<tr style="background:#111">' + ''.join(
                [f'<th style="{hs};text-align:left">{h}</th>' if i<3 else f'<th style="{hs};text-align:right">{h}</th>'
                 for i,h in enumerate(hdrs_f)]) + '</tr>'
            rows_f = ''
            for _, r in df_freq_f.iterrows():
                rows_f += (
                    f'<tr style="border-bottom:1px solid #161616">'
                    f'<td style="padding:9px 12px;color:#666;font-family:monospace;font-size:0.74rem">{r["sku"]}</td>'
                    f'<td style="padding:9px 12px;color:#ccc;max-width:200px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">{str(r["nombre"])[:42]}</td>'
                    f'<td style="padding:9px 12px;color:#888;font-size:0.8rem">{r["local"]}</td>'
                    f'<td style="padding:9px 12px;text-align:right;color:#aaa">{int(r["n_compras"])}</td>'
                    f'<td style="padding:9px 12px;text-align:right">{badge_c(r["dias_entre_compras"])}</td>'
                    f'<td style="padding:9px 12px;text-align:right">{urg_b(r["dias_sin_comprar"], r["dias_entre_compras"])}</td>'
                    f'<td style="padding:9px 12px;text-align:right;color:#7ab8e8;font-variant-numeric:tabular-nums">{r["vol_promedio"]:,.2f}</td>'
                    f'<td style="padding:9px 12px;text-align:right;color:#d4a853;font-variant-numeric:tabular-nums">${r["gasto_promedio"]:,.0f}</td>'
                    f'<td style="padding:9px 12px;text-align:right;color:#555;font-size:0.78rem">{str(r["primera_compra"])[:10]}</td>'
                    f'<td style="padding:9px 12px;text-align:right;color:#888;font-size:0.78rem">{str(r["ultima_compra"])[:10]}</td>'
                    f'</tr>'
                )
            st.markdown(
                '<div style="overflow-x:auto;border-radius:14px;border:1px solid #1e1e1e;background:#0d0d0d">'
                '<table style="width:100%;border-collapse:collapse;font-family:DM Sans,sans-serif;font-size:0.83rem">'
                f'<thead>{thead_f}</thead><tbody>{rows_f}</tbody></table></div>',
                unsafe_allow_html=True)
            st.markdown("<div style='height:0.75rem'></div>", unsafe_allow_html=True)
            buf_f = io.BytesIO()
            with pd.ExcelWriter(buf_f, engine='openpyxl') as w:
                df_freq_f[['sku','nombre','local','n_compras','dias_entre_compras',
                            'dias_sin_comprar','vol_promedio','gasto_promedio',
                            'primera_compra','ultima_compra']].to_excel(
                    w, sheet_name='Frecuencia_Bar', index=False)
            st.download_button("📥 Exportar Excel", buf_f.getvalue(), "Bar_Frecuencia.xlsx")
        else:
            st.info("Presiona ▶ Cargar análisis Bar para ejecutar.")
