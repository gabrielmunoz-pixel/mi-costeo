import streamlit as st
import pandas as pd
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
    filtro_local_c = "AND UPPER(local) = UPPER(:l)" if local != "Todos" else ""
    params_c = {"i": fecha_i, "f": fecha_f}
    if local != "Todos":
        params_c["l"] = local

    q_c = f"""
        SELECT
            sku,
            subcat,
            SUM(cant_conv) AS cant_real_comprada,
            AVG(muc) AS muc_promedio
        FROM compras
        WHERE fecha_dte::date BETWEEN :i AND :f
          AND subcat IN ('Directo', 'Indirecto')
        {filtro_local_c}
        GROUP BY 1, 2
    """
    df_c = run_query(q_c, params_c)

    # Fallback: si el período no tiene compras, mostrar histórico completo
    if df_c.empty:
        q_c2 = f"""
            SELECT
                sku,
                subcat,
                SUM(cant_conv) AS cant_real_comprada,
                AVG(muc) AS muc_promedio
            FROM compras
            WHERE subcat IN ('Directo', 'Indirecto')
            {filtro_local_c}
            GROUP BY 1, 2
        """
        df_c = run_query(q_c2, params_c)
        if not df_c.empty:
            st.warning("⚠️ Sin compras en el período seleccionado — mostrando totales históricos.")

    # Aplicar equivalencias de SKU antes del join
    df_equiv = run_query("SELECT sku_compra, sku_receta FROM sku_equivalencias")
    if not df_equiv.empty:
        dict_equiv = dict(zip(df_equiv['sku_compra'], df_equiv['sku_receta']))
        df_c['sku'] = df_c['sku'].map(lambda x: dict_equiv.get(x, x))
        # Re-agrupar por si hay múltiples SKUs que ahora apuntan al mismo
        df_c = df_c.groupby(['sku', 'subcat'], as_index=False).agg({
            'cant_real_comprada': 'sum',
            'muc_promedio': 'mean'
        })

    # Nombre canónico desde compras — incluir equivalencias para SKUs que solo existen como destino
    q_nom = """
        SELECT sku, MIN(nombre_producto) as nombre_compra
        FROM compras
        WHERE subcat IN ('Directo', 'Indirecto')
        GROUP BY sku
    """
    nombres_compras = run_query(q_nom)
    dict_nombres = dict(zip(nombres_compras['sku'], nombres_compras['nombre_compra'])) if not nombres_compras.empty else {}

    # Para SKUs de receta que no están en compras directamente (solo via equivalencias),
    # usar el nombre de uno de sus equivalentes como fallback
    if not df_equiv.empty:
        for _, row in df_equiv.iterrows():
            sku_dest = row['sku_receta']
            sku_orig = row['sku_compra']
            if sku_dest not in dict_nombres and sku_orig in dict_nombres:
                dict_nombres[sku_dest] = dict_nombres[sku_orig]

    informe = pd.merge(
        cons_teo, df_c,
        left_on='sku_ingrediente', right_on='sku', how='outer'
    )
    informe = informe.fillna(0)

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


def save_compras(df):
    engine = get_engine()
    if engine is None:
        return
    df.columns = df.columns.str.strip().str.lower()
    df.columns = [c.replace('suma de ', '').replace(' ', '_').replace('(', '').replace(')', '')
                  for c in df.columns]
    cols_req = ['local', 'fecha_dte', 'rut_proveedor', 'nombre_proveedor', 'tipo_dte',
                'folio', 'nombre_producto', 'sku', 'subcat', 'codigo_impuesto',
                'cantidad', 'conversion', 'formato', 'categoria_producto',
                'cant_conv', 'monto_real', 'recargo2', 'total_neto2',
                'imp_adic', 'iva_2', 'tootal2', 'costo_realfinal', 'muc']
    try:
        df[cols_req].to_sql('compras', engine, if_exists='append', index=False)
        st.success(f"✅ {len(df)} registros de compras guardados.")
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

    modulo = st.radio(
        "Módulo",
        ["📦 Gestión de Datos", "🧮 Explosión MRP", "📊 Informes"],
        label_visibility="collapsed"
    )

    st.divider()
    st.markdown("<div style='font-size:0.75rem; color:#666; text-transform:uppercase; letter-spacing:0.08em;'>Filtros globales</div>", unsafe_allow_html=True)

    f_inicio = st.date_input("Desde", value=date(datetime.now().year, datetime.now().month, 1))
    f_fin    = st.date_input("Hasta", value=date.today())
    locales  = get_locales()
    f_local  = st.selectbox("Local", locales)


# ============================================================
# MÓDULO: GESTIÓN DE DATOS
# ============================================================
if modulo == "📦 Gestión de Datos":
    st.markdown("<div class='section-title'>Gestión de Datos</div>", unsafe_allow_html=True)

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
        st.markdown("<div class='info-box'>Carga el Excel de facturas/compras. Se añade al historial existente (append).</div>", unsafe_allow_html=True)
        f_comp = st.file_uploader("Excel de Compras (.xlsx)", type="xlsx", key="comp")
        if f_comp and st.button("💾 Guardar Compras"):
            save_compras(pd.read_excel(f_comp))

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
elif modulo == "🧮 Explosión MRP":
    st.markdown("<div class='section-title'>Explosión MRP</div>", unsafe_allow_html=True)
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
elif modulo == "📊 Informes":
    st.markdown("<div class='section-title'>Informes</div>", unsafe_allow_html=True)

    informe_sel = st.radio(
        "Seleccionar informe",
        ["Informe 1 — Rentabilidad por Producto", "Informe 2 — Desviación Real vs Teórico", "Informe 3 — Evolución de Precios"],
        horizontal=True,
        label_visibility="collapsed"
    )

    st.markdown("---")

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
    # INFORME 3
    # ----------------------------------------------------------
    elif "Informe 3" in informe_sel:
        st.markdown("### 📊 Evolución de Precios por Ingrediente")
        st.markdown(f"<div class='info-box'>Ventana de 3 meses centrada en el mes seleccionado. Precio = promedio mensual (monto_real / cant_conv). Variación % vs mes anterior.</div>", unsafe_allow_html=True)

        # Selector de mes de referencia
        meses_disp = run_query("""
            SELECT DISTINCT DATE_TRUNC('month', fecha_dte::timestamp)::date as mes
            FROM compras
            WHERE subcat IN ('Directo','Indirecto')
            ORDER BY 1
        """)

        if meses_disp.empty:
            st.warning("No hay datos de compras disponibles.")
        else:
            meses_list = pd.to_datetime(meses_disp['mes']).tolist()
            meses_fmt  = [m.strftime('%B %Y').capitalize() for m in meses_list]

            col_mes, col_cat = st.columns([2, 2])
            with col_mes:
                mes_idx = st.selectbox("Mes de referencia", range(len(meses_fmt)),
                                       format_func=lambda i: meses_fmt[i],
                                       index=len(meses_list)-1)
            mes_ref = meses_list[mes_idx]

            # Calcular ventana de 3 meses
            mes_sig = meses_list[mes_idx + 1] if mes_idx + 1 < len(meses_list) else None
            mes_ant = meses_list[mes_idx - 1] if mes_idx - 1 >= 0 else None
            mes_ant2 = meses_list[mes_idx - 2] if mes_idx - 2 >= 0 else None

            if mes_sig:
                ventana = [m for m in [mes_ant, mes_ref, mes_sig] if m is not None]
            else:
                ventana = [m for m in [mes_ant2, mes_ant, mes_ref] if m is not None]

            # Categorías disponibles
            cats_q = run_query("SELECT DISTINCT subcat FROM compras WHERE subcat IN ('Directo','Indirecto') ORDER BY 1")
            cats_disp = ['Todos'] + cats_q['subcat'].tolist() if not cats_q.empty else ['Todos']

            with col_cat:
                cat_sel = st.selectbox("Categoría", cats_disp)

            if st.button("▶ Generar Informe 3"):
                # Query precios por mes
                filtro_cat = f"AND subcat = '{cat_sel}'" if cat_sel != 'Todos' else ""
                fechas_in  = ", ".join([f"'{m.strftime('%Y-%m-%d')}'" for m in ventana])

                q_precios = f"""
                    SELECT
                        sku,
                        MIN(nombre_producto) as nombre,
                        subcat,
                        DATE_TRUNC('month', fecha_dte::timestamp)::date as mes,
                        SUM(monto_real) / NULLIF(SUM(cant_conv), 0) as precio_prom
                    FROM compras
                    WHERE DATE_TRUNC('month', fecha_dte::timestamp)::date IN ({fechas_in})
                      AND subcat IN ('Directo','Indirecto')
                      AND cant_conv > 0
                      {filtro_cat}
                    GROUP BY sku, subcat, DATE_TRUNC('month', fecha_dte::timestamp)::date
                    ORDER BY sku, mes
                """
                df_p = run_query(q_precios)

                if df_p.empty:
                    st.warning("Sin datos para los meses seleccionados.")
                else:
                    df_p['mes'] = pd.to_datetime(df_p['mes'])
                    meses_cols = sorted(df_p['mes'].unique())
                    meses_labels = {m: pd.Timestamp(m).strftime('%b %Y').capitalize() for m in meses_cols}

                    # Pivot: sku × mes → precio
                    pivot = df_p.pivot_table(index=['sku','nombre','subcat'], columns='mes', values='precio_prom').reset_index()
                    pivot.columns = [meses_labels.get(c, c) if not isinstance(c, str) else c for c in pivot.columns]
                    mes_cols_str = [meses_labels[m] for m in meses_cols]

                    # Calcular Δ% entre columnas de mes disponibles
                    for i in range(1, len(mes_cols_str)):
                        prev_col = mes_cols_str[i-1]
                        curr_col = mes_cols_str[i]
                        delta_col = f'Δ% {curr_col}'
                        if prev_col in pivot.columns and curr_col in pivot.columns:
                            pivot[delta_col] = ((pivot[curr_col] - pivot[prev_col]) / pivot[prev_col].replace(0, None) * 100).round(1)

                    # ---------- VISTA RESUMEN POR CATEGORÍA ----------
                    st.markdown("#### Resumen por Categoría")
                    cat_res = df_p.groupby(['subcat','mes']).agg(
                        precio_prom=('precio_prom','mean'),
                        skus=('sku','nunique')
                    ).reset_index()
                    cat_res['mes'] = pd.to_datetime(cat_res['mes'])
                    cat_pivot = cat_res.pivot_table(index=['subcat','skus'], columns='mes', values='precio_prom').reset_index()
                    cat_pivot.columns = [meses_labels.get(c, c) if not isinstance(c, str) else c for c in cat_pivot.columns]

                    hs = 'padding:11px 14px;font-size:0.7rem;text-transform:uppercase;letter-spacing:0.09em;font-weight:600;color:#444;border-bottom:1px solid #2a2a2a'

                    def badge_delta(val):
                        if val is None or (isinstance(val, float) and pd.isna(val)):
                            return '<span style="color:#444">—</span>'
                        if val > 10:
                            return f'<span style="background:#3a1a1a;color:#e84545;padding:2px 7px;border-radius:10px;font-size:0.75rem;font-weight:600">{val:+.1f}%</span>'
                        elif val > 3:
                            return f'<span style="background:#3a2a1a;color:#e89c45;padding:2px 7px;border-radius:10px;font-size:0.75rem;font-weight:600">{val:+.1f}%</span>'
                        elif val < -3:
                            return f'<span style="background:#1a3a2a;color:#4caf7d;padding:2px 7px;border-radius:10px;font-size:0.75rem;font-weight:600">{val:+.1f}%</span>'
                        return f'<span style="color:#aaa;font-size:0.75rem">{val:+.1f}%</span>'

                    def fmt_precio(val):
                        if val is None or (isinstance(val, float) and pd.isna(val)):
                            return '<span style="color:#333">—</span>'
                        return f'<span style="color:#ccc">${val:,.2f}</span>'

                    # Tabla resumen categorías
                    cat_hdrs = ['Categoría', 'SKUs'] + mes_cols_str
                    cat_rows_html = ''
                    for _, r in cat_pivot.iterrows():
                        cat_rows_html += '<tr style="border-bottom:1px solid #1e1e1e">'
                        cat_rows_html += f'<td style="padding:10px 14px;font-weight:500;color:#e8e4de">{r.get("subcat","")}</td>'
                        cat_rows_html += f'<td style="padding:10px 14px;text-align:right;color:#666">{int(r.get("skus",0)) if not pd.isna(r.get("skus",0)) else 0}</td>'
                        prev_val = None
                        for mc in mes_cols_str:
                            v = r.get(mc, None)
                            delta_html = ''
                            if prev_val is not None and v is not None and not pd.isna(v) and prev_val != 0:
                                delta = (v - prev_val) / prev_val * 100
                                delta_html = f' {badge_delta(delta)}'
                            cat_rows_html += f'<td style="padding:10px 14px;text-align:right">{fmt_precio(v)}{delta_html}</td>'
                            prev_val = v if (v is not None and not pd.isna(v)) else prev_val
                        cat_rows_html += '</tr>'

                    cat_table = (
                        '<div style="overflow-x:auto;border-radius:14px;border:1px solid #1e1e1e;margin-top:0.5rem;background:#0d0d0d">'
                        '<table style="width:100%;border-collapse:collapse;font-family:DM Sans,sans-serif;font-size:0.84rem">'
                        '<thead><tr style="background:#111">'
                        + ''.join([f'<th style="{hs};text-align:{"left" if i<2 else "right"}">{h}</th>' for i, h in enumerate(cat_hdrs)])
                        + f'</tr></thead><tbody>{cat_rows_html}</tbody></table></div>'
                    )
                    st.markdown(cat_table, unsafe_allow_html=True)

                    # ---------- FILTROS + TABLA DETALLE ----------
                    st.markdown("---")
                    st.markdown("#### Detalle por Ingrediente")

                    fc1, fc2 = st.columns(2)
                    with fc1:
                        filtro_sku  = st.text_input("🔍 Filtrar por SKU", "").strip().upper()
                    with fc2:
                        filtro_nom  = st.text_input("🔍 Filtrar por nombre", "").strip().lower()

                    df_det = pivot.copy()
                    if filtro_sku:
                        df_det = df_det[df_det['sku'].str.upper().str.contains(filtro_sku, na=False)]
                    if filtro_nom:
                        df_det = df_det[df_det['nombre'].str.lower().str.contains(filtro_nom, na=False)]

                    det_hdrs = ['SKU', 'Ingrediente', 'Cat.'] + mes_cols_str
                    det_rows_html = ''
                    for _, r in df_det.iterrows():
                        det_rows_html += '<tr style="border-bottom:1px solid #1e1e1e">'
                        det_rows_html += f'<td style="padding:10px 14px;color:#666;font-family:monospace;font-size:0.76rem">{r.get("sku","")}</td>'
                        det_rows_html += f'<td style="padding:10px 14px;font-weight:500;color:#e8e4de">{r.get("nombre","")}</td>'
                        det_rows_html += f'<td style="padding:10px 14px;color:#555;font-size:0.8rem">{r.get("subcat","")}</td>'
                        prev_val = None
                        for mc in mes_cols_str:
                            v = r.get(mc, None)
                            delta_html = ''
                            if prev_val is not None and v is not None and not pd.isna(v) and prev_val != 0:
                                delta = (v - prev_val) / prev_val * 100
                                delta_html = f'<br>{badge_delta(delta)}'
                            det_rows_html += f'<td style="padding:10px 14px;text-align:right;vertical-align:middle">{fmt_precio(v)}{delta_html}</td>'
                            prev_val = v if (v is not None and not pd.isna(v)) else prev_val
                        det_rows_html += '</tr>'

                    det_table = (
                        '<div style="overflow-x:auto;border-radius:14px;border:1px solid #1e1e1e;margin-top:0.5rem;background:#0d0d0d">'
                        '<table style="width:100%;border-collapse:collapse;font-family:DM Sans,sans-serif;font-size:0.84rem">'
                        '<thead><tr style="background:#111">'
                        + ''.join([f'<th style="{hs};text-align:{"left" if i<3 else "right"}">{h}</th>' for i, h in enumerate(det_hdrs)])
                        + f'</tr></thead><tbody>{det_rows_html}</tbody></table></div>'
                    )
                    st.markdown(det_table, unsafe_allow_html=True)

                    # Descarga
                    st.markdown("<br>", unsafe_allow_html=True)
                    buf4 = io.BytesIO()
                    with pd.ExcelWriter(buf4, engine='openpyxl') as w:
                        pivot.to_excel(w, sheet_name='Precios', index=False)
                    st.download_button("📥 Descargar Excel", buf4.getvalue(), "Informe3_Precios.xlsx")
