import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime
import io

# --- CONFIGURACIÓN DE BASE DE DATOS ---
def get_db_engine():
    try:
        db = st.secrets["connections"]["supabase"]
        conn_str = f"postgresql://{db['user']}:{db['password']}@{db['host']}:{db['port']}/{db['database']}?sslmode=require"
        return create_engine(conn_str, pool_pre_ping=True, pool_recycle=300)
    except Exception as e:
        st.error(f"Error de conexión: {e}")
        return None

# --- FUNCIONES DE PERSISTENCIA (GUARDADO) ---

def save_purchases(df):
    engine = get_db_engine()
    # Limpieza de nombres de columna para coincidir con DB
    df.columns = df.columns.str.strip().str.lower()
    df.columns = [col.replace('suma de ', '').replace(' ', '_').replace('(', '').replace(')', '') 
                  for col in df.columns]
    
    columnas_db_compras = [
        'local', 'fecha_dte', 'rut_proveedor', 'nombre_proveedor', 'tipo_dte', 
        'folio', 'nombre_producto', 'sku', 'subcat', 'codigo_impuesto', 
        'cantidad', 'conversion', 'formato', 'categoria_producto', 
        'cant_conv', 'monto_real', 'recargo2', 'total_neto2', 
        'imp_adic', 'iva_2', 'tootal2', 'costo_realfinal', 'muc'
    ]
    
    try:
        df_final = df[columnas_db_compras]
        df_final.to_sql('compras', engine, if_exists='append', index=False)
        st.success("✅ Compras guardadas correctamente.")
    except Exception as e:
        st.error(f"Error al guardar compras: {e}")

def save_recetas_from_excel(df_directos, df_procesados):
    engine = get_db_engine()
    
    # 1. Preparar Directos (Columnas: CODIGO VENTA, Plato, Ingrediente, SKU, CantReal, Eficiencia, UM)
    df_dir = df_directos.copy()
    df_dir = df_dir.rename(columns={
        'CODIGO VENTA': 'codigo_venta', 'Plato': 'nombre_plato',
        'SKU': 'sku_ingrediente', 'Ingrediente': 'nombre_ingrediente',
        'CantReal': 'cant_calculo', 'Eficiencia': 'rendimiento',
        'UM': 'um_salida', 'EsOpcion': 'es_opcion'
    })
    df_dir['es_procesado'] = False
    df_dir['porcion_marcador'] = 0 
    df_dir['volumen_receta_lote'] = 0 

    # 2. Preparar Procesados (Columnas: Ingrediente Proc, Codigo Venta, SKU Ingrediente, CantEfic, Porcion, CantReceta)
    df_proc = df_procesados.copy()
    df_proc = df_proc.rename(columns={
        'Codigo Venta': 'codigo_venta', 'Ingrediente Proc': 'nombre_plato',
        'SKU Ingrediente': 'sku_ingrediente', 'Ingrediente': 'nombre_ingrediente',
        'CantEfic': 'cant_calculo', 'CantReceta': 'volumen_receta_lote',
        'UM Salida': 'um_salida', 'Eficiencia': 'rendimiento',
        'Porcion': 'porcion_marcador'
    })
    df_proc['es_procesado'] = True
    df_proc['es_opcion'] = 0

    df_final = pd.concat([df_dir, df_proc], ignore_index=True)
    
    try:
        with engine.connect() as conn:
            conn.execute(text("DROP VIEW IF EXISTS vista_costo_recetas CASCADE;"))
            conn.commit()

        df_final['codigo_venta'] = df_final['codigo_venta'].astype(str).str.strip()
        df_final['sku_ingrediente'] = df_final['sku_ingrediente'].astype(str).str.strip()
        df_final.to_sql('recetas', engine, if_exists='replace', index=False)

        with engine.connect() as conn:
            # LÓGICA DE EXPLOSIÓN MRP: Lote (0) vs Porción (1)
            sql_vista = """
            CREATE VIEW vista_costo_recetas AS
            WITH 
            precios_mercado AS (
                SELECT DISTINCT ON (sku) sku, muc as costo_unitario_compra
                FROM compras ORDER BY sku, created_at DESC
            ),
            rendimientos_procesados AS (
                SELECT codigo_venta as sku_pro, SUM(cant_calculo) as volumen_neto_total
                FROM recetas WHERE es_procesado = true GROUP BY codigo_venta
            ),
            costo_calculado_procesados AS (
                SELECT 
                    r.codigo_venta as sku_pro,
                    SUM(
                        CASE 
                            WHEN r.porcion_marcador = 1 THEN (r.cant_calculo * COALESCE(pm.costo_unitario_compra, 0))
                            ELSE ( (r.cant_calculo / NULLIF(rp.volumen_neto_total, 0)) * COALESCE(pm.costo_unitario_compra, 0) )
                        END
                    ) as costo_final_producido
                FROM recetas r
                INNER JOIN precios_mercado pm ON r.sku_ingrediente = pm.sku
                LEFT JOIN rendimientos_procesados rp ON r.codigo_venta = rp.sku_pro
                WHERE r.es_procesado = true
                GROUP BY r.codigo_venta
            ),
            maestra_precios AS (
                SELECT sku, costo_unitario_compra as precio_ref FROM precios_mercado
                UNION ALL
                SELECT sku_pro, costo_final_producido FROM costo_calculado_procesados
                WHERE sku_pro NOT IN (SELECT sku FROM precios_mercado)
            )
            SELECT 
                r.*, 
                COALESCE(mp.precio_ref, 0) as precio_unitario_final,
                CASE 
                    WHEN r.es_procesado = true THEN
                        CASE 
                            WHEN r.porcion_marcador = 1 THEN (r.cant_calculo * COALESCE(mp.precio_ref, 0))
                            ELSE ( (r.cant_calculo / NULLIF((SELECT volumen_neto_total FROM rendimientos_procesados WHERE sku_pro = r.codigo_venta), 0)) * COALESCE(mp.precio_ref, 0) )
                        END
                    ELSE (r.cant_calculo * COALESCE(mp.precio_ref, 0))
                END as costo_parcial_insumo
            FROM recetas r
            LEFT JOIN maestra_precios mp ON r.sku_ingrediente = mp.sku;
            """
            conn.execute(text(sql_vista))
            conn.commit()
        st.success("✅ Estructura reconstruida con éxito.")
    except Exception as e:
        st.error(f"Error en SQL: {e}")

def save_ventas(df):
    engine = get_db_engine()
    df_v = df.copy()
    df_v.columns = df_v.columns.str.strip().str.lower()
    df_v['fecha_pura'] = pd.to_datetime(df_v['fecha_pura'], dayfirst=True, errors='coerce')
    df_v = df_v.dropna(subset=['fecha_pura'])
    df_v = df_v.rename(columns={
        'local': 'local', 'fecha_pura': 'fecha_venta',
        'cat_menu': 'categoria_menu', 'nombre': 'nombre_producto',
        'id_producto': 'sku_producto', 'cantidad': 'cantidad_vendida',
        'venta_real': 'monto_venta_real'
    })
    df_v['fecha_venta'] = df_v['fecha_venta'].dt.date
    df_v['sku_producto'] = df_v['sku_producto'].astype(str).str.strip()
    try:
        df_v.to_sql('ventas', engine, if_exists='append', index=False, method='multi')
        st.success(f"✅ Se han cargado {len(df_v)} registros.")
    except Exception as e:
        st.error(f"Error al cargar ventas: {e}")

# --- FUNCIONES DE ANÁLISIS ---

def get_recetario_costeado():
    engine = get_db_engine()
    try:
        return pd.read_sql("SELECT * FROM vista_costo_recetas", engine)
    except:
        return pd.DataFrame()

def get_informe_desviacion(fecha_i, fecha_f, local="Todos"):
    engine = get_db_engine()
    q_v = "SELECT sku_producto, cantidad_vendida FROM ventas WHERE fecha_venta BETWEEN :i AND :f"
    if local != "Todos": q_v += " AND local = :l"
    df_v = pd.read_sql(text(q_v), engine, params={"i": fecha_i, "f": fecha_f, "l": local})
    df_rec = get_recetario_costeado()
    q_c = "SELECT sku, cant_conv, muc, subcat FROM compras WHERE created_at::date BETWEEN :i AND :f"
    if local != "Todos": q_c += " AND local = :l"
    df_c = pd.read_sql(text(q_c), engine, params={"i": fecha_i, "f": fecha_f, "l": local})
    if df_v.empty or df_rec.empty: return pd.DataFrame()
    teorico = pd.merge(df_v, df_rec, left_on='sku_producto', right_on='codigo_venta')
    teorico['consumo_teorico'] = teorico['cantidad_vendida'] * teorico['cant_calculo']
    cons_teorico = teorico.groupby(['sku_ingrediente', 'nombre_ingrediente']).agg({'consumo_teorico': 'sum'}).reset_index()
    comp_real = df_c.groupby(['sku', 'subcat']).agg({'cant_conv': 'sum', 'muc': 'mean'}).reset_index()
    informe = pd.merge(cons_teorico, comp_real, left_on='sku_ingrediente', right_on='sku', how='outer').fillna(0)
    informe['desviacion_cantidad'] = informe['consumo_teorico'] - informe['cant_conv']
    informe['desviacion_dinero'] = informe['desviacion_cantidad'] * informe['muc']
    return informe

# --- INTERFAZ STREAMLIT ---
st.set_page_config(page_title="Control de Costos - Gabriel Muñoz", layout="wide")

with st.sidebar:
    st.title("📂 Navegación")
    menu_principal = st.radio("Módulo", ["Gestión BD", "Informes"])
    st.divider()
    st.subheader("🗓️ Filtros Globales")
    f_inicio = st.date_input("Inicio", value=datetime(2025, 11, 1))
    f_fin = st.date_input("Fin")
    locales_list = ["Todos"]
    try:
        locales_db = pd.read_sql("SELECT DISTINCT local FROM ventas", get_db_engine())['local'].tolist()
        locales_list.extend(locales_db)
    except: pass
    f_local = st.selectbox("Local", locales_list)

if menu_principal == "Gestión BD":
    st.title("⚙️ Gestión de Base de Datos")
    sub = st.selectbox("Operación", ["Explosión MRP", "Historial de Compras", "Gestión de Recetario", "Carga Ventas"])
    
    if sub == "Explosión MRP":
        st.subheader("📦 Explosión Rápida (MRP)")
        file_v_fast = st.file_uploader("Ventas del día", type="xlsx")
        df_view = get_recetario_costeado()
        if file_v_fast and not df_view.empty:
            df_v_f = pd.read_excel(file_v_fast)
            mrp = pd.merge(df_v_f, df_view, left_on='SKU', right_on='codigo_venta')
            mrp['total_necesidad'] = mrp['Cantidad'] * mrp['cant_calculo']
            mrp['total_costo'] = mrp['Cantidad'] * mrp['costo_parcial_insumo']
            res = mrp.groupby(['nombre_ingrediente', 'um_salida']).agg({'total_necesidad': 'sum', 'total_costo': 'sum'}).reset_index()
            st.dataframe(res)
            st.metric("Costo Total Teórico", f"$ {res['total_costo'].sum():,.0f}")

    elif sub == "Gestión de Recetario":
        st.subheader("📖 Master de Recetas")
        col1, col2 = st.columns(2)
        with col1: f_dir = st.file_uploader("Directos", type="xlsx")
        with col2: f_proc = st.file_uploader("Procesados", type="xlsx")
        if f_dir and f_proc and st.button("Sincronizar"):
            save_recetas_from_excel(pd.read_excel(f_dir), pd.read_excel(f_proc))
        df_v = get_recetario_costeado()
        if not df_v.empty: st.dataframe(df_v, hide_index=True)

    elif sub == "Historial de Compras":
        st.subheader("🛒 Carga de Facturas")
        file_c = st.file_uploader("Subir Excel de Compras", type="xlsx")
        if file_c and st.button("Guardar Compras"):
            save_purchases(pd.read_excel(file_c))

    elif sub == "Carga Ventas":
        st.subheader("📈 Inyección de Ventas")
        file_h = st.file_uploader("Excel Ventas", type="xlsx")
        if file_h and st.button("Inyectar"):
            save_ventas(pd.read_excel(file_h))

elif menu_principal == "Informes":
    st.title("📊 Módulo de Informes")
    target_sku = st.text_input("Ingrese SKU a auditar (ej: AGR-028, AGREX-027, PRO-05)", "").strip()
    engine = get_db_engine()

    if target_sku:
        # --- 1. AUDITOR DE MUC ---
        with st.expander("⚖️ Auditor de MUC (Integrado)", expanded=True):
            query = text("""
                SELECT 
                    sku_ingrediente as sku, 
                    nombre_ingrediente, 
                    precio_unitario_final, 
                    costo_parcial_insumo 
                FROM vista_costo_recetas 
                WHERE codigo_venta = :t
            """)
            try:
                df_muc = pd.read_sql(query, engine, params={"t": target_sku})
                if not df_muc.empty:
                    st.dataframe(df_muc)
                else:
                    st.warning("SKU no encontrado en la vista de costos.")
            except Exception as e:
                st.error(f"Error en Auditor de MUC: {e}")

        # --- 2. RASTREADOR DE CASCADA ---
        with st.expander("🔍 Auditor de Costos (Rastreador de Cascada)", expanded=True):
            df_c = pd.read_sql(text("SELECT * FROM vista_costo_recetas WHERE codigo_venta = :t"), engine, params={"t": target_sku})
            if not df_c.empty:
                for _, r in df_c.iterrows():
                    with st.container():
                        col1, col2 = st.columns([1, 4])
                        is_pro = str(r['sku_ingrediente']).startswith('PRO-')
                        col1.metric("Cant.", f"{r['cant_calculo']} {r['um_salida']}")
                        col2.markdown(f"**{'🧪' if is_pro else '📦'} {r['nombre_ingrediente']}** ({r['sku_ingrediente']})")
                        st.caption(f"↳ Costo Unitario Ref: ${r['precio_unitario_final']:,.2f}")
                st.divider()
                st.metric("COSTO TOTAL EXPLOTADO", f"${df_c['costo_parcial_insumo'].sum():,.2f}")

        # --- 3. DIAGNÓSTICO TÉCNICO ---
        with st.expander("🔍 Diagnóstico Técnico vista_costo_recetas", expanded=False):
            st.write("Datos Crudos de la Vista:")
            st.dataframe(pd.read_sql(text("SELECT * FROM vista_costo_recetas WHERE codigo_venta = :t"), engine, params={"t": target_sku}))

    # --- INFORMES GENERALES ---
    sub_inf = st.selectbox("Seleccione Informe", ["1: Rentabilidad por Producto", "2: Rentabilidad por Ingrediente"])
    
    if st.button("Generar Informe"):
        if sub_inf == "1: Rentabilidad por Producto":
            q = "SELECT categoria_menu, sku_producto, nombre_producto, SUM(monto_venta_real) as venta, SUM(cantidad_vendida) as cant FROM ventas WHERE fecha_venta BETWEEN :i AND :f GROUP BY 1, 2, 3"
            df_v = pd.read_sql(text(q), engine, params={"i": f_inicio, "f": f_fin})
            df_rec_costos = get_recetario_costeado().groupby('codigo_venta')['costo_parcial_insumo'].sum().reset_index()
            
            res = pd.merge(df_v, df_rec_costos, left_on='sku_producto', right_on='codigo_venta', how='left').fillna(0)
            res['costo_total'] = res['cant'] * res['costo_parcial_insumo']
            res['rentabilidad'] = res['venta'] - res['costo_total']
            res['%_margen'] = res.apply(lambda x: (x['rentabilidad'] / x['venta'] * 100) if x['venta'] > 0 else 0, axis=1)
            
            def semaforo(v):
                c = 'green' if v > 65 else 'orange' if v > 50 else 'red'
                return f'background-color: {c}'
            
            st.dataframe(res.style.applymap(semaforo, subset=['%_margen']).format({
                'venta': '${:,.0f}', 
                'costo_total': '${:,.0f}', 
                'rentabilidad': '${:,.0f}', 
                '%_margen': '{:.1f}%'
            }))

        elif sub_inf == "2: Rentabilidad por Ingrediente":
            inf = get_informe_desviacion(f_inicio, f_fin, f_local)
            if not inf.empty:
                st.dataframe(inf[['nombre_ingrediente', 'consumo_teorico', 'cant_conv', 'desviacion_dinero']].style.format({
                    'consumo_teorico': '{:.2f}',
                    'cant_conv': '{:.2f}',
                    'desviacion_dinero': '${:,.0f}'
                }))
