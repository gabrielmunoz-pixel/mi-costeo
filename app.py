import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime

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
    
    df_dir = df_directos.copy()
    df_dir = df_dir.rename(columns={
        'CODIGO VENTA': 'codigo_venta', 'Plato': 'nombre_plato',
        'SKU': 'sku_ingrediente', 'Ingrediente': 'nombre_ingrediente',
        'CantReal': 'cant_real', 'Eficiencia': 'rendimiento',
        'UM': 'um_salida', 'EsOpcion': 'es_opcion'
    })
    df_dir['es_procesado'] = False

    df_proc = df_procesados.copy()
    df_proc = df_proc.rename(columns={
        'Codigo Venta': 'codigo_venta', 'Ingrediente Proc': 'nombre_plato',
        'SKU Ingrediente': 'sku_ingrediente', 'Ingrediente': 'nombre_ingrediente',
        'CantReceta': 'cant_real', 'CantEfic': 'cant_efic',
        'UM Salida': 'um_salida', 'Eficiencia': 'rendimiento'
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
            sql_vista = """
            CREATE VIEW vista_costo_recetas AS
            WITH 
            precios_mercado AS (
                SELECT DISTINCT ON (sku) 
                    sku, 
                    muc as costo_unitario_compra
                FROM compras 
                ORDER BY sku, created_at DESC
            ),
            costo_calculado_procesados AS (
                SELECT 
                    r.codigo_venta as sku_pro,
                    SUM(r.cant_real * COALESCE(pm.costo_unitario_compra, 0)) as costo_final_producido
                FROM recetas r
                INNER JOIN precios_mercado pm ON r.sku_ingrediente = pm.sku
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
                mp.precio_ref as precio_unitario_final,
                (r.cant_real * COALESCE(mp.precio_ref, 0)) as costo_parcial_insumo
            FROM recetas r
            LEFT JOIN maestra_precios mp ON r.sku_ingrediente = mp.sku;
            """
            conn.execute(text(sql_vista))
            conn.commit()
        st.success("✅ Estructura sólida de costos reconstruida.")
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
        st.success(f"✅ Se han cargado {len(df_v)} registros correctamente.")
    except Exception as e:
        st.error(f"Error al cargar ventas: {e}")

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

    if df_v.empty or df_rec.empty:
        return pd.DataFrame()

    teorico = pd.merge(df_v, df_rec, left_on='sku_producto', right_on='codigo_venta')
    teorico['consumo_teorico'] = teorico['cantidad_vendida'] * teorico['cant_real']
    cons_teorico = teorico.groupby(['sku_ingrediente', 'nombre_ingrediente']).agg({'consumo_teorico': 'sum'}).reset_index()
    comp_real = df_c.groupby(['sku', 'subcat']).agg({'cant_conv': 'sum', 'muc': 'mean'}).reset_index()
    informe = pd.merge(cons_teorico, comp_real, left_on='sku_ingrediente', right_on='sku', how='outer').fillna(0)
    informe['desviacion_cantidad'] = informe['consumo_teorico'] - informe['cant_conv']
    informe['desviacion_dinero'] = informe['desviacion_cantidad'] * informe['muc']
    return informe

st.set_page_config(page_title="Control de Costos - Gabriel Muñoz", layout="wide")

with st.sidebar:
    st.title("📂 Navegación")
    menu_principal = st.radio("Seleccione Módulo", ["Gestión BD", "Informes"])
    st.divider()
    st.subheader("🗓️ Filtros Globales")
    f_inicio = st.date_input("Fecha Inicio", value=datetime(2025, 11, 1))
    f_fin = st.date_input("Fecha Fin")
    locales_list = ["Todos"]
    try:
        engine_temp = get_db_engine()
        locales_db = pd.read_sql("SELECT DISTINCT local FROM ventas", engine_temp)['local'].tolist()
        locales_list.extend(locales_db)
    except: pass
    f_local = st.selectbox("Local", locales_list)

if menu_principal == "Gestión BD":
    st.title("⚙️ Gestión de Base de Datos")
    sub_gestion = st.selectbox("Operación", ["Explosión MRP", "Historial de Compras", "Gestión de Recetario", "Carga Histórica de Ventas"])
    
    if sub_gestion == "Explosión MRP":
        st.subheader("📦 Explosión Rápida (MRP)")
        file_v_fast = st.file_uploader("Subir Ventas del día", type="xlsx")
        df_view = get_recetario_costeado()
        if file_v_fast and not df_view.empty:
            df_v_f = pd.read_excel(file_v_fast)
            mrp = pd.merge(df_v_f, df_view, left_on='SKU', right_on='codigo_venta')
            mrp['total_necesidad'] = mrp['Cantidad'] * mrp['cant_real']
            mrp['total_costo'] = mrp['Cantidad'] * mrp['costo_parcial_insumo']
            res = mrp.groupby(['nombre_ingrediente', 'um_salida']).agg({'total_necesidad': 'sum', 'total_costo': 'sum'}).reset_index()
            st.dataframe(res)
            st.metric("Costo Total Teórico", f"$ {res['total_costo'].sum():,.0f}")

    elif sub_gestion == "Historial de Compras":
        st.subheader("🛒 Carga de Facturas")
        file_c = st.file_uploader("Subir Excel de Compras", type="xlsx")
        if file_c and st.button("Guardar Compras"):
            save_purchases(pd.read_excel(file_c))

    elif sub_gestion == "Gestión de Recetario":
        st.subheader("📖 Master de Recetas")
        col_a, col_b = st.columns(2)
        with col_a: file_dir = st.file_uploader("Subir Directos", type="xlsx")
        with col_b: file_proc = st.file_uploader("Subir Procesados", type="xlsx")
        if file_dir and file_proc and st.button("Sincronizar Recetario"):
            save_recetas_from_excel(pd.read_excel(file_dir), pd.read_excel(file_proc))
        df_view = get_recetario_costeado()
        if not df_view.empty:
            st.dataframe(df_view, hide_index=True)

    elif sub_gestion == "Carga Histórica de Ventas":
        st.subheader("📈 Inyección de Ventas")
        file_h = st.file_uploader("Excel Ventas", type="xlsx")
        if file_h and st.button("Inyectar a Base de Datos"):
            save_ventas(pd.read_excel(file_h))

elif menu_principal == "Informes":
    st.title("📊 Módulo de Informes")
    engine = get_db_engine()
    target_sku = st.text_input("Ingrese el SKU a auditar (ej: AGR-028, AGREX-027, PRO-05)", "").strip()

    with st.expander("⚖️ Auditor de MUC (Estructural)", expanded=True):
        st.subheader("Análisis de Conversión y Costo por Unidad Mínima")
        if target_sku:
            try:
                query_muc = text("""
                    SELECT 
                        sku_ingrediente as sku, 
                        nombre_ingrediente, 
                        precio_unitario_final as costo_referencia,
                        costo_parcial_insumo as subtotal_en_receta
                    FROM vista_costo_recetas 
                    WHERE codigo_venta = :sku_target
                """)
                with engine.connect() as conn:
                    df_muc = pd.read_sql(query_muc, conn, params={"sku_target": target_sku})
                
                if not df_muc.empty:
                    st.write(f"Desglose de costos para **{target_sku}** (Incluye Procesados):")
                    st.dataframe(df_muc.style.format({'costo_referencia': '${:,.2f}', 'subtotal_en_receta': '${:,.2f}'}))
                    if df_muc['sku'].str.contains('PRO-', na=False).any():
                        st.info("💡 Este costo incluye procesados calculados por sub-receta.")
                else:
                    st.error("No se encontraron ingredientes para este SKU.")
            except Exception as e:
                st.error(f"Error de consulta: {e}. Intenta sincronizar el recetario.")

    st.markdown("---")
    with st.expander("🔍 Auditor de Costos (Rastreador de Cascada)", expanded=False):
        if target_sku:
            st.markdown(f"**Análisis de Jerarquía para: {target_sku}**")
            q_analisis = text("SELECT * FROM vista_costo_recetas WHERE codigo_venta = :sku")
            df_analisis = pd.read_sql(q_analisis, engine, params={"sku": target_sku})
            
            if df_analisis.empty:
                st.error(f"❌ El SKU '{target_sku}' no existe en recetas.")
            else:
                for _, row in df_analisis.iterrows():
                    with st.container():
                        col1, col2 = st.columns([1, 4])
                        is_pro = str(row['sku_ingrediente']).startswith('PRO-')
                        icon = "🧪" if is_pro else "📦"
                        col1.metric("Cant.", f"{row['cant_real']} {row['um_salida']}")
                        col2.markdown(f"**{icon} {row['nombre_ingrediente']}** ({row['sku_ingrediente']})")
                        if is_pro:
                            st.caption(f"↳ PROCESADO. Costo calculado: ${row['precio_unitario_final']:,.2f}")
                        elif row['precio_unitario_final'] == 0:
                            st.warning(f"⚠️ Insumo {row['sku_ingrediente']} sin facturas.")
                        else:
                            st.success(f"↳ Insumo directo. Costo MUC: ${row['precio_unitario_final']:,.2f}")
                
                st.divider()
                st.metric("COSTO TOTAL EXPLOTADO", f"${df_analisis['costo_parcial_insumo'].sum():,.2f}")

    sub_informe = st.selectbox("Seleccione Informe", ["1: Rentabilidad por Categoría/Producto", "2: Rentabilidad por Ingrediente"])
    
    if sub_informe == "1: Rentabilidad por Categoría/Producto":
        if st.button("Generar Informe 1"):
            q = """
                SELECT categoria_menu, sku_producto, nombre_producto, 
                SUM(monto_venta_real) as venta, SUM(cantidad_vendida) as cant 
                FROM ventas WHERE fecha_venta BETWEEN :i AND :f
            """
            if f_local != "Todos": q += " AND local = :l"
            q += " GROUP BY 1, 2, 3"
            df_v = pd.read_sql(text(q), engine, params={"i": f_inicio, "f": f_fin, "l": f_local})
            df_v['sku_producto'] = df_v['sku_producto'].astype(str).str.strip()
            df_rec_raw = get_recetario_costeado()
            if not df_rec_raw.empty:
                df_rec = df_rec_raw.groupby('codigo_venta')['costo_parcial_insumo'].sum().reset_index()
            else:
                df_rec = pd.DataFrame(columns=['codigo_venta', 'costo_parcial_insumo'])
            
            df_res = pd.merge(df_v, df_rec, left_on='sku_producto', right_on='codigo_venta', how='left').fillna(0)
            df_res['costo_total'] = df_res['cant'] * df_res['costo_parcial_insumo']
            df_res['rentabilidad'] = df_res['venta'] - df_res['costo_total']
            df_res['%_margen'] = df_res.apply(lambda x: (x['rentabilidad'] / x['venta'] * 100) if x['venta'] > 0 else -100, axis=1)
            
            columnas_mostrar = ['sku_producto', 'categoria_menu', 'nombre_producto', 'venta', 'costo_total', 'rentabilidad', '%_margen']
            st.dataframe(df_res[columnas_mostrar].style.format({
                'venta': '${:,.0f}', 'costo_total': '${:,.0f}', 'rentabilidad': '${:,.0f}', '%_margen': '{:.1f}%'
            }), use_container_width=True)

    elif sub_informe == "2: Rentabilidad por Ingrediente":
        if st.button("Generar Informe 2"):
            inf = get_informe_desviacion(f_inicio, f_fin, f_local)
            jerarquia = {"COCINA": ["carnes", "verduras", "alimentos", "panes"], "BAR": ["Cocteles", "Cervezas", "Vinos", "Espumantes", "Bebidas", "Moctails", "Jugos y Aguas"]}
            if not inf.empty:
                for cat_p, subcats in jerarquia.items():
                    with st.expander(f"📂 {cat_p}", expanded=True):
                        for s in subcats:
                            df_s = inf[inf['subcat'].astype(str).str.lower() == s.lower()] if 'subcat' in inf.columns else pd.DataFrame()
                            if not df_s.empty:
                                st.subheader(f"📍 {s.capitalize()}")
                                st.dataframe(df_s[['nombre_ingrediente', 'consumo_teorico', 'cant_conv', 'desviacion_dinero']])
