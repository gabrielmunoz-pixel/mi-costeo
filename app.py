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
    
    # 1. Adaptar Directos
    df_dir = df_directos.copy()
    df_dir = df_dir.rename(columns={
        'CODIGO VENTA': 'codigo_venta',
        'Plato': 'nombre_plato',
        'SKU': 'sku_ingrediente',
        'Ingrediente': 'nombre_ingrediente',
        'CantReal': 'cant_real',
        'Eficiencia': 'rendimiento',
        'UM': 'um_salida',
        'EsOpcion': 'es_opcion'
    })
    df_dir['es_procesado'] = False

    # 2. Adaptar Procesados
    df_proc = df_procesados.copy()
    df_proc = df_proc.rename(columns={
        'Codigo Venta': 'codigo_venta',
        'Ingrediente Proc': 'nombre_plato',
        'SKU Ingrediente': 'sku_ingrediente',
        'Ingrediente': 'nombre_ingrediente',
        'CantReceta': 'cant_real',
        'CantEfic': 'cant_efic',
        'UM Salida': 'um_salida',
        'Eficiencia': 'rendimiento'
    })
    df_proc['es_procesado'] = True
    df_proc['es_opcion'] = 0

    df_final = pd.concat([df_dir, df_proc], ignore_index=True)
    columnas_db = ['codigo_venta', 'nombre_plato', 'sku_ingrediente', 'nombre_ingrediente', 
                   'cant_real', 'rendimiento', 'cant_efic', 'um_salida', 'es_procesado', 'es_opcion']
    
    try:
        with engine.connect() as conn:
            conn.execute(text("DROP VIEW IF EXISTS vista_costo_recetas CASCADE;"))
            conn.commit()

        df_to_save = df_final[columnas_db].copy()
        df_to_save['rendimiento'] = pd.to_numeric(df_to_save['rendimiento'], errors='coerce').fillna(1)
        df_to_save.to_sql('recetas', engine, if_exists='replace', index=False)

        with engine.connect() as conn:
            sql_vista = """
            CREATE VIEW vista_costo_recetas AS
            WITH ultimo_muc AS (
                SELECT DISTINCT ON (sku) sku, muc as precio_unitario_compra
                FROM compras ORDER BY sku, created_at DESC
            )
            SELECT r.*, u.precio_unitario_compra,
            (r.cant_real * u.precio_unitario_compra) as costo_parcial_insumo
            FROM recetas r
            LEFT JOIN ultimo_muc u ON r.sku_ingrediente = u.sku;
            """
            conn.execute(text(sql_vista))
            conn.commit()
        st.success("✅ Recetario y Vista de Costos actualizados.")
    except Exception as e:
        st.error(f"Error en base de datos: {e}")

def save_ventas(df):
    engine = get_db_engine()
    df_v = df.copy()
    
    # 1. Limpieza de nombres de columnas (por si acaso)
    df_v.columns = df_v.columns.str.strip().str.lower()

    # 2. CONVERSIÓN DE FECHA (Solución al error 22008)
    # 'dayfirst=True' le dice a Python que el 13 es el día y no el mes.
    # 'errors=coerce' convierte lo que no sea fecha en NaT (nulo) para no romper el proceso.
    df_v['fecha_pura'] = pd.to_datetime(df_v['fecha_pura'], dayfirst=True, errors='coerce')

    # 3. Eliminar filas que quedaron vacías o con fechas imposibles
    df_v = df_v.dropna(subset=['fecha_pura'])

    # 4. Mapeo a los nombres de tu tabla en Supabase
    df_v = df_v.rename(columns={
        'local': 'local',
        'fecha_pura': 'fecha_venta',
        'cat_menu': 'categoria_menu',
        'nombre': 'nombre_producto',
        'id_producto': 'sku_producto',
        'cantidad': 'cantidad_vendida',
        'venta_real': 'monto_venta_real'
    })
    
    # 5. Asegurar que solo enviamos la fecha (sin horas)
    df_v['fecha_venta'] = df_v['fecha_venta'].dt.date

    try:
        # Usamos 'method=multi' para que la carga sea más eficiente en PostgreSQL
        df_v.to_sql('ventas', engine, if_exists='append', index=False, method='multi')
        st.success(f"✅ Se han cargado {len(df_v)} registros correctamente.")
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
    
    # Query de Ventas con filtro de Local
    q_v = "SELECT sku_producto, cantidad_vendida FROM ventas WHERE fecha_venta BETWEEN :i AND :f"
    if local != "Todos": q_v += " AND local = :l"
    df_v = pd.read_sql(text(q_v), engine, params={"i": fecha_i, "f": fecha_f, "l": local})
    
    df_rec = get_recetario_costeado()
    
    # Query de Compras con filtro de Local
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

# --- INTERFAZ STREAMLIT ---
st.set_page_config(page_title="Control de Costos - Gabriel Muñoz", layout="wide")

# MENÚ LATERAL (SIDEBAR)
with st.sidebar:
    st.title("📂 Navegación")
    menu_principal = st.radio("Seleccione Módulo", ["Gestión BD", "Informes"])
    
    st.divider()
    st.subheader("🗓️ Filtros Globales")
    f_inicio = st.date_input("Fecha Inicio", value=datetime(2025, 11, 1))
    f_fin = st.date_input("Fecha Fin")
    
    # Obtención dinámica de locales
    locales_list = ["Todos"]
    try:
        engine_temp = get_db_engine()
        locales_db = pd.read_sql("SELECT DISTINCT local FROM ventas", engine_temp)['local'].tolist()
        locales_list.extend(locales_db)
    except: pass
    f_local = st.selectbox("Local", locales_list)

# --- CONTENIDO PRINCIPAL ---
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
    sub_informe = st.selectbox("Seleccione Informe", ["1: Rentabilidad por Categoría/Producto", "2: Rentabilidad por Ingrediente"])
    
    if sub_informe == "1: Rentabilidad por Categoría/Producto":
        if st.button("Generar Informe 1"):
            engine = get_db_engine()
            q = "SELECT categoria_menu, nombre_producto, sku_producto, SUM(monto_venta_real) as venta, SUM(cantidad_vendida) as cant FROM ventas WHERE fecha_venta BETWEEN :i AND :f"
            if f_local != "Todos": q += " AND local = :l"
            q += " GROUP BY 1, 2, 3"
            
            df_v = pd.read_sql(text(q), engine, params={"i": f_inicio, "f": f_fin, "l": f_local})
            df_rec = get_recetario_costeado().groupby('codigo_venta')['costo_parcial_insumo'].sum().reset_index()
            
            df_res = pd.merge(df_v, df_rec, left_on='sku_producto', right_on='codigo_venta', how='left')
            df_res['costo_total'] = df_res['cant'] * df_res['costo_parcial_insumo']
            df_res['rentabilidad'] = df_res['venta'] - df_res['costo_total']
            df_res['%_margen'] = (df_res['rentabilidad'] / df_res['venta']) * 100
            
            def color_semaforo(val):
                color = 'green' if val > 65 else 'orange' if val > 50 else 'red'
                return f'background-color: {color}'

            st.dataframe(df_res[['categoria_menu', 'nombre_producto', 'venta', 'costo_total', 'rentabilidad', '%_margen']]
                         .style.applymap(color_semaforo, subset=['%_margen'])
                         .format({'venta': '${:,.0f}', 'costo_total': '${:,.0f}', 'rentabilidad': '${:,.0f}', '%_margen': '{:.1f}%'}))

    elif sub_informe == "2: Rentabilidad por Ingrediente":
        if st.button("Generar Informe 2"):
            inf = get_informe_desviacion(f_inicio, f_fin, f_local)
            
            jerarquia = {
                "COCINA": ["carnes", "verduras", "alimentos", "panes"],
                "BAR": ["Cocteles", "Cervezas", "Vinos", "Espumantes", "Bebidas", "Moctails", "Jugos y Aguas"]
            }
            
            for cat_p, subcats in jerarquia.items():
                with st.expander(f"📂 {cat_p}", expanded=True):
                    for s in subcats:
                        st.subheader(f"📍 {s.capitalize()}")
                       df_s = inf[inf['subcat'].astype(str).str.lower() == s.lower()] if 'subcat' in inf.columns else pd.DataFrame()
                        if not df_s.empty:
                            st.dataframe(df_s[['nombre_ingrediente', 'consumo_teorico', 'cant_conv', 'desviacion_dinero']])
                        else:
                            st.caption(f"No se encontraron datos para {s}")
