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
    # Mapeo exacto según tus nuevas columnas
    df_v = df_v.rename(columns={
        'local': 'local',
        'fecha_pura': 'fecha_venta',
        'cat_menu': 'categoria_menu',
        'nombre': 'nombre_producto',
        'id_producto': 'sku_producto',
        'cantidad': 'cantidad_vendida',
        'venta_real': 'monto_venta_real'
    })
    df_v['fecha_venta'] = pd.to_datetime(df_v['fecha_venta']).dt.date
    try:
        df_v.to_sql('ventas', engine, if_exists='append', index=False)
        st.success(f"✅ Se han cargado {len(df_v)} registros de ventas.")
    except Exception as e:
        st.error(f"Error al cargar ventas: {e}")

# --- FUNCIONES DE ANÁLISIS ---

def get_recetario_costeado():
    engine = get_db_engine()
    try:
        return pd.read_sql("SELECT * FROM vista_costo_recetas", engine)
    except:
        return pd.DataFrame()

def get_informe_desviacion(fecha_i, fecha_f):
    engine = get_db_engine()
    # 1. Obtener Ventas del periodo filtrado
    query_v = text("SELECT sku_producto, cantidad_vendida FROM ventas WHERE fecha_venta BETWEEN :i AND :f")
    df_v = pd.read_sql(query_v, engine, params={"i": fecha_i, "f": fecha_f})
    
    # 2. Obtener Recetario Maestro
    df_rec = get_recetario_costeado()
    
    # 3. Obtener Compras del periodo filtrado
    query_c = text("SELECT sku, cant_conv, muc FROM compras WHERE created_at::date BETWEEN :i AND :f")
    df_c = pd.read_sql(query_c, engine, params={"i": fecha_i, "f": fecha_f})

    if df_v.empty or df_rec.empty:
        return pd.DataFrame()

    # Explosión Teórica: Ventas x Receta
    teorico = pd.merge(df_v, df_rec, left_on='sku_producto', right_on='codigo_venta')
    teorico['consumo_teorico'] = teorico['cantidad_vendida'] * teorico['cant_real']
    cons_teorico = teorico.groupby(['sku_ingrediente', 'nombre_ingrediente']).agg({'consumo_teorico': 'sum'}).reset_index()

    # Consolidado de Compras Reales
    comp_real = df_c.groupby('sku').agg({'cant_conv': 'sum', 'muc': 'mean'}).reset_index()

    # Cruce y Cálculo de Desviación
    informe = pd.merge(cons_teorico, comp_real, left_on='sku_ingrediente', right_on='sku', how='outer').fillna(0)
    informe['desviacion_cantidad'] = informe['consumo_teorico'] - informe['cant_conv']
    informe['desviacion_dinero'] = informe['desviacion_cantidad'] * informe['muc']
    
    return informe

# --- INTERFAZ STREAMLIT ---
st.set_page_config(page_title="Control de Costos - Gabriel Muñoz", layout="wide")
st.title("📊 Sistema de Inteligencia de Costos e Inventario")

t1, t2, t3, t4 = st.tabs(["Explosión MRP", "Historial de Compras", "Gestión de Recetario", "Informes Gerenciales"])

with t2:
    st.header("🛒 Carga de Facturas")
    file_c = st.file_uploader("Subir Excel de Compras", type="xlsx")
    if file_c and st.button("Guardar Compras"):
        save_purchases(pd.read_excel(file_c))

with t3:
    st.header("📖 Master de Recetas")
    col_a, col_b = st.columns(2)
    with col_a: file_dir = st.file_uploader("Subir Directos", type="xlsx")
    with col_b: file_proc = st.file_uploader("Subir Procesados", type="xlsx")
    if file_dir and file_proc and st.button("Sincronizar Recetario"):
        save_recetas_from_excel(pd.read_excel(file_dir), pd.read_excel(file_proc))
    
    df_view = get_recetario_costeado()
    if not df_view.empty:
        st.subheader("Visualización de Costos")
        st.dataframe(df_view, hide_index=True)

with t1:
    st.header("📦 Explosión Rápida (MRP)")
    file_v_fast = st.file_uploader("Subir Ventas del día", type="xlsx")
    if file_v_fast and not df_view.empty:
        df_v_f = pd.read_excel(file_v_fast)
        mrp = pd.merge(df_v_f, df_view, left_on='SKU', right_on='codigo_venta')
        mrp['total_necesidad'] = mrp['Cantidad'] * mrp['cant_real']
        mrp['total_costo'] = mrp['Cantidad'] * mrp['costo_parcial_insumo']
        res = mrp.groupby(['nombre_ingrediente', 'um_salida']).agg({'total_necesidad': 'sum', 'total_costo': 'sum'}).reset_index()
        st.dataframe(res)
        st.metric("Costo Total Teórico", f"$ {res['total_costo'].sum():,.0f}")

with t4:
    st.header("📉 Dashboard de Decisiones")
    sub = st.selectbox("Acción", ["Carga Histórica de Ventas", "Análisis de Desviación"])
    
    if sub == "Carga Histórica de Ventas":
        file_h = st.file_uploader("Excel Ventas (Nov-Dic-Ene)", type="xlsx")
        if file_h and st.button("Inyectar a Base de Datos"):
            save_ventas(pd.read_excel(file_h))
            
    elif sub == "Análisis de Desviación":
        c1, c2 = st.columns(2)
        f_i = c1.date_input("Inicio", value=datetime(2025, 11, 1))
        f_f = c2.date_input("Fin")
        
        if st.button("Generar Informe"):
            inf = get_informe_desviacion(f_i, f_f)
            if not inf.empty:
                # KPIs Maestros
                perdida_total = inf[inf['desviacion_dinero'] < 0]['desviacion_dinero'].sum()
                st.metric("Pérdida por Desviación (Fuga)", f"$ {abs(perdida_total):,.0f}", delta_color="inverse")
                
                # Gráfico Pareto de Pérdidas
                st.subheader("Top 10 Insumos con Mayor Desviación ($)")
                top_perdidas = inf[inf['desviacion_dinero'] < 0].sort_values('desviacion_dinero').head(10)
                top_perdidas['desviacion_dinero'] = top_perdidas['desviacion_dinero'].abs()
                st.bar_chart(top_perdidas.set_index('nombre_ingrediente')['desviacion_dinero'])
                
                # Tabla Detallada
                st.subheader("Detalle de Conciliación")
                st.dataframe(inf.style.highlight_min(subset=['desviacion_dinero'], color='lightcoral'))
            else:
                st.warning("No hay datos en el rango seleccionado.")
