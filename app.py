import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text

# --- CONFIGURACIÓN DE BASE DE DATOS ---
def get_db_engine():
    try:
        db = st.secrets["connections"]["supabase"]
        # Conexión vía Transaction Pooler (Puerto 6543) para compatibilidad IPv4
        conn_str = f"postgresql://{db['user']}:{db['password']}@{db['host']}:{db['port']}/{db['database']}?sslmode=require"
        return create_engine(conn_str, pool_pre_ping=True, pool_recycle=300)
    except Exception as e:
        st.error(f"Error de conexión: {e}")
        return None

def save_purchases(df):
    engine = get_db_engine()
    # Normalización de nombres de columnas para la tabla 'compras'
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
        st.success("✅ Compras guardadas correctamente en la base de datos.")
    except Exception as e:
        st.error(f"Error al guardar compras: {e}")

def save_recetas_from_excel(df_directos, df_procesados):
    engine = get_db_engine()
    
    # 1. Adaptar Directos (Columnas: CODIGO VENTA, Plato, SKU, CantReal, Eficiencia, etc.)
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
    if 'cant_efic' not in df_dir.columns:
        df_dir['cant_efic'] = df_dir['cant_real'] * pd.to_numeric(df_dir['rendimiento'], errors='coerce').fillna(1)

    # 2. Adaptar Procesados (Columnas: Ingrediente Proc, Codigo Venta, SKU Ingrediente, CantReceta, etc.)
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

    # Unificamos para la base de datos
    df_final = pd.concat([df_dir, df_proc], ignore_index=True)
    
    columnas_db = [
        'codigo_venta', 'nombre_plato', 'sku_ingrediente', 'nombre_ingrediente', 
        'cant_real', 'rendimiento', 'cant_efic', 'um_salida', 'es_procesado', 'es_opcion'
    ]
    
    try:
        # 1. Conexión manual para limpiar dependencias
        with engine.connect() as conn:
            # Borramos la vista para que nos deje reemplazar la tabla
            conn.execute(text("DROP VIEW IF EXISTS vista_costo_recetas CASCADE;"))
            conn.commit()

        # 2. Guardamos los nuevos datos (esto recrea la tabla 'recetas')
        df_to_save = df_final[columnas_db].copy()
        df_to_save['rendimiento'] = pd.to_numeric(df_to_save['rendimiento'], errors='coerce').fillna(1)
        df_to_save.to_sql('recetas', engine, if_exists='replace', index=False)

        # 3. Recreamos la Vista de Costos (ahora con la tabla nueva)
        with engine.connect() as conn:
            sql_vista = """
            CREATE VIEW vista_costo_recetas AS
            WITH ultimo_muc AS (
                SELECT DISTINCT ON (sku) sku, muc as precio_unitario_compra
                FROM compras 
                ORDER BY sku, created_at DESC
            )
            SELECT r.*, u.precio_unitario_compra,
            (r.cant_real * u.precio_unitario_compra) as costo_parcial_insumo
            FROM recetas r
            LEFT JOIN ultimo_muc u ON r.sku_ingrediente = u.sku;
            """
            conn.execute(text(sql_vista))
            conn.commit()

        st.success("✅ Recetario y Vista de Costos actualizados correctamente.")
    except Exception as e:
        st.error(f"Error al insertar en la base de datos: {e}")

def save_ventas(df):
    engine = get_db_engine()
    
    # Mapeo de tus columnas a la base de datos
    df_v = df.copy()
    df_v = df_v.rename(columns={
        'local': 'local',
        'fecha_pura': 'fecha_venta',
        'cat_menu': 'categoria_menu',
        'nombre': 'nombre_producto',
        'id_producto': 'sku_producto',
        'cantidad': 'cantidad_vendida',
        'venta_real': 'monto_venta_real'
    })
    
    # Asegurar formato de fecha
    df_v['fecha_venta'] = pd.to_datetime(df_v['fecha_venta']).dt.date
    
    try:
        # Usamos append para ir acumulando los meses (Nov, Dic, Ene)
        df_v.to_sql('ventas', engine, if_exists='append', index=False)
        st.success(f"✅ Se han cargado {len(df_v)} registros de ventas correctamente.")
    except Exception as e:
        st.error(f"Error al cargar ventas: {e}")

def get_informe_desviacion(df_ventas, df_recetario, df_compras_periodo):
    # 1. Calculamos el Consumo Teórico (Ventas x Receta)
    # Explotamos las ventas para saber cuánto debimos usar de cada SKU ingrediente
    teorico = pd.merge(df_ventas, df_recetario, left_on='SKU', right_on='codigo_venta')
    teorico['consumo_teorico'] = teorico['Cantidad'] * teorico['cant_real']
    
    consumo_total_teorico = teorico.groupby(['sku_ingrediente', 'nombre_ingrediente']).agg({
        'consumo_teorico': 'sum'
    }).reset_index()

    # 2. Calculamos las Compras Reales del periodo
    # Sumamos todo lo que entró según las facturas en ese rango de fechas
    compras_totales = df_compras_periodo.groupby('sku').agg({
        'cant_conv': 'sum', # Usamos la cantidad convertida (ej: kg)
        'muc': 'mean'       # Precio promedio para valorizar la pérdida
    }).reset_index()

    # 3. Cruzamos ambos mundos
    informe = pd.merge(
        consumo_total_teorico, 
        compras_totales, 
        left_on='sku_ingrediente', 
        right_on='sku', 
        how='outer'
    ).fillna(0)

    # 4. Calculamos Desviaciones
    informe['desviacion_cantidad'] = informe['consumo_teorico'] - informe['cant_conv']
    informe['desviacion_dinero'] = informe['desviacion_cantidad'] * informe['muc']
    
    return informe

def get_recetario_costeado():
    engine = get_db_engine()
    query = "SELECT * FROM vista_costo_recetas"
    try:
        return pd.read_sql(query, engine)
    except:
        return pd.DataFrame()

# --- INTERFAZ STREAMLIT ---
st.set_page_config(page_title="Control de Costos - Gabriel Muñoz", layout="wide")
st.title("📊 Sistema de Inteligencia de Costos e Inventario")

t1, t2, t3 = st.tabs(["Explosión MRP", "Historial de Compras", "Gestión de Recetario"])

with t2:
    st.header("🛒 Carga de Facturas y MUC")
    file_c = st.file_uploader("Subir Excel de Compras (datosventas 2)", type="xlsx")
    if file_c and st.button("Guardar en Supabase", key="btn_compras"):
        df_c = pd.read_excel(file_c)
        save_purchases(df_c)

with t3:
    st.header("📖 Master de Recetas y Rendimientos")
    
    col_a, col_b = st.columns(2)
    with col_a:
        file_dir = st.file_uploader("1. Subir Excel Directos", type="xlsx")
    with col_b:
        file_proc = st.file_uploader("2. Subir Excel Procesados", type="xlsx")
        
    if file_dir and file_proc and st.button("Sincronizar Recetario Maestro"):
        df_directos = pd.read_excel(file_dir)
        df_procesados = pd.read_excel(file_proc)
        save_recetas_from_excel(df_directos, df_procesados)

    st.divider()
    df_view = get_recetario_costeado()
    if not df_view.empty:
        st.subheader("Visualización de Costos Reales (Base de Datos)")
        st.data_editor(
            df_view,
            column_config={
                "precio_unitario_compra": st.column_config.NumberColumn("Último MUC ($)", format="$ %d"),
                "costo_parcial_insumo": st.column_config.NumberColumn("Costo Bruto ($)", format="$ %d"),
                "rendimiento": st.column_config.NumberColumn("Rendimiento", format="%.2f")
            },
            disabled=True,
            hide_index=True
        )

with t1:
    st.header("📦 Explosión de Insumos (MRP)")
    file_v = st.file_uploader("Subir Ventas del Periodo", type="xlsx")
    if file_v and not df_view.empty:
        df_v = pd.read_excel(file_v)
        # Cruce de ventas con recetas costeadas
        mrp = pd.merge(df_v, df_view, left_on='SKU', right_on='codigo_venta')
        mrp['total_necesidad'] = mrp['Cantidad'] * mrp['cant_real']
        mrp['total_costo'] = mrp['Cantidad'] * mrp['costo_parcial_insumo']
        
        resumen_mrp = mrp.groupby(['nombre_ingrediente', 'um_salida']).agg({
            'total_necesidad': 'sum',
            'total_costo': 'sum'
        }).reset_index()
        
        st.dataframe(resumen_mrp)
        st.metric("Inversión Total Materia Prima", f"$ {resumen_mrp['total_costo'].sum():,.0f}")
