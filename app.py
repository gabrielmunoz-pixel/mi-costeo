import streamlit as st
import pandas as pd
from sqlalchemy import create_engine

# --- CONFIGURACIÓN DE BASE DE DATOS ---
def get_db_engine():
    try:
        db = st.secrets["connections"]["supabase"]
        # Usamos sslmode=require y el puerto 6543 para el Transaction Pooler (IPv4 compatible)
        conn_str = f"postgresql://{db['user']}:{db['password']}@{db['host']}:{db['port']}/{db['database']}?sslmode=require"
        return create_engine(conn_str, pool_pre_ping=True, pool_recycle=300)
    except Exception as e:
        st.error(f"Error de conexión: {e}")
        return None

def save_purchases(df):
    engine = get_db_engine()
    # Limpieza de nombres para que coincidan con la tabla 'compras'
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
        st.success("✅ Compras guardadas correctamente en Supabase.")
    except Exception as e:
        st.error(f"Error al guardar compras: {e}")

# --- NUEVA FUNCIÓN DE RECETAS CON RENDIMIENTO ---
def save_recetas_from_excel(df_directos, df_procesados):
    engine = get_db_engine()
    
    # 1. Adaptar Directos
    # Columnas: CODIGO VENTA, Plato, Ingrediente, SKU, EsOpcion, Cantidad, CantReal, UM, Proc, Precio, Eficiencia, Costo, Tipo
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
    # Calculamos CantEfic si no viene explícita en directos
    if 'cant_efic' not in df_dir.columns:
        df_dir['cant_efic'] = df_dir['cant_real'] * df_dir['rendimiento'].fillna(1)
    df_dir['es_procesado'] = False

    # 2. Adaptar Procesados
    # Columnas: Ingrediente Proc, Codigo Venta, Ingrediente, SKU Ingrediente, Precio, UM, Eficiencia, CantEfic, UM Salida, Porcion, CantReceta, Costo Total
    df_proc = df_procesados.copy()
    df_proc = df_proc.rename(columns={
        'Codigo Venta': 'codigo_venta',
        'Ingrediente Proc': 'nombre_plato',
        'SKU Ingrediente': 'sku_ingrediente',
        'Ingrediente': 'nombre_ingrediente',
        'CantReceta': 'cant_real',      # En tus procesados, CantReceta es la base
        'CantEfic': 'cant_efic',
        'UM Salida': 'um_salida',
        'Eficiencia': 'rendimiento'
    })
    df_proc['es_procesado'] = True
    df_proc['es_opcion'] = 0 # Valor por defecto para procesados

    # Unificamos para la base de datos
    df_final = pd.concat([df_dir, df_proc], ignore_index=True)
    
    # Columnas exactas que espera tu tabla SQL 'recetas'
    columnas_db = [
        'codigo_venta', 'nombre_plato', 'sku_ingrediente', 'nombre_ingrediente', 
        'cant_real', 'rendimiento', 'cant_efic', 'um_salida', 'es_procesado', 'es_opcion'
    ]
    
    try:
        # Filtramos solo las columnas necesarias y manejamos valores nulos
        df_to_save = df_final[columnas_db].copy()
        df_to_save['rendimiento'] = pd.to_numeric(df_to_save['rendimiento'], errors='coerce').fillna(1)
        
        # Guardamos en Supabase (if_exists='replace' para mantener el maestro actualizado)
        df_to_save.to_sql('recetas', engine, if_exists='replace', index=False)
        st.success("✅ Recetario unificado y cargado correctamente.")
    except Exception as e:
        st.error(f"Error al insertar en la base de datos: {e}")

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
    if file_c and st.button("Guardar en Supabase"):
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
        st.subheader("Editor y Visualizador de Costos Reales")
        st.data_editor(
            df_view,
            column_config={
                "precio_unitario_compra": st.column_config.NumberColumn("Último MUC ($)", format="$ %d"),
                "costo_parcial_insumo": st.column_config.NumberColumn("Costo Bruto ($)", format="$ %d"),
                "rendimiento": st.column_config.ProgressColumn("Rendimiento", min_value=0, max_value=1)
            },
            disabled=["precio_unitario_compra", "costo_parcial_insumo"]
        )

with t1:
    st.header("📦 Explosión de Insumos (MRP)")
    file_v = st.file_uploader("Subir Ventas del Periodo", type="xlsx")
    if file_v and not df_view.empty:
        df_v = pd.read_excel(file_v)
        # Lógica de explosión cruzando ventas con la vista de recetas
        mrp = pd.merge(df_v, df_view, left_on='SKU', right_on='codigo_venta')
        mrp['total_necesidad'] = mrp['Cantidad'] * mrp['cant_real']
        mrp['total_costo'] = mrp['Cantidad'] * mrp['costo_parcial_insumo']
        
        resumen_mrp = mrp.groupby(['nombre_ingrediente', 'um_salida']).agg({
            'total_necesidad': 'sum',
            'total_costo': 'sum'
        }).reset_index()
        
        st.dataframe(resumen_mrp)
        st.metric("Inversión Total Materia Prima", f"$ {resumen_mrp['total_costo'].sum():,.0f}")
