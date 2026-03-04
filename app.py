import streamlit as st
import pandas as pd
import io
from sqlalchemy import create_engine

# --- CONFIGURACIÓN DE CONEXIÓN ---
def get_db_engine():
    try:
        db = st.secrets["connections"]["supabase"]
        # Agregamos sslmode=require para asegurar la compatibilidad con Supabase
        conn_str = f"postgresql://{db['user']}:{db['password']}@{db['host']}:{db['port']}/{db['database']}?sslmode=require"
        
        # Agregamos pool_pre_ping para reconectar si la sesión se cae
        return create_engine(conn_str, pool_pre_ping=True)
    except Exception as e:
        st.error(f"Error al configurar el motor: {e}")
        return None

# Función para guardar compras
def save_purchases(df):
    engine = get_db_engine()
    # Limpiamos nombres de columnas para que coincidan con SQL (minúsculas y sin espacios)
    df.columns = df.columns.str.lower().str.replace(' ', '_').str.replace('(', '').str.replace(')', '')
    df.to_sql('compras', engine, if_exists='append', index=False)
    st.success("✅ Base de datos de compras actualizada en Supabase.")

# Función para extraer el MUC más reciente
def get_muc_data():
    engine = get_db_engine()
    # Buscamos el último MUC registrado por cada SKU
    query = """
    SELECT DISTINCT ON (sku) 
           sku, 
           muc as muc_registrado,
           costo_realfinal,
           cant_conv,
           formato,
           nombre_proveedor
    FROM compras 
    ORDER BY sku, created_at DESC
    """
    try:
        df = pd.read_sql(query, engine)
        # Si el MUC viene nulo en la tabla, lo calculamos en tiempo real
        df['muc_final'] = df.apply(
            lambda r: r['muc_registrado'] if pd.notna(r['muc_registrado']) 
            else (r['costo_realfinal'] / (r['cant_conv'] * r['formato']) if (r['cant_conv'] * r['formato']) > 0 else 0),
            axis=1
        )
        return dict(zip(df['sku'], df['muc_final']))
    except:
        return {}

# --- PROCESAMIENTO MRP (Tu lógica original blindada) ---
def process_bom(df_v, df_d, df_p, muc_map):
    for df in [df_v, df_d, df_p]:
        df.columns = df.columns.str.strip()

    # Actualización de precios desde Supabase
    if muc_map:
        df_d['Precio'] = df_d['SKU'].map(muc_map).fillna(df_d['Precio'])
        df_p['Precio'] = df_p['SKU Ingrediente'].map(muc_map).fillna(df_p['Precio'])

    # Lógica de Ventas
    df_v = df_v.rename(columns={'SKU': 'SKU_VENTA', 'Cantidad': 'CANT_VENTA'})
    skus_vendidos = set(df_v['SKU_VENTA'].astype(str).str.strip().str.upper())

    def validar_opcion(row):
        es_op = str(row['EsOpcion']).strip()
        if pd.isna(row['EsOpcion']) or es_op in ["", "0", "4"]: return True
        return str(row['SKU']).strip().upper() in skus_vendidos

    m1 = pd.merge(df_v, df_d[df_d.apply(validar_opcion, axis=1)], left_on='SKU_VENTA', right_on='CODIGO VENTA')

    # Explosión Procesados
    es_proc = m1['SKU'].str.startswith('PRO-', na=False)
    if not m1[es_proc].empty:
        rendimientos = df_p.groupby('Codigo Venta')['CantReceta'].sum().reset_index().rename(columns={'CantReceta': 'TOTAL_RECETA_AUTO'})
        df_p_clean = df_p.rename(columns={'Codigo Venta':'COD_P', 'Ingrediente':'NOM_P', 'CantEfic':'CE_P', 'Porcion':'MARK_P', 'UM Salida':'UM_P', 'SKU Ingrediente':'SKU_P'})
        df_p_final = pd.merge(df_p_clean, rendimientos, left_on='COD_P', right_on='Codigo Venta')
        
        m2 = pd.merge(m1[es_proc], df_p_final, left_on='SKU', right_on='COD_P')
        
        def calc_m2(row):
            divisor = row['TOTAL_RECETA_AUTO'] if row['TOTAL_RECETA_AUTO'] > 0 else 1
            if row['MARK_P'] == 1: return row['CANT_VENTA'] * row['CantReal'] * row['CE_P']
            return row['CANT_VENTA'] * row['CantReal'] * (row['CE_P'] / divisor)

        m2['CANT_OUT'] = m2.apply(calc_m2, axis=1)
        m2['COSTO_P'] = m2['CANT_OUT'] * m2['Precio']
        exp_f = m2[['SKU_P', 'NOM_P', 'CANT_OUT', 'UM_P', 'COSTO_P']].rename(columns={'SKU_P':'SKU_F', 'NOM_P':'ING_F', 'UM_P':'UM_F'})
    else: exp_f = pd.DataFrame()

    # Insumos Directos
    df_dir = m1[~es_proc].copy()
    df_dir['CANT_OUT'] = df_dir['CANT_VENTA'] * df_dir['CantReal']
    df_dir['COSTO_P'] = df_dir['CANT_OUT'] * df_dir['Precio']
    dir_out = df_dir[['SKU', 'Ingrediente', 'CANT_OUT', 'UM', 'COSTO_P']].rename(columns={'SKU':'SKU_F', 'Ingrediente':'ING_F', 'UM':'UM_F'})

    # Consolidado
    total = pd.concat([dir_out, exp_f])
    resumen = total.groupby(['SKU_F', 'UM_F'], as_index=False).agg({'CANT_OUT':'sum', 'COSTO_P':'sum', 'ING_F':'first'})
    resumen['Total'] = resumen.apply(lambda r: r['CANT_OUT']/1000 if str(r['UM_F']).upper() in ['G','ML','CC'] else r['CANT_OUT'], axis=1)
    
    return resumen[['SKU_F', 'ING_F', 'UM_F', 'Total', 'COSTO_P']].rename(
        columns={'SKU_F':'SKU', 'ING_F':'Insumo', 'UM_F':'UM', 'Total':'Cant Final', 'COSTO_P':'Costo Real $'})

# --- UI ---
st.title("👨‍🍳 MRP & Intelligence Compras (Supabase)")

t1, t2 = st.tabs(["Explosión MRP", "Historial de Compras"])

with t2:
    st.subheader("Módulo de Carga de Facturas")
    file_c = st.file_uploader("Subir Excel de Compras", type="xlsx")
    if file_c:
        df_c = pd.read_excel(file_c)
        st.dataframe(df_c.head())
        if st.button("Guardar en Supabase"):
            save_purchases(df_c)

with t1:
    file_m = st.file_uploader("Subir Estructura Recetas/Ventas", type="xlsx")
    if file_m:
        xls = pd.ExcelFile(file_m)
        muc_map = get_muc_data() # Obtenemos MUC desde la DB
        
        if muc_map:
            st.success(f"📈 Se han sincronizado {len(muc_map)} precios reales desde la nube.")
        
        res = process_bom(pd.read_excel(xls, 'Ventas'), pd.read_excel(xls, 'Directos'), pd.read_excel(xls, 'Procesados'), muc_map)
        
        st.dataframe(res.style.format({"Cant Final": "{:,.3f}", "Costo Real $": "$ {:,.0f}"}))
        st.metric("Inversión Total en Materia Prima", f"$ {res['Costo Real $'].sum():,.0f}")
