import streamlit as st
import pandas as pd
import io

st.set_page_config(page_title="BOM Processor Pro - Multi-Nivel", layout="wide")

def process_bom(df_v, df_d, df_p, df_dist):
    # 1. Limpieza de nombres de columnas
    for df in [df_v, df_d, df_p, df_dist]:
        df.columns = df.columns.str.strip()

    # 2. Pre-procesar Distribución (Mix)
    # Aseguramos que el % sea decimal
    if '%' in df_dist.columns:
        if df_dist['%'].dtype == object:
            df_dist['%'] = df_dist['%'].str.replace('%', '').astype(float) / 100

    # 3. Cruzar Ventas con Distribución para obtener "Venta Real por Opción"
    # Si un SKU de venta está en DistAper, lo expandimos
    df_v_exp = pd.merge(df_v, df_dist, left_on='SKU', right_on='Codigo', how='left')
    
    # Si no tiene distribución (es un producto simple), usamos la venta original al 100%
    df_v_exp['Venta_Ponderada'] = df_v_exp.apply(
        lambda x: x['Cantidad'] * x['%'] if pd.notnull(x['%']) else x['Cantidad'], axis=1
    )
    # El nombre de la opción nos servirá para filtrar en la hoja Directos si es necesario
    # o para identificar la variante del plato.

    # 4. Nivel 1: Cruzar Ventas Ponderadas con Directos
    # Unimos por CODIGO VENTA. 
    # Nota: Aquí filtramos que si es una opción, coincida el nombre de la Opcion con el Plato
    m1 = pd.merge(df_v_exp, df_d, left_on='Codigo', right_on='CODIGO VENTA', how='inner')
    
    # Lógica de filtrado por EsOpcion: 
    # Si EsOpcion es 1, 2 o 3 (Proteina, Pan, Acompañamiento), 
    # validamos que el "Plato" en Directos contenga el nombre de la "Opcion" en DistAper
    def filter_options(row):
        if pd.isna(row['EsOpcion']) or row['EsOpcion'] == '':
            return True
        # Si es una de las opciones distribuidas, el nombre de la opción debe estar en la descripción del plato
        return str(row['Opcion']).lower() in str(row['Plato']).lower()

    m1 = m1[m1.apply(filter_options, axis=1)]

    # Cálculo de Requerimiento Nivel 1
    m1['REQ_N1'] = m1['Venta_Ponderada'] * m1['CantReal']

    # 5. Separar Procesados (PRO-)
    es_proc = m1['SKU_y'].str.startswith('PRO-', na=False)
    df_dir_f = m1[~es_proc].copy()
    df_a_exp = m1[es_proc].copy()

    # 6. Nivel 2: Procesados (Usando CantEfic)
    if not df_a_exp.empty:
        # Calcular rendimiento del lote sumando CantEfic
        yields = df_p.groupby('Codigo Venta')['CantEfic'].sum().reset_index().rename(columns={'CantEfic': 'YIELD'})
        yields = yields[yields['YIELD'] > 0]
        
        df_p_ready = pd.merge(df_p, yields, on='Codigo Venta')
        m2 = pd.merge(df_a_exp, df_p_ready, left_on='SKU_y', right_on='Codigo Venta', how='left', suffixes=('_d', '_p'))
        
        # Lógica de Batch
        def formula_batch(r):
            if pd.isna(r['CantEfic']): return 0
            if r['Porcion'] == 0:
                return (r['REQ_N1'] / r['YIELD']) * r['CantEfic']
            return r['REQ_N1'] * r['CantEfic']

        m2['TOTAL'] = m2.apply(formula_batch, axis=1)
        exp_f = m2[['SKU Ingrediente', 'Ingrediente_p', 'TOTAL', 'UM_p']].rename(
            columns={'SKU Ingrediente': 'SKU_FIN', 'Ingrediente_p': 'ING_FIN', 'UM_p': 'UM_FIN'})
    else:
        exp_f = pd.DataFrame()

    # 7. Consolidación Final
    dir_res = df_dir_f[['SKU_y', 'Ingrediente_x', 'REQ_N1', 'UM']].rename(
        columns={'SKU_y': 'SKU_FIN', 'Ingrediente_x': 'ING_FIN', 'REQ_N1': 'TOTAL', 'UM': 'UM_FIN'})
    
    final = pd.concat([dir_res, exp_f], ignore_index=True)
    return final.groupby(['SKU_FIN', 'ING_FIN', 'UM_FIN'], as_index=False)['TOTAL'].sum()

# --- INTERFAZ STREAMLIT ---
st.title("👨‍🍳 Sistema MRP Gastronómico v3.0")
st.info("Estructura: Ventas + Directos (Opciones) + DistAper + Procesados (CantEfic)")

file = st.file_uploader("Subir Archivo de Costeo", type="xlsx")

if file:
    try:
        xls = pd.ExcelFile(file)
        sheets = xls.sheet_names
        if all(h in sheets for h in ['Ventas', 'Directos', 'Procesados', 'DistAper']):
            df_v = pd.read_excel(xls, 'Ventas')
            df_d = pd.read_excel(xls, 'Directos')
            df_p = pd.read_excel(xls, 'Procesados')
            df_dist = pd.read_excel(xls, 'DistAper')

            res = process_bom(df_v, df_d, df_p, df_dist)

            st.subheader("📋 Consolidado de Requerimientos")
            st.dataframe(res.style.format({"TOTAL": "{:,.2f}"}), use_container_width=True)

            buf = io.BytesIO()
            with pd.ExcelWriter(buf, engine='openpyxl') as writer:
                res.to_excel(writer, index=False)
            st.download_button("📥 Descargar Reporte", buf.getvalue(), "requerimiento_total.xlsx")
        else:
            st.error(f"Faltan hojas. Detectadas: {sheets}")
    except Exception as e:
        st.error(f"Error en el proceso: {e}")
