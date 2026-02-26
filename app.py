import streamlit as st
import pandas as pd
import io

st.set_page_config(page_title="BOM Processor Pro v3", layout="wide")

def process_bom(df_v, df_d, df_p, df_dist):
    # 1. Limpieza de nombres de columnas
    for df in [df_v, df_d, df_p, df_dist]:
        df.columns = df.columns.str.strip()

    # 2. Procesar Distribución (Mix de Ventas)
    if '%' in df_dist.columns:
        if df_dist['%'].dtype == object:
            df_dist['%'] = df_dist['%'].str.replace('%', '').astype(float) / 100

    # 3. Cruzar Ventas con Distribución
    # Renombramos para evitar el error de index
    df_v = df_v.rename(columns={'SKU': 'SKU_VENTA', 'Cantidad': 'CANT_VENTA'})
    df_dist = df_dist.rename(columns={'Codigo': 'SKU_DIST', 'Opcion': 'OPCION_NOM', '%': 'PORCENTAJE'})

    df_v_exp = pd.merge(df_v, df_dist, left_on='SKU_VENTA', right_on='SKU_DIST', how='left')
    
    # Si no hay distribución, la ponderación es 1 (100%)
    df_v_exp['VENTA_POND'] = df_v_exp.apply(
        lambda x: x['CANT_VENTA'] * x['PORCENTAJE'] if pd.notnull(x['PORCENTAJE']) else x['CANT_VENTA'], axis=1
    )

    # 4. Nivel 1: Cruzar con Directos
    # Renombramos Directos para control total
    df_d_ready = df_d.rename(columns={'CODIGO VENTA': 'SKU_RECETA_PADRE', 'SKU': 'SKU_INSUMO', 'Ingrediente': 'NOM_INSUMO'})
    
    m1 = pd.merge(df_v_exp, df_d_ready, left_on='SKU_VENTA', right_on='SKU_RECETA_PADRE', how='inner')
    
    # Filtrado por EsOpcion (Proteína, Pan, Acompañamiento)
    def filter_options(row):
        if pd.isna(row['EsOpcion']) or row['EsOpcion'] == '':
            return True
        return str(row['OPCION_NOM']).lower() in str(row['Plato']).lower()

    m1 = m1[m1.apply(filter_options, axis=1)]
    m1['REQ_N1'] = m1['VENTA_POND'] * m1['CantReal']

    # 5. Separar Procesados (PRO-)
    es_proc = m1['SKU_INSUMO'].str.startswith('PRO-', na=False)
    df_dir_f = m1[~es_proc].copy()
    df_a_exp = m1[es_proc].copy()

    # 6. Nivel 2: Procesados (CantEfic)
    if not df_a_exp.empty:
        # Renombramos Procesados para evitar "Ingrediente_x"
        df_p_ready = df_p.rename(columns={
            'Codigo Venta': 'SKU_PROC_PADRE',
            'Ingrediente': 'NOM_ING_PROC',
            'SKU Ingrediente': 'SKU_ING_PROC',
            'CantEfic': 'VALOR_EFIC',
            'UM': 'UM_PROC'
        })

        # Rendimiento del Lote
        yields = df_p_ready.groupby('SKU_PROC_PADRE')['VALOR_EFIC'].sum().reset_index().rename(columns={'VALOR_EFIC': 'YIELD_LOTE'})
        df_p_final = pd.merge(df_p_ready, yields, on='SKU_PROC_PADRE')
        
        m2 = pd.merge(df_a_exp, df_p_final, left_on='SKU_INSUMO', right_on='SKU_PROC_PADRE', how='left')
        
        def calc_batch(r):
            if pd.isna(r['VALOR_EFIC']): return 0
            if r['Porcion'] == 0:
                return (r['REQ_N1'] / r['YIELD_LOTE']) * r['VALOR_EFIC']
            return r['REQ_N1'] * r['VALOR_EFIC']

        m2['TOTAL_FINAL'] = m2.apply(calc_batch, axis=1)
        
        exp_f = m2[['SKU_ING_PROC', 'NOM_ING_PROC', 'TOTAL_FINAL', 'UM_PROC']].rename(
            columns={'SKU_ING_PROC': 'SKU_OUT', 'NOM_ING_PROC': 'ING_OUT', 'TOTAL_FINAL': 'CANT_OUT', 'UM_PROC': 'UM_OUT'})
    else:
        exp_f = pd.DataFrame()

    # 7. Consolidación
    dir_out = df_dir_f[['SKU_INSUMO', 'NOM_INSUMO', 'REQ_N1', 'UM']].rename(
        columns={'SKU_INSUMO': 'SKU_OUT', 'NOM_INSUMO': 'ING_OUT', 'REQ_N1': 'CANT_OUT', 'UM': 'UM_OUT'})
    
    consolidado = pd.concat([dir_out, exp_f], ignore_index=True)
    resumen = consolidado.groupby(['SKU_OUT', 'ING_OUT', 'UM_OUT'], as_index=False)['CANT_OUT'].sum()
    
    return resumen.rename(columns={'SKU_OUT': 'SKU', 'ING_OUT': 'Ingrediente', 'CANT_OUT': 'Total Requerido', 'UM_OUT': 'UM'})

# --- INTERFAZ ---
st.title("📊 Procesador de Producción v3.1")
st.markdown("Cálculo ponderado por Mix de Ventas y Eficiencia de Lotes.")

file = st.file_uploader("Subir Archivo de Costeo", type="xlsx")

if file:
    try:
        xls = pd.ExcelFile(file)
        # Validación de hojas
        required = ['Ventas', 'Directos', 'Procesados', 'DistAper']
        if all(h in xls.sheet_names for h in required):
            res = process_bom(pd.read_excel(xls, 'Ventas'), 
                              pd.read_excel(xls, 'Directos'), 
                              pd.read_excel(xls, 'Procesados'), 
                              pd.read_excel(xls, 'DistAper'))
            
            st.dataframe(res.style.format({"Total Requerido": "{:,.2f}"}), use_container_width=True)
            
            buf = io.BytesIO()
            with pd.ExcelWriter(buf, engine='openpyxl') as writer:
                res.to_excel(writer, index=False)
            st.download_button("📥 Descargar Reporte", buf.getvalue(), "requerimiento_total.xlsx")
        else:
            st.error(f"Faltan hojas. Se requieren: {required}")
    except Exception as e:
        st.error(f"Error detectado: {e}")
