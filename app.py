import streamlit as st
import pandas as pd
import io

st.set_page_config(page_title="BOM Processor Final", layout="wide")

def process_bom(df_v, df_d, df_p):
    # 1. Limpieza de nombres de columnas
    df_v.columns = df_v.columns.str.strip()
    df_d.columns = df_d.columns.str.strip()
    df_p.columns = df_p.columns.str.strip()

    # 2. Sanity Check: SKUs sin receta
    faltantes = set(df_v['SKU'].unique()) - set(df_d['CODIGO VENTA'].unique())
    if faltantes:
        st.warning(f"⚠️ SKUs en Ventas sin receta en Directos: {list(faltantes)[:5]}...")

    # 3. Nivel 1: Ventas + Directos
    m1 = pd.merge(df_v.rename(columns={'SKU': 'SKU_V', 'Cantidad': 'CANT_V'}), 
                  df_d, left_on='SKU_V', right_on='CODIGO VENTA', how='inner')
    m1['REQ_N1'] = m1['CANT_V'] * m1['CantReal']

    # 4. Separar Directos de Procesados
    es_proc = m1['SKU'].str.startswith('PRO-', na=False)
    df_dir = m1[~es_proc].copy()
    df_a_exp = m1[es_proc].copy()

    # 5. Nivel 2: Procesados con Lógica de Lotes
    if not df_a_exp.empty:
        # Sumar el rendimiento total del lote
        yields = df_p.groupby('Codigo Venta')['CantReceta'].sum().reset_index().rename(columns={'CantReceta': 'YIELD'})
        yields = yields[yields['YIELD'] > 0] # Evitar división por cero
        
        df_p_ready = pd.merge(df_p, yields, on='Codigo Venta')
        
        # Merge Nivel 2
        m2 = pd.merge(df_a_exp, df_p_ready, left_on='SKU', right_on='Codigo Venta', how='left', suffixes=('_d', '_p'))
        
        # Fórmula de cálculo
        def formula(r):
            if pd.isna(r['CantReceta']): return 0
            if r['Porcion_p'] == 0:
                return (r['REQ_N1'] / r['YIELD']) * r['CantReceta']
            return r['REQ_N1'] * r['CantReceta']

        m2['TOTAL'] = m2.apply(formula, axis=1)
        
        exp_f = m2[['SKU Ingrediente', 'Ingrediente_p', 'TOTAL', 'UM_p']].rename(
            columns={'SKU Ingrediente': 'SKU', 'Ingrediente_p': 'ING', 'UM_p': 'UM'}
        )
    else:
        exp_f = pd.DataFrame()

    # 6. Unir todo y agrupar
    df_dir_f = df_dir[['SKU', 'Ingrediente', 'REQ_N1', 'UM']].rename(columns={'Ingrediente': 'ING', 'REQ_N1': 'TOTAL'})
    res = pd.concat([df_dir_f, exp_f], ignore_index=True)
    return res.groupby(['SKU', 'ING', 'UM'], as_index=False)['TOTAL'].sum()

# --- INTERFAZ ---
st.title("📊 Procesador de Producción Blindado")
file = st.file_uploader("Subir Prueba_costeo.xlsx", type="xlsx")

if file:
    try:
        xls = pd.ExcelFile(file)
        if all(h in xls.sheet_names for h in ['Ventas', 'Directos', 'Procesados']):
            res = process_bom(pd.read_excel(xls, 'Ventas'), pd.read_excel(xls, 'Directos'), pd.read_excel(xls, 'Procesados'))
            st.dataframe(res.style.format({"TOTAL": "{:,.2f}"}), use_container_width=True)
            
            output = io.BytesIO()
            with pd.ExcelWriter(output, engine='openpyxl') as writer:
                res.to_excel(writer, index=False)
            st.download_button("📥 Descargar", output.getvalue(), "requerimiento.xlsx")
    except Exception as e:
        st.error(f"Error: {e}")
