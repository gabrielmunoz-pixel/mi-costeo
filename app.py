import streamlit as st
import pandas as pd
import io

st.set_page_config(page_title="BOM Processor Senior - CantEfic", layout="wide")

def process_bom(df_v, df_d, df_p):
    # 1. LIMPIEZA INICIAL
    for df in [df_v, df_d, df_p]:
        df.columns = df.columns.str.strip()

    # 2. BLOQUE DE BLINDAJE (SANITY CHECK)
    ventas_skus = set(df_v['SKU'].unique())
    directos_skus = set(df_d['CODIGO VENTA'].unique())
    faltantes = ventas_skus - directos_skus
    if faltantes:
        st.warning(f"⚠️ SKUs en Ventas sin receta en Directos: {list(faltantes)[:10]}")

    # 3. LOGICA DE NIVEL 1 (Ventas + Directos)
    df_v_ready = df_v.rename(columns={'SKU': 'SKU_VENTA', 'Cantidad': 'CANT_VENTA'})
    
    m1 = pd.merge(df_v_ready, df_d, left_on='SKU_VENTA', right_on='CODIGO VENTA', how='inner')
    m1['REQ_N1'] = m1['CANT_VENTA'] * m1['CantReal']

    es_proc = m1['SKU'].str.startswith('PRO-', na=False)
    df_dir_final = m1[~es_proc].copy()
    df_a_explotar = m1[es_proc].copy()

    # 4. LOGICA DE NIVEL 2 (Procesados usando CantEfic)
    if not df_a_explotar.empty:
        # Renombramos hoja Procesados incluyendo CantEfic
        df_p_ready = df_p.rename(columns={
            'Codigo Venta': 'COD_BOM_PROC',
            'CantEfic': 'CANT_EFIC_PROC', # CAMBIO CLAVE: Usamos CantEfic
            'Porcion': 'PORCION_PROC',
            'UM': 'UM_PROC',
            'Ingrediente': 'NOM_ING_PROC'
        })

        # Calcular Rendimiento Total del Lote sumando CantEfic
        yields = df_p_ready.groupby('COD_BOM_PROC')['CANT_EFIC_PROC'].sum().reset_index()
        yields.columns = ['COD_BOM_PROC', 'TOTAL_YIELD_BATCH']
        
        # Evitar división por cero
        yields = yields[yields['TOTAL_YIELD_BATCH'] > 0]
        
        # Unir recetas con sus rendimientos
        df_p_final = pd.merge(df_p_ready, yields, on='COD_BOM_PROC')

        # Cruzar con la necesidad del Nivel 1
        m2 = pd.merge(df_a_explotar, df_p_final, left_on='SKU', right_on='COD_BOM_PROC', how='left')

        # Lógica de Lote usando CANT_EFIC_PROC
        def calc_batch(row):
            if pd.isna(row['CANT_EFIC_PROC']): return 0
            if row['PORCION_PROC'] == 0:
                # (Necesidad del plato / Rendimiento Total del Lote) * Cantidad Eficiente en receta
                return (row['REQ_N1'] / row['TOTAL_YIELD_BATCH']) * row['CANT_EFIC_PROC']
            return row['REQ_N1'] * row['CANT_EFIC_PROC']

        m2['TOTAL_FINAL'] = m2.apply(calc_batch, axis=1)

        explosion = m2[['SKU Ingrediente', 'NOM_ING_PROC', 'TOTAL_FINAL', 'UM_PROC']].rename(columns={
            'SKU Ingrediente': 'SKU_OUT',
            'NOM_ING_PROC': 'ING_OUT',
            'TOTAL_FINAL': 'CANT_OUT',
            'UM_PROC': 'UM_OUT'
        })
    else:
        explosion = pd.DataFrame()

    # 5. CONSOLIDACIÓN
    dir_out = df_dir_final[['SKU', 'Ingrediente', 'REQ_N1', 'UM']].rename(columns={
        'SKU': 'SKU_OUT',
        'Ingrediente': 'ING_OUT',
        'REQ_N1': 'CANT_OUT',
        'UM': 'UM_OUT'
    })

    consolidado = pd.concat([dir_out, explosion], ignore_index=True)
    resumen = consolidado.groupby(['SKU_OUT', 'ING_OUT', 'UM_OUT'], as_index=False)['CANT_OUT'].sum()
    
    return resumen.rename(columns={'SKU_OUT': 'SKU', 'ING_OUT': 'Ingrediente', 'CANT_OUT': 'Total Requerido', 'UM_OUT': 'UM'})

# --- INTERFAZ ---
st.title("👨‍🍳 BOM Processor: CantEfic Edition")

file = st.file_uploader("Subir Prueba_costeo.xlsx", type="xlsx")

if file:
    try:
        xls = pd.ExcelFile(file)
        if all(h in xls.sheet_names for h in ['Ventas', 'Directos', 'Procesados']):
            df_v = pd.read_excel(xls, 'Ventas')
            df_d = pd.read_excel(xls, 'Directos')
            df_p = pd.read_excel(xls, 'Procesados')

            resultado = process_bom(df_v, df_d, df_p)

            st.subheader("📋 Requerimiento Final (Basado en CantEfic)")
            st.dataframe(resultado.style.format({"Total Requerido": "{:,.2f}"}), use_container_width=True)

            buf = io.BytesIO()
            with pd.ExcelWriter(buf, engine='openpyxl') as writer:
                resultado.to_excel(writer, index=False)
            st.download_button("📥 Descargar Requerimiento", buf.getvalue(), "requerimiento_cant_efic.xlsx")
        else:
            st.error("Asegúrate de que las hojas se llamen: Ventas, Directos y Procesados.")
    except Exception as e:
        st.error(f"Error técnico: {e}")
