import streamlit as st
import pandas as pd
import io

st.set_page_config(page_title="BOM Processor Final", layout="wide")

def process_bom(df_v, df_d, df_p):
    # 1. Limpieza de nombres de columnas
    df_v.columns = df_v.columns.str.strip()
    df_d.columns = df_d.columns.str.strip()
    df_p.columns = df_p.columns.str.strip()

    # 2. Preparar Ventas (Renombrar para evitar colisiones)
    df_v = df_v.rename(columns={'SKU': 'SKU_VENTA', 'Cantidad': 'CANT_VENTA'})

    # 3. Join Nivel 1: Ventas + Directos
    m1 = pd.merge(
        df_v, 
        df_d, 
        left_on='SKU_VENTA', 
        right_on='CODIGO VENTA', 
        how='inner'
    )
    
    # Requerimiento N1 = Venta * CantReal de la receta del plato
    m1['REQ_N1'] = m1['CANT_VENTA'] * m1['CantReal']

    # 4. Separar Directos de Procesados (SKU en la hoja Directos empieza con PRO-)
    # Nota: En m1, 'SKU' es el de la hoja Directos
    es_proc = m1['SKU'].str.startswith('PRO-', na=False)
    final_dir = m1[~es_proc].copy()
    a_explotar = m1[es_proc].copy()

    # 5. Explosión Nivel 2 (Procesados)
    if not a_explotar.empty:
        # Renombrar columnas de Procesados para control TOTAL
        df_p_clean = df_p.rename(columns={
            'Codigo Venta': 'COD_PROC_MAESTRO',
            'Ingrediente': 'NOM_ING_PROC',
            'SKU Ingrediente': 'SKU_ING_PROC',
            'UM': 'UM_PROC',
            'CantReceta': 'CANT_REC_PROC',
            'Porcion': 'PORCION_PROC'
        })

        # Calcular Rendimiento Total de cada Batch
        yields = df_p_clean.groupby('COD_PROC_MAESTRO')['CANT_REC_PROC'].sum().reset_index()
        yields.columns = ['COD_PROC_MAESTRO', 'RENDIMIENTO_LOTE']

        # Unir Procesados con sus rendimientos
        df_p_ready = pd.merge(df_p_clean, yields, on='COD_PROC_MAESTRO')

        # Join con la necesidad de nivel 1
        # Unimos SKU de Directos con el Código de la receta procesada
        m2 = pd.merge(
            a_explotar,
            df_p_ready,
            left_on='SKU',
            right_on='COD_PROC_MAESTRO',
            how='left'
        )

        # Lógica de Lotes (PORCION_PROC == 0)
        def calc_final(row):
            if pd.isna(row['CANT_REC_PROC']): return 0
            if row['PORCION_PROC'] == 0:
                # (Gramos del sándwich / Total lote) * Cantidad ingrediente en receta
                return (row['REQ_N1'] / row['RENDIMIENTO_LOTE']) * row['CANT_REC_PROC']
            else:
                return row['REQ_N1'] * row['CANT_REC_PROC']

        m2['TOTAL_CALC'] = m2.apply(calc_final, axis=1)

        exp_final = m2[['SKU_ING_PROC', 'NOM_ING_PROC', 'TOTAL_CALC', 'UM_PROC']].rename(columns={
            'SKU_ING_PROC': 'SKU_FINAL',
            'NOM_ING_PROC': 'INGREDIENTE',
            'TOTAL_CALC': 'TOTAL',
            'UM_PROC': 'UM'
        })
    else:
        exp_final = pd.DataFrame()

    # 6. Formatear Directos
    # 'Ingrediente' y 'UM' vienen de la hoja Directos
    final_dir = final_dir[['SKU', 'Ingrediente', 'REQ_N1', 'UM']].rename(columns={
        'SKU': 'SKU_FINAL',
        'Ingrediente': 'INGREDIENTE',
        'REQ_N1': 'TOTAL'
    })

    # 7. Consolidación Final
    resultado = pd.concat([final_dir, exp_final], ignore_index=True)
    resumen = resultado.groupby(['SKU_FINAL', 'INGREDIENTE', 'UM'], as_index=False)['TOTAL'].sum()
    
    return resumen

# --- Interfaz ---
st.title("👨‍🍳 Procesador BOM - Versión Corregida")

file = st.file_uploader("Cargar Prueba_costeo.xlsx", type="xlsx")

if file:
    try:
        xls = pd.ExcelFile(file)
        df_v = pd.read_excel(xls, 'Ventas')
        df_d = pd.read_excel(xls, 'Directos')
        df_p = pd.read_excel(xls, 'Procesados')

        res = process_bom(df_v, df_d, df_p)

        st.subheader("📊 Requerimiento Total Consolidado")
        st.dataframe(res.style.format({"TOTAL": "{:,.2f}"}), use_container_width=True)

        buf = io.BytesIO()
        with pd.ExcelWriter(buf, engine='openpyxl') as writer:
            res.to_excel(writer, index=False)
        st.download_button("📥 Descargar Resultados", buf.getvalue(), "requerimiento_final.xlsx")

    except Exception as e:
        st.error(f"Error detectado: {e}")
