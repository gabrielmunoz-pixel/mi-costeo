import streamlit as st
import pandas as pd
import io

st.set_page_config(page_title="BOM Processor Final", layout="wide")

def process_bom(df_v, df_d, df_p):
    # 1. Limpieza de nombres de columnas
    df_v.columns = df_v.columns.str.strip()
    df_d.columns = df_d.columns.str.strip()
    df_p.columns = df_p.columns.str.strip()

    # 2. Join Nivel 1: Ventas + Directos
    # SKU (Ventas) contra CODIGO VENTA (Directos)
    m1 = pd.merge(
        df_v, 
        df_d, 
        left_on='SKU', 
        right_on='CODIGO VENTA', 
        how='inner'
    )
    
    # Requerimiento = Cantidad (Ventas) * CantReal (Directos)
    # Pandas crea SKU_x e Ingrediente_x para Ventas y SKU_y e Ingrediente_y para Directos
    m1['Req_N1'] = m1['Cantidad_x'] * m1['CantReal']

    # 3. Separar Procesados (Empiezan con PRO-)
    # El SKU del ingrediente en la hoja Directos es 'SKU_y'
    es_proc = m1['SKU_y'].str.startswith('PRO-', na=False)
    final_dir = m1[~es_proc].copy()
    a_explotar = m1[es_proc].copy()

    # 4. Explosión Nivel 2 (Procesados)
    if not a_explotar.empty:
        # Calcular Rendimiento Total de cada Batch en Procesados
        # Sumamos CantReceta agrupando por 'Codigo Venta'
        yields = df_p.groupby('Codigo Venta')['CantReceta'].sum().reset_index()
        yields.columns = ['Codigo Venta', 'Rendimiento_Lote']

        # Unir Procesados con sus rendimientos totales
        df_p_ready = pd.merge(df_p, yields, on='Codigo Venta')

        # Join con la necesidad de nivel 1
        m2 = pd.merge(
            a_explotar,
            df_p_ready,
            left_on='SKU_y', # SKU del ingrediente en Directos (ej: PRO-05)
            right_on='Codigo Venta', # Código en la hoja Procesados
            how='left'
        )

        # Lógica de Lotes (Porcion == 0)
        def calc_final(row):
            if pd.isna(row['CantReceta']): return 0
            if row['Porcion'] == 0:
                # (Uso en sándwich / Total lote) * Cantidad del ingrediente en la receta
                return (row['Req_N1'] / row['Rendimiento_Lote']) * row['CantReceta']
            else:
                return row['Req_N1'] * row['CantReceta']

        m2['TOTAL_CALC'] = m2.apply(calc_final, axis=1)

        # Usamos los nombres de columnas resultantes del merge de m2
        # 'SKU Ingrediente' e 'Ingrediente_y' (el de la hoja Procesados)
        exp_final = m2[['SKU Ingrediente', 'Ingrediente_y', 'TOTAL_CALC', 'UM_y']].rename(columns={
            'SKU Ingrediente': 'SKU_FINAL',
            'Ingrediente_y': 'INGREDIENTE',
            'TOTAL_CALC': 'TOTAL',
            'UM_y': 'UM'
        })
    else:
        exp_final = pd.DataFrame()

    # 5. Formatear Directos
    # SKU_y e Ingrediente_y son los de la hoja Directos en el primer merge m1
    final_dir = final_dir[['SKU_y', 'Ingrediente_y', 'Req_N1', 'UM']].rename(columns={
        'SKU_y': 'SKU_FINAL',
        'Ingrediente_y': 'INGREDIENTE',
        'Req_N1': 'TOTAL'
    })

    # 6. Consolidación Final
    resultado = pd.concat([final_dir, exp_final], ignore_index=True)
    resumen = resultado.groupby(['SKU_FINAL', 'INGREDIENTE', 'UM'], as_index=False)['TOTAL'].sum()
    
    return resumen

# --- Streamlit UI ---
st.title("📊 Procesador BOM Profesional")

uploaded_file = st.file_uploader("Cargar Excel", type="xlsx")

if uploaded_file:
    try:
        xls = pd.ExcelFile(uploaded_file)
        df_v = pd.read_excel(xls, 'Ventas')
        df_d = pd.read_excel(xls, 'Directos')
        df_p = pd.read_excel(xls, 'Procesados')

        res = process_bom(df_v, df_d, df_p)

        st.subheader("📋 Resultados de Requerimiento Total")
        st.dataframe(res.style.format({"TOTAL": "{:,.2f}"}), use_container_width=True)

        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            res.to_excel(writer, index=False)
        st.download_button("📥 Descargar Reporte", output.getvalue(), "requerimientos_bom.xlsx")

    except Exception as e:
        st.error(f"Error técnico detectado: {e}")
