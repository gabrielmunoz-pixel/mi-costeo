import streamlit as st
import pandas as pd
import io

st.set_page_config(page_title="BOM Processor Final", layout="wide")

def process_bom(df_v, df_d, df_p):
    # 1. Limpieza de nombres de columnas (quitar espacios invisibles)
    df_v.columns = df_v.columns.str.strip()
    df_d.columns = df_d.columns.str.strip()
    df_p.columns = df_p.columns.str.strip()

    # 2. Join Nivel 1: Ventas + Directos
    # Unimos por el código del plato. 
    # En Ventas es 'SKU' y en Directos es 'CODIGO VENTA'
    m1 = pd.merge(
        df_v, 
        df_d, 
        left_on='SKU', 
        right_on='CODIGO VENTA', 
        how='inner',
        suffixes=('_Venta', '_Directo')
    )
    
    # Cálculo Nivel 1: Cantidad (de Ventas) * CantReal (de Directos)
    m1['Requerimiento_N1'] = m1['Cantidad_Venta'] * m1['CantReal']

    # 3. Separar Directos de Procesados (SKU_Directo empieza con PRO-)
    es_procesado = m1['SKU_Directo'].str.startswith('PRO-', na=False)
    final_directos = m1[~es_procesado].copy()
    a_explotar = m1[es_procesado].copy()

    # 4. Explosión Nivel 2 (Procesados)
    if not a_explotar.empty:
        # Calcular Rendimiento Total de cada Batch en Procesados
        rendimientos = df_p.groupby('Codigo Venta')['CantReceta'].sum().reset_index()
        rendimientos.columns = ['Codigo Venta', 'Rendimiento_Lote']

        # Unir Procesados con sus rendimientos
        df_p_ready = pd.merge(df_p, rendimientos, on='Codigo Venta')

        # Join con la necesidad de nivel 1
        m2 = pd.merge(
            a_explotar,
            df_p_ready,
            left_on='SKU_Directo',
            right_on='Codigo Venta',
            how='left',
            suffixes=('_m1', '_proc')
        )

        # Lógica de Lotes (Porcion == 0)
        def calc_final(row):
            if pd.isna(row['CantReceta']): return 0
            if row['Porcion_proc'] == 0:
                # Factor: (Uso en plato / Rendimiento Total Lote) * CantReceta Ingrediente
                return (row['Requerimiento_N1'] / row['Rendimiento_Lote']) * row['CantReceta']
            else:
                return row['Requerimiento_N1'] * row['CantReceta']

        m2['Total_Calculado'] = m2.apply(calc_final, axis=1)

        explosion_final = m2[['SKU Ingrediente', 'Ingrediente_proc', 'Total_Calculado', 'UM_proc']].rename(columns={
            'SKU Ingrediente': 'SKU_FINAL',
            'Ingrediente_proc': 'INGREDIENTE',
            'Total_Calculado': 'TOTAL',
            'UM_proc': 'UM'
        })
    else:
        explosion_final = pd.DataFrame()

    # 5. Formatear Directos
    final_directos = final_directos[['SKU_Directo', 'Ingrediente_Directo', 'Requerimiento_N1', 'UM']].rename(columns={
        'SKU_Directo': 'SKU_FINAL',
        'Ingrediente_Directo': 'INGREDIENTE',
        'Requerimiento_N1': 'TOTAL',
        'UM': 'UM'
    })

    # 6. Consolidación Final
    resultado = pd.concat([final_directos, explosion_final], ignore_index=True)
    resumen = resultado.groupby(['SKU_FINAL', 'INGREDIENTE', 'UM'], as_index=False)['TOTAL'].sum()
    
    return resumen

# --- Streamlit UI ---
st.title("🚀 Procesador de Costeo BOM")

uploaded_file = st.file_uploader("Cargar Prueba_costeo.xlsx", type="xlsx")

if uploaded_file:
    try:
        xls = pd.ExcelFile(uploaded_file)
        df_v = pd.read_excel(xls, 'Ventas')
        df_d = pd.read_excel(xls, 'Directos')
        df_p = pd.read_excel(xls, 'Procesados')

        res = process_bom(df_v, df_d, df_p)

        st.subheader("📊 Resultado Consolidado")
        st.dataframe(res.style.format({"TOTAL": "{:,.2f}"}), use_container_width=True)

        # Exportar
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            res.to_excel(writer, index=False)
        st.download_button("📥 Descargar Excel", output.getvalue(), "requerimiento_total.xlsx")

    except Exception as e:
        st.error(f"Error técnico: {e}")
