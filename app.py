import streamlit as st
import pandas as pd
import io

st.set_page_config(page_title="DataChef: BOM Processor", layout="wide")

def process_bom(df_ventas, df_directos, df_procesados):
    # Estandarización manual para asegurar compatibilidad con tus archivos
    # En Ventas, tu columna se llama 'SKU', la renombramos para el join
    df_ventas = df_ventas.rename(columns={'SKU': 'CODIGO_V_LINK', 'Cantidad': 'Cant_Vendida'})
    
    # 1. Join Ventas con Directos
    # Unimos usando el SKU de ventas contra el CODIGO VENTA de directos
    merged_1 = pd.merge(
        df_ventas, 
        df_directos, 
        left_on='CODIGO_V_LINK', 
        right_on='CODIGO VENTA', 
        how='inner'
    )

    # Cantidad necesaria nivel 1
    merged_1['Cant_Necesaria_N1'] = merged_1['Cant_Vendida'] * merged_1['CantReal']

    # 2. Separar lo que es Procesado (PRO-)
    es_procesado = merged_1['SKU'].str.startswith('PRO-', na=False)
    directos_finales = merged_1[~es_procesado].copy()
    a_explotar = merged_1[es_procesado].copy()

    # 3. Explosión Nivel 2 (Procesados)
    if not a_explotar.empty:
        # En tu hoja Procesados, la columna de unión es 'Codigo Venta' o 'CODIGO PROCESADO'
        # Usaremos el nombre que aparece en tu archivo: 'Codigo Venta'
        explosion = pd.merge(
            a_explotar,
            df_procesados,
            left_on='SKU',
            right_on='Codigo Venta', 
            how='left',
            suffixes=('_dir', '_proc')
        )

        explosion['Total_Calculado'] = explosion['Cant_Necesaria_N1'] * explosion['CantReceta']
        
        explosion_final = explosion[[
            'SKU Ingrediente', 'Ingrediente_proc', 'Total_Calculado', 'UM_proc'
        ]].rename(columns={
            'SKU Ingrediente': 'SKU',
            'Ingrediente_proc': 'Nombre Ingrediente',
            'Total_Calculado': 'Total Requerido',
            'UM_proc': 'UM'
        })
    else:
        explosion_final = pd.DataFrame()

    # 4. Formatear Directos
    directos_finales = directos_finales[[
        'SKU', 'Ingrediente', 'Cant_Necesaria_N1', 'UM'
    ]].rename(columns={
        'Ingrediente': 'Nombre Ingrediente',
        'Cant_Necesaria_N1': 'Total Requerido'
    })

    # 5. Consolidación
    resultado = pd.concat([directos_finales, explosion_final], ignore_index=True)
    resumen = resultado.groupby(['SKU', 'Nombre Ingrediente', 'UM'], as_index=False)['Total Requerido'].sum()
    
    return resumen

st.title("👨‍🍳 Procesador de Costeo y BOM")
uploaded_file = st.file_uploader("Carga tu archivo Prueba_costeo.xlsx", type=["xlsx"])

if uploaded_file:
    try:
        df_v = pd.read_excel(uploaded_file, sheet_name='Ventas')
        df_d = pd.read_excel(uploaded_file, sheet_name='Directos')
        df_p = pd.read_excel(uploaded_file, sheet_name='Procesados')

        df_resumen = process_bom(df_v, df_d, df_p)

        st.subheader("📊 Requerimiento Total de Insumos")
        st.dataframe(df_resumen, use_container_width=True)
        
        # Opción de descarga
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            df_resumen.to_excel(writer, index=False)
        st.download_button("📥 Descargar Resultados", output.getvalue(), "resultado_bom.xlsx")

    except Exception as e:
        st.error(f"Error en el procesamiento: {e}")
        st.info("Asegúrate de que las hojas se llamen exactamente: Ventas, Directos y Procesados")
