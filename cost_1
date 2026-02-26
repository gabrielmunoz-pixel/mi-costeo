import streamlit as st
import pandas as pd
import io

# Configuración de página
st.set_page_config(page_title="DataChef: BOM Processor", layout="wide")

def process_bom(df_ventas, df_directos, df_procesados):
    """
    Lógica core de explosión de ingredientes.
    """
    # 1. Estandarización de nombres de columnas para evitar errores de espacios
    df_ventas.columns = df_ventas.columns.str.strip()
    df_directos.columns = df_directos.columns.str.strip()
    df_procesados.columns = df_procesados.columns.str.strip()

    # 2. Join Inicial: Ventas con Ingredientes Directos
    # Unimos por CODIGO VENTA
    merged_1 = pd.merge(
        df_ventas, 
        df_directos, 
        left_on='Codigo de Venta', 
        right_on='CODIGO VENTA', 
        how='inner'
    )

    # Calculamos la necesidad base del primer nivel
    # Cantidad Total = Cantidad Vendida * CantReal (la cantidad neta por plato)
    merged_1['Cantidad_Intermedia'] = merged_1['Cantidad Vendida'] * merged_1['CantReal']

    # 3. Identificación de Procesados vs Directos
    # Separamos lo que es SKU de compra directa de lo que requiere explosión (PRO-)
    es_procesado = merged_1['SKU'].str.startswith('PRO-', na=False)
    
    directos_finales = merged_1[~es_procesado].copy()
    a_explotar = merged_1[es_procesado].copy()

    # 4. Explosión Nivel 2: Procesados
    if not a_explotar.empty:
        # Join con la hoja de Procesados usando SKU (Directos) -> CODIGO PROCESADO (Procesados)
        explosion = pd.merge(
            a_explotar,
            df_procesados,
            left_on='SKU',
            right_on='CODIGO PROCESADO',
            how='left',
            suffixes=('_dir', '_proc')
        )

        # Manejo de errores: SKUs "PRO-" que no existen en la hoja de Procesados
        missing_skus = explosion[explosion['SKU Ingrediente'].isna()]['SKU'].unique()
        if len(missing_skus) > 0:
            st.warning(f"⚠️ SKUs procesados no encontrados en maestro: {missing_skus}")

        # Cálculo de sub-ingredientes:
        # Total = Cantidad_Intermedia (del nivel 1) * CantReceta (del nivel 2)
        explosion['Total_Calculado'] = explosion['Cantidad_Intermedia'] * explosion['CantReceta']
        
        # Normalización de columnas para el append
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

    # 5. Preparación de Directos que no requieren explosión
    directos_finales = directos_finales[[
        'SKU', 'Ingrediente', 'Cantidad_Intermedia', 'UM'
    ]].rename(columns={
        'Ingrediente': 'Nombre Ingrediente',
        'Cantidad_Intermedia': 'Total Requerido'
    })

    # 6. Consolidación Final
    resultado_final = pd.concat([directos_finales, explosion_final], ignore_index=True)
    
    # Agrupación por SKU para el resumen total
    resumen = resultado_final.groupby(['SKU', 'Nombre Ingrediente', 'UM'], as_index=False)['Total Requerido'].sum()
    
    return resumen

# --- INTERFAZ STREAMLIT ---

st.title("👨‍🍳 Senior Data BOM Processor")
st.markdown("Carga el archivo Excel con las pestañas `Ventas`, `Directos` y `Procesados`.")

uploaded_file = st.file_uploader("Seleccionar archivo Excel", type=["xlsx"])

if uploaded_file:
    try:
        # Carga de datos
        with st.spinner('Leyendo capas de datos...'):
            xlsx = pd.ExcelFile(uploaded_file)
            
            if not all(sheet in xlsx.sheet_names for sheet in ['Ventas', 'Directos', 'Procesados']):
                st.error("❌ El archivo debe contener las hojas: 'Ventas', 'Directos' y 'Procesados'.")
            else:
                df_v = pd.read_excel(xlsx, 'Ventas')
                df_d = pd.read_excel(xlsx, 'Directos')
                df_p = pd.read_excel(xlsx, 'Procesados')

                # Procesamiento
                df_resumen = process_bom(df_v, df_d, df_p)

                # Visualización
                st.subheader("📊 Resumen de Requerimientos Totales")
                st.dataframe(df_resumen.style.format({"Total Requerido": "{:.4f}"}), use_container_width=True)

                # Botón de descarga
                output = io.BytesIO()
                with pd.ExcelWriter(output, engine='openpyxl') as writer:
                    df_resumen.to_excel(writer, index=False, sheet_name='Requerimiento Final')
                
                st.download_button(
                    label="📥 Descargar Reporte Consolidado",
                    data=output.getvalue(),
                    file_name="requerimiento_ingredientes.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )

    except Exception as e:
        st.error(f"Ocurrió un error crítico: {e}")
