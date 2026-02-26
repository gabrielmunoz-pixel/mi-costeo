import streamlit as st
import pandas as pd
import io

# Configuración de página para Senior Data View
st.set_page_config(page_title="DataChef BOM: Batch Logic Edition", layout="wide")

def process_bom(df_ventas, df_directos, df_procesados):
    """
    Lógica de explosión BOM con manejo de lotes (Batch Yield).
    """
    # 1. Estandarización y Limpieza de Nombres de Columna
    df_ventas.columns = df_ventas.columns.str.strip()
    df_directos.columns = df_directos.columns.str.strip()
    df_procesados.columns = df_procesados.columns.str.strip()

    # Mapeo de columnas dinámico según tus archivos
    df_ventas = df_ventas.rename(columns={'SKU': 'COD_VENTA_LINK', 'Cantidad': 'CANT_VENDIDA'})
    
    # 2. Join Nivel 1: Ventas + Directos
    merged_1 = pd.merge(
        df_ventas, 
        df_directos, 
        left_on='COD_VENTA_LINK', 
        right_on='CODIGO VENTA', 
        how='inner'
    )

    # Cantidad total requerida de cada línea de la hoja Directos
    merged_1['Uso_Nivel_1'] = merged_1['CANT_VENDIDA'] * merged_1['CantReal']

    # 3. Clasificación: ¿Es ingrediente directo o procesado?
    es_procesado = merged_1['SKU'].str.startswith('PRO-', na=False)
    directos_finales = merged_1[~es_procesado].copy()
    a_explotar = merged_1[es_procesado].copy()

    # 4. Explosión Nivel 2: Lógica de Lotes (Procesados)
    if not a_explotar.empty:
        # CALCULO DE RENDIMIENTO DEL LOTE (BATCH YIELD)
        # Sumamos todas las CantReceta de un mismo "Codigo Venta" en la hoja Procesados
        rendimiento_lotes = df_procesados.groupby('Codigo Venta')['CantReceta'].sum().reset_index()
        rendimiento_lotes = rendimiento_lotes.rename(columns={'CantReceta': 'Rendimiento_Total_Batch'})

        # Unimos los ingredientes procesados con el rendimiento total de su batch
        df_p_ready = pd.merge(df_procesados, rendimiento_lotes, on='Codigo Venta', how='left')

        # Join con la necesidad que viene de Ventas/Directos
        explosion = pd.merge(
            a_explotar,
            df_p_ready,
            left_on='SKU',
            right_on='Codigo Venta',
            how='left',
            suffixes=('_dir', '_proc')
        )

        # APLICACIÓN DE LÓGICA PROPORCIONAL
        # Si Porcion == 0: Uso = (Necesidad_N1 / Rendimiento_Batch) * CantReceta_Ingrediente
        # Si Porcion != 0: Uso = Necesidad_N1 * CantReceta_Ingrediente
        def aplicar_proporcion(row):
            if pd.isna(row['CantReceta_proc']): return 0
            
            if row['Porcion'] == 0:
                # El sándwich pide X gramos de un lote que rinde Y gramos.
                factor_proporcion = row['Uso_Nivel_1'] / row['Rendimiento_Total_Batch']
                return factor_proporcion * row['CantReceta_proc']
            else:
                return row['Uso_Nivel_1'] * row['CantReceta_proc']

        explosion['Total_Calculado'] = explosion.apply(aplicar_proporcion, axis=1)
        
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

    # 5. Formatear Directos para Consolidación
    directos_finales = directos_finales[[
        'SKU', 'Ingrediente', 'Uso_Nivel_1', 'UM'
    ]].rename(columns={
        'Ingrediente': 'Nombre Ingrediente',
        'Uso_Nivel_1': 'Total Requerido'
    })

    # 6. Consolidación Final y Agrupación por SKU
    resultado = pd.concat([directos_finales, explosion_final], ignore_index=True)
    
    # Agrupar para sumar consumos de mismos ingredientes en distintos platos
    resumen = resultado.groupby(['SKU', 'Nombre Ingrediente', 'UM'], as_index=False)['Total Requerido'].sum()
    
    return resumen

# --- INTERFAZ DE USUARIO ---

st.title("📊 Calculador BOM de Producción (Lógica de Lotes)")
st.info("Este script detecta automáticamente si un ingrediente es un lote (Porción = 0) y prorratea el consumo.")

uploaded_file = st.file_uploader("Subir Prueba_costeo.xlsx", type=["xlsx"])

if uploaded_file:
    try:
        # Carga de Hojas
        xlsx = pd.ExcelFile(uploaded_file)
        df_v = pd.read_excel(xlsx, 'Ventas')
        df_d = pd.read_excel(xlsx, 'Directos')
        df_p = pd.read_excel(xlsx, 'Procesados')

        # Procesamiento
        df_final = process_bom(df_v, df_d, df_p)

        # Mostrar Resultados
        st.subheader("✅ Requerimiento Consolidado de Insumos")
        
        # Formateo para leer mejor los números grandes
        st.dataframe(
            df_final.style.format({"Total Requerido": "{:,.2f}"}), 
            use_container_width=True
        )

        # Descarga
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            df_final.to_excel(writer, index=False, sheet_name='Totales_Compra')
        
        st.download_button(
            label="📥 Descargar Excel de Resultados",
            data=output.getvalue(),
            file_name="requerimiento_insumos_ajustado.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

    except Exception as e:
        st.error(f"Se detectó un error en la estructura: {e}")
        st.warning("Verifica que las hojas se llamen: Ventas, Directos y Procesados.")
