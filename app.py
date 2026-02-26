import streamlit as st
import pandas as pd
import io

st.set_page_config(page_title="DataChef BOM: Senior Edition", layout="wide")

def process_bom(df_ventas, df_directos, df_procesados):
    # 1. Limpieza de nombres de columnas
    df_ventas.columns = df_ventas.columns.str.strip()
    df_directos.columns = df_directos.columns.str.strip()
    df_procesados.columns = df_procesados.columns.str.strip()

    # Normalización de nombres clave para evitar errores de mapeo
    df_ventas = df_ventas.rename(columns={'SKU': 'SKU_VENTA', 'Cantidad': 'CANT_VENDIDA'})
    
    # 2. Join Nivel 1: Ventas + Directos
    # Unimos el SKU vendido con el CODIGO VENTA de la receta del plato
    m1 = pd.merge(
        df_ventas, 
        df_directos, 
        left_on='SKU_VENTA', 
        right_on='CODIGO VENTA', 
        how='inner'
    )
    # Calculamos el requerimiento del ingrediente (o del procesado PRO-)
    m1['Requerimiento_N1'] = m1['CANT_VENDIDA'] * m1['CantReal']

    # 3. Separación: Insumos Directos vs Procesados
    es_procesado = m1['SKU'].str.startswith('PRO-', na=False)
    directos_finales = m1[~es_procesado].copy()
    a_explotar = m1[es_procesado].copy()

    # 4. Explosión Nivel 2: Lógica de Lotes (Batch Yield)
    if not a_explotar.empty:
        # Calculamos el rendimiento total de cada lote (Suma de CantReceta)
        rendimientos = df_procesados.groupby('Codigo Venta')['CantReceta'].sum().reset_index()
        rendimientos.columns = ['Codigo Venta', 'Total_Batch_Yield']

        # Unimos la tabla de procesados con su rendimiento total
        # Renombramos columnas antes del merge para evitar sufijos _x, _y
        df_p_ready = pd.merge(df_procesados, rendimientos, on='Codigo Venta')

        # Join con la necesidad de nivel 1
        m2 = pd.merge(
            a_explotar,
            df_p_ready,
            left_on='SKU', # El SKU 'PRO-05' en Directos
            right_on='Codigo Venta', # El ID del lote en Procesados
            how='left'
        )

        # Lógica de cálculo proporcional basada en el campo "Porcion"
        def calc_batch(row):
            if pd.isna(row['CantReceta']): return 0
            
            if row['Porcion'] == 0:
                # Caso Mayonesa: (Necesidad del plato / Rendimiento Lote) * CantReceta Ingrediente
                # Ejemplo Aceite: (554,400 / 5,380) * 5,000
                factor = row['Requerimiento_N1'] / row['Total_Batch_Yield']
                return factor * row['CantReceta']
            else:
                # Si no es lote, multiplicación directa
                return row['Requerimiento_N1'] * row['CantReceta']

        m2['Total_Final'] = m2.apply(calc_batch, axis=1)
        
        # Limpieza de columnas de salida (usamos 'SKU Ingrediente' y 'Ingrediente_y')
        # Nota: Pandas usa _y para la segunda tabla en el merge
        explosion_final = m2[['SKU Ingrediente', 'Ingrediente_y', 'Total_Final', 'UM_proc']].rename(columns={
            'SKU Ingrediente': 'SKU',
            'Ingrediente_y': 'Nombre Ingrediente',
            'Total_Final': 'Total Requerido',
            'UM_proc': 'UM'
        })
    else:
        explosion_final = pd.DataFrame()

    # 5. Formateo de Directos (Insumos que no son PRO-)
    directos_finales = directos_finales[['SKU', 'Ingrediente', 'Requerimiento_N1', 'UM']].rename(columns={
        'Ingrediente': 'Nombre Ingrediente',
        'Requerimiento_N1': 'Total Requerido'
    })

    # 6. Consolidación de ambos niveles
    resultado = pd.concat([directos_finales, explosion_final], ignore_index=True)
    
    # Agrupación final: Sumamos todo lo que tenga el mismo SKU
    resumen = resultado.groupby(['SKU', 'Nombre Ingrediente', 'UM'], as_index=False)['Total Requerido'].sum()
    
    return resumen

# --- INTERFAZ STREAMLIT ---
st.title("👨‍🍳 Senior BOM Processor: Lógica de Lotes")
st.markdown("Carga el archivo Excel con las pestañas: **Ventas**, **Directos** y **Procesados**.")

file = st.file_uploader("Archivo Excel", type=["xlsx"])

if file:
    try:
        xls = pd.ExcelFile(file)
        # Validación de hojas
        required = ['Ventas', 'Directos', 'Procesados']
        if all(s in xls.sheet_names for s in required):
            df_v = pd.read_excel(xls, 'Ventas')
            df_d = pd.read_excel(xls, 'Directos')
            df_p = pd.read_excel(xls, 'Procesados')

            # Procesar
            df_final = process_bom(df_v, df_d, df_p)

            # Mostrar tabla
            st.subheader("📋 Requerimiento de Insumos para Compra")
            st.dataframe(df_final.style.format({"Total Requerido": "{:,.2f}"}), use_container_width=True)

            # Descarga
            buf = io.BytesIO()
            with pd.ExcelWriter(buf, engine='openpyxl') as writer:
                df_final.to_excel(writer, index=False)
            st.download_button("📥 Descargar Reporte", buf.getvalue(), "requerimientos_produccion.xlsx")
            
        else:
            st.error(f"Error: Asegúrate de que el Excel tenga las hojas: {required}")
    except Exception as e:
        st.error(f"Error durante el proceso: {e}")
