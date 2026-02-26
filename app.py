import streamlit as st
import pandas as pd
import io

st.set_page_config(page_title="DataChef BOM: Senior Edition", layout="wide")

def process_bom(df_ventas, df_directos, df_procesados):
    # 1. Limpieza de espacios en blanco en los nombres de columnas
    df_ventas.columns = df_ventas.columns.str.strip()
    df_directos.columns = df_directos.columns.str.strip()
    df_procesados.columns = df_procesados.columns.str.strip()

    # 2. Join Nivel 1: Ventas + Directos
    # Unimos 'SKU' (Ventas) con 'CODIGO VENTA' (Directos)
    m1 = pd.merge(
        df_ventas, 
        df_directos, 
        left_on='SKU', 
        right_on='CODIGO VENTA', 
        how='inner'
    )
    # Requerimiento = Cantidad (de Ventas) * CantReal (de Directos)
    m1['Requerimiento_N1'] = m1['Cantidad'] * m1['CantReal']

    # 3. Clasificación: Directos vs Procesados (PRO-)
    # Usamos la columna SKU de la hoja Directos (que en m1 es 'SKU_y' si no se limpia, 
    # pero al ser inner join sobre SKU_x y CODIGO VENTA, usaremos 'SKU_y')
    # Para evitar confusión, forzamos el nombre de la columna SKU de Directos
    es_procesado = m1['SKU_y'].str.startswith('PRO-', na=False)
    directos_finales = m1[~es_procesado].copy()
    a_explotar = m1[es_procesado].copy()

    # 4. Explosión Nivel 2: Lógica de Lotes (Batch Yield)
    if not a_explotar.empty:
        # Calculamos el rendimiento total sumando 'CantReceta' de la hoja Procesados
        # Agrupamos por 'Codigo Venta' de la hoja Procesados
        yield_lotes = df_procesados.groupby('Codigo Venta')['CantReceta'].sum().reset_index()
        yield_lotes.columns = ['Codigo Venta', 'Rendimiento_Total_Lote']

        # Unimos la tabla de procesados con su rendimiento
        df_p_ready = pd.merge(df_procesados, yield_lotes, on='Codigo Venta')

        # Join con la necesidad de nivel 1
        m2 = pd.merge(
            a_explotar,
            df_p_ready,
            left_on='SKU_y', # El código PRO-05
            right_on='Codigo Venta', # El ID en la hoja procesados
            how='left'
        )

        # Lógica de cálculo proporcional (Batch)
        def calc_batch(row):
            if pd.isna(row['CantReceta']): return 0
            # Si Porcion == 0, es un lote
            if row['Porcion'] == 0:
                factor = row['Requerimiento_N1'] / row['Rendimiento_Total_Lote']
                return factor * row['CantReceta']
            else:
                return row['Requerimiento_N1'] * row['CantReceta']

        m2['Total_Final'] = m2.apply(calc_batch, axis=1)
        
        # Selección de columnas finales de la hoja Procesados
        # SKU Ingrediente, Ingrediente (de la hoja procesados), Total_Final, UM (de procesados)
        explosion_final = m2[['SKU Ingrediente', 'Ingrediente', 'Total_Final', 'UM_y']].rename(columns={
            'SKU Ingrediente': 'SKU',
            'Ingrediente': 'Nombre Ingrediente',
            'Total_Final': 'Total Requerido',
            'UM_y': 'UM'
        })
    else:
        explosion_final = pd.DataFrame()

    # 5. Formateo de Directos
    # SKU (de directos), Ingrediente (de directos), Requerimiento_N1, UM (de directos)
    directos_finales = directos_finales[['SKU_y', 'Ingrediente_x', 'Requerimiento_N1', 'UM_x']].rename(columns={
        'SKU_y': 'SKU',
        'Ingrediente_x': 'Nombre Ingrediente',
        'Requerimiento_N1': 'Total Requerido',
        'UM_x': 'UM'
    })

    # 6. Consolidación
    resultado = pd.concat([directos_finales, explosion_final], ignore_index=True)
    resumen = resultado.groupby(['SKU', 'Nombre Ingrediente', 'UM'], as_index=False)['Total Requerido'].sum()
    
    return resumen

# --- INTERFAZ ---
st.title("👨‍🍳 Procesador BOM Senior (Estructura Fija)")

file = st.file_uploader("Subir Excel", type=["xlsx"])

if file:
    try:
        xls = pd.ExcelFile(file)
        df_v = pd.read_excel(xls, 'Ventas')
        df_d = pd.read_excel(xls, 'Directos')
        df_p = pd.read_excel(xls, 'Procesados')

        df_res = process_bom(df_v, df_d, df_p)

        st.subheader("📋 Consolidado de Insumos")
        st.dataframe(df_res.style.format({"Total Requerido": "{:,.2f}"}), use_container_width=True)

        buf = io.BytesIO()
        with pd.ExcelWriter(buf, engine='openpyxl') as writer:
            df_res.to_excel(writer, index=False)
        st.download_button("📥 Descargar Resultados", buf.getvalue(), "reporte_bom.xlsx")
    except Exception as e:
        st.error(f"Error: {e}")
