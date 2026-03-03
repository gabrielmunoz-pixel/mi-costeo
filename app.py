import streamlit as st
import pandas as pd
import io

st.set_page_config(page_title="MRP Gastronómico - Explosión Total", layout="wide")

def process_bom(df_v, df_d, df_p):
    # 1. Limpieza de nombres de columnas para evitar errores de espacios
    df_v.columns = df_v.columns.str.strip()
    df_d.columns = df_d.columns.str.strip()
    df_p.columns = df_p.columns.str.strip()

    # 2. Preparar Ventas y obtener SKUs para lógica de opciones
    df_v = df_v.rename(columns={'SKU': 'SKU_VENTA', 'Cantidad': 'CANT_VENTA'})
    skus_vendidos = set(df_v['SKU_VENTA'].astype(str).str.strip().str.upper())

    # 3. Filtrar Hoja Directos (Lógica EsOpcion que ya tenías)
    def validar_opcion(row):
        es_op = str(row['EsOpcion']).strip()
        if pd.isna(row['EsOpcion']) or es_op in ["", "0", "4"]:
            return True
        return str(row['SKU']).strip().upper() in skus_vendidos

    df_d_ready = df_d[df_d.apply(validar_opcion, axis=1)].copy()
    
    # 4. Cruzar Ventas con Directos (Nivel 1 de la receta)
    # Nota: Aquí pandas creará sufijos _x e _y si hay nombres duplicados
    m1 = pd.merge(df_v, df_d_ready, left_on='SKU_VENTA', right_on='CODIGO VENTA', how='inner')

    # 5. Separar Insumos Directos de los Procesados (PRO-)
    es_proc = m1['SKU'].str.startswith('PRO-', na=False)
    df_insumos_directos = m1[~es_proc].copy()
    df_procesados_a_explotar = m1[es_proc].copy()

    # 6. Explosión de Procesados (Nivel 2) con Lógica de Marcador "Porcion"
    if not df_procesados_a_explotar.empty:
        # Unimos con la hoja Procesados. 
        # Importante: Usamos sufijos claros para no perdernos con los nombres
        m2 = pd.merge(df_procesados_a_explotar, df_p, left_on='SKU', right_on='Codigo Venta', how='left', suffixes=('_dir', '_proc'))
        
        # CÁLCULO SEGÚN TU MARCADOR: 1 = Unitario / 0 = Lote
        def calcular_m2(row):
            # Porcion_proc es el marcador de la hoja Procesados
            if row['Porcion_proc'] == 1:
                # Caso Pollo Panko: Usa la CantEfic directa por cada unidad vendida
                return row['CANT_VENTA'] * row['CantReal'] * row['CantEfic_proc']
            else:
                # Caso Mechada: Divide el total del lote (CantEfic) por lo que rinde (CantReceta)
                divisor = row['CantReceta_proc'] if row['CantReceta_proc'] > 0 else 1
                factor_unitario = row['CantEfic_proc'] / divisor
                return row['CANT_VENTA'] * row['CantReal'] * factor_unitario

        m2['CANT_OUT'] = m2.apply(calcular_m2, axis=1)
        
        # Seleccionamos y normalizamos nombres para el concat final
        exp_f = m2[['SKU Ingrediente', 'Ingrediente_proc', 'CANT_OUT', 'UM Salida']].rename(
            columns={'SKU Ingrediente': 'SKU_FIN', 'Ingrediente_proc': 'ING_FIN', 'UM Salida': 'UM_FIN'})
    else:
        exp_f = pd.DataFrame()

    # 7. Insumos Directos (Cálculo simple: Ventas * Cantidad en Directos)
    df_insumos_directos['CANT_OUT'] = df_insumos_directos['CANT_VENTA'] * df_insumos_directos['CantReal']
    # Aquí 'Ingrediente' viene de la hoja Directos
    dir_out = df_insumos_directos[['SKU', 'Ingrediente', 'CANT_OUT', 'UM']].rename(
        columns={'SKU': 'SKU_FIN', 'Ingrediente': 'ING_FIN', 'UM': 'UM_FIN'})
    
    # 8. Consolidación Final de ambas ramas
    consolidado = pd.concat([dir_out, exp_f], ignore_index=True)
    consolidado['SKU_FIN'] = consolidado['SKU_FIN'].astype(str).str.strip().upper()
    
    # 9. Agrupar resultados y convertir a Kg/L si corresponde
    resumen = consolidado.groupby(['SKU_FIN', 'UM_FIN'], as_index=False).agg({
        'CANT_OUT': 'sum', 
        'ING_FIN': 'first'
    })
    
    def formatear_totales(row):
        um = str(row['UM_FIN']).upper()
        # Si es G, ML o CC, dividimos por 1000 para entregar Kg o Litros
        if um in ['G', 'ML', 'CC']:
            return row['CANT_OUT'] / 1000
        return row['CANT_OUT']

    resumen['TOTAL_FINAL'] = resumen.apply(formatear_totales, axis=1)
    
    # Retornamos el DataFrame con los nombres de columna limpios
    return resumen[['SKU_FIN', 'ING_FIN', 'UM_FIN', 'TOTAL_FINAL']].rename(
        columns={
            'SKU_FIN': 'SKU', 
            'ING_FIN': 'Ingrediente', 
            'UM_FIN': 'UM Original', 
            'TOTAL_FINAL': 'Total Requerido (Kg/L/Un)'
        })

# --- Bloque de Interfaz Streamlit ---
st.title("👨‍🍳 MRP Sistema de Costeos - Lógica Corregida")
st.markdown("Cálculo basado en marcador **Porcion** (1 = Unitario, 0 = Lote).")

uploaded_file = st.file_uploader("Sube tu archivo Excel con las 3 hojas (Ventas, Directos, Procesados)", type="xlsx")

if uploaded_file:
    try:
        xls = pd.ExcelFile(uploaded_file)
        # Procesamos las tres hojas
        resultado = process_bom(
            pd.read_excel(xls, 'Ventas'), 
            pd.read_excel(xls, 'Directos'), 
            pd.read_excel(xls, 'Procesados')
        )
        
        st.subheader("📋 Resumen de Compra / Producción")
        st.dataframe(resultado.style.format({"Total Requerido (Kg/L/Un)": "{:,.3f}"}), use_container_width=True)
        
        # Botón para descargar en Excel
        buffer = io.BytesIO()
        with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
            resultado.to_excel(writer, index=False)
        st.download_button(
            label="📥 Descargar Reporte MRP",
            data=buffer.getvalue(),
            file_name="MRP_Consolidado.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
    except Exception as e:
        st.error(f"Error al procesar el archivo: {e}")
