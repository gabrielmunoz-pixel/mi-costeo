import streamlit as st
import pandas as pd
import io

st.set_page_config(page_title="MRP Gastronómico - Explosión", layout="wide")

def process_bom(df_v, df_d, df_p):
    # 1. Limpieza de nombres de columnas
    df_v.columns = df_v.columns.str.strip()
    df_d.columns = df_d.columns.str.strip()
    df_p.columns = df_p.columns.str.strip()

    # 2. Identificación de SKUs vendidos para filtrar opciones
    df_v = df_v.rename(columns={'SKU': 'SKU_VENTA', 'Cantidad': 'CANT_VENTA'})
    skus_vendidos = set(df_v['SKU_VENTA'].astype(str).str.strip().str.upper())

    # 3. Filtrado de Hoja Directos (Lógica EsOpcion)
    def validar_opcion(row):
        es_op = str(row['EsOpcion']).strip()
        if pd.isna(row['EsOpcion']) or es_op in ["", "0", "4"]:
            return True
        return str(row['SKU']).strip().upper() in skus_vendidos

    df_d_ready = df_d[df_d.apply(validar_opcion, axis=1)].copy()
    
    # 4. Unir Ventas con Recetas Directas
    m1 = pd.merge(df_v, df_d_ready, left_on='SKU_VENTA', right_on='CODIGO VENTA', how='inner')

    # 5. Separar Insumos Base de Procesados (PRO-)
    es_proc = m1['SKU'].str.startswith('PRO-', na=False)
    df_insumos_directos = m1[~es_proc].copy()
    df_procesados_a_explotar = m1[es_proc].copy()

    # 6. Explosión de Procesados con tu Lógica de Marcador (1=Porcion, 0=Lote)
    if not df_procesados_a_explotar.empty:
        # Cruce con la hoja de Procesados
        m2 = pd.merge(df_procesados_a_explotar, df_p, left_on='SKU', right_on='Codigo Venta', how='left')
        
        # AJUSTE SOLICITADO: Lógica de Porcionado vs Lote
        def calcular_explosion(row):
            # Si el marcador Porcion es 1, la CantEfic ya es por unidad (no se divide)
            if row['Porcion'] == 1:
                return row['CANT_VENTA'] * row['CantReal'] * row['CantEfic']
            # Si el marcador es 0, es un lote y dividimos por CantReceta
            else:
                factor = row['CantEfic'] / row['CantReceta'] if row['CantReceta'] > 0 else 0
                return row['CANT_VENTA'] * row['CantReal'] * factor

        m2['CANT_OUT'] = m2.apply(calcular_explosion, axis=1)
        
        exp_f = m2[['SKU Ingrediente', 'Ingrediente_y', 'CANT_OUT', 'UM Salida']].rename(
            columns={'SKU Ingrediente': 'SKU_OUT', 'Ingrediente_y': 'ING_OUT', 'UM Salida': 'UM_OUT'})
    else:
        exp_f = pd.DataFrame()

    # 7. Cálculo de Insumos Directos (Venta * CantReal)
    df_insumos_directos['CANT_OUT'] = df_insumos_directos['CANT_VENTA'] * df_insumos_directos['CantReal']
    dir_out = df_insumos_directos[['SKU', 'Ingrediente_x', 'CANT_OUT', 'UM']].rename(
        columns={'SKU': 'SKU_OUT', 'Ingrediente_x': 'ING_OUT', 'UM': 'UM_OUT'})
    
    # 8. Consolidación Final
    consolidado = pd.concat([dir_out, exp_f], ignore_index=True)
    consolidado['SKU_OUT'] = consolidado['SKU_OUT'].astype(str).str.strip().upper()
    
    # 9. Agrupar y Convertir a Kg/L/Un
    resumen = consolidado.groupby(['SKU_OUT', 'UM_OUT'], as_index=False).agg({'CANT_OUT': 'sum', 'ING_OUT': 'first'})
    
    def convertir_unidades(row):
        um = str(row['UM_OUT']).upper()
        if um in ['G', 'ML', 'CC']:
            return row['CANT_OUT'] / 1000
        return row['CANT_OUT']

    resumen['TOTAL_FINAL'] = resumen.apply(convertir_unidades, axis=1)
    
    return resumen[['SKU_OUT', 'ING_OUT', 'UM_OUT', 'TOTAL_FINAL']].rename(
        columns={'SKU_OUT': 'SKU', 'ING_OUT': 'Insumo', 'TOTAL_FINAL': 'Total', 'UM_OUT': 'UM'})

# --- INTERFAZ ---
st.title("👨‍🍳 MRP Sistema de Costeos Corregido")

file = st.file_uploader("Sube tu archivo Excel", type="xlsx")

if file:
    xls = pd.ExcelFile(file)
    resultado = process_bom(pd.read_excel(xls, 'Ventas'), pd.read_excel(xls, 'Directos'), pd.read_excel(xls, 'Procesados'))
    
    st.subheader("📋 Requerimiento Consolidado")
    st.dataframe(resultado.style.format({"Total": "{:,.3f}"}), use_container_width=True)
    
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        resultado.to_excel(writer, index=False)
    st.download_button("📥 Descargar MRP", output.getvalue(), "MRP_Final.xlsx")
