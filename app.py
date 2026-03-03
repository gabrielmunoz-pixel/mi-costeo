import streamlit as st
import pandas as pd
import io

st.set_page_config(page_title="MRP Gastronómico", layout="wide")

def process_bom(df_v, df_d, df_p):
    # 1. Limpieza de columnas
    df_v.columns = df_v.columns.str.strip()
    df_d.columns = df_d.columns.str.strip()
    df_p.columns = df_p.columns.str.strip()

    # 2. Preparar Ventas (Mantenemos tus nombres)
    df_v = df_v.rename(columns={'SKU': 'SKU_VENTA', 'Cantidad': 'CANT_VENTA'})
    skus_vendidos = set(df_v['SKU_VENTA'].astype(str).str.strip().str.upper())

    # 3. Lógica EsOpcion
    def validar_opcion(row):
        es_op = str(row['EsOpcion']).strip()
        if pd.isna(row['EsOpcion']) or es_op in ["", "0", "4"]:
            return True
        return str(row['SKU']).strip().upper() in skus_vendidos

    df_d_ready = df_d[df_d.apply(validar_opcion, axis=1)].copy()
    
    # 4. Primer Merge (Ventas x Directos)
    m1 = pd.merge(df_v, df_d_ready, left_on='SKU_VENTA', right_on='CODIGO VENTA', how='inner')

    # 5. Separar Procesados de Insumos Directos
    es_proc = m1['SKU'].str.startswith('PRO-', na=False)
    df_insumos_directos = m1[~es_proc].copy()
    df_procesados_a_explotar = m1[es_proc].copy()

    # 6. EXPLOSIÓN DE PROCESADOS (Aquí está el arreglo del marcador)
    if not df_procesados_a_explotar.empty:
        # Unimos con la hoja Procesados
        m2 = pd.merge(df_procesados_a_explotar, df_p, left_on='SKU', right_on='Codigo Venta', how='left')
        
        def calcular_m2(row):
            # Lógica: 1 es porción unitaria, 0 es lote
            if row['Porcion_y'] == 1:
                return row['CANT_VENTA'] * row['CantReal'] * row['CantEfic']
            else:
                divisor = row['CantReceta_y'] if row['CantReceta_y'] > 0 else 1
                return row['CANT_VENTA'] * row['CantReal'] * (row['CantEfic'] / divisor)

        m2['CANT_OUT'] = m2.apply(calcular_m2, axis=1)
        
        # Seleccionamos columnas usando los nombres de la hoja Procesados
        exp_f = m2[['SKU Ingrediente', 'Ingrediente_y', 'CANT_OUT', 'UM Salida']].rename(
            columns={'SKU Ingrediente': 'SKU_FINAL', 'Ingrediente_y': 'NOM_FINAL', 'UM Salida': 'UM_FINAL'})
    else:
        exp_f = pd.DataFrame()

    # 7. INSUMOS DIRECTOS (Usamos los nombres de la hoja Directos)
    df_insumos_directos['CANT_OUT'] = df_insumos_directos['CANT_VENTA'] * df_insumos_directos['CantReal']
    dir_out = df_insumos_directos[['SKU', 'Ingrediente', 'CANT_OUT', 'UM']].rename(
        columns={'SKU': 'SKU_FINAL', 'Ingrediente': 'NOM_FINAL', 'UM': 'UM_FINAL'})
    
    # 8. Consolidación
    consolidado = pd.concat([dir_out, exp_f], ignore_index=True)
    
    # 9. Agrupar y convertir a Kg/L
    resumen = consolidado.groupby(['SKU_FINAL', 'UM_FINAL'], as_index=False).agg({
        'CANT_OUT': 'sum', 
        'NOM_FINAL': 'first'
    })
    
    def format_unidades(row):
        um = str(row['UM_FINAL']).upper()
        if um in ['G', 'ML', 'CC']:
            return row['CANT_OUT'] / 1000
        return row['CANT_OUT']

    resumen['TOTAL'] = resumen.apply(format_unidades, axis=1)
    
    return resumen[['SKU_FINAL', 'NOM_FINAL', 'UM_FINAL', 'TOTAL']].rename(
        columns={'SKU_FINAL': 'SKU', 'NOM_FINAL': 'Ingrediente', 'TOTAL': 'Total Kg/L/Un', 'UM_FINAL': 'UM'})

# --- Ejecución Streamlit ---
file = st.file_uploader("Sube tu Excel", type="xlsx")
if file:
    xls = pd.ExcelFile(file)
    df_final = process_bom(pd.read_excel(xls, 'Ventas'), pd.read_excel(xls, 'Directos'), pd.read_excel(xls, 'Procesados'))
    st.dataframe(df_final.style.format({"Total Kg/L/Un": "{:,.3f}"}), use_container_width=True)
