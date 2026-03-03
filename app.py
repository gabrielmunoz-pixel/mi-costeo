import streamlit as st
import pandas as pd
import io

st.set_page_config(page_title="MRP Original - Fix Recetas Citadas", layout="wide")

def process_bom(df_v, df_d, df_p):
    # 1. Limpieza inicial de columnas
    for df in [df_v, df_d, df_p]:
        df.columns = df.columns.str.strip()

    # 2. Lista de SKUs vendidos
    skus_vendidos = set(df_v['SKU'].astype(str).str.strip().str.upper())

    # 3. Preparación de Ventas
    df_v = df_v.rename(columns={'SKU': 'SKU_VENTA', 'Cantidad': 'CANT_VENTA'})

    # 4. Filtrar Directos
    def validar_opcion(row):
        if str(row['EsOpcion']) == "4":
            return True
        if pd.isna(row['EsOpcion']) or str(row['EsOpcion']).strip() == "":
            return True
        return str(row['SKU']).strip().upper() in skus_vendidos

    df_d_filtrado = df_d[df_d.apply(validar_opcion, axis=1)].copy()
    
    df_d_ready = df_d_filtrado.rename(columns={
        'CODIGO VENTA': 'SKU_RECETA_PADRE', 
        'SKU': 'SKU_INSUMO', 
        'Ingrediente': 'NOM_INSUMO'
    })
    
    # 5. Nivel 1: Cruzar Ventas con Directos
    m1 = pd.merge(df_v, df_d_ready, left_on='SKU_VENTA', right_on='SKU_RECETA_PADRE', how='inner')
    
    # --- EXPLOSIÓN DE RECETA (LO QUE FALTABA) ---
    # Calculamos el requerimiento neto del plato (ej: 250g de Pollo Panko x 10 platos = 2500g)
    m1['REQ_CALC'] = m1['CANT_VENTA'] * m1['CantReal']
    
    # 6. Separar Procesados (PRO-)
    es_proc = m1['SKU_INSUMO'].str.startswith('PRO-', na=False)
    df_dir_f = m1[~es_proc].copy()
    df_a_exp = m1[es_proc].copy()

    # 7. Nivel 2: Explosión de Procesados (FIX MATEMÁTICO)
    if not df_a_exp.empty:
        df_p_ready = df_p.rename(columns={
            'Codigo Venta': 'SKU_PROC_PADRE',
            'Ingrediente': 'NOM_ING_PROC',
            'SKU Ingrediente': 'SKU_ING_PROC',
            'CantEfic': 'VALOR_EFIC',
            'UM': 'UM_PROC',
            'CantReceta': 'CANT_REC_PROC'
        })

        # Unimos las 2500g que necesitamos con la receta del procesado
        m2 = pd.merge(df_a_exp, df_p_ready, left_on='SKU_INSUMO', right_on='SKU_PROC_PADRE', how='inner')
        
        # CÁLCULO FINAL: (Requerimiento total del plato) * (Eficiencia / Base Receta)
        # Si el Pollo Panko usa Pechuga, aquí se hace la explosión real
        m2['TOTAL_FINAL'] = m2['REQ_CALC'] * (m2['VALOR_EFIC'] / m2['CANT_REC_PROC'])

        exp_f = m2[['SKU_ING_PROC', 'NOM_ING_PROC', 'TOTAL_FINAL', 'UM_PROC']].rename(
            columns={
                'SKU_ING_PROC': 'SKU_OUT', 
                'NOM_ING_PROC': 'ING_OUT', 
                'TOTAL_FINAL': 'CANT_OUT', 
                'UM_PROC': 'UM_OUT'
            })
    else:
        exp_f = pd.DataFrame()

    # 8. CONSOLIDACIÓN FINAL
    dir_out = df_dir_f[['SKU_INSUMO', 'NOM_INSUMO', 'REQ_CALC', 'UM']].rename(
        columns={'SKU_INSUMO': 'SKU_OUT', 'NOM_INSUMO': 'ING_OUT', 'REQ_CALC': 'CANT_OUT', 'UM': 'UM_OUT'})
    
    consolidado = pd.concat([dir_out, exp_f], ignore_index=True)
    consolidado['SKU_OUT'] = consolidado['SKU_OUT'].astype(str).str.strip().str.upper()

    resumen = consolidado.groupby(['SKU_OUT', 'UM_OUT'], as_index=False).agg({
        'CANT_OUT': 'sum',
        'ING_OUT': 'first'
    })

    # Convertimos a Kg al final
    resumen['TOTAL_KG_L'] = resumen['CANT_OUT'] / 1000
    
    return resumen[['SKU_OUT', 'ING_OUT', 'UM_OUT', 'TOTAL_KG_L']].rename(
        columns={
            'SKU_OUT': 'SKU', 
            'ING_OUT': 'Ingrediente', 
            'TOTAL_KG_L': 'Total (Kg/L)', 
            'UM_OUT': 'UM Original'
        }
    )

# --- INTERFAZ STREAMLIT ---
st.title("📊 MRP Gastronómico - Auditor de Combos")
file = st.file_uploader("Subir Archivo de Costeo (Excel)", type="xlsx")

if file:
    try:
        xls = pd.ExcelFile(file)
        resultado = process_bom(pd.read_excel(xls, 'Ventas'), pd.read_excel(xls, 'Directos'), pd.read_excel(xls, 'Procesados'))
        st.subheader("📋 Consolidado de Insumos")
        st.dataframe(resultado.style.format({"Total (Kg/L)": "{:,.3f}"}), use_container_width=True)
    except Exception as e:
        st.error(f"Error: {e}")
