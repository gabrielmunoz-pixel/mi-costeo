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

    # 4. Filtrar Directos (Mantenemos valor 4 siempre)
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
    
    # --- LÓGICA DE EXPLOSIÓN PARA VALOR 4 ---
    es_link = m1['EsOpcion'].astype(str) == "4"
    
    # Separamos insumos normales
    df_normal = m1[~es_link].copy()
    df_normal['REQ_CALC'] = df_normal['CANT_VENTA'] * df_normal['CantReal']
    
    # Separamos platos citados (valor 4)
    df_links = m1[es_link].copy()
    
    if not df_links.empty:
        df_recetas_base = df_d_ready[df_d_ready['EsOpcion'].astype(str) != "4"]
        m_recursivo = pd.merge(df_links, df_recetas_base, left_on='SKU_INSUMO', right_on='SKU_RECETA_PADRE', how='inner')
        m_recursivo['REQ_CALC'] = m_recursivo['CANT_VENTA'] * m_recursivo['CantReal_y']
        
        df_links_final = m_recursivo[['SKU_INSUMO_y', 'NOM_INSUMO_y', 'REQ_CALC', 'UM_y']].rename(
            columns={'SKU_INSUMO_y': 'SKU_INSUMO', 'NOM_INSUMO_y': 'NOM_INSUMO', 'UM_y': 'UM'}
        )
    else:
        df_links_final = pd.DataFrame()

    # Unificamos todo en una sola lista base
    df_unificado = pd.concat([
        df_normal[['SKU_INSUMO', 'NOM_INSUMO', 'REQ_CALC', 'UM']], 
        df_links_final
    ], ignore_index=True)

    # 6. Separar Procesados (PRO-)
    es_proc = df_unificado['SKU_INSUMO'].str.startswith('PRO-', na=False)
    df_dir_f = df_unificado[~es_proc].copy()
    df_a_exp = df_unificado[es_proc].copy()

    # 7. Nivel 2: Explosión de Procesados (AQUÍ ESTÁ LA CORRECCIÓN)
    if not df_a_exp.empty:
        df_p_ready = df_p.rename(columns={
            'Codigo Venta': 'SKU_PROC_PADRE',
            'Ingrediente': 'NOM_ING_PROC',
            'SKU Ingrediente': 'SKU_ING_PROC',
            'CantEfic': 'VALOR_EFIC',
            'CantReceta': 'CANT_REC_PROC'
        })

        # Unimos lo que necesitamos (REQ_CALC) con la receta de procesados
        m2 = pd.merge(df_a_exp, df_p_ready, left_on='SKU_INSUMO', right_on='SKU_PROC_PADRE', how='inner')
        
        # LA CLAVE: Cantidad Cruda = Requerimiento Neto * (Eficiencia / Base Receta)
        # Si REQ_CALC es 1500g (10 platos de 150g) y el factor es 1.25, debe dar 1875g.
        m2['TOTAL_FINAL'] = m2['REQ_CALC'] * (m2['VALOR_EFIC'] / m2['CANT_REC_PROC'])

        exp_f = m2[['SKU_ING_PROC', 'NOM_ING_PROC', 'TOTAL_FINAL', 'UM_Salida']].rename(
            columns={
                'SKU_ING_PROC': 'SKU_OUT', 
                'NOM_ING_PROC': 'ING_OUT', 
                'TOTAL_FINAL': 'CANT_OUT', 
                'UM_Salida': 'UM_OUT'
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

    # Dividimos por 1000 solo al final para el reporte
    resumen['TOTAL_KG_L'] = resumen['CANT_OUT'] / 1000
    
    return resumen[['SKU_OUT', 'ING_OUT', 'UM_OUT', 'TOTAL_KG_L']].rename(
        columns={'SKU_OUT': 'SKU', 'ING_OUT': 'Ingrediente', 'TOTAL_KG_L': 'Total (Kg/L)', 'UM_OUT': 'UM Original'}
    )

# --- INTERFAZ STREAMLIT ---
st.title("📊 MRP Gastronómico - Auditor de Combos2")
file = st.file_uploader("Subir Archivo de Costeo (Excel)", type="xlsx")

if file:
    try:
        xls = pd.ExcelFile(file)
        resultado = process_bom(pd.read_excel(xls, 'Ventas'), pd.read_excel(xls, 'Directos'), pd.read_excel(xls, 'Procesados'))
        st.subheader("📋 Consolidado de Insumos")
        st.dataframe(resultado.style.format({"Total (Kg/L)": "{:,.3f}"}), use_container_width=True)
    except Exception as e:
        st.error(f"Error: {e}")
