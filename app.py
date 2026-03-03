import streamlit as st
import pandas as pd
import io

st.set_page_config(page_title="MRP Original - Fix Mechada", layout="wide")

def process_bom(df_v, df_d, df_p):
    # 1. Limpieza inicial de columnas (Tu lógica original)
    for df in [df_v, df_d, df_p]:
        df.columns = df.columns.str.strip()

    # 2. Lista de SKUs vendidos para el discriminador de opciones
    skus_vendidos = set(df_v['SKU'].astype(str).str.strip().str.upper())

    # 3. Preparación de Ventas
    df_v = df_v.rename(columns={'SKU': 'SKU_VENTA', 'Cantidad': 'CANT_VENTA'})

    # 4. Filtrar Directos según EsOpcion (Tu lógica original de validación)
    def validar_opcion(row):
        if pd.isna(row['EsOpcion']) or str(row['EsOpcion']).strip() == "":
            return True
        # Valor 4 se mantiene para ser procesado luego como link
        if str(row['EsOpcion']) == "4":
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
    
    # --- AJUSTE SOLICITADO: Lógica para Valor 4 (Citar Plato) ---
    # Identificamos filas que citan a otro plato (EsOpcion 4)
    es_link = m1['EsOpcion'].astype(str) == "4"
    
    # Insumos normales
    m1_normal = m1[~es_link].copy()
    m1_normal['REQ_N1'] = m1_normal['CANT_VENTA'] * m1_normal['CantReal']
    
    # Insumos que son links (Recursión de 1 nivel)
    m1_links = m1[es_link].copy()
    if not m1_links.empty:
        # Buscamos en la tabla de directos la receta del SKU citado
        m_citado = pd.merge(m1_links, df_d_ready, left_on='SKU_INSUMO', right_on='SKU_RECETA_PADRE', how='inner')
        m_citado['REQ_N1'] = m_citado['CANT_VENTA'] * m_citado['CantReal_y']
        
        # Unificamos columnas para que coincidan con la estructura original
        m1_links_final = m_citado[['SKU_INSUMO_y', 'NOM_INSUMO_y', 'REQ_N1', 'UM_y']].rename(
            columns={'SKU_INSUMO_y': 'SKU_INSUMO', 'NOM_INSUMO_y': 'NOM_INSUMO', 'UM_y': 'UM'}
        )
    else:
        m1_links_final = pd.DataFrame()

    # Combinamos ambos resultados antes de pasar a procesados
    df_base_calculo = pd.concat([
        m1_normal[['SKU_INSUMO', 'NOM_INSUMO', 'REQ_N1', 'UM']], 
        m1_links_final
    ], ignore_index=True)
    # ------------------------------------------------------------

    # 6. Separar Procesados (PRO-)
    es_proc = df_base_calculo['SKU_INSUMO'].str.startswith('PRO-', na=False)
    df_dir_f = df_base_calculo[~es_proc].copy()
    df_a_exp = df_base_calculo[es_proc].copy()

    # 7. Nivel 2: Explosión de Procesados
    if not df_a_exp.empty:
        df_p_ready = df_p.rename(columns={
            'Codigo Venta': 'SKU_PROC_PADRE',
            'Ingrediente': 'NOM_ING_PROC',
            'SKU Ingrediente': 'SKU_ING_PROC',
            'CantEfic': 'VALOR_EFIC',
            'UM': 'UM_PROC',
            'CantReceta': 'CANT_REC_PROC'
        })

        m2 = pd.merge(df_a_exp, df_p_ready, left_on='SKU_INSUMO', right_on='SKU_PROC_PADRE', how='left')
        
        m2['FACTOR_GASTO'] = m2['VALOR_EFIC'] / m2['CANT_REC_PROC']
        m2['TOTAL_FINAL'] = m2['REQ_N1'] * m2['FACTOR_GASTO']

        exp_f = m2[['SKU_ING_PROC', 'NOM_ING_PROC', 'TOTAL_FINAL', 'UM_PROC']].rename(
            columns={
                'SKU_ING_PROC': 'SKU_OUT', 
                'NOM_ING_PROC': 'ING_OUT', 
                'TOTAL_FINAL': 'CANT_OUT', 
                'UM_PROC': 'UM_OUT'
            })
    else:
        exp_f = pd.DataFrame()

    # 8. CONSOLIDACIÓN Y NORMALIZACIÓN
    dir_out = df_dir_f[['SKU_INSUMO', 'NOM_INSUMO', 'REQ_N1', 'UM']].rename(
        columns={'SKU_INSUMO': 'SKU_OUT', 'NOM_INSUMO': 'ING_OUT', 'REQ_N1': 'CANT_OUT', 'UM': 'UM_OUT'})
    
    consolidado = pd.concat([dir_out, exp_f], ignore_index=True)

    consolidado['SKU_OUT'] = consolidado['SKU_OUT'].astype(str).str.strip().str.upper()
    consolidado['ING_OUT'] = consolidado['ING_OUT'].astype(str).str.strip()
    consolidado['UM_OUT'] = consolidado['UM_OUT'].astype(str).str.strip().str.upper()

    resumen = consolidado.groupby(['SKU_OUT', 'UM_OUT'], as_index=False).agg({
        'CANT_OUT': 'sum',
        'ING_OUT': 'first'
    })

    resumen['TOTAL_KG_L'] = resumen['CANT_OUT'] / 1000
    
    return resumen[['SKU_OUT', 'ING_OUT', 'UM_OUT', 'TOTAL_KG_L']].rename(
        columns={
            'SKU_OUT': 'SKU', 
            'ING_OUT': 'Ingrediente', 
            'TOTAL_KG_L': 'Total (Kg/L)', 
            'UM_OUT': 'UM Original'
        }
    )

# --- INTERFAZ STREAMLIT ORIGINAL ---
st.title("📊 MRP Gastronómico - Cálculo de Merma Corregido")

file = st.file_uploader("Subir Archivo de Costeo (Excel)", type="xlsx")

if file:
    try:
        xls = pd.ExcelFile(file)
        if all(h in xls.sheet_names for h in ['Ventas', 'Directos', 'Procesados']):
            resultado = process_bom(
                pd.read_excel(xls, 'Ventas'), 
                pd.read_excel(xls, 'Directos'), 
                pd.read_excel(xls, 'Procesados')
            )
            
            st.subheader("📋 Consolidado de Insumos")
            st.dataframe(resultado.style.format({"Total (Kg/L)": "{:,.3f}"}), use_container_width=True)
            
            buf = io.BytesIO()
            with pd.ExcelWriter(buf, engine='openpyxl') as writer:
                resultado.to_excel(writer, index=False)
            st.download_button("📥 Descargar Reporte", buf.getvalue(), "requerimiento_final.xlsx")
        else:
            st.error("Faltan hojas en el Excel (Ventas, Directos, Procesados).")
    except Exception as e:
        st.error(f"Error técnico: {e}")
