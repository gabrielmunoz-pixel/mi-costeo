import streamlit as st
import pandas as pd
import io

st.set_page_config(page_title="MRP Original - Fix Panko", layout="wide")

def process_bom(df_v, df_d, df_p):
    # 1. Limpieza inicial de columnas
    for df in [df_v, df_d, df_p]:
        df.columns = df.columns.str.strip()

    # 2. Lista de SKUs vendidos
    skus_vendidos = set(df_v['SKU'].astype(str).str.strip().str.upper())

    # 3. Preparación de Ventas
    df_v = df_v.rename(columns={'SKU': 'SKU_VENTA', 'Cantidad': 'CANT_VENTA'})

    # 4. Filtrar Directos según EsOpcion (Tu lógica original)
    def validar_opcion(row):
        if pd.isna(row['EsOpcion']) or str(row['EsOpcion']).strip() in ["", "0"]:
            return True
        if str(row['EsOpcion']).strip() == "4":
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

    # 6. Separar Procesados (PRO-)
    es_proc = m1['SKU_INSUMO'].str.startswith('PRO-', na=False)
    df_dir_f = m1[~es_proc].copy()
    df_a_exp = m1[es_proc].copy()

    # 7. Nivel 2: Explosión de Procesados (FIX ESPECÍFICO)
    if not df_a_exp.empty:
        df_p_ready = df_p.rename(columns={
            'Codigo Venta': 'SKU_PROC_PADRE',
            'Ingrediente': 'NOM_ING_PROC',
            'SKU Ingrediente': 'SKU_ING_PROC',
            'CantEfic': 'CANT_EFIC_RECETA',
            'UM': 'UM_PROC',
            'CantReceta': 'BASE_RECETA'
        })

        m2 = pd.merge(df_a_exp, df_p_ready, left_on='SKU_INSUMO', right_on='SKU_PROC_PADRE', how='left')
        
        # EL CÁLCULO CORRECTO:
        # Si el plato pide "1 unidad" de Pollo Panko, el requerimiento es:
        # Venta * CantEfic_del_insumo (los 55,56g de panko o 297g de pollo)
        m2['TOTAL_FINAL'] = m2['CANT_VENTA'] * m2['CANT_EFIC_RECETA']

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
    # Insumos directos (no procesados)
    df_dir_f['CANT_OUT'] = df_dir_f['CANT_VENTA'] * df_dir_f['CantReal']
    dir_out = df_dir_f[['SKU_INSUMO', 'NOM_INSUMO', 'CANT_OUT', 'UM']].rename(
        columns={'SKU_INSUMO': 'SKU_OUT', 'NOM_INSUMO': 'ING_OUT', 'UM': 'UM_OUT'})
    
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

# --- INTERFAZ STREAMLIT ---
st.title("📊 MRP Gastronómico - Fix Final Panko")

file = st.file_uploader("Subir Archivo de Costeo (Excel)", type="xlsx")

if file:
    try:
        xls = pd.ExcelFile(file)
        resultado = process_bom(pd.read_excel(xls, 'Ventas'), pd.read_excel(xls, 'Directos'), pd.read_excel(xls, 'Procesados'))
        
        st.subheader("📋 Consolidado de Insumos")
        st.dataframe(resultado.style.format({"Total (Kg/L)": "{:,.3f}"}), use_container_width=True)
        
        buf = io.BytesIO()
        with pd.ExcelWriter(buf, engine='openpyxl') as writer:
            resultado.to_excel(writer, index=False)
        st.download_button("📥 Descargar Reporte", buf.getvalue(), "requerimiento_final.xlsx")
        
    except Exception as e:
        st.error(f"Error técnico: {e}")
