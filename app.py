import streamlit as st
import pandas as pd
import io

st.set_page_config(page_title="BOM Processor v4.0 - Modular", layout="wide")

def process_bom(df_v, df_d, df_p):
    # 1. Limpieza de nombres de columnas
    for df in [df_v, df_d, df_p]:
        df.columns = df.columns.str.strip()

    # 2. Preparación de Ventas
    df_v = df_v.rename(columns={'SKU': 'SKU_VENTA', 'Cantidad': 'CANT_VENTA'})

    # 3. Nivel 1: Cruzar Ventas con Directos (Estructura Modular)
    # Aquí unimos por Código de Venta. Se traerán todos los ingredientes (Base + Opciones)
    df_d_ready = df_d.rename(columns={
        'CODIGO VENTA': 'SKU_RECETA_PADRE', 
        'SKU': 'SKU_INSUMO', 
        'Ingrediente': 'NOM_INSUMO'
    })
    
    m1 = pd.merge(df_v, df_d_ready, left_on='SKU_VENTA', right_on='SKU_RECETA_PADRE', how='inner')
    
    # Calculamos requerimiento inicial (Venta * CantReal)
    # Nota: Si el usuario vendió 100 HAC-002, el script calculará 100 de cada opción listada.
    m1['REQ_N1'] = m1['CANT_VENTA'] * m1['CantReal']

    # 4. Separar Procesados (PRO-)
    es_proc = m1['SKU_INSUMO'].str.startswith('PRO-', na=False)
    df_dir_f = m1[~es_proc].copy()
    df_a_exp = m1[es_proc].copy()

    # 5. Nivel 2: Explosión de Procesados (Lógica CantEfic)
    if not df_a_exp.empty:
        df_p_ready = df_p.rename(columns={
            'Codigo Venta': 'SKU_PROC_PADRE',
            'Ingrediente': 'NOM_ING_PROC',
            'SKU Ingrediente': 'SKU_ING_PROC',
            'CantEfic': 'VALOR_EFIC',
            'UM': 'UM_PROC'
        })

        # Rendimiento del Lote por suma de CantEfic (Cimiento técnico del costeo)
        yields = df_p_ready.groupby('SKU_PROC_PADRE')['VALOR_EFIC'].sum().reset_index().rename(columns={'VALOR_EFIC': 'YIELD_LOTE'})
        df_p_final = pd.merge(df_p_ready, yields, on='SKU_PROC_PADRE')
        
        m2 = pd.merge(df_a_exp, df_p_final, left_on='SKU_INSUMO', right_on='SKU_PROC_PADRE', how='left')
        
        def calc_batch(r):
            if pd.isna(r['VALOR_EFIC']): return 0
            # Lógica de Batch vs Porción Directa
            if r['Porcion'] == 0:
                return (r['REQ_N1'] / r['YIELD_LOTE']) * r['VALOR_EFIC']
            return r['REQ_N1'] * r['VALOR_EFIC']

        m2['TOTAL_FINAL'] = m2.apply(calc_batch, axis=1)
        
        exp_f = m2[['SKU_ING_PROC', 'NOM_ING_PROC', 'TOTAL_FINAL', 'UM_PROC']].rename(
            columns={'SKU_ING_PROC': 'SKU_OUT', 'NOM_ING_PROC': 'ING_OUT', 'TOTAL_FINAL': 'CANT_OUT', 'UM_PROC': 'UM_OUT'})
    else:
        exp_f = pd.DataFrame()

    # 6. Consolidación Final y Conversión
    dir_out = df_dir_f[['SKU_INSUMO', 'NOM_INSUMO', 'REQ_N1', 'UM']].rename(
        columns={'SKU_INSUMO': 'SKU_OUT', 'NOM_INSUMO': 'ING_OUT', 'REQ_N1': 'CANT_OUT', 'UM': 'UM_OUT'})
    
    consolidado = pd.concat([dir_out, exp_f], ignore_index=True)
    
    # Agrupamos por Insumo para sumar usos en diferentes recetas
    resumen = consolidado.groupby(['SKU_OUT', 'ING_OUT', 'UM_OUT'], as_index=False)['CANT_OUT'].sum()
    
    # CONVERSIÓN A KILOS/LITROS (División por 1000)
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
st.title("📊 MRP Modular: Explosión de Recetas")
st.markdown("Este sistema calcula el requerimiento total considerando cada ingrediente y opción como una unidad de receta.")

file = st.file_uploader("Subir Archivo de Costeo (Excel)", type="xlsx")

if file:
    try:
        xls = pd.ExcelFile(file)
        required = ['Ventas', 'Directos', 'Procesados']
        if all(h in xls.sheet_names for h in required):
            resultado = process_bom(
                pd.read_excel(xls, 'Ventas'), 
                pd.read_excel(xls, 'Directos'), 
                pd.read_excel(xls, 'Procesados')
            )
            
            st.subheader("📋 Consolidado de Insumos (Kilos/Litros)")
            st.dataframe(resultado.style.format({"Total (Kg/L)": "{:,.3f}"}), use_container_width=True)
            
            buf = io.BytesIO()
            with pd.ExcelWriter(buf, engine='openpyxl') as writer:
                resultado.to_excel(writer, index=False)
            st.download_button("📥 Descargar Reporte en Kg/L", buf.getvalue(), "requerimiento_modular_Kilos.xlsx")
        else:
            st.error(f"El archivo debe contener las hojas: {required}")
    except Exception as e:
        st.error(f"Error técnico detectado: {e}")
