import streamlit as st
import pandas as pd
import io

st.set_page_config(page_title="MRP Original - Full Procesados", layout="wide")

def process_bom(df_v, df_d, df_p):
    # 1. Limpieza de nombres de columnas
    for df in [df_v, df_d, df_p]:
        df.columns = df.columns.str.strip()

    # 2. Identificación de SKUs vendidos (para EsOpcion)
    skus_vendidos = set(df_v['SKU'].astype(str).str.strip().str.upper())
    df_v = df_v.rename(columns={'SKU': 'SKU_VENTA', 'Cantidad': 'CANT_VENTA'})

    # 3. Filtrado de Hoja Directos
    def validar_opcion(row):
        # Mantenemos lógica original: si es opción 4 o vacía, pasa. Si tiene SKU, debe estar en ventas.
        if pd.isna(row['EsOpcion']) or str(row['EsOpcion']).strip() in ["", "0", "4"]:
            return True
        return str(row['SKU']).strip().upper() in skus_vendidos

    df_d_ready = df_d[df_d.apply(validar_opcion, axis=1)].copy().rename(columns={
        'CODIGO VENTA': 'SKU_RECETA_PADRE', 
        'SKU': 'SKU_INSUMO', 
        'Ingrediente': 'NOM_INSUMO'
    })
    
    # 4. Cruzar Ventas con Directos (Nivel 1)
    m1 = pd.merge(df_v, df_d_ready, left_on='SKU_VENTA', right_on='SKU_RECETA_PADRE', how='inner')

    # 5. Separación de Insumos Directos vs Procesados (PRO-)
    es_proc = m1['SKU_INSUMO'].str.startswith('PRO-', na=False)
    df_dir_f = m1[~es_proc].copy()
    df_a_exp = m1[es_proc].copy()

    # 6. Explosión de Procesados (Nivel 2)
    if not df_a_exp.empty:
        # Renombramos columnas de la hoja Procesados para el cálculo
        df_p_ready = df_p.rename(columns={
            'Codigo Venta': 'SKU_PROC_PADRE', 
            'Ingrediente': 'NOM_ING_PROC',
            'SKU Ingrediente': 'SKU_ING_PROC', 
            'CantEfic': 'C_EFIC', 
            'CantReceta': 'C_RECETA',
            'UM Salida': 'UM_S'
        })
        
        m2 = pd.merge(df_a_exp, df_p_ready, left_on='SKU_INSUMO', right_on='SKU_PROC_PADRE', how='left')
        
        # --- LÓGICA MATEMÁTICA UNIVERSAL ---
        # Factor = Cantidad necesaria del insumo / Base de la receta
        # Total = Ventas * Cantidad pedida en el plato * Factor
        m2['FACTOR_INSUMO'] = m2['C_EFIC'] / m2['C_RECETA']
        m2['CANT_OUT'] = m2['CANT_VENTA'] * m2['CantReal'] * m2['FACTOR_INSUMO']
        
        exp_f = m2[['SKU_ING_PROC', 'NOM_ING_PROC', 'CANT_OUT', 'UM_S']].rename(
            columns={'SKU_ING_PROC': 'SKU_OUT', 'NOM_ING_PROC': 'ING_OUT', 'UM_S': 'UM_OUT'})
    else:
        exp_f = pd.DataFrame()

    # 7. Cálculo de Insumos Directos
    df_dir_f['CANT_OUT'] = df_dir_f['CANT_VENTA'] * df_dir_f['CantReal']
    dir_out = df_dir_f[['SKU_INSUMO', 'NOM_INSUMO', 'CANT_OUT', 'UM']].rename(
        columns={'SKU_INSUMO': 'SKU_OUT', 'NOM_INSUMO': 'ING_OUT', 'UM': 'UM_OUT'})
    
    # 8. Consolidación Final
    consolidado = pd.concat([dir_out, exp_f], ignore_index=True)
    
    # Normalización de texto
    consolidado['SKU_OUT'] = consolidado['SKU_OUT'].astype(str).str.strip().str.upper()
    consolidado['ING_OUT'] = consolidado['ING_OUT'].astype(str).str.strip()
    
    # Agrupar por SKU y UM
    resumen = consolidado.groupby(['SKU_OUT', 'UM_OUT'], as_index=False).agg({
        'CANT_OUT': 'sum', 
        'ING_OUT': 'first'
    })
    
    # Conversión a KG/L (dividir por 1000)
    resumen['TOTAL_FINAL'] = resumen['CANT_OUT'] / 1000
    
    return resumen[['SKU_OUT', 'ING_OUT', 'UM_OUT', 'TOTAL_FINAL']].rename(
        columns={
            'SKU_OUT': 'SKU', 
            'ING_OUT': 'Ingrediente', 
            'TOTAL_FINAL': 'Total (Kg/L/Un)', 
            'UM_OUT': 'UM Original'
        })

# --- INTERFAZ ---
st.title("📊 MRP Original - Sistema de Explosión Completo")
st.markdown("Este script procesa recetas unitarias (Pollo Panko) y por lotes (Mechada, Salsas).")

file = st.file_uploader("Cargar archivo Excel de Costeo", type="xlsx")

if file:
    try:
        xls = pd.ExcelFile(file)
        # Verificamos que existan las hojas necesarias
        if all(sheet in xls.sheet_names for sheet in ['Ventas', 'Directos', 'Procesados']):
            df_resultado = process_bom(
                pd.read_excel(xls, 'Ventas'), 
                pd.read_excel(xls, 'Directos'), 
                pd.read_excel(xls, 'Procesados')
            )
            
            st.subheader("📋 Requerimiento Consolidado")
            st.dataframe(df_resultado.style.format({"Total (Kg/L/Un)": "{:,.3f}"}), use_container_width=True)
            
            # Botón de Descarga
            buf = io.BytesIO()
            with pd.ExcelWriter(buf, engine='openpyxl') as writer:
                df_resultado.to_excel(writer, index=False)
            st.download_button("📥 Descargar Reporte en Excel", buf.getvalue(), "MRP_Consolidado.xlsx")
        else:
            st.error("El archivo debe contener las hojas: 'Ventas', 'Directos' y 'Procesados'.")
    except Exception as e:
        st.error(f"Error al procesar: {e}")
