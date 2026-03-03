import streamlit as st
import pandas as pd
import io

st.set_page_config(page_title="MRP Pro v4.5 - Factor de Gasto", layout="wide")

def process_bom(df_v, df_d, df_p):
    # 1. Limpieza inicial
    for df in [df_v, df_d, df_p]:
        df.columns = df.columns.str.strip()
    
    # 2. Lista de SKUs realmente vendidos para filtrar opciones
    skus_vendidos = set(df_v['SKU'].astype(str).str.strip().str.upper())

    # 3. Filtro de Opciones: Solo pasan si son base o si el SKU individual está en ventas
    df_d_filtrado = df_d[df_d.apply(lambda r: True if (pd.isna(r['EsOpcion']) or str(r['EsOpcion']).strip() == "") 
                                    else str(r['SKU']).strip().upper() in skus_vendidos, axis=1)].copy()

    # 4. Preparar Ventas y Directos
    df_v_ready = df_v.rename(columns={'SKU': 'SKU_VENTA', 'Cantidad': 'CANT_VENTA'})
    df_d_ready = df_d_filtrado.rename(columns={
        'CODIGO VENTA': 'SKU_RECETA_PADRE', 
        'SKU': 'SKU_INSUMO', 
        'Ingrediente': 'NOM_INSUMO',
        'CantReal': 'CANT_EN_PLATO'
    })
    
    # Cruce Nivel 1
    m1 = pd.merge(df_v_ready, df_d_ready, left_on='SKU_VENTA', right_on='SKU_RECETA_PADRE', how='inner')
    m1['REQ_BASE'] = m1['CANT_VENTA'] * m1['CANT_EN_PLATO']

    # 5. LÓGICA DE PROCESADOS (PRO-) CON FACTOR DE GASTO
    es_proc = m1['SKU_INSUMO'].str.startswith('PRO-', na=False)
    df_dir_f = m1[~es_proc].copy()
    df_a_exp = m1[es_proc].copy()

    if not df_a_exp.empty:
        # Calculamos el factor de gasto para cada fila de la sub-receta
        # Ejemplo Mechada: 20000 / 10000 = Factor 2.0
        df_p['FACTOR_GASTO'] = df_p['CantEfic'] / df_p['CantReceta']
        
        m2 = pd.merge(df_a_exp, df_p, left_on='SKU_INSUMO', right_on='Codigo Venta', how='left')
        
        # El requerimiento final del insumo dentro del proceso es:
        # (Venta * Cantidad en Plato) * (Factor de Gasto del Insumo)
        m2['TOTAL_FINAL'] = m2['REQ_BASE'] * m2['FACTOR_GASTO']
        
        exp_f = m2[['SKU Ingrediente', 'Ingrediente_y', 'TOTAL_FINAL', 'UM_y']].rename(
            columns={
                'SKU Ingrediente': 'SKU_OUT', 
                'Ingrediente_y': 'ING_OUT', 
                'TOTAL_FINAL': 'CANT_OUT', 
                'UM_y': 'UM_OUT'
            })
    else:
        exp_f = pd.DataFrame()

    # 6. Consolidación Final y Normalización de Identidad
    dir_out = df_dir_f[['SKU_INSUMO', 'NOM_INSUMO', 'REQ_BASE', 'UM']].rename(
        columns={'SKU_INSUMO': 'SKU_OUT', 'NOM_INSUMO': 'ING_OUT', 'REQ_BASE': 'CANT_OUT', 'UM': 'UM_OUT'})
    
    consolidado = pd.concat([dir_out, exp_f], ignore_index=True)
    
    # Normalización para que "Aceite" y "aceite" con mismo SKU sean lo mismo
    consolidado['SKU_OUT'] = consolidado['SKU_OUT'].astype(str).str.strip().str.upper()
    consolidado['ING_OUT'] = consolidado['ING_OUT'].astype(str).str.strip()
    consolidado['UM_OUT'] = consolidado['UM_OUT'].astype(str).str.strip().str.upper()

    # Agrupar y Sumar todo en Kilos/Litros
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
st.title("👨‍🍳 MRP Gastronómico Pro v4.5")
st.info("Nueva lógica aplicada: La Mechada ahora se calcula por factor de gasto directo (CantEfic / CantReceta).")

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
            
            st.subheader("📋 Requerimiento de Compras (Normalizado)")
            st.dataframe(resultado.style.format({"Total (Kg/L)": "{:,.3f}"}), use_container_width=True)
            
            buf = io.BytesIO()
            with pd.ExcelWriter(buf, engine='openpyxl') as writer:
                resultado.to_excel(writer, index=False)
            st.download_button("📥 Descargar Reporte en Kg/L", buf.getvalue(), "requerimiento_final.xlsx")
        else:
            st.error("Asegúrate de que el Excel tenga las hojas: Ventas, Directos y Procesados.")
    except Exception as e:
        st.error(f"Error técnico: {e}")
