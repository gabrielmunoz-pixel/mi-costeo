import streamlit as st
import pandas as pd
import io

st.set_page_config(page_title="BOM Processor v4.2 - Modular Pro", layout="wide")

def process_bom(df_v, df_d, df_p):
    # 1. Limpieza de nombres de columnas y espacios
    for df in [df_v, df_d, df_p]:
        df.columns = df.columns.str.strip()
    
    # 2. Crear "Lista de la Verdad" de lo que realmente se vendió
    # Esto incluye tanto el Código de Plato como los SKUs individuales de opciones
    skus_vendidos = set(df_v['SKU'].astype(str).str.strip().str.upper())

    # 3. Filtrar la hoja de Directos antes de procesar
    def validar_presencia_opcion(row):
        # Si NO es opción (celda vacía), es un ingrediente BASE: se queda.
        if pd.isna(row['EsOpcion']) or str(row['EsOpcion']).strip() == "":
            return True
        
        # Si SÍ es opción (1, 2, 3, 6), solo se queda si su SKU está en la hoja de Ventas
        sku_insumo = str(row['SKU']).strip().upper()
        return sku_insumo in skus_vendidos

    df_d_filtrado = df_d[df_d.apply(validar_presencia_opcion, axis=1)].copy()

    # 4. Preparar DataFrames para el cruce
    df_v_ready = df_v.rename(columns={'SKU': 'SKU_VENTA', 'Cantidad': 'CANT_VENTA'})
    df_d_ready = df_d_filtrado.rename(columns={
        'CODIGO VENTA': 'SKU_RECETA_PADRE', 
        'SKU': 'SKU_INSUMO', 
        'Ingrediente': 'NOM_INSUMO'
    })
    
    # 5. Nivel 1: Cruzar Ventas con Directos (Solo los que pasaron el filtro)
    m1 = pd.merge(df_v_ready, df_d_ready, left_on='SKU_VENTA', right_on='SKU_RECETA_PADRE', how='inner')
    m1['REQ_N1'] = m1['CANT_VENTA'] * m1['CantReal']

    # 6. Nivel 2: Separar y procesar sub-recetas (PRO-)
    es_proc = m1['SKU_INSUMO'].str.startswith('PRO-', na=False)
    df_dir_f = m1[~es_proc].copy()
    df_a_exp = m1[es_proc].copy()

    if not df_a_exp.empty:
        df_p_ready = df_p.rename(columns={
            'Codigo Venta': 'SKU_PROC_PADRE',
            'Ingrediente': 'NOM_ING_PROC',
            'SKU Ingrediente': 'SKU_ING_PROC',
            'CantEfic': 'VALOR_EFIC',
            'UM': 'UM_PROC'
        })

        # Cálculo de rendimiento de lote (CantEfic)
        yields = df_p_ready.groupby('SKU_PROC_PADRE')['VALOR_EFIC'].sum().reset_index().rename(columns={'VALOR_EFIC': 'YIELD_LOTE'})
        df_p_final = pd.merge(df_p_ready, yields, on='SKU_PROC_PADRE')
        
        m2 = pd.merge(df_a_exp, df_p_final, left_on='SKU_INSUMO', right_on='SKU_PROC_PADRE', how='left')
        
        def calc_batch(r):
            if pd.isna(r['VALOR_EFIC']) or r['YIELD_LOTE'] == 0: return 0
            if r['Porcion'] == 0:
                return (r['REQ_N1'] / r['YIELD_LOTE']) * r['VALOR_EFIC']
            return r['REQ_N1'] * r['VALOR_EFIC']

        m2['TOTAL_FINAL'] = m2.apply(calc_batch, axis=1)
        
        exp_f = m2[['SKU_ING_PROC', 'NOM_ING_PROC', 'TOTAL_FINAL', 'UM_PROC']].rename(
            columns={'SKU_ING_PROC': 'SKU_OUT', 'NOM_ING_PROC': 'ING_OUT', 'TOTAL_FINAL': 'CANT_OUT', 'UM_PROC': 'UM_OUT'})
    else:
        exp_f = pd.DataFrame()

    # 7. Consolidación Final y Normalización
    dir_out = df_dir_f[['SKU_INSUMO', 'NOM_INSUMO', 'REQ_N1', 'UM']].rename(
        columns={'SKU_INSUMO': 'SKU_OUT', 'NOM_INSUMO': 'ING_OUT', 'REQ_N1': 'CANT_OUT', 'UM': 'UM_OUT'})
    
    consolidado = pd.concat([dir_out, exp_f], ignore_index=True)
    
    # Normalización de SKUs y Nombres para evitar duplicados por texto
    consolidado['SKU_OUT'] = consolidado['SKU_OUT'].astype(str).str.strip().str.upper()
    consolidado['ING_OUT'] = consolidado['ING_OUT'].astype(str).str.strip()
    consolidado['UM_OUT'] = consolidado['UM_OUT'].astype(str).str.strip().str.upper()

    # Agrupación final: SUMA de cantidades por SKU único
    resumen = consolidado.groupby(['SKU_OUT', 'UM_OUT'], as_index=False).agg({
        'CANT_OUT': 'sum',
        'ING_OUT': 'first'
    })

    # Conversión a Kilos/Litros
    resumen['TOTAL_KG_L'] = resumen['CANT_OUT'] / 1000
    
    return resumen[['SKU_OUT', 'ING_OUT', 'UM_OUT', 'TOTAL_KG_L']].rename(
        columns={'SKU_OUT': 'SKU', 'ING_OUT': 'Ingrediente', 'TOTAL_KG_L': 'Total (Kg/L)', 'UM_OUT': 'UM Original'}
    )

# --- INTERFAZ DE USUARIO ---
st.title("📊 MRP Gastronómico Pro v4.2")
st.markdown("""
Esta versión utiliza un **Discriminador de Opciones**:
- Ingredientes **Base**: Se calculan siempre.
- Ingredientes con **EsOpcion (1,2,3,6)**: Solo se calculan si su SKU individual está en la hoja de Ventas.
""")

uploaded_file = st.file_uploader("Subir Excel de Costeo", type="xlsx")

if uploaded_file:
    try:
        xls = pd.ExcelFile(uploaded_file)
        required = ['Ventas', 'Directos', 'Procesados']
        if all(h in xls.sheet_names for h in required):
            # Cargar datos
            df_v = pd.read_excel(xls, 'Ventas')
            df_d = pd.read_excel(xls, 'Directos')
            df_p = pd.read_excel(xls, 'Procesados')
            
            # Procesar
            resultado = process_bom(df_v, df_d, df_p)
            
            # Mostrar resultados
            st.subheader("📋 Consolidado de Requerimientos")
            st.dataframe(resultado.style.format({"Total (Kg/L)": "{:,.3f}"}), use_container_width=True)
            
            # Botón de Descarga
            buf = io.BytesIO()
            with pd.ExcelWriter(buf, engine='openpyxl') as writer:
                resultado.to_excel(writer, index=False)
            st.download_button(
                label="📥 Descargar Reporte en Excel",
                data=buf.getvalue(),
                file_name="requerimiento_compras_modular.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
        else:
            st.error(f"Faltan hojas. El archivo debe tener: {required}")
    except Exception as e:
        st.error(f"Error al procesar el archivo: {e}")
