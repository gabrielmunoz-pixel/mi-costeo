import streamlit as st
import pandas as pd
import io

st.set_page_config(page_title="MRP Gastronómico - Explosión Total", layout="wide")

def process_bom(df_v, df_d, df_p):
    # 1. Limpieza de nombres de columnas y datos
    df_v.columns = df_v.columns.str.strip()
    df_d.columns = df_d.columns.str.strip()
    df_p.columns = df_p.columns.str.strip()

    # 2. Preparar Ventas
    df_v = df_v.rename(columns={'SKU': 'SKU_VENTA', 'Cantidad': 'CANT_VENTA'})
    skus_vendidos = set(df_v['SKU_VENTA'].astype(str).str.strip().str.upper())

    # 3. Lógica EsOpcion
    def validar_opcion(row):
        es_op = str(row['EsOpcion']).strip()
        if pd.isna(row['EsOpcion']) or es_op in ["", "0", "4"]:
            return True
        return str(row['SKU']).strip().upper() in skus_vendidos

    df_d_ready = df_d[df_d.apply(validar_opcion, axis=1)].copy()
    
    # 4. Cruzar Ventas con Directos
    m1 = pd.merge(df_v, df_d_ready, left_on='SKU_VENTA', right_on='CODIGO VENTA', how='inner')

    # 5. Separar Insumos Directos de Procesados
    es_proc = m1['SKU'].str.startswith('PRO-', na=False)
    df_insumos_directos = m1[~es_proc].copy()
    df_procesados_a_explotar = m1[es_proc].copy()

    # 6. EXPLOSIÓN DE PROCESADOS (Con cálculo automático de Total Receta)
    if not df_procesados_a_explotar.empty:
        # --- AJUSTE: CÁLCULO INTERNO DE SUMAR.SI ---
        # Creamos una tabla temporal que suma las CantEfic por cada Codigo Venta
        rendimientos = df_p.groupby('Codigo Venta')['CantEfic'].sum().reset_index()
        rendimientos = rendimientos.rename(columns={'CantEfic': 'TOTAL_RECETA_AUTO'})

        # Preparamos la hoja de Procesados con nombres únicos
        df_p_clean = df_p.rename(columns={
            'Codigo Venta': 'COD_P',
            'Ingrediente': 'NOM_P',
            'CantEfic': 'CE_P',
            'CantReceta': 'CR_P', # Mantenemos esta por si acaso, pero usaremos la calculada
            'Porcion': 'MARK_P',
            'UM Salida': 'UM_P',
            'SKU Ingrediente': 'SKU_P'
        })

        # Unimos Procesados con sus rendimientos calculados
        df_p_final = pd.merge(df_p_clean, rendimientos, left_on='COD_P', right_on='Codigo Venta', how='left')

        # Unimos las ventas con esta nueva tabla de procesados robusta
        m2 = pd.merge(df_procesados_a_explotar, df_p_final, left_on='SKU', right_on='COD_P', how='left')
        
        # Lógica del Marcador: 1=Porción, 0=Lote
        def calcular_m2(row):
            if row['MARK_P'] == 1:
                return row['CANT_VENTA'] * row['CantReal'] * row['CE_P']
            else:
                # Caso Lote: Usamos TOTAL_RECETA_AUTO (el SUMAR.SI interno) como divisor
                divisor = row['TOTAL_RECETA_AUTO'] if row['TOTAL_RECETA_AUTO'] > 0 else 1
                return row['CANT_VENTA'] * row['CantReal'] * (row['CE_P'] / divisor)

        m2['CANT_OUT'] = m2.apply(calcular_m2, axis=1)
        
        exp_f = m2[['SKU_P', 'NOM_P', 'CANT_OUT', 'UM_P']].rename(
            columns={'SKU_P': 'SKU_FIN', 'NOM_P': 'ING_FIN', 'UM_P': 'UM_FIN'})
    else:
        exp_f = pd.DataFrame()

    # 7. INSUMOS DIRECTOS
    df_insumos_directos['CANT_OUT'] = df_insumos_directos['CANT_VENTA'] * df_insumos_directos['CantReal']
    dir_out = df_insumos_directos[['SKU', 'Ingrediente', 'CANT_OUT', 'UM']].rename(
        columns={'SKU': 'SKU_FIN', 'Ingrediente': 'ING_FIN', 'UM': 'UM_FIN'})
    
    # 8. Consolidación
    consolidado = pd.concat([dir_out, exp_f], ignore_index=True)
    
    # 9. Agrupación y Conversión
    resumen = consolidado.groupby(['SKU_FIN', 'UM_FIN'], as_index=False).agg({'CANT_OUT': 'sum', 'ING_FIN': 'first'})
    
    def formatear(row):
        um = str(row['UM_FIN']).upper()
        if um in ['G', 'ML', 'CC']:
            return row['CANT_OUT'] / 1000
        return row['CANT_OUT']

    resumen['TOTAL'] = resumen.apply(formatear, axis=1)
    
    return resumen[['SKU_FIN', 'ING_FIN', 'UM_FIN', 'TOTAL']].rename(
        columns={'SKU_FIN': 'SKU', 'ING_FIN': 'Insumo', 'UM_FIN': 'UM', 'TOTAL': 'Total Kg/L/Un'})

# --- UI (Mantenida igual) ---
st.title("👨‍🍳 MRP Sistema de Costeos - Versión Final Blindada")
file = st.file_uploader("Sube tu archivo Excel", type="xlsx")

if file:
    try:
        xls = pd.ExcelFile(file)
        res = process_bom(pd.read_excel(xls, 'Ventas'), pd.read_excel(xls, 'Directos'), pd.read_excel(xls, 'Procesados'))
        st.subheader("📋 Resultados")
        st.dataframe(res.style.format({"Total Kg/L/Un": "{:,.3f}"}), use_container_width=True)
        buffer = io.BytesIO()
        with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
            res.to_excel(writer, index=False)
        st.download_button("📥 Descargar MRP", buffer.getvalue(), "MRP_Final.xlsx")
    except Exception as e:
        st.error(f"Error detectado: {e}")
