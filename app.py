import streamlit as st
import pandas as pd
import numpy as np
import io
from sqlalchemy import create_engine, text
from datetime import datetime, date

# ============================================================
# CONFIGURACIÓN
# ============================================================
st.set_page_config(
    page_title="MRP Gastronómico",
    page_icon="🍽️",
    layout="wide",
    initial_sidebar_state="expanded"
)

st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=DM+Serif+Display&family=DM+Sans:wght@300;400;500;600&display=swap');

    html, body, [class*="css"] {
        font-family: 'DM Sans', sans-serif;
    }

    .main { background-color: #0f0f0f; color: #f0ede8; }
    .stApp { background-color: #0f0f0f; }

    section[data-testid="stSidebar"] {
        background-color: #1a1a1a;
        border-right: 1px solid #2a2a2a;
    }

    .block-container { padding-top: 2rem; }

    h1, h2, h3 {
        font-family: 'DM Serif Display', serif;
        color: #f0ede8;
        letter-spacing: -0.02em;
    }

    .metric-card {
        background: #1a1a1a;
        border: 1px solid #2a2a2a;
        border-radius: 12px;
        padding: 1.2rem 1.5rem;
        margin-bottom: 0.5rem;
    }
    .metric-card .label {
        font-size: 0.75rem;
        text-transform: uppercase;
        letter-spacing: 0.1em;
        color: #888;
        margin-bottom: 0.3rem;
    }
    .metric-card .value {
        font-family: 'DM Serif Display', serif;
        font-size: 1.8rem;
        color: #d4a853;
    }

    .tag-green  { background:#1a3a2a; color:#4caf7d; padding:3px 10px; border-radius:20px; font-size:0.8rem; }
    .tag-orange { background:#3a2a1a; color:#e89c45; padding:3px 10px; border-radius:20px; font-size:0.8rem; }
    .tag-red    { background:#3a1a1a; color:#e84545; padding:3px 10px; border-radius:20px; font-size:0.8rem; }

    .stDataFrame { border-radius: 10px; overflow: hidden; }

    div[data-testid="stMetric"] {
        background: #1a1a1a;
        border: 1px solid #2a2a2a;
        border-radius: 12px;
        padding: 1rem 1.2rem;
    }
    div[data-testid="stMetric"] label { color: #888 !important; font-size: 0.8rem; }
    div[data-testid="stMetric"] div[data-testid="stMetricValue"] { color: #d4a853 !important; }

    .stButton>button {
        background: #d4a853;
        color: #0f0f0f;
        font-weight: 600;
        border: none;
        border-radius: 8px;
        padding: 0.5rem 1.5rem;
        transition: all 0.2s;
    }
    .stButton>button:hover { background: #e8c07a; transform: translateY(-1px); }

    .stSelectbox>div>div, .stDateInput>div>div>input {
        background: #1a1a1a !important;
        border-color: #2a2a2a !important;
        color: #f0ede8 !important;
    }

    .section-title {
        font-family: 'DM Serif Display', serif;
        font-size: 1.6rem;
        color: #f0ede8;
        border-bottom: 1px solid #2a2a2a;
        padding-bottom: 0.5rem;
        margin-bottom: 1.5rem;
    }

    .info-box {
        background: #1a1f2e;
        border-left: 3px solid #d4a853;
        border-radius: 0 8px 8px 0;
        padding: 0.8rem 1rem;
        margin: 0.5rem 0 1rem 0;
        font-size: 0.88rem;
        color: #aaa;
    }
</style>
""", unsafe_allow_html=True)


# ============================================================
# TABLA DE CONVERSIÓN INVENTARIO (desde Tablas 1 en INV_AJUSTE.xlsx)
# Fórmula: total_kg = Total × convertor_porcion
# ============================================================
TABLA_CONV_INV = {
    'CHULETA KASSLER':                                              {'control': 'CHULETA KASSLER',          'porcion': 1.0,    'cocido': 1.0},
    'COSTILLAS':                                                    {'control': 'COSTILLAS',                'porcion': 1.0,    'cocido': 0.75},
    'JAMÓN':                                                        {'control': 'JAMÓN',                    'porcion': 1.0,    'cocido': 1.0},
    'LOMO DE CENTRO':                                               {'control': 'LOMO DE CENTRO',           'porcion': 1.0,    'cocido': 0.7},
    'LOMO DE CENTRO(PORCIONADAS)':                                  {'control': 'LOMO DE CENTRO',           'porcion': 0.18,   'cocido': 1.0},
    'PANCETA LAMINADA':                                             {'control': 'PANCETA LAMINADA',         'porcion': 1.0,    'cocido': 0.5},
    'DESPUNTE PECHUGA DE POLLO':                                    {'control': 'PECHUGA DE POLLO',         'porcion': 1.0,    'cocido': 1.0},
    'PECHUGA DE POLLO':                                             {'control': 'PECHUGA DE POLLO',         'porcion': 1.0,    'cocido': 0.8},
    'PECHUGA DE POLLO  - FRITAS ESPECIALES ( 90 GR - PRECOCIDO)':  {'control': 'PECHUGA DE POLLO',         'porcion': 0.09,   'cocido': 1.0},
    'PECHUGA DE POLLO - AVE - SANDWICH-  (200 GR - PRECOCIDO)':    {'control': 'PECHUGA DE POLLO',         'porcion': 0.2,    'cocido': 1.0},
    'PECHUGA DE POLLO - CESAR ( 150 GR - PRECOCIDO)':              {'control': 'PECHUGA DE POLLO',         'porcion': 0.16,   'cocido': 1.0},
    'PECHUGA DE POLLO - PANKO - PLATO (250 GR  - CRUDO)':          {'control': 'PECHUGA DE POLLO',         'porcion': 0.25,   'cocido': 1.0},
    'PECHUGA DE POLLO - PANKO - ENSALADA - NIÑO (160 GR - CRUDO)': {'control': 'PECHUGA DE POLLO',         'porcion': 0.16,   'cocido': 1.0},
    'PECHUGA DE POLLO - TIMBAL ( 160 GR - PRECOCIDO)':             {'control': 'PECHUGA DE POLLO',         'porcion': 0.16,   'cocido': 1.0},
    'PERNIL':                                                       {'control': 'PERNIL',                   'porcion': 1.0,    'cocido': 1.0},
    'PERNIL(PORCIONADAS)':                                          {'control': 'PERNIL',                   'porcion': 0.16,   'cocido': 1.0},
    'TOCINO AHUMADO':                                               {'control': 'TOCINO AHUMADO',           'porcion': 1.0,    'cocido': 1.0},
    'FILETE':                                                       {'control': 'FILETE',                   'porcion': 1.0,    'cocido': 1.0},
    'GRASA DE WAGYU':                                               {'control': 'GRASA DE WAGYU',           'porcion': 1.0,    'cocido': 1.0},
    'LOMO LISO':                                                    {'control': 'LOMO LISO',                'porcion': 1.0,    'cocido': 1.0},
    'LOMO VETADO':                                                  {'control': 'LOMO VETADO',              'porcion': 1.0,    'cocido': 1.0},
    'PLATEADA':                                                     {'control': 'PLATEADA',                 'porcion': 1.0,    'cocido': 0.5},
    'PLATEADA(PORCIONADAS)':                                        {'control': 'PLATEADA',                 'porcion': 0.3,    'cocido': 1.0},
    'DESPUNTE CARNE ROJA':                                          {'control': 'POSTA',                    'porcion': 1.0,    'cocido': 1.0},
    'ESCALOPA':                                                     {'control': 'POSTA',                    'porcion': 0.12,   'cocido': 1.0},
    'HAMBURGUESA GRAN EXPERTO':                                     {'control': 'POSTA',                    'porcion': 0.2,    'cocido': 1.0},
    'HAMBURGUESA NIÑO':                                             {'control': 'POSTA',                    'porcion': 0.15,   'cocido': 1.0},
    'HAMBURGUESAS':                                                 {'control': 'POSTA',                    'porcion': 0.2,    'cocido': 1.0},
    'POSTA':                                                        {'control': 'POSTA',                    'porcion': 1.0,    'cocido': 1.0},
    'POSTA - FRITAS ESPECIALES ( 90 GR - CRUDO)':                  {'control': 'POSTA',                    'porcion': 0.09,   'cocido': 1.0},
    'PAPAS FRITAS':                                                 {'control': 'PAPAS FRITAS',             'porcion': 1.0,    'cocido': 1.0},
    'QUESO CHEDDAR':                                                {'control': 'QUESO CHEDDAR',            'porcion': 1.0,    'cocido': 1.0},
    'QUESO PARMESANO':                                              {'control': 'QUESO PARMESANO',          'porcion': 1.0,    'cocido': 1.0},
    'QUESO RANCO':                                                  {'control': 'QUESO RANCO',              'porcion': 1.0,    'cocido': 1.0},
    'FRICA 14 CMS':                                                 {'control': 'FRICA 14 CMS',             'porcion': 1.0,    'cocido': 1.0},
    'HOT - DOG 19 CM.':                                             {'control': 'HOT - DOG 19 CM.',         'porcion': 1.0,    'cocido': 1.0},
    'MOLDE BANQUETE':                                               {'control': 'MOLDE BANQUETE',           'porcion': 1.0,    'cocido': 1.0},
    'MOLDE BANQUETE INTEGRAL':                                      {'control': 'MOLDE BANQUETE INTEGRAL',  'porcion': 1.0,    'cocido': 1.0},
    'PAN FRICA 12 CM':                                              {'control': 'PAN FRICA 12 CM',          'porcion': 1.0,    'cocido': 1.0},
    'PAN FRICA N8':                                                 {'control': 'PAN FRICA N8',             'porcion': 1.0,    'cocido': 1.0},
    'ATUN':                                                         {'control': 'ATUN',                     'porcion': 1.0,    'cocido': 1.0},
    'CAMARON':                                                      {'control': 'CAMARON',                  'porcion': 1.0,    'cocido': 0.31},
    'CAMARON APANADO':                                              {'control': 'CAMARON APANADO',          'porcion': 1.0,    'cocido': 1.0},
    'ERIZOS':                                                       {'control': 'ERIZOS',                   'porcion': 1.0,    'cocido': 1.0},
    'FILETE SALMON':                                                {'control': 'FILETE SALMON',            'porcion': 1.0,    'cocido': 1.0},
    'LOCOS':                                                        {'control': 'LOCOS',                    'porcion': 0.5,    'cocido': 1.0},
    'LOCOS(DRENADOS)':                                              {'control': 'LOCOS',                    'porcion': 1.0,    'cocido': 1.0},
    'SALMON SLICE LAMINADO':                                        {'control': 'SALMON SLICE LAMINADO',    'porcion': 1.0,    'cocido': 1.0},
    'LECHUGA HIDROPONICA':                                          {'control': 'LECHUGA VERDE',            'porcion': 1.0,    'cocido': 1.0},
    'MIX DE LECHUGA':                                               {'control': 'MIX DE LECHUGA',           'porcion': 0.8,    'cocido': 1.0},
    'PALTA':                                                        {'control': 'PALTA',                    'porcion': 1.0,    'cocido': 1.0},
    'TOMATE':                                                       {'control': 'TOMATE',                   'porcion': 1.0,    'cocido': 1.0},
}

# Mapeo TIPO de BAR → producto_control del informe
TIPO_BAR_CONTROL = {
    'SCHOP':    'SCHOP',
    'PULPAS':   'JUGOS',
    # El resto (AGUAS, BEBIDAS, CERVEZAS, ESPUMANTE, VINOS) no son productos de control
    # pero se guardan con su TIPO como control para trazabilidad
}

# ============================================================
# TABLA DE CONVERSIÓN — hardcodeada, no cambia
# conv_cocido: divisor del cocido
# conv_porcion: multiplicador final
# Fórmula: total_kg = (crudo + produccion + cocido/conv_cocido) * conv_porcion
# ============================================================
CONV_TABLE = {
    # producto_control           conv_cocido  conv_porcion
    'CHULETA KASSLER':          (1.0,         1.0),
    'COSTILLAS':                (0.75,        1.0),
    'JAMÓN':                    (1.0,         1.0),
    'LOMO DE CENTRO':           (1.0,         1.0),
    'LOMO DE CENTRO_PORC':      (1.0,         0.18),   # LOMO DE CENTRO(PORCIONADAS)
    'PANCETA LAMINADA':         (0.5,         1.0),
    'PECHUGA DE POLLO':         (0.8,         1.0),
    'PECHUGA DE POLLO_F90':     (1.0,         0.1125), # FRITAS 90GR
    'PECHUGA DE POLLO_AVE':     (1.0,         0.25),   # AVE SANDWICH 200GR
    'PECHUGA DE POLLO_CES':     (1.0,         0.2),    # CESAR 150GR
    'PECHUGA DE POLLO_PKP':     (1.0,         0.25),   # PANKO PLATO 250GR
    'PECHUGA DE POLLO_PKE':     (1.0,         0.16),   # PANKO ENSALADA 160GR
    'PECHUGA DE POLLO_TIM':     (1.0,         0.2),    # TIMBAL 160GR
    'PERNIL':                   (1.0,         1.0),
    'PERNIL_PORC':              (1.0,         0.16),   # PERNIL(PORCIONADAS)
    'TOCINO AHUMADO':           (1.0,         1.0),
    'FILETE':                   (1.0,         1.0),
    'GRASA DE WAGYU':           (1.0,         1.0),
    'LOMO LISO':                (1.0,         1.0),
    'LOMO VETADO':              (1.0,         1.0),
    'PLATEADA':                 (0.5,         1.0),
    'PLATEADA_PORC':            (1.0,         0.315789474), # PLATEADA(PORCIONADAS)
    'POSTA':                    (1.0,         1.0),
    'POSTA_ESC':                (1.0,         0.12),   # ESCALOPA
    'POSTA_HGE':                (1.0,         0.2),    # HAMBURGUESA GRAN EXPERTO
    'POSTA_HNI':                (1.0,         0.15),   # HAMBURGUESA NIÑO
    'POSTA_HAM':                (1.0,         0.2),    # HAMBURGUESAS
    'POSTA_F90':                (1.0,         0.09),   # FRITAS 90GR
    'PAPAS FRITAS':             (1.0,         1.0),
    'QUESO CHEDDAR':            (1.0,         1.0),
    'QUESO PARMESANO':          (1.0,         1.0),
    'QUESO RANCO':              (1.0,         1.0),
    'FRICA 14 CMS':             (1.0,         1.0),
    'HOT - DOG 19 CM.':         (1.0,         1.0),
    'MOLDE BANQUETE':           (1.0,         1.0),
    'MOLDE BANQUETE INTEGRAL':  (1.0,         1.0),
    'PAN FRICA 12 CM':          (1.0,         1.0),
    'PAN FRICA N8':             (1.0,         1.0),
    'ATUN':                     (1.0,         1.0),
    'CAMARON':                  (0.31,        1.0),
    'CAMARON APANADO':          (1.0,         1.0),
    'ERIZOS':                   (1.0,         1.0),
    'FILETE SALMON':            (1.0,         1.0),
    'LOCOS':                    (1.0,         0.5),
    'LOCOS_DREN':               (1.0,         1.0),    # LOCOS(DRENADOS)
    'SALMON SLICE LAMINADO':    (1.0,         1.0),
    'LECHUGA VERDE':            (1.0,         1.0),
    'MIX DE LECHUGA':           (1.0,         0.8),
    'PALTA':                    (1.0,         1.0),
    'TOMATE':                   (1.0,         1.0),
}

# Mapa nombre_producto → clave en CONV_TABLE + producto_control
PROD_CONV_MAP = {
    'CHULETA KASSLER':                                              ('CHULETA KASSLER',         'CHULETA KASSLER'),
    'COSTILLAS':                                                    ('COSTILLAS',               'COSTILLAS'),
    'JAMÓN':                                                        ('JAMÓN',                   'JAMÓN'),
    'LOMO DE CENTRO':                                               ('LOMO DE CENTRO',          'LOMO DE CENTRO'),
    'LOMO DE CENTRO(PORCIONADAS)':                                  ('LOMO DE CENTRO_PORC',     'LOMO DE CENTRO'),
    'PANCETA LAMINADA':                                             ('PANCETA LAMINADA',        'PANCETA LAMINADA'),
    'DESPUNTE PECHUGA DE POLLO':                                    ('PECHUGA DE POLLO',        'PECHUGA DE POLLO'),
    'PECHUGA DE POLLO':                                             ('PECHUGA DE POLLO',        'PECHUGA DE POLLO'),
    'PECHUGA DE POLLO  - FRITAS ESPECIALES ( 90 GR - PRECOCIDO)':  ('PECHUGA DE POLLO_F90',    'PECHUGA DE POLLO'),
    'PECHUGA DE POLLO - AVE - SANDWICH-  (200 GR - PRECOCIDO)':    ('PECHUGA DE POLLO_AVE',    'PECHUGA DE POLLO'),
    'PECHUGA DE POLLO - CESAR ( 150 GR - PRECOCIDO)':              ('PECHUGA DE POLLO_CES',    'PECHUGA DE POLLO'),
    'PECHUGA DE POLLO - PANKO - PLATO (250 GR  - CRUDO)':          ('PECHUGA DE POLLO_PKP',    'PECHUGA DE POLLO'),
    'PECHUGA DE POLLO - PANKO - ENSALADA - NIÑO (160 GR - CRUDO)': ('PECHUGA DE POLLO_PKE',    'PECHUGA DE POLLO'),
    'PECHUGA DE POLLO - TIMBAL ( 160 GR - PRECOCIDO)':             ('PECHUGA DE POLLO_TIM',    'PECHUGA DE POLLO'),
    'PERNIL':                                                       ('PERNIL',                  'PERNIL'),
    'PERNIL(PORCIONADAS)':                                          ('PERNIL_PORC',             'PERNIL'),
    'TOCINO AHUMADO':                                               ('TOCINO AHUMADO',          'TOCINO AHUMADO'),
    'FILETE':                                                       ('FILETE',                  'FILETE'),
    'GRASA DE WAGYU':                                               ('GRASA DE WAGYU',          'GRASA DE WAGYU'),
    'LOMO LISO':                                                    ('LOMO LISO',               'LOMO LISO'),
    'LOMO VETADO':                                                  ('LOMO VETADO',             'LOMO VETADO'),
    'PLATEADA':                                                     ('PLATEADA',                'PLATEADA'),
    'PLATEADA(PORCIONADAS)':                                        ('PLATEADA_PORC',           'PLATEADA'),
    'DESPUNTE CARNE ROJA':                                          ('POSTA',                   'POSTA'),
    'ESCALOPA':                                                     ('POSTA_ESC',               'POSTA'),
    'HAMBURGUESA GRAN EXPERTO':                                     ('POSTA_HGE',               'POSTA'),
    'HAMBURGUESA NIÑO':                                             ('POSTA_HNI',               'POSTA'),
    'HAMBURGUESAS':                                                 ('POSTA_HAM',               'POSTA'),
    'POSTA':                                                        ('POSTA',                   'POSTA'),
    'POSTA - FRITAS ESPECIALES ( 90 GR - CRUDO)':                  ('POSTA_F90',               'POSTA'),
    'PAPAS FRITAS':                                                 ('PAPAS FRITAS',            'PAPAS FRITAS'),
    'QUESO CHEDDAR':                                                ('QUESO CHEDDAR',           'QUESO CHEDDAR'),
    'QUESO PARMESANO':                                              ('QUESO PARMESANO',         'QUESO PARMESANO'),
    'QUESO RANCO':                                                  ('QUESO RANCO',             'QUESO RANCO'),
    'FRICA 14 CMS':                                                 ('FRICA 14 CMS',            'FRICA 14 CMS'),
    'HOT - DOG 19 CM.':                                             ('HOT - DOG 19 CM.',        'HOT - DOG 19 CM.'),
    'MOLDE BANQUETE':                                               ('MOLDE BANQUETE',          'MOLDE BANQUETE'),
    'MOLDE BANQUETE INTEGRAL':                                      ('MOLDE BANQUETE INTEGRAL', 'MOLDE BANQUETE INTEGRAL'),
    'PAN FRICA 12 CM':                                              ('PAN FRICA 12 CM',         'PAN FRICA 12 CM'),
    'PAN FRICA N8':                                                 ('PAN FRICA N8',            'PAN FRICA N8'),
    'ATUN':                                                         ('ATUN',                    'ATUN'),
    'CAMARON':                                                      ('CAMARON',                 'CAMARON'),
    'CAMARON APANADO':                                              ('CAMARON APANADO',         'CAMARON APANADO'),
    'ERIZOS':                                                       ('ERIZOS',                  'ERIZOS'),
    'FILETE SALMON':                                                ('FILETE SALMON',           'FILETE SALMON'),
    'LOCOS':                                                        ('LOCOS',                   'LOCOS'),
    'LOCOS(DRENADOS)':                                              ('LOCOS_DREN',              'LOCOS'),
    'SALMON SLICE LAMINADO':                                        ('SALMON SLICE LAMINADO',   'SALMON SLICE LAMINADO'),
    'LECHUGA HIDROPONICA':                                          ('LECHUGA VERDE',           'LECHUGA VERDE'),
    'MIX DE LECHUGA':                                               ('MIX DE LECHUGA',         'MIX DE LECHUGA'),
    'PALTA':                                                        ('PALTA',                   'PALTA'),
    'TOMATE':                                                       ('TOMATE',                  'TOMATE'),
}

def calcular_total_kg(producto, total, crudo=0, produccion=0, cocido=0, producto_control=None,
                      conv_porcion=None, conv_cocido=None):
    """
    Fórmula: total_kg = (crudo + produccion + cocido/conv_cocido) * conv_porcion
    Prioridad: PROD_CONV_MAP > parámetros explícitos > fallback total sin convertir
    """
    key = str(producto).strip().upper()
    crudo      = float(crudo      or 0)
    produccion = float(produccion or 0)
    cocido     = float(cocido     or 0)
    total      = float(total      or 0)

    entry = PROD_CONV_MAP.get(key)
    if entry:
        conv_key, ctrl = entry
        cc, cp = CONV_TABLE[conv_key]
    elif conv_porcion is not None and conv_cocido is not None:
        cc   = float(conv_cocido)  if float(conv_cocido  or 0) > 0 else 1.0
        cp   = float(conv_porcion) if float(conv_porcion or 0) > 0 else 1.0
        ctrl = producto_control or producto
    else:
        # Sin match: devolver total sin convertir
        return total, producto_control or producto

    cc = cc if cc > 0 else 1.0
    total_kg = (crudo + produccion + (cocido / cc)) * cp
    return total_kg, ctrl


# ============================================================
# BASE DE DATOS
# ============================================================
@st.cache_resource
def get_engine():
    try:
        db = st.secrets["connections"]["supabase"]
        conn_str = (
            f"postgresql+psycopg2://{db['user']}:{db['password']}"
            f"@{db['host']}:{db['port']}/{db['database']}?sslmode=require"
        )
        return create_engine(
            conn_str,
            pool_pre_ping=True,
            pool_recycle=300,
            connect_args={"options": "-c statement_timeout=30000"}
        )
    except Exception as e:
        st.error(f"❌ Error de conexión: {e}")
        return None


def run_query(sql, params=None):
    engine = get_engine()
    if engine is None:
        return pd.DataFrame()
    try:
        with engine.connect() as conn:
            return pd.read_sql(text(sql), conn, params=params or {})
    except Exception as e:
        st.error(f"Error en consulta: {e}")
        return pd.DataFrame()


def init_exclusiones():
    """Crea las tablas compras_excluidas y sku_colacion si no existen."""
    engine = get_engine()
    if engine is None:
        return
    try:
        with engine.connect() as conn:
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS compras_excluidas (
                    id         SERIAL PRIMARY KEY,
                    compra_id  INTEGER NOT NULL UNIQUE,
                    sku        TEXT,
                    motivo     TEXT DEFAULT 'compra_emergencia',
                    creado_en  TIMESTAMP DEFAULT NOW()
                )
            """))
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS sku_colacion (
                    id        SERIAL PRIMARY KEY,
                    sku       TEXT NOT NULL UNIQUE,
                    nombre    TEXT,
                    creado_en TIMESTAMP DEFAULT NOW()
                )
            """))
            conn.commit()
    except Exception:
        pass


# ============================================================
# LÓGICA MRP (código 1 preservado íntegramente)
# ============================================================
def process_bom(df_v, df_d, df_p):
    df_v.columns = df_v.columns.str.strip()
    df_d.columns = df_d.columns.str.strip()
    df_p.columns = df_p.columns.str.strip()

    df_v = df_v.rename(columns={'SKU': 'SKU_VENTA', 'Cantidad': 'CANT_VENTA'})
    skus_vendidos = set(df_v['SKU_VENTA'].astype(str).str.strip().str.upper())

    def validar_opcion(row):
        es_op = str(row['EsOpcion']).strip()
        if pd.isna(row['EsOpcion']) or es_op in ["", "0", "4"]:
            return True
        return str(row['SKU']).strip().upper() in skus_vendidos

    df_d_ready = df_d[df_d.apply(validar_opcion, axis=1)].copy()
    m1 = pd.merge(df_v, df_d_ready, left_on='SKU_VENTA', right_on='CODIGO VENTA', how='inner')

    es_proc = m1['SKU'].str.startswith('PRO-', na=False)
    df_insumos_directos = m1[~es_proc].copy()
    df_procesados_a_explotar = m1[es_proc].copy()

    if not df_procesados_a_explotar.empty:
        rendimientos = df_p.groupby('Codigo Venta')['CantReceta'].sum().reset_index()
        rendimientos = rendimientos.rename(columns={'CantReceta': 'TOTAL_RECETA_AUTO'})

        df_p_clean = df_p.rename(columns={
            'Codigo Venta': 'COD_P',
            'Ingrediente': 'NOM_P',
            'CantEfic': 'CE_P',
            'CantReceta': 'CR_P',
            'Porcion': 'MARK_P',
            'UM Salida': 'UM_P',
            'SKU Ingrediente': 'SKU_P'
        })
        df_p_final = pd.merge(df_p_clean, rendimientos, left_on='COD_P', right_on='Codigo Venta', how='left')
        m2 = pd.merge(df_procesados_a_explotar, df_p_final, left_on='SKU', right_on='COD_P', how='left')

        def calcular_m2(row):
            if row['MARK_P'] == 1:
                return row['CANT_VENTA'] * row['CantReal'] * row['CE_P']
            else:
                divisor = row['TOTAL_RECETA_AUTO'] if row['TOTAL_RECETA_AUTO'] > 0 else 1
                return row['CANT_VENTA'] * row['CantReal'] * (row['CE_P'] / divisor)

        m2['CANT_OUT'] = m2.apply(calcular_m2, axis=1)
        exp_f = m2[['SKU_P', 'NOM_P', 'CANT_OUT', 'UM_P']].rename(
            columns={'SKU_P': 'SKU_FIN', 'NOM_P': 'ING_FIN', 'UM_P': 'UM_FIN'})
    else:
        exp_f = pd.DataFrame()

    df_insumos_directos['CANT_OUT'] = (
        df_insumos_directos['CANT_VENTA'] * df_insumos_directos['CantReal']
    )
    dir_out = df_insumos_directos[['SKU', 'Ingrediente', 'CANT_OUT', 'UM']].rename(
        columns={'SKU': 'SKU_FIN', 'Ingrediente': 'ING_FIN', 'UM': 'UM_FIN'})

    consolidado = pd.concat([dir_out, exp_f], ignore_index=True)
    resumen = consolidado.groupby(['SKU_FIN', 'UM_FIN'], as_index=False).agg(
        {'CANT_OUT': 'sum', 'ING_FIN': 'first'})

    def formatear(row):
        um = str(row['UM_FIN']).upper()
        if um in ['G', 'ML', 'CC']:
            return row['CANT_OUT'] / 1000
        return row['CANT_OUT']

    resumen['TOTAL'] = resumen.apply(formatear, axis=1)
    return resumen[['SKU_FIN', 'ING_FIN', 'UM_FIN', 'TOTAL']].rename(
        columns={'SKU_FIN': 'SKU', 'ING_FIN': 'Insumo', 'UM_FIN': 'UM', 'TOTAL': 'Total Kg/L/Un'})


# ============================================================
# CÁLCULO DE COSTO TEÓRICO POR PLATO (Informe 1)
# Directos: CantReal × MUC
# Procesados: CantEfic × MUC  (usando último precio por SKU)
# ============================================================
def calcular_costo_platos(engine, fecha_i, fecha_f, local):
    """
    Devuelve DataFrame con costo teórico por código de venta (plato).
    Precio unitario = monto_real / cant_conv (último registro por SKU).
    Aplica factor_um para convertir unidades del recetario a unidades de compra.
    """
    # Precio unitario real = monto_real / cant_conv (último registro por SKU)
    precio_sql = """
        SELECT DISTINCT ON (sku) sku,
               monto_real / NULLIF(cant_conv, 0) as precio_unitario
        FROM compras
        WHERE cant_conv > 0
        ORDER BY sku, fecha_dte DESC
    """
    df_precio = run_query(precio_sql)
    if df_precio.empty:
        return pd.DataFrame()

    # Factor conversión unidades: G/CC/ML → /1000, resto → 1
    def factor_um(um):
        if pd.isna(um): return 1
        um = str(um).strip().upper()
        if um in ['G', 'CC', 'ML']: return 1/1000
        return 1

    # Recetario completo
    df_rec = run_query("SELECT * FROM recetas")
    if df_rec.empty:
        return pd.DataFrame()

    df_dir  = df_rec[df_rec['es_procesado'] == False].copy()
    df_proc = df_rec[df_rec['es_procesado'] == True].copy()

    # ---- DIRECTOS: cant_real × factor_um × precio_unitario ----
    dir_m = pd.merge(df_dir, df_precio, left_on='sku_ingrediente', right_on='sku', how='left')
    dir_m['cant_real']      = pd.to_numeric(dir_m['cant_real'], errors='coerce').fillna(0)
    dir_m['precio_unitario']= pd.to_numeric(dir_m['precio_unitario'], errors='coerce').fillna(0)
    dir_m['factor']         = dir_m['um_salida'].apply(factor_um)
    dir_m['costo_parcial']  = dir_m['cant_real'] * dir_m['factor'] * dir_m['precio_unitario']
    costo_dir = dir_m.groupby('codigo_venta')['costo_parcial'].sum().reset_index()

    # ---- PROCESADOS: cant_efic × factor_um × precio_unitario ----
    proc_m = pd.merge(df_proc, df_precio, left_on='sku_ingrediente', right_on='sku', how='left')
    proc_m['cant_efic']      = pd.to_numeric(proc_m['cant_efic'], errors='coerce').fillna(0)
    proc_m['precio_unitario']= pd.to_numeric(proc_m['precio_unitario'], errors='coerce').fillna(0)
    proc_m['factor']         = proc_m['um_salida'].apply(factor_um)
    proc_m['costo_parcial']  = proc_m['cant_efic'] * proc_m['factor'] * proc_m['precio_unitario']
    costo_proc = proc_m.groupby('codigo_venta')['costo_parcial'].sum().reset_index()

    # ---- Combinar ----
    costo_total  = pd.concat([costo_dir, costo_proc], ignore_index=True)
    costo_platos = costo_total.groupby('codigo_venta')['costo_parcial'].sum().reset_index()
    costo_platos.columns = ['sku_producto', 'costo_unitario_teorico']

    return costo_platos


# ============================================================
# COSTO PERÍODO: MUC ponderado del período, fallback última compra
# ============================================================
def calcular_costo_platos_periodo(engine, fecha_i, fecha_f):
    """
    Igual que calcular_costo_platos pero usa MUC ponderado del período.
    Si un SKU no tiene compras en el período, usa la última compra histórica.
    """
    # MUC ponderado del período
    precio_periodo_sql = """
        SELECT sku,
               SUM(costo_realfinal) / NULLIF(SUM(costo_realfinal / NULLIF(muc, 0)), 0) AS precio_unitario
        FROM compras
        WHERE fecha_dte::date BETWEEN :i AND :f
          AND muc > 0 AND costo_realfinal > 0 AND monto_real > 0
        GROUP BY sku
    """
    # Fallback: última compra histórica
    precio_fallback_sql = """
        SELECT DISTINCT ON (sku) sku,
               monto_real / NULLIF(cant_conv, 0) AS precio_unitario
        FROM compras
        WHERE cant_conv > 0 AND monto_real > 0
        ORDER BY sku, fecha_dte DESC
    """
    df_periodo  = run_query(precio_periodo_sql,  {'i': str(fecha_i), 'f': str(fecha_f)})
    df_fallback = run_query(precio_fallback_sql)
    if df_fallback.empty:
        return pd.DataFrame()

    # Combinar: período primero, fallback para los que no están
    skus_periodo = set(df_periodo['sku'].tolist()) if not df_periodo.empty else set()
    df_fb_needed = df_fallback[~df_fallback['sku'].isin(skus_periodo)]
    df_precio    = pd.concat([df_periodo, df_fb_needed], ignore_index=True)

    def factor_um(um):
        if pd.isna(um): return 1
        um = str(um).strip().upper()
        if um in ['G', 'CC', 'ML']: return 1/1000
        return 1

    df_rec = run_query("SELECT * FROM recetas")
    if df_rec.empty:
        return pd.DataFrame()

    df_dir  = df_rec[df_rec['es_procesado'] == False].copy()
    df_proc = df_rec[df_rec['es_procesado'] == True].copy()

    dir_m = pd.merge(df_dir, df_precio, left_on='sku_ingrediente', right_on='sku', how='left')
    dir_m['cant_real']       = pd.to_numeric(dir_m['cant_real'],       errors='coerce').fillna(0)
    dir_m['precio_unitario'] = pd.to_numeric(dir_m['precio_unitario'], errors='coerce').fillna(0)
    dir_m['factor']          = dir_m['um_salida'].apply(factor_um)
    dir_m['costo_parcial']   = dir_m['cant_real'] * dir_m['factor'] * dir_m['precio_unitario']
    costo_dir = dir_m.groupby('codigo_venta')['costo_parcial'].sum().reset_index()

    proc_m = pd.merge(df_proc, df_precio, left_on='sku_ingrediente', right_on='sku', how='left')
    proc_m['cant_efic']       = pd.to_numeric(proc_m['cant_efic'],       errors='coerce').fillna(0)
    proc_m['precio_unitario'] = pd.to_numeric(proc_m['precio_unitario'], errors='coerce').fillna(0)
    proc_m['factor']          = proc_m['um_salida'].apply(factor_um)
    proc_m['costo_parcial']   = proc_m['cant_efic'] * proc_m['factor'] * proc_m['precio_unitario']
    costo_proc = proc_m.groupby('codigo_venta')['costo_parcial'].sum().reset_index()

    costo_total  = pd.concat([costo_dir, costo_proc], ignore_index=True)
    costo_platos = costo_total.groupby('codigo_venta')['costo_parcial'].sum().reset_index()
    costo_platos.columns = ['sku_producto', 'costo_unitario_periodo']
    return costo_platos


# ============================================================
# INFORME 1: RENTABILIDAD POR PRODUCTO / CATEGORÍA
# ============================================================
def informe_rentabilidad(fecha_i, fecha_f, local):
    engine = get_engine()
    if engine is None:
        return pd.DataFrame()

    filtro_local_r = "AND UPPER(local) = UPPER(:l)" if local != "Todos" else ""
    params = {"i": str(fecha_i), "f": str(fecha_f)}
    if local != "Todos":
        params["l"] = local

    q_v = f"""
        SELECT sku_producto, nombre_producto, categoria_menu,
               SUM(cantidad_vendida) as cant,
               SUM(monto_venta_real) as venta
        FROM ventas
        WHERE fecha_venta BETWEEN :i AND :f
        {filtro_local_r}
        GROUP BY 1, 2, 3
    """

    df_v = run_query(q_v, params)
    if df_v.empty:
        st.warning("No hay ventas para el período/local seleccionado.")
        return pd.DataFrame()

    costo_teorico = calcular_costo_platos(engine, fecha_i, fecha_f, local)
    costo_periodo = calcular_costo_platos_periodo(engine, fecha_i, fecha_f)

    df = pd.merge(df_v, costo_teorico, on='sku_producto', how='left')
    df = pd.merge(df,   costo_periodo, on='sku_producto', how='left')

    df['costo_unitario_teorico'] = pd.to_numeric(df.get('costo_unitario_teorico'), errors='coerce').fillna(0)
    df['costo_unitario_periodo'] = pd.to_numeric(df.get('costo_unitario_periodo'), errors='coerce').fillna(0)
    df['venta']                  = pd.to_numeric(df['venta'], errors='coerce').fillna(0)

    # Rentabilidad teórica (último precio)
    df['costo_total_teorico']  = df['cant'] * df['costo_unitario_teorico']
    df['rentabilidad_teorica'] = df['venta'] - df['costo_total_teorico']
    df['margen_teorico']       = df.apply(
        lambda x: (x['rentabilidad_teorica'] / x['venta'] * 100) if x['venta'] > 0 else 0, axis=1)

    # Rentabilidad período (MUC ponderado + fallback)
    df['costo_total_periodo']  = df['cant'] * df['costo_unitario_periodo']
    df['rentabilidad_periodo'] = df['venta'] - df['costo_total_periodo']
    df['margen_periodo']       = df.apply(
        lambda x: (x['rentabilidad_periodo'] / x['venta'] * 100) if x['venta'] > 0 else 0, axis=1)

    # Mantener compatibilidad con código existente
    df['costo_total']  = df['costo_total_periodo']
    df['rentabilidad'] = df['rentabilidad_periodo']
    df['margen_pct']   = df['margen_periodo']

    return df.sort_values('venta', ascending=False)


# ============================================================
# INFORME 2: DESVIACIÓN REAL VS TEÓRICO
# ============================================================
def informe_desviacion(fecha_i, fecha_f, local):
    engine = get_engine()
    if engine is None:
        return pd.DataFrame()

    # Ventas del período — casteamos fechas a string para evitar problemas de tipo con SQLAlchemy
    filtro_local_v = "AND UPPER(local) = UPPER(:l)" if local != "Todos" else ""
    params = {"i": str(fecha_i), "f": str(fecha_f)}
    if local != "Todos":
        params["l"] = local

    q_v = f"""
        SELECT sku_producto, SUM(cantidad_vendida) as cant_vendida
        FROM ventas
        WHERE fecha_venta BETWEEN :i AND :f
        {filtro_local_v}
        GROUP BY 1
    """
    df_v = run_query(q_v, params)

    # Recetario completo
    df_rec = run_query("SELECT * FROM recetas")
    if df_rec.empty or df_v.empty:
        return pd.DataFrame()

    # Filtrar opcionales — NULL se trata como 0 (siempre va en el plato)
    df_rec['es_opcion'] = pd.to_numeric(df_rec['es_opcion'], errors='coerce').fillna(0)
    df_rec['cant_real'] = pd.to_numeric(df_rec['cant_real'], errors='coerce').fillna(0)
    df_rec['cant_efic'] = pd.to_numeric(df_rec['cant_efic'], errors='coerce').fillna(0)
    df_rec = df_rec[df_rec['es_opcion'] == 0].copy()  # solo fijos (null→0 por fillna, opciones 1/2/3/5/6 excluidas)

    # Separar directos y procesados
    df_dir  = df_rec[df_rec['es_procesado'] == False].copy()
    df_proc = df_rec[df_rec['es_procesado'] == True].copy()

    # Factor de conversión según um_salida: G/CC/ML → /1000, UN/KG/LT → 1
    def factor_um(um):
        if pd.isna(um): return 1
        um = str(um).strip().upper()
        if um in ['G', 'CC', 'ML']: return 1/1000
        return 1

    # ---- DIRECTOS que no son PRO- ----
    dir_no_pro = df_dir[~df_dir['sku_ingrediente'].str.startswith('PRO-', na=False)].copy()
    dir_no_pro['factor_um'] = dir_no_pro['um_salida'].apply(factor_um)
    merge_dir = pd.merge(df_v, dir_no_pro, left_on='sku_producto', right_on='codigo_venta', how='inner')
    merge_dir['consumo_parcial'] = merge_dir['cant_vendida'] * merge_dir['cant_real'] * merge_dir['factor_um']
    dir_out = merge_dir[['sku_ingrediente', 'nombre_ingrediente', 'consumo_parcial']]



    # ---- EXPLOSIÓN PROCESADOS ----
    # Paso 1: platos que usan un PRO- como ingrediente
    dir_pro = df_dir[df_dir['sku_ingrediente'].str.startswith('PRO-', na=False)].copy()
    merge_pro = pd.merge(df_v, dir_pro, left_on='sku_producto', right_on='codigo_venta', how='inner')

    exp_out = pd.DataFrame()
    if not merge_pro.empty and not df_proc.empty:
        # Paso 2: rendimiento total y porcion de cada procesado
        # Usa MAX(rendimiento) si está definido (>1), sino SUM(cant_real)
        rend_exp = df_proc.groupby('codigo_venta').agg(
            rendimiento_explicito=('rendimiento', 'max'),
            rendimiento_suma=('cant_real', 'sum'),
            porcion=('porcion', 'first')
        ).reset_index()
        rend_exp['rendimiento_total'] = rend_exp.apply(
            lambda r: r['rendimiento_explicito'] if pd.notna(r['rendimiento_explicito']) and r['rendimiento_explicito'] > 1
            else r['rendimiento_suma'], axis=1
        )
        rend = rend_exp[['codigo_venta', 'rendimiento_total', 'porcion']]

        # Agregar ventas por codigo_venta + sku_ingrediente (PRO-XX) para evitar iterar fila por fila
        merge_pro_agg = merge_pro.groupby(['codigo_venta', 'sku_ingrediente']).agg(
            cant_vendida=('cant_vendida', 'sum'),
            cant_real=('cant_real', 'first'),
            um_salida=('um_salida', 'first')
        ).reset_index()

        rows = []
        for _, plato_row in merge_pro_agg.iterrows():
            pro_sku    = plato_row['sku_ingrediente']  # PRO-XX
            cant_plato = pd.to_numeric(plato_row['cant_real'], errors='coerce') or 0
            ventas     = pd.to_numeric(plato_row['cant_vendida'], errors='coerce') or 0

            base_rows = df_proc[df_proc['codigo_venta'] == pro_sku]
            if base_rows.empty:
                continue

            rend_row   = rend[rend['codigo_venta'] == pro_sku]
            rend_total = float(rend_row['rendimiento_total'].values[0]) if not rend_row.empty else 1
            porcion    = int(rend_row['porcion'].values[0]) if not rend_row.empty else 0
            if rend_total == 0:
                rend_total = 1

            um_plato = factor_um(plato_row['um_salida'] if 'um_salida' in plato_row.index else '')
            cant_plato_conv = cant_plato * um_plato

            for _, base in base_rows.iterrows():
                cant_base = pd.to_numeric(base['cant_real'], errors='coerce') or 0
                um_base   = factor_um(base['um_salida'] if pd.notna(base.get('um_salida')) else '')
                if porcion == 1:
                    consumo = ventas * cant_plato_conv * cant_base * um_base
                else:
                    consumo = ventas * (cant_plato_conv / rend_total) * cant_base * um_base
                rows.append({
                    'sku_ingrediente':    base['sku_ingrediente'],
                    'nombre_ingrediente': base['nombre_ingrediente'],
                    'consumo_parcial':    consumo
                })

        if rows:
            exp_out = pd.DataFrame(rows)



    # ---- CONSOLIDAR ----
    todo = pd.concat([df for df in [dir_out, exp_out] if not df.empty], ignore_index=True)
    cons_teo = todo.groupby('sku_ingrediente').agg(
        consumo_teorico=('consumo_parcial', 'sum'),
        nombre_ingrediente=('nombre_ingrediente', 'first')
    ).reset_index()




    # Compras reales del período — fecha_dte es timestamp, cant_conv ya está en unidades
    filtro_local_c  = "AND UPPER(local) = UPPER(:l)" if local != "Todos" else ""
    filtro_local_c2 = "AND UPPER(c.local) = UPPER(:l)" if local != "Todos" else ""
    params_c = {"i": fecha_i, "f": fecha_f}
    if local != "Todos":
        params_c["l"] = local

    q_c = f"""
        SELECT
            COALESCE(e.sku_receta, c.sku) as sku,
            SUM(c.cant_conv) AS cant_real_comprada,
            AVG(c.muc) AS muc_promedio
        FROM compras c
        LEFT JOIN sku_equivalencias e ON c.sku = e.sku_compra
        WHERE c.fecha_dte::date BETWEEN :i AND :f
          AND c.subcat = 'Directo'
        {filtro_local_c2}
        GROUP BY 1
    """
    df_c = run_query(q_c, params_c)

    # Fallback: si el período no tiene compras, mostrar histórico completo
    if df_c.empty:
        q_c2 = f"""
            SELECT
                COALESCE(e.sku_receta, c.sku) as sku,
                SUM(c.cant_conv) AS cant_real_comprada,
                AVG(c.muc) AS muc_promedio
            FROM compras c
            LEFT JOIN sku_equivalencias e ON c.sku = e.sku_compra
            WHERE c.subcat = 'Directo'
            {filtro_local_c2}
            GROUP BY 1
        """
        df_c = run_query(q_c2, params_c)
        if not df_c.empty:
            st.warning("⚠️ Sin compras en el período seleccionado — mostrando totales históricos.")

    # Equivalencias ya aplicadas en SQL — no necesita remapeo en Python

    # Nombre canónico desde compras — incluir equivalencias para SKUs que solo existen como destino
    q_nom = """
        SELECT sku, MIN(nombre_producto) as nombre_compra
        FROM compras
        WHERE subcat IN ('Directo', 'Indirecto')
        GROUP BY sku
    """
    nombres_compras = run_query(q_nom)
    dict_nombres = dict(zip(nombres_compras['sku'], nombres_compras['nombre_compra'])) if not nombres_compras.empty else {}

    # Fallback de nombres via equivalencias
    df_equiv = run_query("SELECT sku_compra, sku_receta FROM sku_equivalencias")
    if not df_equiv.empty:
        for _, row in df_equiv.iterrows():
            sku_dest = row['sku_receta']
            sku_orig = row['sku_compra']
            if sku_dest not in dict_nombres and sku_orig in dict_nombres:
                dict_nombres[sku_dest] = dict_nombres[sku_orig]

    # Subcat por SKU (para categorización en informe)
    df_subcat = run_query("""
        SELECT sku, MIN(subcat) as subcat
        FROM compras
        WHERE subcat IN ('Directo','Indirecto')
        GROUP BY sku
    """)
    dict_subcat = dict(zip(df_subcat['sku'], df_subcat['subcat'])) if not df_subcat.empty else {}

    informe = pd.merge(
        cons_teo, df_c,
        left_on='sku_ingrediente', right_on='sku', how='outer'
    )
    informe = informe.fillna(0)

    # Eliminar filas que son SKUs originales ya consolidados via equivalencias
    # (aparecen solo en compras con consumo_teorico=0 porque ya fueron mapeados a su sku_receta)
    skus_compra_equiv = set(df_equiv['sku_compra'].tolist()) if not df_equiv.empty else set()
    informe = informe[~(
        (informe['consumo_teorico'] == 0) &
        (informe['sku'].isin(skus_compra_equiv))
    )]
    informe['subcat'] = informe.apply(
        lambda r: dict_subcat.get(str(r['sku_ingrediente']), dict_subcat.get(str(r['sku']), '')), axis=1
    )

    # SKU final: unificar sku_ingrediente y sku en una sola columna
    informe['sku_final'] = informe.apply(
        lambda r: r['sku_ingrediente'] if r['sku_ingrediente'] not in [0, '', None]
        else r['sku'], axis=1
    )

    # Nombre final: recetario primero, compras como fallback para ingredientes sin receta
    informe['nombre_final'] = informe.apply(
        lambda r: r['nombre_ingrediente']
        if (r['nombre_ingrediente'] not in [0, '', None] and str(r['nombre_ingrediente']).strip() != '')
        else dict_nombres.get(str(r['sku_final']), str(r['sku_final'])), axis=1
    )

    informe['desviacion_cant']   = informe['cant_real_comprada'] - informe['consumo_teorico']
    informe['desviacion_dinero'] = informe['desviacion_cant'] * informe['muc_promedio']

    # Renombrar para consistencia con el resto del informe
    informe['sku_ingrediente']   = informe['sku_final']
    informe['nombre_ingrediente']= informe['nombre_final']

    return informe.sort_values('desviacion_dinero', ascending=False)


# ============================================================
# PERSISTENCIA
# ============================================================
def save_recetario(df_directos, df_procesados):
    engine = get_engine()
    if engine is None:
        return

    df_dir = df_directos.copy()
    df_dir.columns = df_dir.columns.str.strip()
    df_dir = df_dir.rename(columns={
        'CODIGO VENTA': 'codigo_venta', 'Plato': 'nombre_plato',
        'SKU': 'sku_ingrediente', 'Ingrediente': 'nombre_ingrediente',
        'CantReal': 'cant_real', 'Eficiencia': 'rendimiento',
        'UM': 'um_salida', 'EsOpcion': 'es_opcion'
    })
    df_dir['es_procesado'] = False
    df_dir['cant_efic'] = None
    df_dir['porcion'] = 0

    df_proc = df_procesados.copy()
    df_proc.columns = df_proc.columns.str.strip()
    # Detectar columna porcion con cualquier variación de nombre o espacios
    col_porcion = next((c for c in df_proc.columns if c.strip().lower() == 'porcion'), None)
    rename_map = {
        'Codigo Venta': 'codigo_venta', 'Ingrediente Proc': 'nombre_plato',
        'SKU Ingrediente': 'sku_ingrediente', 'Ingrediente': 'nombre_ingrediente',
        'CantReceta': 'cant_real', 'CantEfic': 'cant_efic',
        'UM Salida': 'um_salida', 'Eficiencia': 'rendimiento'
    }
    if col_porcion:
        rename_map[col_porcion] = 'porcion'
    df_proc = df_proc.rename(columns=rename_map)
    df_proc['es_procesado'] = True
    df_proc['es_opcion'] = 0
    if 'porcion' not in df_proc.columns:
        df_proc['porcion'] = 0

    cols_base = ['codigo_venta', 'nombre_plato', 'sku_ingrediente', 'nombre_ingrediente',
                 'cant_real', 'cant_efic', 'rendimiento', 'um_salida', 'es_procesado', 'es_opcion', 'porcion']

    df_final = pd.concat([df_dir, df_proc], ignore_index=True)
    cols = [c for c in cols_base if c in df_final.columns]
    df_final['rendimiento'] = pd.to_numeric(df_final['rendimiento'], errors='coerce').fillna(1)
    df_final['cant_real'] = pd.to_numeric(df_final['cant_real'], errors='coerce').fillna(0)
    df_final['cant_efic'] = pd.to_numeric(df_final['cant_efic'], errors='coerce').fillna(0)

    # Consolidar duplicados: mismo ingrediente en el mismo plato → sumar cantidades
    df_final = df_final[cols].copy()
    agg_dict = {
        'nombre_plato':      'first',
        'nombre_ingrediente':'first',
        'cant_real':         'sum',
        'cant_efic':         'sum',
        'rendimiento':       'first',
        'um_salida':         'first',
        'es_opcion':         'first'
    }
    if 'porcion' in df_final.columns:
        agg_dict['porcion'] = 'first'

    df_agg = df_final.groupby(
        ['codigo_venta', 'sku_ingrediente', 'es_procesado'],
        as_index=False
    ).agg(agg_dict)

    # cols final solo con columnas que existen en df_agg
    cols = [c for c in cols_base if c in df_agg.columns]

    duplicados = len(df_final) - len(df_agg)
    if duplicados > 0:
        st.warning(f"⚠️ Se consolidaron {duplicados} filas duplicadas (mismo SKU en mismo plato).")

    try:
        with engine.connect() as conn:
            conn.execute(text("DROP VIEW IF EXISTS vista_costo_recetas CASCADE"))
            conn.commit()
        df_agg[cols].to_sql('recetas', engine, if_exists='replace', index=False)
        st.success(f"✅ Recetario sincronizado — {len(df_agg)} filas únicas cargadas.")
    except Exception as e:
        st.error(f"Error al guardar recetario: {e}")


# ============================================================
# PROCESADO DE COMPRAS
# ============================================================

TASAS_IMP_ADIC = {
    '271': 0.18,
    '27':  0.10,
    '26':  0.21,
    '25':  0.21,
    '24':  0.3155,
    '19':  0.12,
    '18':  0.05,
}

# Columnas mínimas que debe traer el archivo fuente
COLS_REQUERIDAS = [
    'local', 'fecha_dte', 'rut_proveedor', 'nombre_proveedor',
    'tipo_dte', 'folio', 'nombre_producto',
    'cantidad', 'total_item', 'codigo_impuesto', 'iva',
    'descuento_global', 'recargo_global', 'total',
    'sku', 'subcat', 'conversion', 'formato', 'categoria_producto',
]

def _normalizar_columnas(df: pd.DataFrame) -> pd.DataFrame:
    """Limpia y normaliza los nombres de columna del Excel fuente."""
    df = df.copy()
    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(r'[\s]+', '_', regex=True)
        .str.replace(r'[áàä]', 'a', regex=True)
        .str.replace(r'[éèë]', 'e', regex=True)
        .str.replace(r'[íìï]', 'i', regex=True)
        .str.replace(r'[óòö]', 'o', regex=True)
        .str.replace(r'[úùü]', 'u', regex=True)
        .str.replace(r'[^a-z0-9_]', '_', regex=True)
    )
    # Mapear columnas _res del formato nuevo del robot
    res_map = {
        'sku':               'sku_res',
        'subcat':            'subcat_res',
        'conversion':        'conversion_res',
        'formato':           'formato_res',
        'categoria_producto':'categoria_res',
    }
    for canonical, src in res_map.items():
        if src in df.columns and canonical not in df.columns:
            df = df.rename(columns={src: canonical})

    # Alias frecuentes
    aliases = {
        'categoria_producto': ['categoria_producto', 'categoria producto', 'categoria'],
        'recargo_global':     ['recargo_global', 'recargo global', 'recargo_global'],
        'descuento_global':   ['descuento_global', 'descuento global', 'descuento_global'],
        'codigo_impuesto':    ['codigo_impuesto', 'codigo impuesto', 'cod_impuesto'],
        'iva':                ['iva', 'iva_'],
        'total':              ['total', 'total_'],
    }
    for canonical, variants in aliases.items():
        for v in variants:
            v_norm = v.replace(' ', '_')
            if v_norm in df.columns and canonical not in df.columns:
                df = df.rename(columns={v_norm: canonical})
    return df


def procesar_compras(df_raw: pd.DataFrame) -> tuple[pd.DataFrame, list[str]]:
    """
    Recibe el DataFrame crudo del Excel de compras y devuelve
    (df_procesado, lista_de_advertencias).

    Columnas calculadas:
        cant_conv       = cantidad × conversion
        monto_real      = total_item  (negativo si tipo_dte == 61)
        recargo2        = (Recargo_Global - Descuento_Global) × participación línea en folio
        total_neto2     = monto_real + recargo2
        imp_adic        = monto_real × tasa según codigo_impuesto
        IVA_2           = total_neto2 × 0.19  (0 si IVA del folio == 0)
        tootal2         = total_neto2 + imp_adic + IVA_2
        costo_realfinal = tootal2 + despacho_distribuido + ajuste_redondeo  (0 en líneas de despacho)
        MUC             = costo_realfinal / (cant_conv × formato)
                          si formato == 1 → MUC = costo_realfinal / cant_conv
    """
    warnings = []
    df = _normalizar_columnas(df_raw)

    # ── Verificar columnas mínimas ──────────────────────────────────────────
    faltantes = [c for c in COLS_REQUERIDAS if c not in df.columns]
    if faltantes:
        warnings.append(
            f"⚠️ Columnas no encontradas tras normalizar nombres: **{', '.join(faltantes)}**\n"
            f"Columnas recibidas: {', '.join(df.columns.tolist())}"
        )
    # Columnas críticas para el cálculo — si faltan el resultado será incorrecto
    criticas = {
        'total_item':       'monto_real será 0',
        'recargo_global':   'recargo2 será 0 (no se distribuye recargo)',
        'descuento_global': 'descuento no se aplicará',
        'iva':              'IVA_2 será 0 en todos los folios',
        'total':            'no se podrá ajustar redondeo ni distribuir despacho',
        'conversion':       'cant_conv = cantidad (sin conversión)',
        'formato':          'MUC calculado como por unidad en todos los casos',
    }
    for col, impacto in criticas.items():
        if col not in df.columns:
            warnings.append(f"🔴 Columna crítica **'{col}'** no encontrada → {impacto}")

    # ── Tipos básicos ────────────────────────────────────────────────────────
    df['tipo_dte']        = pd.to_numeric(df.get('tipo_dte', 33), errors='coerce').fillna(33).astype(int)
    df['total_item']      = pd.to_numeric(df.get('total_item', 0), errors='coerce').fillna(0)
    df['cantidad']        = pd.to_numeric(df.get('cantidad', 1), errors='coerce').fillna(1)
    df['conversion']      = pd.to_numeric(df.get('conversion', 1), errors='coerce').fillna(1)
    df['formato']         = pd.to_numeric(df.get('formato', 1), errors='coerce').fillna(1)
    df['recargo_global']  = pd.to_numeric(df.get('recargo_global', 0), errors='coerce').fillna(0)
    df['descuento_global']= pd.to_numeric(df.get('descuento_global', 0), errors='coerce').fillna(0)
    df['iva']             = pd.to_numeric(df.get('iva', 0), errors='coerce').fillna(0)
    df['total']           = pd.to_numeric(df.get('total', 0), errors='coerce').fillna(0)

    # ── PASO 1: cant_conv ────────────────────────────────────────────────────
    df['cant_conv'] = df['cantidad'] * df['conversion']

    # ── PASO 2: monto_real ───────────────────────────────────────────────────
    df['monto_real'] = np.where(df['tipo_dte'] == 61, -df['total_item'], df['total_item'])

    # ── PASO 3: recargo2  (distribución proporcional por folio) ─────────────
    # participación = monto_real_línea / suma_monto_real_folio
    df['_tot_folio'] = df.groupby(['folio','rut_proveedor'])['monto_real'].transform('sum')
    df['_recargo_neto'] = df['recargo_global'] - df['descuento_global']
    df['_part'] = np.where(df['_tot_folio'] != 0, df['monto_real'] / df['_tot_folio'], 0)
    df['recargo2'] = df['_part'] * df['_recargo_neto']
    df['total_neto2'] = df['monto_real'] + df['recargo2']

    # ── PASO 4: imp_adic ─────────────────────────────────────────────────────
    cod_str = (
        df.get('codigo_impuesto', pd.Series([''] * len(df)))
        .fillna('')
        .astype(str)
        .str.strip()
        .str.replace(r'\.0$', '', regex=True)
        .str.replace(r'^nan$', '', regex=True)
    )
    tasa = cod_str.map(TASAS_IMP_ADIC).fillna(0)
    df['imp_adic'] = df['monto_real'] * tasa

    # ── PASO 5: IVA_2  (por folio: si el folio tiene IVA registrado > 0) ────
    df['_tiene_iva'] = df.groupby(['folio','rut_proveedor'])['iva'].transform('max') != 0
    df['iva_2'] = np.where(df['_tiene_iva'], df['total_neto2'] * 0.19, 0)

    # ── PASO 6: tootal2 ──────────────────────────────────────────────────────
    df['tootal2'] = df['total_neto2'] + df['imp_adic'] + df['iva_2']

    # ── PASO 7: identificar líneas de despacho ───────────────────────────────
    nombre_lower = df['nombre_producto'].str.lower().fillna('')
    df['_es_despacho'] = (
        nombre_lower.str.contains('despacho', na=False) |
        nombre_lower.str.contains('flete',    na=False) |
        nombre_lower.str.contains('distribucion', na=False)
    )

    # ── PASO 8: Desp_Folio = suma(monto_real de líneas despacho) × 1.19 ─────
    df['_desp_linea'] = np.where(df['_es_despacho'], df['monto_real'] * 1.19, 0)
    df['_desp_folio'] = df.groupby(['folio','rut_proveedor'])['_desp_linea'].transform('sum')

    # ── PASO 9: ajuste redondeo = Total_factura - suma(tootal2) del folio ────
    df['_suma_tootal2_folio'] = df.groupby(['folio','rut_proveedor'])['tootal2'].transform('sum')
    df['_total_factura']      = df.groupby(['folio','rut_proveedor'])['total'].transform('max')
    df['_diferencia']         = df['_total_factura'] - df['_suma_tootal2_folio']

    # desp+red2 por folio = Desp_Folio + diferencia
    df['_desp_red2'] = df['_desp_folio'] + df['_diferencia']

    # ── PASO 10: Part_Item (excluye despachos del denominador) ───────────────
    df['_monto_limpio'] = np.where(df['_es_despacho'], 0, df['monto_real'].abs())
    df['_tot_limpio_folio'] = df.groupby(['folio','rut_proveedor'])['_monto_limpio'].transform('sum')
    df['_part_item'] = np.where(
        df['_tot_limpio_folio'] != 0,
        df['_monto_limpio'] / df['_tot_limpio_folio'],
        0
    )

    # ── PASO 11: dist_desp = part_item × desp_red2  (redondeado a entero) ───
    df['_dist_desp'] = (df['_part_item'] * df['_desp_red2']).round(0)

    # ── PASO 12: costo_realfinal ─────────────────────────────────────────────
    df['costo_realfinal'] = np.where(
        df['_es_despacho'],
        0,
        df['tootal2'] + df['_dist_desp']
    )

    # ── PASO 13: MUC ─────────────────────────────────────────────────────────
    denominador = np.where(
        df['formato'] == 1,
        df['cant_conv'],
        df['cant_conv'] * df['formato']
    )
    df['muc'] = np.where(
        (denominador != 0) & (~df['_es_despacho']),
        df['costo_realfinal'] / denominador,
        0
    )

    # ── Limpiar columnas temporales ──────────────────────────────────────────
    cols_temp = [c for c in df.columns if c.startswith('_')]
    df = df.drop(columns=cols_temp)

    # ── Renombrar IVA_2 para consistencia con BD ─────────────────────────────
    df = df.rename(columns={'iva_2': 'iva_2'})  # ya en minúsculas

    # ── Advertencias sobre datos ─────────────────────────────────────────────
    sin_sku = df['sku'].isna().sum() if 'sku' in df.columns else 0
    if sin_sku > 0:
        warnings.append(f"⚠️ {sin_sku} líneas sin SKU asignado.")
    sin_conv = (df['conversion'] == 0).sum()
    if sin_conv > 0:
        warnings.append(f"⚠️ {sin_conv} líneas con Conversion = 0.")

    return df, warnings


def save_compras(df: pd.DataFrame):
    """Guarda el DataFrame ya procesado en la tabla compras de Supabase.
    Elimina previamente los registros del mismo período (mes) y locales
    para evitar duplicados al recargar.
    """
    engine = get_engine()
    if engine is None:
        return
    cols_req = [
        'local', 'fecha_dte', 'rut_proveedor', 'nombre_proveedor', 'tipo_dte',
        'folio', 'nombre_producto', 'sku', 'subcat', 'codigo_impuesto',
        'cantidad', 'conversion', 'formato', 'categoria_producto',
        'cant_conv', 'monto_real', 'recargo2', 'total_neto2',
        'imp_adic', 'iva_2', 'tootal2', 'costo_realfinal', 'muc', 'total'
    ]
    cols_ok = [c for c in cols_req if c in df.columns]
    try:
        # Detectar rango de fechas y locales del archivo a cargar
        fechas = pd.to_datetime(df['fecha_dte'], errors='coerce').dropna()
        if fechas.empty:
            st.error("No se pudo determinar el período del archivo.")
            return
        fecha_min = fechas.min().date().replace(day=1)
        fecha_max = (fechas.max().to_period('M').to_timestamp('M')).date()
        locales   = df['local'].dropna().unique().tolist() if 'local' in df.columns else []

        with engine.connect() as conn:
            if locales:
                conn.execute(text("""
                    DELETE FROM compras
                    WHERE fecha_dte::date BETWEEN :fi AND :ff
                      AND local = ANY(:locales)
                """), {'fi': fecha_min, 'ff': fecha_max, 'locales': locales})
            else:
                conn.execute(text("""
                    DELETE FROM compras
                    WHERE fecha_dte::date BETWEEN :fi AND :ff
                """), {'fi': fecha_min, 'ff': fecha_max})
            conn.commit()

        total = len(df)
        with st.spinner(f"Insertando {total:,} registros..."):
            df[cols_ok].to_sql('compras', engine, if_exists='append',
                               index=False, method='multi', chunksize=500)
        st.success(f"✅ {total:,} registros guardados ({fecha_min} → {fecha_max}). Período anterior reemplazado.")
    except Exception as e:
        st.error(f"Error al guardar compras: {e}")


def save_ventas(df_raw):
    engine = get_engine()
    if engine is None:
        return

    df = df_raw.copy()
    # Si viene como string único (CSV con sep=;), re-parsear
    if len(df.columns) == 1 and ';' in str(df.columns[0]):
        import io as _io
        raw_bytes = df_raw.to_csv(index=False).encode()
        df = pd.read_csv(_io.BytesIO(raw_bytes), sep=';', dtype=str)
    df.columns = df.columns.str.strip()

    mapeo = {
        'ID de orden': 'id_orden', 'ID Producto': 'sku_producto',
        'Nombre': 'nombre_producto', 'Cantidad': 'cantidad_vendida',
        'Precio a Pagar': 'precio_pagar', 'Precio Base': 'precio_base',
        'Costo': 'costo_receta', 'Descuento': 'descuento', 'Impuesto': 'impuesto',
        'AB.': 'ab_categoria',
        'Categorias de Productos/Platos': 'categoria_menu',
        'Categorías de Productos/Platos': 'categoria_menu',
        'BA.': 'ba_opcion',
        'Jerarquia de Extras': 'jerarquia_extras',
        'Jerarquía de Extras': 'jerarquia_extras',
        'AC.': 'ac_excepcion',
        'Jerarquia de Excp.': 'jerarquia_excepcion',
        'Jerarquía de Excp.': 'jerarquia_excepcion',
        'Local': 'local', 'fechahora_pedido': 'fecha_pedido',
        'fechahora_creacion': 'fecha_creacion', 'fechahora_cierre': 'fecha_cierre',
        'Nombre de mesa': 'mesa', 'Sector': 'sector', 'Origen': 'origen',
        'Nombre garzon apertura': 'garzon', 'Nombre garzón apertura': 'garzon',
        'Folio': 'folio', 'Forma de Pago': 'forma_pago',
        'Nombre Lista Precio': 'lista_precio',
        # Compatibilidad formato anterior
        'fecha_pura': 'fecha_pedido', 'cat_menu': 'categoria_menu',
        'id_producto': 'sku_producto', 'cantidad': 'cantidad_vendida',
        'venta_real': 'precio_pagar',
    }
    df = df.rename(columns={k: v for k, v in mapeo.items() if k in df.columns})

    # Fecha venta
    fecha_col = next((c for c in ['fecha_pedido','fecha_creacion','Fecha Pedido','Fecha de creacion']
                      if c in df.columns), None)
    if fecha_col:
        df['fecha_venta'] = pd.to_datetime(
            df[fecha_col].astype(str).str[:10], errors='coerce').dt.date
    elif 'fecha_venta' not in df.columns:
        st.error("No se encontró columna de fecha."); return

    df = df.dropna(subset=['fecha_venta', 'sku_producto'])
    df = df[df['sku_producto'].astype(str).str.strip() != '']

    if 'precio_pagar' in df.columns and 'monto_venta_real' not in df.columns:
        df['monto_venta_real'] = pd.to_numeric(df['precio_pagar'], errors='coerce').fillna(0)

    for col in ['cantidad_vendida','monto_venta_real','precio_base','costo_receta','descuento','impuesto']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

    if 'ba_opcion' in df.columns:
        df['es_opcion'] = df['ba_opcion'].notna() & (df['ba_opcion'].astype(str).str.strip() != '')
    else:
        df['es_opcion'] = False

    cols_bd = ['fecha_venta','id_orden','sku_producto','nombre_producto',
               'cantidad_vendida','monto_venta_real','precio_base',
               'costo_receta','descuento','impuesto',
               'ab_categoria','categoria_menu','ba_opcion','jerarquia_extras',
               'ac_excepcion','jerarquia_excepcion','es_opcion',
               'local','mesa','sector','origen','garzon',
               'folio','forma_pago','lista_precio',
               'fecha_pedido','fecha_creacion','fecha_cierre']
    cols_ok = [c for c in cols_bd if c in df.columns]
    df_save = df[cols_ok].copy()

    try:
        fechas    = pd.to_datetime(df_save['fecha_venta'].astype(str), errors='coerce').dropna()
        fecha_min = fechas.min().date().replace(day=1)
        fecha_max = (fechas.max().to_period('M').to_timestamp('M')).date()
        locales   = df_save['local'].dropna().unique().tolist() if 'local' in df_save.columns else []

        with engine.connect() as conn:
            if locales:
                conn.execute(text(
                    "DELETE FROM ventas WHERE fecha_venta BETWEEN :fi AND :ff AND local = ANY(:locales)"),
                    {'fi': fecha_min, 'ff': fecha_max, 'locales': locales})
            else:
                conn.execute(text(
                    "DELETE FROM ventas WHERE fecha_venta BETWEEN :fi AND :ff"),
                    {'fi': fecha_min, 'ff': fecha_max})
            conn.commit()

        df_save.to_sql('ventas', engine, if_exists='append', index=False)
        st.success(f"✅ {len(df_save):,} registros guardados ({fecha_min} → {fecha_max}). Período anterior reemplazado.")
    except Exception as e:
        st.error(f"Error al guardar ventas: {e}")
        st.exception(e)


def save_ventas_chunk(df_raw, engine, skip_delete=False):
    """Versión de save_ventas que inserta un chunk sin hacer DELETE previo."""
    df = df_raw.copy()
    df.columns = df.columns.str.strip()

    mapeo = {
        'ID de orden': 'id_orden', 'ID Producto': 'sku_producto',
        'Nombre': 'nombre_producto', 'Cantidad': 'cantidad_vendida',
        'Precio a Pagar': 'precio_pagar', 'Precio Base': 'precio_base',
        'Costo': 'costo_receta', 'Descuento': 'descuento', 'Impuesto': 'impuesto',
        'AB.': 'ab_categoria',
        'Categorias de Productos/Platos': 'categoria_menu',
        'Categorías de Productos/Platos': 'categoria_menu',
        'BA.': 'ba_opcion',
        'Jerarquia de Extras': 'jerarquia_extras',
        'Jerarquía de Extras': 'jerarquia_extras',
        'AC.': 'ac_excepcion',
        'Jerarquia de Excp.': 'jerarquia_excepcion',
        'Jerarquía de Excp.': 'jerarquia_excepcion',
        'Local': 'local', 'fechahora_pedido': 'fecha_pedido',
        'fechahora_creacion': 'fecha_creacion', 'fechahora_cierre': 'fecha_cierre',
        'Nombre de mesa': 'mesa', 'Sector': 'sector', 'Origen': 'origen',
        'Nombre garzon apertura': 'garzon', 'Nombre garzón apertura': 'garzon',
        'Folio': 'folio', 'Forma de Pago': 'forma_pago',
        'Nombre Lista Precio': 'lista_precio',
    }
    df = df.rename(columns={k: v for k, v in mapeo.items() if k in df.columns})

    fecha_col = next((c for c in ['fecha_pedido','fecha_creacion'] if c in df.columns), None)
    if fecha_col:
        df['fecha_venta'] = pd.to_datetime(
            df[fecha_col].astype(str).str[:10], errors='coerce').dt.date
    else:
        return

    df = df.dropna(subset=['fecha_venta','sku_producto'])
    df = df[df['sku_producto'].astype(str).str.strip() != '']

    if 'precio_pagar' in df.columns and 'monto_venta_real' not in df.columns:
        df['monto_venta_real'] = pd.to_numeric(df['precio_pagar'], errors='coerce').fillna(0)

    for col in ['cantidad_vendida','monto_venta_real','precio_base','costo_receta','descuento','impuesto']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

    if 'ba_opcion' in df.columns:
        df['es_opcion'] = df['ba_opcion'].notna() & (df['ba_opcion'].astype(str).str.strip() != '')
    else:
        df['es_opcion'] = False

    cols_bd = ['fecha_venta','id_orden','sku_producto','nombre_producto',
               'cantidad_vendida','monto_venta_real','precio_base',
               'costo_receta','descuento','impuesto',
               'ab_categoria','categoria_menu','ba_opcion','jerarquia_extras',
               'ac_excepcion','jerarquia_excepcion','es_opcion',
               'local','mesa','sector','origen','garzon',
               'folio','forma_pago','lista_precio',
               'fecha_pedido','fecha_creacion','fecha_cierre']
    cols_ok = [c for c in cols_bd if c in df.columns]
    df[cols_ok].to_sql('ventas', engine, if_exists='append', index=False, method='multi')


# ============================================================
# HELPERS UI
# ============================================================
def semaforo_margen(val):
    if val >= 65:
        return 'background-color: #1a3a2a; color: #4caf7d'
    elif val >= 50:
        return 'background-color: #3a2a1a; color: #e89c45'
    else:
        return 'background-color: #3a1a1a; color: #e84545'


def get_locales():
    df = run_query("SELECT DISTINCT local FROM ventas WHERE local IS NOT NULL ORDER BY 1")
    return ["Todos"] + df['local'].tolist() if not df.empty else ["Todos"]


# ============================================================
# INIT
# ============================================================
init_exclusiones()

# ============================================================
# SIDEBAR
# ============================================================
with st.sidebar:
    st.markdown("""
    <div style='padding: 1rem 0 0.5rem 0;'>
        <span style='font-family: DM Serif Display, serif; font-size: 1.4rem; color: #d4a853;'>
            🍽️ MRP Gastro
        </span><br>
        <span style='font-size: 0.75rem; color: #666; letter-spacing: 0.05em;'>
            SISTEMA DE COSTEOS
        </span>
    </div>
    """, unsafe_allow_html=True)

    st.divider()

    # Menú en cascada elegante
    menu_items = {
        "📦 Gestión de Datos": ["Recetario", "Compras", "Ventas", "Equivalencias SKU"],
        "🧮 Explosión MRP":    [],
        "📊 Informes":         ["Rentabilidad", "Desviación", "Variación Precio Compras", "Informe de Costos"],
        "🔬 Auditor Categorías": [],
        "🍹 Tendencias Bar":   [],
    }

    if 'menu_abierto' not in st.session_state:
        st.session_state['menu_abierto'] = None
    if 'modulo' not in st.session_state:
        st.session_state['modulo'] = "📦 Gestión de Datos"

    # CSS menú
    st.markdown("""
    <style>
    section[data-testid="stSidebar"] button {
        background: transparent !important;
        border: none !important;
        border-radius: 6px !important;
        color: #c8c4be !important;
        font-size: 0.88rem !important;
        font-weight: 500 !important;
        text-align: left !important;
        padding: 8px 12px !important;
        transition: background 0.15s, color 0.15s !important;
        letter-spacing: 0.02em !important;
    }
    section[data-testid="stSidebar"] button:hover {
        background: #1f1f1f !important;
        color: #d4a853 !important;
    }
    section[data-testid="stSidebar"] button p {
        text-align: left !important;
    }
    </style>
    """, unsafe_allow_html=True)

    for item, subitems in menu_items.items():
        es_activo = st.session_state['modulo'].startswith(item[:3])
        label = f"**{item}**" if es_activo else item

        if st.sidebar.button(label, key=f"menu_{item}", use_container_width=True):
            if st.session_state['menu_abierto'] == item and not subitems:
                pass
            elif st.session_state['menu_abierto'] == item:
                st.session_state['menu_abierto'] = None
            else:
                st.session_state['menu_abierto'] = item
            st.session_state['modulo'] = item

        # Subitems
        if subitems and st.session_state['menu_abierto'] == item:
            for sub in subitems:
                sub_key = f"{item} — {sub}"
                es_sub  = st.session_state['modulo'] == sub_key
                prefix  = "▸ " if es_sub else "  · "
                sub_label = f"**{prefix}{sub}**" if es_sub else f"{prefix}{sub}"
                if st.sidebar.button(sub_label, key=f"sub_{sub_key}", use_container_width=True):
                    st.session_state['modulo'] = sub_key
                    st.session_state['menu_abierto'] = item

    modulo = st.session_state['modulo']

    st.divider()
    modulo_actual  = st.session_state.get('modulo','')
    submenu_actual = st.session_state.get('submenu','')
    # El Informe de Costos tiene sus propios filtros — los globales no aplican
    es_informe_costos = 'Informe de Costos' in submenu_actual or 'Informe de Costos' in modulo_actual
    locales = get_locales()
    if not es_informe_costos:
        st.markdown("<div style='font-size:0.75rem; color:#666; text-transform:uppercase; letter-spacing:0.08em;'>Filtros globales</div>", unsafe_allow_html=True)
        f_inicio = st.date_input("Desde", value=date(datetime.now().year, datetime.now().month, 1))
        f_fin    = st.date_input("Hasta", value=date.today())
        f_local  = st.selectbox("Local", locales)
    else:
        f_inicio = date(datetime.now().year, datetime.now().month, 1)
        f_fin    = date.today()
        f_local  = "Todos"



# ============================================================
# FUNCIÓN: GENERAR PDF VARIACIÓN DE PRECIOS
# ============================================================
LOGO_PATH = '/mount/src/mi-costeo/Logo_AE.jpg'

def generar_pdf_variacion(df, mes_base, mes_comp, local='Cadena Completa'):
    import os
    from reportlab.lib.pagesizes import A4, landscape
    from reportlab.lib import colors as rc
    from reportlab.lib.units import mm
    from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer, HRFlowable
    from reportlab.lib.styles import ParagraphStyle
    from reportlab.lib.enums import TA_LEFT, TA_CENTER, TA_RIGHT
    from reportlab.platypus import Image as RLImage

    CB  = rc.HexColor('#0d0d0d')
    CP  = rc.HexColor('#1a1a1a')
    CG  = rc.HexColor('#d4a853')
    CT  = rc.HexColor('#f0ede8')
    CM  = rc.HexColor('#666666')
    CM2 = rc.HexColor('#2a2a2a')
    CR  = rc.HexColor('#e84545')
    CGr = rc.HexColor('#4caf7d')
    CH  = rc.HexColor('#0d0d0d')
    CBR = rc.HexColor('#1e0808')
    CBG = rc.HexColor('#081408')
    CBo = rc.HexColor('#2a2a2a')

    PAGE  = landscape(A4)
    W, H  = PAGE
    LM = RM = 12*mm
    AVAIL = W - LM - RM          # 273mm
    COL   = (AVAIL - 4*mm) / 2   # 134.5mm por columna

    # Columnas tabla (sin categoría para ganar espacio):
    # #(5) | SKU(15) | Producto(flex) | MUCb(20) | MUCc(20) | Δ$(20) | Δ%(14)
    _FIXED = (5 + 15 + 20 + 20 + 20 + 14) * mm
    _PROD  = COL - _FIXED          # ~40.5mm

    def sty(sz, col, bold=False, align=TA_LEFT):
        return ParagraphStyle('_', fontSize=sz, textColor=col,
            fontName='Helvetica-Bold' if bold else 'Helvetica',
            alignment=align, leading=sz * 1.25, spaceAfter=0, spaceBefore=0)

    def P(txt, sz=7, col=None, bold=False, align=TA_RIGHT):
        return Paragraph(str(txt), sty(sz, col or CT, bold=bold, align=align))

    # ── Datos ─────────────────────────────────────────────────
    df = df.copy()
    df['delta_pct_abs'] = df['delta_pct'].abs().fillna(0)
    df['delta_din_abs'] = df['delta_dinero'].abs().fillna(0)
    df_sig   = df[(df['delta_pct_abs'] >= 1.0) | (df['delta_din_abs'] >= 500)]
    top_alza = df_sig[df_sig['delta_dinero'] > 0].nlargest(10, 'delta_dinero')
    top_baja = df_sig[df_sig['delta_dinero'] < 0].nsmallest(10, 'delta_dinero')

    tb = df['impacto_base'].sum()
    tc = df['impacto_comp'].sum()
    td = tc - tb
    tp = (td / tb * 100) if tb > 0 else 0
    n_alza  = int((df['delta_dinero'] > 0).sum())
    n_baja  = int((df['delta_dinero'] < 0).sum())
    n_estab = int((df['delta_dinero'] == 0).sum())
    kc = CR if td > 0 else CGr

    buf = io.BytesIO()
    doc = SimpleDocTemplate(buf, pagesize=PAGE,
        leftMargin=LM, rightMargin=RM, topMargin=8*mm, bottomMargin=7*mm)

    story = []

    # ── ENCABEZADO ────────────────────────────────────────────
    logo_cell = Spacer(28*mm, 22*mm)
    if os.path.exists(LOGO_PATH):
        logo_cell = RLImage(LOGO_PATH, width=28*mm, height=28*mm)

    hdr_data = [[
        logo_cell,
        [Paragraph("INFORME DE VARIACIÓN DE PRECIOS", sty(13, CG, bold=True, align=TA_CENTER)),
         Spacer(1, 1*mm),
         Paragraph("ALEMAN EXPERTO", sty(7, CM, align=TA_CENTER))],
        [Paragraph(local.upper(), sty(10, CT, bold=True, align=TA_RIGHT)),
         Spacer(1, 1*mm),
         Paragraph(f"{mes_base}  →  {mes_comp}", sty(7.5, CM, align=TA_RIGHT))],
    ]]
    hdr_tbl = Table(hdr_data, colWidths=[30*mm, 160*mm, 83*mm])
    hdr_tbl.setStyle(TableStyle([
        ('VALIGN',        (0,0), (-1,-1), 'MIDDLE'),
        ('LINEBELOW',     (0,0), (-1,0),  1.5, CG),
        ('TOPPADDING',    (0,0), (-1,-1), 0),
        ('BOTTOMPADDING', (0,0), (-1,-1), 4),
        ('LEFTPADDING',   (0,0), (-1,-1), 0),
        ('RIGHTPADDING',  (0,0), (-1,-1), 0),
    ]))
    story += [hdr_tbl, Spacer(1, 3*mm)]

    # ── KPIs ─────────────────────────────────────────────────
    kw = AVAIL / 7
    kpi_rows = [
        [P(f"CANASTA {mes_base[:3].upper()}", 5.5, CM, align=TA_CENTER),
         P(f"CANASTA {mes_comp[:3].upper()}",  5.5, CM, align=TA_CENTER),
         P("IMPACTO Δ$",   5.5, CM, align=TA_CENTER),
         P("VARIACIÓN %",  5.5, CM, align=TA_CENTER),
         P("PROD. ALZA ↑", 5.5, CM, align=TA_CENTER),
         P("PROD. BAJA ↓", 5.5, CM, align=TA_CENTER),
         P("SIN CAMBIO",   5.5, CM, align=TA_CENTER)],
        [P(f"${tb:,.0f}",   11, CG,  bold=True, align=TA_CENTER),
         P(f"${tc:,.0f}",   11, CT,  bold=True, align=TA_CENTER),
         P(f"${td:+,.0f}",  11, kc,  bold=True, align=TA_CENTER),
         P(f"{tp:+.1f}%",   11, kc,  bold=True, align=TA_CENTER),
         P(str(n_alza),     11, CR,  bold=True, align=TA_CENTER),
         P(str(n_baja),     11, CGr, bold=True, align=TA_CENTER),
         P(str(n_estab),    11, CM,  bold=True, align=TA_CENTER)],
    ]
    kpi_tbl = Table(kpi_rows, colWidths=[kw]*7)
    kpi_tbl.setStyle(TableStyle([
        ('BACKGROUND',    (0,0), (-1,-1), CP),
        ('BOX',           (0,0), (-1,-1), 0.5, CBo),
        ('LINEBEFORE',    (1,0), (-1,-1), 0.3, CBo),
        ('TOPPADDING',    (0,0), (-1,-1), 3),
        ('BOTTOMPADDING', (0,0), (-1,-1), 3),
        ('LINEABOVE',     (2,0), (3,0),   2, kc),
    ]))
    story += [kpi_tbl, Spacer(1, 4*mm)]

    # ── HELPER tabla top 10 ───────────────────────────────────
    def tabla_top(df_sub, titulo, col_t, col_f):
        if df_sub.empty:
            return [P(titulo, 7, col_t, bold=True),
                    Spacer(1, 2*mm),
                    P("Sin movimientos significativos", 6.5, CM)]
        mb = mes_base[:3]; mc = mes_comp[:3]
        cw   = [5*mm, 15*mm, _PROD, 20*mm, 20*mm, 20*mm, 14*mm]
        hdrs = ['#', 'SKU', 'Producto', f'MUC {mb}', f'MUC {mc}', 'Δ$', 'Δ%']
        rows = [[P(h, 6, CM, bold=True, align=TA_LEFT if i < 3 else TA_RIGHT)
                 for i, h in enumerate(hdrs)]]
        for pos, (_, r) in enumerate(df_sub.iterrows(), 1):
            dd = r.get('delta_dinero', 0) or 0
            dp = r.get('delta_pct',    0) or 0
            dc = CR if dd > 0 else CGr
            rows.append([
                P(str(pos),                         6,   CM,  align=TA_CENTER),
                P(r.get('sku', ''),                 5.5, CM,  align=TA_LEFT),
                P(str(r.get('nombre', '')),         6.5, CT,  align=TA_LEFT),
                P(f"${r.get('precio_base',0):,.0f}",6.5, CM),
                P(f"${r.get('precio_comp',0):,.0f}",6.5, CT),
                P(f"${dd:+,.0f}",                   6.5, dc,  bold=True),
                P(f"{dp:+.1f}%",                    6.5, dc,  bold=True),
            ])
        tbl = Table(rows, colWidths=cw, repeatRows=1)
        rs = [
            ('BACKGROUND',    (0,0), (-1,0),  CH),
            ('LINEBELOW',     (0,0), (-1,0),  0.8, col_t),
            ('TOPPADDING',    (0,0), (-1,-1), 2),
            ('BOTTOMPADDING', (0,0), (-1,-1), 2),
            ('LEFTPADDING',   (0,0), (-1,-1), 3),
            ('RIGHTPADDING',  (0,0), (-1,-1), 3),
        ]
        for i in range(1, len(rows)):
            rs += [('BACKGROUND', (0,i), (-1,i), col_f if i%2==0 else CP),
                   ('LINEBELOW',  (0,i), (-1,i), 0.2, CM2)]
        tbl.setStyle(TableStyle(rs))
        return [P(titulo, 7, col_t, bold=True), Spacer(1, 1.5*mm), tbl]

    # ── DOS COLUMNAS LADO A LADO ──────────────────────────────
    alza = tabla_top(top_alza, "▲  TOP 10 ALZAS  (mayor impacto $)", CR,  CBR)
    baja = tabla_top(top_baja, "▼  TOP 10 BAJAS  (mayor ahorro $)",  CGr, CBG)

    dos_col = Table(
        [[alza, [Spacer(4*mm, 1)], baja]],
        colWidths=[COL, 4*mm, COL]
    )
    dos_col.setStyle(TableStyle([
        ('VALIGN',        (0,0), (-1,-1), 'TOP'),
        ('LINEBEFORE',    (2,0), (2,0),   0.4, CBo),
        ('LEFTPADDING',   (0,0), (-1,-1), 0),
        ('RIGHTPADDING',  (0,0), (-1,-1), 0),
        ('TOPPADDING',    (0,0), (-1,-1), 0),
        ('BOTTOMPADDING', (0,0), (-1,-1), 0),
    ]))
    story += [dos_col, Spacer(1, 3*mm)]

    # ── FOOTER ───────────────────────────────────────────────
    story += [
        HRFlowable(width="100%", thickness=0.4, color=CBo),
        Spacer(1, 1*mm),
        P(f"Aleman Experto  ·  {mes_base} vs {mes_comp}  ·  {local}  ·  Productos con variación ≥1% o Δ$≥$500",
          5.5, CM, align=TA_CENTER),
    ]

    def add_bg(c, d):
        c.saveState()
        c.setFillColor(CB)
        c.rect(0, 0, W, H, fill=1, stroke=0)
        c.restoreState()

    doc.build(story, onFirstPage=add_bg, onLaterPages=add_bg)
    buf.seek(0)
    return buf.getvalue()

# ============================================================
# MÓDULO: GESTIÓN DE DATOS
# ============================================================
if modulo.startswith("📦"):
    st.markdown(f"""
    <div style="margin-bottom:1.5rem">
        <div style="font-size:0.72rem;text-transform:uppercase;letter-spacing:0.12em;color:#555;margin-bottom:4px">Módulo</div>
        <div style="font-family:'DM Serif Display',serif;font-size:2rem;color:#f0ede8;letter-spacing:-0.02em;line-height:1.1">
            📦 Gestión de Datos
        </div>
        <div style="width:40px;height:2px;background:#d4a853;margin-top:8px;border-radius:2px"></div>
    </div>
    """, unsafe_allow_html=True)

    tab1, tab2, tab3, tab4, tab5, tab6, tab7 = st.tabs(["📖 Recetario", "🛒 Compras", "📈 Ventas", "🔀 Equivalencias SKU", "🔍 Auditoría Compras", "📦 Inventario / Uso", "🗂️ Clasificación"])

    with tab1:
        st.markdown("<div class='info-box'>Carga las hojas <b>Directos</b> y <b>Procesados</b> de tu recetario. Esto reemplaza el recetario actual.</div>", unsafe_allow_html=True)
        c1, c2 = st.columns(2)
        with c1:
            f_dir  = st.file_uploader("Hoja Directos (.xlsx)", type="xlsx", key="dir")
        with c2:
            f_proc = st.file_uploader("Hoja Procesados (.xlsx)", type="xlsx", key="proc")
        if f_dir and f_proc:
            if st.button("🔄 Sincronizar Recetario"):
                save_recetario(pd.read_excel(f_dir), pd.read_excel(f_proc))

        st.markdown("---")
        df_rec_view = run_query("SELECT * FROM recetas LIMIT 200")
        if not df_rec_view.empty:
            st.caption(f"Vista previa recetario — {len(df_rec_view)} filas (máx 200)")
            st.dataframe(df_rec_view, use_container_width=True, hide_index=True)

    with tab2:
        st.markdown("""
        <div style="margin-bottom:1.5rem">
            <div style="font-size:0.72rem;text-transform:uppercase;letter-spacing:0.12em;color:#555;margin-bottom:4px">Gestión de Datos</div>
            <div style="font-family:'DM Serif Display',serif;font-size:2rem;color:#f0ede8;letter-spacing:-0.02em;line-height:1.1">
                🧾 Procesado de Compras
            </div>
            <div style="font-size:0.8rem;color:#888;margin-top:4px">Carga · Procesa · Valida · Guarda</div>
            <div style="width:40px;height:2px;background:#d4a853;margin-top:8px;border-radius:2px"></div>
        </div>
        """, unsafe_allow_html=True)
        st.markdown("""
        <div class='info-box'>
        Carga el Excel de facturas del período. El sistema calcula automáticamente
        <strong>cant_conv, monto_real, recargo2, imp_adic, IVA_2, tootal2, costo_realfinal y MUC</strong>,
        distribuye despachos y ajusta redondeos antes de guardar en la base de datos.
        </div>
        """, unsafe_allow_html=True)

        f_comp = st.file_uploader("📂 Excel de Compras fuente (.xlsx)", type="xlsx", key="comp")

        if f_comp:
            # ── Leer archivo ─────────────────────────────────────────────
            if 'df_compras_procesado' not in st.session_state or \
               st.session_state.get('comp_filename') != f_comp.name:
                with st.spinner("Procesando archivo..."):
                    df_raw = pd.read_excel(f_comp)
                    df_proc, warns = procesar_compras(df_raw)
                    st.session_state['df_compras_procesado'] = df_proc
                    st.session_state['comp_warnings'] = warns
                    st.session_state['comp_filename'] = f_comp.name

            df_proc = st.session_state['df_compras_procesado']
            warns   = st.session_state.get('comp_warnings', [])

            # ── Advertencias ─────────────────────────────────────────────
            for w in warns:
                st.warning(w)

            # ── Métricas resumen ─────────────────────────────────────────
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric("Líneas procesadas", f"{len(df_proc):,}")
            with col2:
                n_folios = df_proc['folio'].nunique() if 'folio' in df_proc.columns else 0
                st.metric("Folios únicos", f"{n_folios:,}")
            with col3:
                tot = df_proc['costo_realfinal'].sum() if 'costo_realfinal' in df_proc.columns else 0
                st.metric("Costo total procesado", f"${tot:,.0f}")
            with col4:
                n_desp = df_proc['nombre_producto'].str.lower().str.contains(
                    'despacho|flete|distribucion', na=False).sum()
                st.metric("Líneas despacho", f"{n_desp:,}")

            st.markdown("---")

            # ── Validador: comparar costo_realfinal vs Total factura ──────────
            with st.expander("🔍 Validación por folio — Diferencias vs Total declarado", expanded=False):
                if 'total' in df_proc.columns and 'folio' in df_proc.columns:
                    subcat_col = next((c for c in df_proc.columns if c == 'subcat'), None)

                    if subcat_col:
                        # Solo folios donde TODAS las líneas son Directo o Indirecto
                        # (excluir folios mixtos donde el Total de factura incluye otras subcats)
                        subcat_por_folio = df_proc.groupby('folio')[subcat_col].apply(
                            lambda s: s.isin(['Directo','Indirecto']).all()
                        )
                        folios_puros = subcat_por_folio[subcat_por_folio].index
                        df_val = df_proc[df_proc['folio'].isin(folios_puros)]
                        n_mixtos = df_proc['folio'].nunique() - len(folios_puros)
                    else:
                        df_val = df_proc
                        n_mixtos = 0

                    val = df_val.groupby(['folio','rut_proveedor']).agg(
                        nombre_proveedor=('nombre_proveedor', 'first'),
                        total_declarado=('total', 'max'),
                        costo_calculado=('costo_realfinal', 'sum'),
                        categoria=('categoria_producto', lambda x: x.dropna().mode().iloc[0] if not x.dropna().empty else ''),
                        productos=('nombre_producto', lambda x: ' / '.join(x.dropna().unique()[:3])),
                    ).reset_index()
                    val['diferencia'] = val['total_declarado'] - val['costo_calculado']
                    val['dif_abs'] = val['diferencia'].abs()
                    val_issues = val[val['dif_abs'] > 1].sort_values('dif_abs', ascending=False)

                    c1v, c2v, c3v = st.columns(3)
                    c1v.metric("Folios validados", f"{len(val):,}")
                    c2v.metric("Folios mixtos (excluidos)", f"{n_mixtos:,}",
                               help="Folios con Directo/Indirecto + otras subcats — el Total de factura no es comparable con solo las líneas MRP")
                    c3v.metric("Folios con diferencia > $1", f"{len(val_issues):,}")

                    if val_issues.empty:
                        st.success("✅ Todos los folios cuadran con el total declarado.")
                    else:
                        st.warning(f"⚠️ {len(val_issues)} folio(s) con diferencia > $1 — revisar")
                        st.dataframe(
                            val_issues[['folio','nombre_proveedor','categoria','productos','total_declarado','costo_calculado','diferencia']],
                            use_container_width=True, hide_index=True
                        )
                    st.caption("ℹ️ Se validan solo folios donde el 100% de líneas son Directo o Indirecto. Los folios mixtos tienen un Total de factura que incluye otras categorías.")
                else:
                    st.info("No se encontró columna 'total' para validar.")

            # ── Vista previa del resultado ────────────────────────────────
            cols_preview = [
                'local', 'fecha_dte', 'folio', 'nombre_producto', 'sku', 'subcat',
                'cantidad', 'conversion', 'cant_conv',
                'monto_real', 'recargo2', 'total_neto2',
                'imp_adic', 'iva_2', 'tootal2', 'costo_realfinal', 'muc'
            ]
            cols_preview = [c for c in cols_preview if c in df_proc.columns]

            st.markdown("#### Vista previa")
            filtro_local_c = st.selectbox(
                "Filtrar por local",
                ["Todos"] + sorted(df_proc['local'].dropna().unique().tolist()) if 'local' in df_proc.columns else ["Todos"],
                key="comp_filtro_local"
            )
            df_vista = df_proc if filtro_local_c == "Todos" else df_proc[df_proc['local'] == filtro_local_c]
            st.caption(f"{len(df_vista):,} líneas")
            st.dataframe(df_vista[cols_preview].head(500), use_container_width=True, hide_index=True)

            st.markdown("---")

            # ── Descargar resultado procesado ────────────────────────────
            buf = io.BytesIO()
            df_proc.to_excel(buf, index=False)
            buf.seek(0)
            st.download_button(
                label="⬇️ Descargar Excel procesado",
                data=buf,
                file_name=f"compras_procesadas_{f_comp.name}",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )

            # ── Guardar en base de datos ─────────────────────────────────
            st.markdown("#### Guardar en base de datos")
            st.markdown(
                "<div class='info-box'>Al guardar, el sistema <strong>elimina automáticamente</strong> los registros "
                "del mismo período y locales antes de insertar — puedes recargar el mismo archivo sin riesgo de duplicados.</div>",
                unsafe_allow_html=True
            )
            if st.button("💾 Guardar en base de datos", type="primary"):
                save_compras(df_proc)
        else:
            st.info("Carga el archivo Excel fuente para comenzar el procesado.")

    with tab3:
        st.markdown("<div class='info-box'>Carga el historial de ventas exportado desde tu POS. Se añade al historial existente (append).</div>", unsafe_allow_html=True)
        f_ven = st.file_uploader("Archivo de Ventas (.csv)", type=["csv"], key="ven")
        if f_ven:
            size_mb = f_ven.size / 1024 / 1024
            st.caption(f"Archivo: {f_ven.name} — {size_mb:.1f} MB")
            if st.button("💾 Cargar Ventas"):
                import io as _io2
                raw = f_ven.read()
                sep = ';' if b';' in raw[:500] else ','
                with st.spinner("Cargando..."):
                    df_ven = pd.read_csv(_io2.BytesIO(raw), sep=sep, dtype=str)
                    save_ventas(df_ven)

    with tab4:
        st.markdown("<div class='info-box'>Mapea SKUs de compras sin código de venta hacia SKUs equivalentes que sí tienen receta.<br>Ejemplo: Erdinger Trigo (BA-CA-078) → Erdinger Weissbier (BA-CA-066)</div>", unsafe_allow_html=True)

        df_eq = run_query("SELECT sku_compra, sku_receta, descripcion FROM sku_equivalencias ORDER BY sku_compra")
        if not df_eq.empty:
            st.caption(f"{len(df_eq)} equivalencias registradas")
            st.dataframe(df_eq, use_container_width=True, hide_index=True)
        else:
            st.caption("No hay equivalencias registradas aún.")

        st.markdown("#### Agregar equivalencia")
        c1, c2, c3 = st.columns(3)
        with c1: sku_compra_in = st.text_input("SKU Compras (origen)", placeholder="BA-CA-078")
        with c2: sku_receta_in = st.text_input("SKU Receta (destino)", placeholder="BA-CA-066")
        with c3: desc_in = st.text_input("Descripción", placeholder="Erdinger Trigo -> Weissbier")

        if st.button("Agregar Equivalencia"):
            if sku_compra_in and sku_receta_in:
                engine = get_engine()
                try:
                    with engine.connect() as conn:
                        conn.execute(text(
                            "INSERT INTO sku_equivalencias (sku_compra, sku_receta, descripcion) "
                            "VALUES (:c, :r, :d) "
                            "ON CONFLICT (sku_compra) DO UPDATE SET sku_receta = :r, descripcion = :d"
                        ), {"c": sku_compra_in.strip(), "r": sku_receta_in.strip(), "d": desc_in.strip()})
                        conn.commit()
                    st.success(f"Equivalencia guardada: {sku_compra_in} -> {sku_receta_in}")
                    st.rerun()
                except Exception as e:
                    st.error(f"Error: {e}")
            else:
                st.warning("Completa SKU origen y destino.")

        if not df_eq.empty:
            st.markdown("#### Eliminar equivalencia")
            sku_del = st.selectbox("Seleccionar SKU a eliminar", df_eq['sku_compra'].tolist())
            if st.button("Eliminar"):
                engine = get_engine()
                try:
                    with engine.connect() as conn:
                        conn.execute(text("DELETE FROM sku_equivalencias WHERE sku_compra = :c"), {"c": sku_del})
                        conn.commit()
                    st.success(f"Eliminada equivalencia para {sku_del}")
                    st.rerun()
                except Exception as e:
                    st.error(f"Error: {e}")

    # ================================================================
    # TAB 5 — AUDITORÍA DE COMPRAS
    # ================================================================
    with tab5:
        st.markdown("<div class='info-box'>Detecta inconsistencias en <b>conversion</b> y <b>formato</b> comparando el MUC de cada registro contra la mediana histórica del SKU. Un MUC muy alejado de la mediana indica que el precio, conversion o formato están mal configurados.</div>", unsafe_allow_html=True)

        # Controles
        ac1, ac2 = st.columns([2, 2])
        with ac1:
            umbral_audit = st.slider("Umbral de alerta (× esperado)", min_value=2.0, max_value=20.0, value=5.0, step=0.5,
                                     key='audit_umbral')
        with ac2:
            cat_audit_q = run_query("SELECT DISTINCT categoria_producto FROM compras WHERE categoria_producto IS NOT NULL ORDER BY 1")
            cats_raw = cat_audit_q['categoria_producto'].tolist() if not cat_audit_q.empty else []
            cats_colacion = [c for c in cats_raw if 'colacion' in c.lower().replace('ó','o').replace('ô','o') or c.upper() == 'COLACION']
            cats_normales = [c for c in cats_raw if c not in cats_colacion]
            cats_audit = ['Todas (sin Colación)'] + cats_normales + (['── Colación ──'] if cats_colacion else []) + cats_colacion
            cat_audit_sel = st.selectbox("Categoría", cats_audit, key='audit_cat')

        def _run_audit_query():
            cat_sel = st.session_state.get('audit_cat', 'Todas (sin Colación)')
            if cat_sel in ('Todas (sin Colación)', '── Colación ──'):
                fcat   = "AND UPPER(categoria_producto) NOT LIKE '%COLACION%' AND UPPER(categoria_producto) NOT LIKE '%COLACIÓN%'"
                fcat_c = "AND UPPER(g.categoria) NOT LIKE '%COLACION%' AND UPPER(g.categoria) NOT LIKE '%COLACIÓN%'"
            else:
                fcat   = f"AND categoria_producto = '{cat_sel}'"
                fcat_c = f"AND g.categoria = '{cat_sel}'"
            umbral = st.session_state.get('audit_umbral', 5.0)
            return fcat, fcat_c, umbral

        if st.button("▶ Ejecutar Auditoría"):
            st.session_state.pop('audit_df', None)

        if 'audit_df' not in st.session_state:
            cat_sel = st.session_state.get('audit_cat', 'Todas (sin Colación)')
            umbral_audit = st.session_state.get('audit_umbral', 5.0)
            if cat_sel in ('Todas (sin Colación)', '── Colación ──'):
                filtro_cat_audit   = "AND UPPER(categoria_producto) NOT LIKE '%COLACION%' AND UPPER(categoria_producto) NOT LIKE '%COLACIÓN%'"
                filtro_cat_audit_c = "AND UPPER(g.categoria) NOT LIKE '%COLACION%' AND UPPER(g.categoria) NOT LIKE '%COLACIÓN%'"
            else:
                filtro_cat_audit   = f"AND categoria_producto = '{cat_sel}'"
                filtro_cat_audit_c = f"AND g.categoria = '{cat_sel}'"
            q_audit = f"""
                WITH grupos AS (
                    SELECT
                        sku,
                        ROUND(muc::numeric, 1)                                          AS muc_grupo,
                        COUNT(*)                                                        AS n_registros,
                        ARRAY_AGG(id)                                                   AS ids,
                        MODE() WITHIN GROUP (ORDER BY nombre_producto)                  AS nombre_producto,
                        MODE() WITHIN GROUP (ORDER BY nombre_proveedor)                 AS proveedor,
                        MAX(categoria_producto)                                         AS categoria,
                        MAX(conversion)                                                 AS conversion,
                        MAX(formato)                                                    AS formato,
                        ROUND(AVG(monto_real / NULLIF(cantidad, 0))::numeric, 2)       AS precio_factura
                    FROM compras
                    WHERE muc > 0
                      AND costo_realfinal > 0
                      AND monto_real > 0
                      AND UPPER(sku) != 'COLACION'
                      AND UPPER(sku) NOT IN ('N. CREDITO', 'NCR')
                      AND id NOT IN (SELECT compra_id FROM compras_excluidas)
                      AND sku NOT IN (SELECT sku FROM sku_colacion)
                      AND UPPER(subcat) NOT LIKE '%COLACION%'
                      AND UPPER(subcat) NOT LIKE '%COLACIÓN%'
                      {filtro_cat_audit}
                    GROUP BY sku, ROUND(muc::numeric, 1)
                    HAVING ROUND(muc::numeric, 1) > 0
                ),
                dispersos AS (
                    SELECT
                        sku,
                        MAX(muc_grupo)                                                  AS muc_max,
                        MIN(muc_grupo)                                                  AS muc_min,
                        ROUND((MAX(muc_grupo) / NULLIF(MIN(muc_grupo), 0))::numeric, 1) AS dispersion,
                        COUNT(*)                                                        AS n_grupos
                    FROM grupos
                    WHERE muc_grupo > 0
                    GROUP BY sku
                    HAVING COUNT(*) >= 2
                       AND MAX(muc_grupo) / NULLIF(MIN(muc_grupo), 0) > {umbral_audit}
                )
                SELECT
                    g.sku,
                    g.nombre_producto,
                    g.categoria,
                    g.proveedor,
                    g.conversion,
                    g.formato,
                    g.muc_grupo                                                         AS muc,
                    g.n_registros,
                    g.ids,
                    g.precio_factura,
                    d.muc_min,
                    d.muc_max,
                    d.dispersion
                FROM grupos g
                JOIN dispersos d ON g.sku = d.sku
                  {filtro_cat_audit_c.replace('c.sku', 'g.sku').replace('c.categoria_producto', 'g.categoria')}
                ORDER BY d.dispersion DESC, g.sku, g.muc_grupo
                LIMIT 500
            """
            df_audit = run_query(q_audit)
            st.session_state['audit_df'] = df_audit

        if 'audit_df' in st.session_state:
            df_audit = st.session_state['audit_df'].copy()

            if df_audit.empty:
                st.success("✅ Sin inconsistencias detectadas con el umbral configurado.")
            else:
                # Marcar revisados en session_state
                if 'audit_revisados' not in st.session_state:
                    st.session_state['audit_revisados'] = set()

                n_total = len(df_audit)
                n_rev   = len(st.session_state['audit_revisados'])
                am1, am2, am3 = st.columns(3)
                am1.metric("⚠️ Inconsistencias", n_total)
                am2.metric("✅ Revisadas", n_rev)
                am3.metric("⏳ Pendientes", n_total - n_rev)

                st.markdown("<br>", unsafe_allow_html=True)

                # ── Construir grupos ──────────────────────────────────────
                ids_disp    = df_audit['id'].astype(str).tolist() if 'id' in df_audit.columns else []
                nombres_disp = (df_audit['sku'] + ' — ' + df_audit['nombre_producto']).tolist()

                # Cada fila ya es un grupo SKU+MUC — construir labels para el buscador
                if not df_audit.empty:
                    df_audit['_label'] = df_audit.apply(
                        lambda r: f"{r['sku']} — {r['nombre_producto'][:40]} | MUC {float(r['muc']):.4f} ({int(r['n_registros'])} reg. | {float(r['dispersion']):.0f}×)",
                        axis=1
                    )
                    # Para el selector de SKU (agrupa todas las filas del mismo SKU)
                    grupos = df_audit.groupby('sku').agg(
                        nombre     = ('nombre_producto', 'first'),
                        dispersion = ('dispersion', 'first'),
                        n_filas    = ('n_registros', 'sum'),
                    ).reset_index()
                    grupos = grupos.sort_values('dispersion', ascending=False).reset_index(drop=True)
                    grupos['label'] = grupos.apply(
                        lambda r: f"{r['sku']} — {r['nombre'][:40]} ({int(r['n_filas'])} reg. | {float(r['dispersion']):.0f}×)",
                        axis=1
                    )
                else:
                    grupos = pd.DataFrame()

                # Buscador: selectbox con búsqueda nativa — una opción por SKU+MUC
                if not df_audit.empty:
                    opciones_muc = df_audit.apply(
                        lambda r: f"{r['sku']} — {r['nombre_producto'][:40]} | MUC {float(r['muc']):.1f} ({int(r['n_registros'])} reg.)",
                        axis=1
                    ).tolist()
                else:
                    opciones_muc = []

                label_sel_muc = st.selectbox("🔍 Buscar SKU / producto / MUC",
                                             [None] + opciones_muc,
                                             format_func=lambda x: "— Todos —" if x is None else x,
                                             key='audit_grupo_sel')

                # Extraer SKU seleccionado del label
                if label_sel_muc:
                    sku_activo = label_sel_muc.split(" — ")[0].strip()
                    label_sel = grupos[grupos['sku'] == sku_activo]['label'].iloc[0] if not grupos.empty and sku_activo in grupos['sku'].values else None
                else:
                    label_sel = None

                # ── Inspector libre de SKU ────────────────────────────────
                with st.expander("🔎 Inspeccionar cualquier SKU", expanded=False):
                    sku_inspect = st.text_input("SKU exacto", key='audit_inspect_sku', placeholder="ej: AL-AF-095")
                    if sku_inspect:
                        q_inspect = f"""
                            SELECT
                                ROUND(muc::numeric, 1)                                    AS muc,
                                COUNT(*)                                                  AS n_registros,
                                ARRAY_AGG(id)                                             AS ids,
                                ROUND(AVG(monto_real / NULLIF(cantidad, 0))::numeric, 2) AS precio_factura,
                                MAX(conversion)                                           AS conversion,
                                MAX(formato)                                              AS formato,
                                MODE() WITHIN GROUP (ORDER BY nombre_producto)                                      AS nombre_producto,
                                MAX(categoria_producto)                                   AS categoria
                            FROM compras
                            WHERE UPPER(sku) = UPPER('{sku_inspect}')
                              AND muc > 0 AND costo_realfinal > 0 AND monto_real > 0
                              AND ROUND(muc::numeric, 1) > 0
                            GROUP BY ROUND(muc::numeric, 1)
                            ORDER BY ROUND(muc::numeric, 1)
                        """
                        df_inspect = run_query(q_inspect)
                        if df_inspect.empty:
                            st.warning(f"SKU '{sku_inspect}' no encontrado o sin registros.")
                        else:
                            nombre_insp = df_inspect['nombre_producto'].iloc[0]
                            cat_insp    = df_inspect['categoria'].iloc[0]
                            muc_min_i   = float(df_inspect['muc'].min())
                            muc_max_i   = float(df_inspect['muc'].max())
                            disp_i      = round(muc_max_i / muc_min_i, 1) if muc_min_i > 0 else 0
                            if disp_i > 8:
                                badge = f'🔴 {disp_i:.0f}×'
                            elif disp_i > 2:
                                badge = f'🟡 {disp_i:.1f}×'
                            else:
                                badge = f'⚪ {disp_i:.1f}×'
                            st.caption(f"**{nombre_insp}** | {cat_insp} | {len(df_inspect)} grupos MUC | dispersión {badge}")

                            # Tabla igual al informe
                            hs_i = 'padding:9px 12px;font-size:0.68rem;text-transform:uppercase;letter-spacing:0.09em;font-weight:600;color:#444;border-bottom:1px solid #2a2a2a'
                            rows_i = ''
                            for _, r in df_inspect.iterrows():
                                muc_i  = float(r['muc'])
                                es_min = abs(muc_i - muc_min_i) < 0.0001
                                es_max = abs(muc_i - muc_max_i) < 0.0001
                                es_out = es_min or es_max
                                mc     = '#e84545' if es_out else '#aaa'
                                precio_i = float(r['precio_factura'] or 0)
                                if disp_i > 8:
                                    sc = '#e84545'; sl = f'🔴 {disp_i:.0f}×'
                                elif disp_i > 2:
                                    sc = '#e89c45'; sl = f'🟡 {disp_i:.1f}×'
                                else:
                                    sc = '#aaa';    sl = f'⚪ {disp_i:.1f}×'
                                rows_i += (
                                    f'<tr style="border-bottom:1px solid #1e1e1e">'
                                    f'<td style="padding:9px 12px;color:#666;font-family:monospace;font-size:0.72rem">{sku_inspect.upper()}</td>'
                                    f'<td style="padding:9px 12px;font-weight:500;color:#e8e4de;font-size:0.8rem">{r.get("nombre_producto","")}</td>'
                                    f'<td style="padding:9px 12px;color:#666;font-size:0.75rem">{r.get("categoria","")}</td>'
                                    f'<td style="padding:9px 12px;text-align:right;color:#888">{r["conversion"]}</td>'
                                    f'<td style="padding:9px 12px;text-align:right;color:#888">{r["formato"]}</td>'
                                    f'<td style="padding:9px 12px;text-align:right;color:#aaa;font-variant-numeric:tabular-nums">${precio_i:,.2f}</td>'
                                    f'<td style="padding:9px 12px;text-align:right;color:{mc};font-weight:{"700" if es_out else "400"};font-variant-numeric:tabular-nums">{muc_i:.4f}</td>'
                                    f'<td style="padding:9px 12px;text-align:right;color:#666">{int(r["n_registros"])}</td>'
                                    f'<td style="padding:9px 12px;text-align:center;color:{sc};font-weight:600">{sl}</td>'
                                    f'</tr>'
                                )
                            hdrs_i = ['SKU','Producto','Categoría','Conv.','Formato','Neto Fact/u','MUC','# Reg.','Dispersión']
                            st.markdown(
                                '<div style="overflow-x:auto;border-radius:14px;border:1px solid #1e1e1e;margin-top:0.5rem;background:#0d0d0d">'
                                '<table style="width:100%;border-collapse:collapse;font-family:DM Sans,sans-serif;font-size:0.82rem">'
                                '<thead><tr style="background:#111">'
                                + ''.join([f'<th style="{hs_i};text-align:{"left" if i<3 else "right" if i<8 else "center"}">{h}</th>' for i, h in enumerate(hdrs_i)])
                                + f'</tr></thead><tbody>{rows_i}</tbody></table></div>',
                                unsafe_allow_html=True
                            )

                            # Acciones de corrección
                            st.markdown("<br>", unsafe_allow_html=True)
                            st.markdown("**Corregir grupo**")
                            df_inspect = df_inspect.reset_index(drop=True)
                            opciones_inspect = list(range(len(df_inspect)))
                            idx_fix = st.selectbox(
                                "MUC a corregir",
                                opciones_inspect,
                                format_func=lambda i: f"{float(df_inspect.iloc[i]['muc']):.4f}  ({int(df_inspect.iloc[i]['n_registros'])} reg.)",
                                key='inspect_muc_fix'
                            )
                            fila_fix = df_inspect.iloc[idx_fix]
                            ic1, ic2 = st.columns(2)
                            with ic1:
                                conv_fix = st.number_input("Nueva conversion", value=float(fila_fix['conversion'] or 1),
                                                           min_value=0.001, step=0.1, key='inspect_conv_fix')
                            with ic2:
                                fmt_fix  = st.number_input("Nuevo formato", value=float(fila_fix['formato'] or 1),
                                                           min_value=0.001, step=1.0, key='inspect_fmt_fix')
                            import ast as _ast2
                            raw_ids_i = fila_fix['ids']
                            if isinstance(raw_ids_i, str):
                                raw_ids_i = _ast2.literal_eval(raw_ids_i)
                            ids_fix = [int(i) for i in raw_ids_i]
                            st.caption(f"Afecta **{len(ids_fix)}** registros")
                            if st.button("💾 Aplicar corrección", key='inspect_apply'):
                                engine = get_engine()
                                try:
                                    with engine.connect() as conn:
                                        conn.execute(text("""
                                            UPDATE compras SET
                                                conversion = :conv, formato = :fmt,
                                                cant_conv  = cantidad * :conv,
                                                muc        = CASE WHEN :fmt = 1
                                                             THEN costo_realfinal / NULLIF(cantidad * :conv, 0)
                                                             ELSE costo_realfinal / NULLIF(cantidad * :conv * :fmt, 0) END
                                            WHERE id = ANY(:ids)
                                        """), {"conv": conv_fix, "fmt": fmt_fix, "ids": ids_fix})
                                        conn.commit()
                                    st.success(f"✅ {len(ids_fix)} registros corregidos")
                                    st.session_state.pop('audit_df', None)
                                    st.rerun()
                                except Exception as e:
                                    st.error(f"Error: {e}")



                # ══════════════════════════════════════════════════════════
                # TABLA CON CHECKBOXES + CONTROLES
                # ══════════════════════════════════════════════════════════

                # Filtrar tabla según búsqueda
                if label_sel_muc:
                    sku_fil = label_sel_muc.split(' — ')[0].strip()
                    df_tabla = df_audit[df_audit['sku'] == sku_fil].reset_index(drop=True)
                else:
                    df_tabla = df_audit.reset_index(drop=True)

                st.caption(f"{'SKU: ' + sku_fil if label_sel_muc else 'Todos los SKUs'} — {len(df_tabla)} grupos MUC")

                import ast as _ast
                def _parse_ids(raw):
                    if isinstance(raw, list): return [int(i) for i in raw]
                    try: return [int(i) for i in _ast.literal_eval(str(raw))]
                    except: return []

                def _limpiar_seleccion():
                    st.session_state.pop('audit_df', None)
                    for k in [k for k in st.session_state if k.startswith('chk_')]:
                        del st.session_state[k]

                # Inicializar selección vacía — se llena después de renderizar la tabla
                sel_rows = pd.DataFrame()
                n_sel    = 0
                ids_sel  = []
                skus_sel = []
                # Leer selección del multiselect (si ya existe en session_state)
                _prev_sel = st.session_state.get('audit_multisel', [])
                if _prev_sel:
                    def _opt_label(r):
                        d = float(r["dispersion"])
                        emoji = "🔴" if d > 8 else "🟡" if d > 2 else "⚪"
                        df = f"{d:.0f}" if d > 8 else f"{d:.1f}"
                        return f'{r["sku"]} | MUC {float(r["muc"]):.4f} | {int(r["n_registros"])} reg. | {emoji} {df}×'
                    _opciones_tmp = [_opt_label(r) for _, r in df_tabla.iterrows()]
                    sel_indices_prev = [i for i, opt in enumerate(_opciones_tmp) if opt in _prev_sel]
                    if sel_indices_prev:
                        sel_rows = df_tabla.iloc[sel_indices_prev].reset_index(drop=True)
                        n_sel    = len(sel_rows)
                        for _, r in sel_rows.iterrows():
                            ids_sel  += _parse_ids(r['ids'])
                            skus_sel.append(r['sku'])
                        skus_sel = list(set(skus_sel))

                # Panel de acciones ARRIBA de la tabla
                if n_sel > 0:
                    st.markdown(f'**⚙️ {n_sel} grupo(s) — {len(ids_sel)} registros**')
                    pa1, pa2, pa3, pa4, pa5 = st.columns([2, 2, 1, 1, 1])
                    with pa1:
                        nuevo_conv = st.number_input('Nueva conversion', value=float(sel_rows.iloc[0]['conversion'] or 1), min_value=0.001, step=0.1, key='audit_conv_multi')
                    with pa2:
                        nuevo_fmt = st.number_input('Nuevo formato', value=float(sel_rows.iloc[0]['formato'] or 1), min_value=0.001, step=1.0, key='audit_fmt_multi')
                    with pa3:
                        if st.button('💾 Aplicar', key='audit_apply_multi'):
                            engine = get_engine()
                            try:
                                with engine.connect() as conn:
                                    check = pd.read_sql(text('SELECT id, sku FROM compras WHERE id = ANY(:ids)'), conn, params={'ids': ids_sel})
                                    ids_incorrectos = check[~check['sku'].isin(skus_sel)]['id'].tolist()
                                    if ids_incorrectos:
                                        st.error(f'🚫 {len(ids_incorrectos)} IDs no coinciden. Cancelado.')
                                    else:
                                        conn.execute(text('UPDATE compras SET conversion=:conv,formato=:fmt,cant_conv=cantidad*:conv,muc=CASE WHEN :fmt=1 THEN costo_realfinal/NULLIF(cantidad*:conv,0) ELSE costo_realfinal/NULLIF(cantidad*:conv*:fmt,0) END WHERE id=ANY(:ids) AND sku=ANY(:skus)'), {'conv': nuevo_conv, 'fmt': nuevo_fmt, 'ids': ids_sel, 'skus': skus_sel})
                                        conn.commit()
                                        st.success(f'✅ {len(ids_sel)} registros corregidos')
                                        _limpiar_seleccion()
                                        st.rerun()
                            except Exception as e:
                                st.error(f'Error: {e}')
                    with pa4:
                        if st.button('🚨 Emergencia', key='audit_emerg_multi'):
                            engine = get_engine()
                            try:
                                with engine.connect() as conn:
                                    for _, r in sel_rows.iterrows():
                                        conn.execute(text('INSERT INTO compras_excluidas (compra_id, sku, motivo) SELECT unnest(:ids), :sku, :motivo ON CONFLICT (compra_id) DO NOTHING'), {'ids': _parse_ids(r['ids']), 'sku': r['sku'], 'motivo': 'compra_emergencia'})
                                    conn.commit()
                                st.success(f'🚨 {len(ids_sel)} registros excluidos')
                                _limpiar_seleccion()
                                st.rerun()
                            except Exception as e:
                                st.error(f'Error: {e}')
                    with pa5:
                        if st.button('✅ Revisado', key='audit_rev_multi'):
                            nota_rev = st.session_state.get('audit_nota_rev', '')
                            engine = get_engine()
                            try:
                                with engine.connect() as conn:
                                    for _, r in sel_rows.iterrows():
                                        conn.execute(text('INSERT INTO audit_revisados (sku, muc, nombre, n_registros, nota) VALUES (:sku, :muc, :nombre, :n, :nota)'), {'sku': r['sku'], 'muc': float(r['muc']), 'nombre': str(r['nombre_producto']), 'n': int(r['n_registros']), 'nota': nota_rev})
                                    conn.commit()
                                st.success(f'✅ {n_sel} grupo(s) revisados')
                                _limpiar_seleccion()
                                st.rerun()
                            except Exception as e:
                                st.error(f'Error: {e}')
                    pn1, pn2 = st.columns([3, 1])
                    with pn1:
                        st.text_input('📝 Nota para Revisado (opcional)', key='audit_nota_rev', placeholder='ej: cambio de proveedor, precio puntual...')
                    with pn2:
                        if st.button('🍱 Colación (SKU)', key='audit_col_multi'):
                            engine = get_engine()
                            try:
                                with engine.connect() as conn:
                                    for sku_c in skus_sel:
                                        nombre_c = str(sel_rows[sel_rows['sku']==sku_c]['nombre_producto'].iloc[0]) if not sel_rows[sel_rows['sku']==sku_c].empty else ''
                                        conn.execute(text('INSERT INTO sku_colacion (sku, nombre) VALUES (:sku, :nombre) ON CONFLICT (sku) DO NOTHING'), {'sku': sku_c, 'nombre': nombre_c})
                                    conn.commit()
                                st.success(f'🍱 {len(skus_sel)} SKU(s) colación')
                                _limpiar_seleccion()
                                st.rerun()
                            except Exception as e:
                                st.error(f'Error: {e}')
                    st.markdown('---')
                else:
                    st.caption('☝️ Selecciona grupos en la tabla para aplicar acciones.')

                # ── Tabla HTML pura — una fila por grupo MUC ─────────────
                # Construir HTML completo de una vez (sin elementos Streamlit por fila)
                hs_a = 'padding:8px 12px;font-size:0.67rem;text-transform:uppercase;letter-spacing:0.09em;font-weight:600;color:#444;border-bottom:1px solid #2a2a2a'
                rows_html = ''
                opciones_sel = []  # para el multiselect

                for idx, r in df_tabla.iterrows():
                    muc        = float(r.get('muc', 0) or 0)
                    muc_min    = float(r.get('muc_min', 0) or 0)
                    muc_max    = float(r.get('muc_max', 0) or 0)
                    dispersion = float(r.get('dispersion', 1) or 1)
                    n_reg      = int(r.get('n_registros', 1) or 1)
                    precio     = float(r.get('precio_factura', 0) or 0)
                    sku_r      = r.get('sku', '')
                    nombre_r   = str(r.get('nombre_producto', ''))
                    es_out     = muc_min > 0 and (abs(muc - muc_min) < 0.0001 or abs(muc - muc_max) < 0.0001)
                    if dispersion > 8:   dc = '#e84545'; dl = f'🔴 {dispersion:.0f}×'
                    elif dispersion > 2: dc = '#e89c45'; dl = f'🟡 {dispersion:.1f}×'
                    else:                dc = '#aaa';    dl = f'⚪ {dispersion:.1f}×'
                    mc = '#e84545' if es_out else '#aaa'
                    mw = '700' if es_out else '400'
                    rows_html += (
                        f'<tr style="border-bottom:1px solid #1e1e1e">'
                        f'<td style="padding:9px 12px;color:#666;font-family:monospace;font-size:0.72rem;width:8%">{sku_r}</td>'
                        f'<td style="padding:9px 12px;font-weight:500;color:#e8e4de;font-size:0.8rem;width:20%">{nombre_r}</td>'
                        f'<td style="padding:9px 12px;color:#666;font-size:0.75rem;width:9%">{r.get("categoria","")}</td>'
                        f'<td style="padding:9px 12px;color:#777;font-size:0.75rem;width:13%;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">{r.get("proveedor","")}</td>'
                        f'<td style="padding:9px 12px;text-align:right;color:#888;width:6%">{r.get("conversion","")}</td>'
                        f'<td style="padding:9px 12px;text-align:right;color:#888;width:7%">{r.get("formato","")}</td>'
                        f'<td style="padding:9px 12px;text-align:right;color:#aaa;width:10%">${precio:,.2f}</td>'
                        f'<td style="padding:9px 12px;text-align:right;color:{mc};font-weight:{mw};width:10%">{muc:,.4f}</td>'
                        f'<td style="padding:9px 12px;text-align:right;color:#666;width:6%">{n_reg}</td>'
                        f'<td style="padding:9px 12px;text-align:center;color:{dc};font-weight:600;width:8%">{dl}</td>'
                        f'</tr>'
                    )
                    opciones_sel.append(f'{sku_r} | MUC {muc:.4f} | {n_reg} reg. | {dl}')

                tabla_html = (
                    '<div style="overflow-x:auto;border-radius:14px;border:1px solid #1e1e1e;margin-top:0.5rem;background:#0d0d0d">'
                    '<table style="width:100%;border-collapse:collapse;font-family:DM Sans,sans-serif;font-size:0.82rem">'
                    '<thead><tr style="background:#111">'
                    + ''.join([f'<th style="{hs_a};text-align:{"left" if i<4 else "right" if i<9 else "center"}">{h}</th>'
                               for i, h in enumerate(['SKU','Producto','Categoría','Proveedor','Conv.','Formato','Neto Fact/u','MUC','# Reg.','Dispersión'])])
                    + f'</tr></thead><tbody>{rows_html}</tbody></table></div>'
                )
                st.markdown(tabla_html, unsafe_allow_html=True)

                # ── Selector de grupos a trabajar ──────────────────────────
                st.markdown('<div style="height:8px"></div>', unsafe_allow_html=True)
                sel_labels = st.multiselect(
                    '☝️ Selecciona grupos MUC para aplicar acciones',
                    opciones_sel,
                    key='audit_multisel',
                    placeholder='Busca por SKU, producto o MUC...'
                )

                # Recuperar filas seleccionadas por label
                sel_rows = pd.DataFrame()
                if sel_labels:
                    sel_indices = [opciones_sel.index(l) for l in sel_labels if l in opciones_sel]
                    sel_rows    = df_tabla.iloc[sel_indices].reset_index(drop=True)

                n_sel    = len(sel_rows)
                ids_sel  = []
                skus_sel = []
                if n_sel > 0:
                    for _, r in sel_rows.iterrows():
                        ids_sel  += _parse_ids(r['ids'])
                        skus_sel.append(r['sku'])
                    skus_sel = list(set(skus_sel))
    with tab6:
        st.markdown("<div class='info-box'>Carga el <b>Uso de Ingredientes</b> (Toteat) y el <b>Inventario</b> por local (hojas Alimentos y Bar). Puedes cargar un local individual o todos los locales.</div>", unsafe_allow_html=True)

        t6a, t6b, t6c = st.tabs(["📊 Uso de Ingredientes", "🏪 Inventario", "🔄 No Registrado / Venta Inter."])

        with t6a:
            st.markdown("#### Uso de Ingredientes (Toteat)")
            st.caption("Archivo con hoja **UsoIngredientes**: columnas Código Ingrediente, Ingrediente, Cantidad, Medida, Costo, Local")

            _MESES_ES_USO = {1:'Ene',2:'Feb',3:'Mar',4:'Abr',5:'May',6:'Jun',
                             7:'Jul',8:'Ago',9:'Sep',10:'Oct',11:'Nov',12:'Dic'}

            u1, u2, u3 = st.columns([2, 2, 3])
            with u1:
                uso_fecha_i = st.date_input("Inicio semana", key="uso_fecha_i", value=None)
            with u2:
                uso_fecha_f = st.date_input("Fin semana", key="uso_fecha_f", value=None)
            with u3:
                f_uso = st.file_uploader("Archivo Uso de Ingredientes (.xlsx)", type=["xlsx"], key="uso_ing")

            # Construir período igual que inventario e informe
            if uso_fecha_i and uso_fecha_f:
                if uso_fecha_i.month == uso_fecha_f.month:
                    periodo_uso = f"{uso_fecha_i.day}-{uso_fecha_f.day} {_MESES_ES_USO[uso_fecha_i.month]} {uso_fecha_i.year}"
                else:
                    periodo_uso = f"{uso_fecha_i.day} {_MESES_ES_USO[uso_fecha_i.month]}-{uso_fecha_f.day} {_MESES_ES_USO[uso_fecha_f.month]} {uso_fecha_i.year}"
                st.caption(f"Período: **{periodo_uso}**")
            else:
                periodo_uso = None

            if f_uso and periodo_uso:
                locales_uso_disponibles = []
                try:
                    df_uso_prev = pd.read_excel(f_uso, sheet_name='UsoIngredientes', header=0)
                    locales_uso_disponibles = sorted(df_uso_prev['Local'].dropna().unique().tolist()) if 'Local' in df_uso_prev.columns else []
                except: pass

                col_local_uso, _ = st.columns([2,3])
                with col_local_uso:
                    modo_uso = st.radio("Modo de carga", ["Todos los locales", "Local específico"], key="modo_uso", horizontal=True)
                    local_uso_sel = None
                    if modo_uso == "Local específico" and locales_uso_disponibles:
                        local_uso_sel = st.selectbox("Local", locales_uso_disponibles, key="local_uso_sel")

                if st.button("💾 Cargar Uso de Ingredientes", key="btn_uso"):
                    try:
                        df_uso = pd.read_excel(f_uso, sheet_name='UsoIngredientes', header=0)
                        df_uso.columns = df_uso.columns.str.strip()
                        df_uso = df_uso.rename(columns={
                            'Código Ingrediente': 'sku_ingrediente',
                            'Ingrediente': 'nombre_ingrediente',
                            'Cantidad': 'cantidad',
                            'Medida': 'medida',
                            'Costo': 'costo',
                            'Local': 'local'
                        })
                        df_uso['cantidad'] = pd.to_numeric(df_uso['cantidad'], errors='coerce').fillna(0)
                        df_uso['costo']    = pd.to_numeric(df_uso['costo'],    errors='coerce').fillna(0)
                        df_uso = df_uso.dropna(subset=['sku_ingrediente','local'])

                        if local_uso_sel:
                            df_uso = df_uso[df_uso['local'] == local_uso_sel]

                        df_uso['periodo'] = periodo_uso

                        engine = get_engine()
                        with engine.connect() as conn:
                            if local_uso_sel:
                                conn.execute(text(
                                    "DELETE FROM uso_ingredientes WHERE LOWER(TRIM(local))=:l AND TRIM(periodo)=:p"),
                                    {'l': local_uso_sel.lower().strip(), 'p': periodo_uso})
                            else:
                                conn.execute(text(
                                    "DELETE FROM uso_ingredientes WHERE TRIM(periodo)=:p"),
                                    {'p': periodo_uso})
                            conn.commit()

                        cols_bd = ['sku_ingrediente','nombre_ingrediente','cantidad','medida','costo','local','periodo']
                        df_uso[[c for c in cols_bd if c in df_uso.columns]].to_sql(
                            'uso_ingredientes', engine, if_exists='append', index=False)
                        st.success(f"✅ {len(df_uso):,} registros cargados — período {periodo_uso} — {df_uso['local'].nunique()} local(es)")
                    except Exception as e:
                        st.error(f"Error: {e}")
                        st.exception(e)

            # Vista de lo cargado
            df_uso_bd = run_query("SELECT periodo, local, COUNT(*) as registros FROM uso_ingredientes GROUP BY periodo, local ORDER BY periodo DESC, local")
            if not df_uso_bd.empty:
                st.markdown("**Uso de ingredientes en BD:**")
                st.dataframe(df_uso_bd, use_container_width=True, hide_index=True)

                # Migración de registros con formato de período antiguo
                st.markdown("---")
                st.markdown("**🔧 Migrar período a formato nuevo**")
                st.caption("Si hay registros con formato de período antiguo (ej: '2-8 Mar'), asígna el período correcto.")
                m1, m2, m3 = st.columns([2, 2, 2])
                with m1:
                    periodo_viejo = st.text_input("Período actual en BD", key="uso_per_viejo", placeholder="ej: 2-8 Mar")
                with m2:
                    mig_fi = st.date_input("Inicio semana correcta", key="uso_mig_fi", value=None)
                with m3:
                    mig_ff = st.date_input("Fin semana correcta",   key="uso_mig_ff", value=None)
                if periodo_viejo and mig_fi and mig_ff:
                    if mig_fi.month == mig_ff.month:
                        periodo_nuevo = f"{mig_fi.day}-{mig_ff.day} {_MESES_ES_USO[mig_fi.month]} {mig_fi.year}"
                    else:
                        periodo_nuevo = f"{mig_fi.day} {_MESES_ES_USO[mig_fi.month]}-{mig_ff.day} {_MESES_ES_USO[mig_ff.month]} {mig_fi.year}"
                    st.caption(f"Cambiará: **{periodo_viejo}** → **{periodo_nuevo}**")
                    if st.button("▶ Migrar período", key="btn_uso_migrar"):
                        engine = get_engine()
                        try:
                            with engine.connect() as conn:
                                res = conn.execute(text(
                                    "UPDATE uso_ingredientes SET periodo=:nuevo WHERE TRIM(periodo)=:viejo"),
                                    {'nuevo': periodo_nuevo, 'viejo': periodo_viejo.strip()})
                                conn.commit()
                            st.success(f"✅ {res.rowcount} registros actualizados: '{periodo_viejo}' → '{periodo_nuevo}'")
                            st.rerun()
                        except Exception as e:
                            st.error(f"Error: {e}")

        with t6b:
            st.markdown("#### Inventario por Local")

            ci1, ci2, ci3, ci4 = st.columns(4)
            with ci1:
                tipo_inv = st.selectbox("Tipo", ["Inicial","Final"], key="tipo_inv")
            with ci2:
                inv_fecha_i = st.date_input("Inicio semana", key="inv_fecha_i", value=None)
            with ci3:
                inv_fecha_f = st.date_input("Fin semana", key="inv_fecha_f", value=None)
            with ci4:
                formato_inv = st.radio("Formato", [
                    "Forma A (por local)",
                    "Forma B (Resumen KG)",
                    "Forma C (Consolidado todos los locales)"
                ], key="formato_inv",
                help="A: archivo por local con hojas Alimentos+Bar.\nB: tabla LOCAL/FACTOR/PRODUCTO/TOTAL 2 (ya viene en KG, sin conversión).\nC: archivo consolidado con hojas 'Alimentos consolidado' y 'BAR CONSOLIDADO' (todos los locales).")

            _MESES_ES = {1:'Ene',2:'Feb',3:'Mar',4:'Abr',5:'May',6:'Jun',
                         7:'Jul',8:'Ago',9:'Sep',10:'Oct',11:'Nov',12:'Dic'}
            if inv_fecha_i and inv_fecha_f:
                if inv_fecha_i.month == inv_fecha_f.month:
                    periodo_inv = f"{inv_fecha_i.day}-{inv_fecha_f.day} {_MESES_ES[inv_fecha_i.month]} {inv_fecha_i.year}"
                else:
                    periodo_inv = f"{inv_fecha_i.day} {_MESES_ES[inv_fecha_i.month]}-{inv_fecha_f.day} {_MESES_ES[inv_fecha_f.month]} {inv_fecha_i.year}"
                st.caption(f"Período: **{periodo_inv}**")
            else:
                periodo_inv = None

            fmt_sel = st.session_state.get("formato_inv", "Forma A")
            es_forma_a = "A" in fmt_sel
            es_forma_b = "B" in fmt_sel
            es_forma_c = "C" in fmt_sel

            if es_forma_a:
                local_inv = st.selectbox("Local", ["Chicureo","La Dehesa","La Reina","Las Condes",
                                                    "Los Trapenses","Macul","Nueva Providencia",
                                                    "Providencia","Quilin","Vitacura"], key="local_inv")
            else:
                modo_inv_multi = st.radio("Locales", ["Todos los locales", "Local específico"],
                                          key="modo_inv_multi", horizontal=True)
                local_inv = None
                if modo_inv_multi == "Local específico":
                    local_inv = st.selectbox("Local", ["Chicureo","La Dehesa","La Reina","Las Condes",
                                                        "Los Trapenses","Macul","Nueva Providencia",
                                                        "Providencia","Quilin","Vitacura"], key="local_inv")

            f_inv = st.file_uploader(
                "Archivo Inventario (.xlsx o .csv)",
                type=["xlsx","csv"], key="inv_file")

            if f_inv and periodo_inv and inv_fecha_i and inv_fecha_f:
                if st.button("💾 Cargar Inventario", key="btn_inv"):
                    fmt = st.session_state.get("formato_inv", "Forma A")
                    es_forma_c_btn = "C" in fmt
                    try:
                        engine = get_engine()

                        # ══ FORMA B: tabla consolidada LOCAL/FACTOR/PRODUCTO/TOTAL 2 ══
                        if "B" in fmt:
                            import io as _io3
                            raw_inv = f_inv.read()
                            if f_inv.name.endswith('.csv'):
                                sep  = ';' if b';' in raw_inv[:200] else ','
                                df_b = pd.read_csv(_io3.BytesIO(raw_inv), sep=sep, dtype=str)
                            else:
                                df_b = pd.read_excel(_io3.BytesIO(raw_inv), dtype=str)

                            df_b.columns = df_b.columns.str.strip()

                            # Normalizar decimales con coma
                            for col in ['FACTOR','TOTAL','TOTAL 2']:
                                if col in df_b.columns:
                                    df_b[col] = pd.to_numeric(
                                        df_b[col].astype(str).str.replace(',','.', regex=False),
                                        errors='coerce').fillna(0)

                            # Filtrar por local si se especificó uno
                            if local_inv and 'LOCAL' in df_b.columns:
                                df_b = df_b[df_b['LOCAL'].astype(str).str.strip().str.lower() == local_inv.lower()]

                            registros = []
                            for _, row in df_b.iterrows():
                                prod = str(row.get('PRODUCTO','')).strip()
                                if not prod or prod == 'nan': continue
                                local_fila = local_inv or str(row.get('LOCAL', '')).strip()
                                um       = str(row.get('UND','')).strip()
                                total_og = float(row.get('TOTAL', 0) or 0)
                                total_kg = float(row.get('TOTAL 2', total_og) or 0)
                                _, prod_ctrl_b = calcular_total_kg(prod, 1)
                                registros.append({
                                    'local': local_fila, 'periodo': periodo_inv,
                                    'tipo_inventario': tipo_inv,
                                    'producto': prod, 'producto_control': prod_ctrl_b,
                                    'um': um, 'crudo': 0, 'produccion': 0, 'cocido': 0,
                                    'total_original': total_og, 'total_kg': total_kg,
                                    'tipo': '', 'fuente': 'forma_b'
                                })

                            df_inv_save = pd.DataFrame(registros)
                            with engine.connect() as conn:
                                if local_inv:
                                    conn.execute(text(
                                        "DELETE FROM inventarios WHERE LOWER(TRIM(local))=:l AND TRIM(periodo)=:p AND tipo_inventario=:t"),
                                        {'l': local_inv.lower().strip(), 'p': periodo_inv, 't': tipo_inv})
                                else:
                                    locales_archivo = [l.lower().strip() for l in df_inv_save['local'].dropna().unique().tolist()]
                                    conn.execute(text(
                                        "DELETE FROM inventarios WHERE LOWER(TRIM(local))=ANY(:ls) AND TRIM(periodo)=:p AND tipo_inventario=:t"),
                                        {'ls': locales_archivo, 'p': periodo_inv, 't': tipo_inv})
                                conn.commit()
                            df_inv_save.to_sql('inventarios', engine, if_exists='append', index=False)
                            n_locales = df_inv_save['local'].nunique()
                            st.success(f"✅ {len(df_inv_save)} productos cargados (Forma B) — {n_locales} local(es) · {tipo_inv} · {periodo_inv}")

                        elif es_forma_c_btn:
                        # ══ FORMA C: consolidado multi-local ══
                            import io as _io4
                            raw_c = f_inv.read()
                            xls_c = pd.ExcelFile(_io4.BytesIO(raw_c))

                            # Nombres de hoja tolerantes a variaciones
                            def _find_sheet(names, candidates):
                                nl = [n.strip().lower() for n in names]
                                for c in candidates:
                                    if c.lower() in nl:
                                        return names[[n.strip().lower() for n in names].index(c.lower())]
                                return None

                            sheet_ali = _find_sheet(xls_c.sheet_names,
                                ['alimentos consolidado', 'alimentos_consolidado', 'alimentos'])
                            sheet_bar = _find_sheet(xls_c.sheet_names,
                                ['bar consolidado', 'bar_consolidado', 'bar'])

                            if not sheet_ali:
                                st.error(f"No se encontró hoja de Alimentos. Hojas disponibles: {xls_c.sheet_names}")
                                st.stop()

                            registros = []

                            # ── Alimentos consolidado ──────────────────────
                            df_ac = pd.read_excel(xls_c, sheet_ali, header=0)
                            if local_inv:
                                df_ac = df_ac[df_ac['Local'].astype(str).str.strip().str.lower() == local_inv.lower()]

                            for _, row in df_ac.iterrows():
                                loc  = str(row.get('Local','')).strip()
                                prod = str(row.get('PRODUCTO','')).strip()
                                if not prod or prod == 'nan': continue
                                um     = str(row.get('Unidad de Medida','')).strip()
                                crudo  = pd.to_numeric(row.get('Crudo',    0), errors='coerce') or 0
                                prod_  = pd.to_numeric(row.get('Producción',0), errors='coerce') or 0
                                cocido = pd.to_numeric(row.get('Cocido',   0), errors='coerce') or 0
                                total  = pd.to_numeric(row.get('Total',    0), errors='coerce') or 0
                                tipo   = str(row.get('TIPO','') or '').strip()
                                # control del archivo tiene prioridad para el mapeo
                                prod_ctrl_hint = str(row.get('control', '') or '').strip()
                                if prod_ctrl_hint == 'nan': prod_ctrl_hint = ''
                                total_kg, prod_ctrl = calcular_total_kg(
                                    prod, total, crudo, prod_, cocido,
                                    producto_control=prod_ctrl_hint or None
                                )
                                registros.append({
                                    'local': loc, 'periodo': periodo_inv,
                                    'tipo_inventario': tipo_inv,
                                    'producto': prod, 'producto_control': prod_ctrl,
                                    'um': um, 'crudo': crudo, 'produccion': prod_,
                                    'cocido': cocido, 'total_original': total,
                                    'total_kg': total_kg, 'tipo': tipo, 'fuente': 'alimentos_c'
                                })

                            # ── BAR CONSOLIDADO ────────────────────────────
                            if sheet_bar:
                                df_bc = pd.read_excel(xls_c, sheet_bar, header=0)
                                if local_inv:
                                    df_bc = df_bc[df_bc['Local'].astype(str).str.strip().str.lower() == local_inv.lower()]

                                for _, row in df_bc.iterrows():
                                    loc  = str(row.get('Local','')).strip()
                                    prod = str(row.get('PRODUCTO','')).strip()
                                    if not prod or prod == 'nan': continue
                                    um    = str(row.get('Unidad de Medida','')).strip()
                                    total = pd.to_numeric(row.get('Total',0), errors='coerce') or 0
                                    tipo  = str(row.get('TIPO','') or '').strip().upper()
                                    ctrl  = TIPO_BAR_CONTROL.get(tipo, tipo)
                                    registros.append({
                                        'local': loc, 'periodo': periodo_inv,
                                        'tipo_inventario': tipo_inv,
                                        'producto': prod, 'producto_control': ctrl,
                                        'um': um, 'crudo': 0, 'produccion': 0, 'cocido': 0,
                                        'total_original': total, 'total_kg': total,
                                        'tipo': tipo, 'fuente': 'bar_c'
                                    })

                            df_inv_save = pd.DataFrame(registros)
                            locales_c = [l.lower().strip() for l in df_inv_save['local'].dropna().unique().tolist()]
                            with engine.connect() as conn:
                                conn.execute(text(
                                    "DELETE FROM inventarios WHERE LOWER(TRIM(local))=ANY(:ls) AND TRIM(periodo)=:p AND tipo_inventario=:t"),
                                    {'ls': locales_c, 'p': periodo_inv, 't': tipo_inv})
                                conn.commit()
                            df_inv_save.to_sql('inventarios', engine, if_exists='append', index=False)
                            n_loc = df_inv_save['local'].nunique()
                            st.success(f"✅ {len(df_inv_save)} registros cargados (Forma C) — {n_loc} locales · {tipo_inv} · {periodo_inv}")

                        else:
                        # ══ FORMA A: hojas Alimentos + Bar ══
                            conversores = {
                                'CHULETA KASSLER':    {'control':'CHULETA KASSLER',    'porcion':1.0,  'cocido':1.0},
                                'COSTILLAS':          {'control':'COSTILLAS',          'porcion':1.0,  'cocido':0.75},
                                'JAMÓN':              {'control':'JAMÓN',              'porcion':1.0,  'cocido':1.0},
                                'LOMO DE CENTRO':     {'control':'LOMO DE CENTRO',     'porcion':1.0,  'cocido':1.0},
                                'LOMO DE CENTRO(PORCIONADAS)': {'control':'LOMO DE CENTRO','porcion':0.18,'cocido':1.0},
                                'PANCETA LAMINADA':   {'control':'PANCETA LAMINADA',   'porcion':1.0,  'cocido':0.5},
                                'DESPUNTE PECHUGA DE POLLO': {'control':'PECHUGA DE POLLO','porcion':1.0,'cocido':1.0},
                                'PECHUGA DE POLLO':   {'control':'PECHUGA DE POLLO',   'porcion':1.0,  'cocido':0.8},
                                'PERNIL':             {'control':'PERNIL',             'porcion':1.0,  'cocido':1.0},
                                'PERNIL(PORCIONADAS)':{'control':'PERNIL',             'porcion':0.18, 'cocido':1.0},
                                'TOCINO AHUMADO':     {'control':'TOCINO AHUMADO',     'porcion':1.0,  'cocido':1.0},
                                'FILETE':             {'control':'FILETE',             'porcion':1.0,  'cocido':1.0},
                                'PLATEADA':           {'control':'PLATEADA',           'porcion':1.0,  'cocido':0.5},
                                'LOMO LISO':          {'control':'LOMO LISO',          'porcion':1.0,  'cocido':1.0},
                                'LOMO VETADO':        {'control':'LOMO VETADO',        'porcion':1.0,  'cocido':1.0},
                                'POSTA':              {'control':'POSTA',              'porcion':1.0,  'cocido':1.0},
                                'PALTA':              {'control':'PALTA',              'porcion':1.0,  'cocido':1.0},
                                'TOMATE':             {'control':'TOMATE',             'porcion':1.0,  'cocido':1.0},
                                'LECHUGA':            {'control':'LECHUGA',            'porcion':1.0,  'cocido':1.0},
                                'QUESO RANCO':        {'control':'QUESO RANCO',        'porcion':1.0,  'cocido':1.0},
                                'QUESO CHEDDAR':      {'control':'QUESO CHEDDAR',      'porcion':1.0,  'cocido':1.0},
                                'QUESO PARMESANO':    {'control':'QUESO PARMESANO',    'porcion':1.0,  'cocido':1.0},
                                'PAPAS FRITAS':       {'control':'PAPAS FRITAS',       'porcion':1.0,  'cocido':1.0},
                                'FILETE SALMON':      {'control':'FILETE SALMON',      'porcion':1.0,  'cocido':1.0},
                                'ATUN':               {'control':'ATUN',               'porcion':1.0,  'cocido':1.0},
                                'CAMARON':            {'control':'CAMARON',            'porcion':1.0,  'cocido':1.0},
                                'CAMARON APANADO':    {'control':'CAMARON APANADO',    'porcion':1.0,  'cocido':1.0},
                                'SALMON SLICE LAMINADO':{'control':'SALMON SLICE LAMINADO','porcion':1.0,'cocido':1.0},
                                'LOCOS':              {'control':'LOCOS',              'porcion':1.0,  'cocido':1.0},
                                'ERIZOS':             {'control':'ERIZOS',             'porcion':1.0,  'cocido':1.0},
                                'GRASA DE WAGYU':     {'control':'GRASA DE WAGYU',     'porcion':1.0,  'cocido':1.0},
                            }

                            registros = []

                            # Alimentos
                            df_ali = pd.read_excel(f_inv, sheet_name='Alimentos', header=None)
                            df_ali.columns = df_ali.iloc[1]
                            df_ali = df_ali.iloc[2:].reset_index(drop=True)
                            df_ali = df_ali[df_ali['PRODUCTO'].notna()].copy()

                            for _, row in df_ali.iterrows():
                                prod   = str(row.get('PRODUCTO','')).strip()
                                if not prod or prod == 'nan': continue
                                um     = str(row.get('Unidad de Medida','')).strip()
                                crudo  = pd.to_numeric(row.get('Crudo',0),     errors='coerce') or 0
                                prod_  = pd.to_numeric(row.get('Producción',0),errors='coerce') or 0
                                cocido = pd.to_numeric(row.get('Cocido',0),    errors='coerce') or 0
                                total  = pd.to_numeric(row.get('Total',0),     errors='coerce') or 0
                                tipo   = str(row.get('TIPO','')).strip()
                                total_kg, prod_ctrl_a = calcular_total_kg(prod, total, crudo, prod_, cocido, prod)
                                registros.append({
                                    'local': local_inv, 'periodo': periodo_inv,
                                    'tipo_inventario': tipo_inv,
                                    'producto': prod, 'producto_control': prod_ctrl_a,
                                    'um': um, 'crudo': crudo, 'produccion': prod_,
                                    'cocido': cocido, 'total_original': total,
                                    'total_kg': total_kg, 'tipo': tipo, 'fuente': 'alimentos'
                                })

                            # Bar
                            df_bar = pd.read_excel(f_inv, sheet_name='Bar', header=None)
                            df_bar.columns = df_bar.iloc[1]
                            df_bar = df_bar.iloc[2:].reset_index(drop=True)
                            df_bar = df_bar[df_bar['PRODUCTO'].notna()].copy()

                            for _, row in df_bar.iterrows():
                                prod  = str(row.get('PRODUCTO','')).strip()
                                if not prod or prod == 'nan': continue
                                um    = str(row.get('Unidad de Medida','')).strip()
                                total = pd.to_numeric(row.get('Total',0), errors='coerce') or 0
                                tipo  = str(row.get('TIPO','')).strip().upper()
                                ctrl  = TIPO_BAR_CONTROL.get(tipo, tipo)
                                registros.append({
                                    'local': local_inv, 'periodo': periodo_inv,
                                    'tipo_inventario': tipo_inv,
                                    'producto': prod, 'producto_control': ctrl,
                                    'um': um, 'crudo': 0, 'produccion': 0, 'cocido': 0,
                                    'total_original': total, 'total_kg': total,
                                    'tipo': tipo, 'fuente': 'bar'
                                })

                            df_inv_save = pd.DataFrame(registros)
                            with engine.connect() as conn:
                                conn.execute(text(
                                    "DELETE FROM inventarios WHERE LOWER(TRIM(local))=:l AND TRIM(periodo)=:p AND tipo_inventario=:t"),
                                    {'l': local_inv.lower().strip(), 'p': periodo_inv, 't': tipo_inv})
                                conn.commit()
                            df_inv_save.to_sql('inventarios', engine, if_exists='append', index=False)
                            st.success(f"✅ {len(df_inv_save)} registros cargados (Forma A) — {local_inv} · {tipo_inv} · {periodo_inv}")

                    except Exception as e:
                        st.error(f"Error: {e}")
                        st.exception(e)

            # Vista de inventarios cargados
            df_inv_bd = run_query("SELECT periodo, tipo_inventario, local, COUNT(*) as items FROM inventarios GROUP BY periodo, tipo_inventario, local ORDER BY periodo DESC, tipo_inventario, local")
            if not df_inv_bd.empty:
                st.markdown("**Inventarios en BD:**")
                st.dataframe(df_inv_bd, use_container_width=True, hide_index=True)

            st.markdown("---")
            st.markdown("**🔧 Re-mapear producto_control en BD**")
            st.caption("Corrige producto_control de registros existentes. No modifica total_kg.")
            if st.button("▶ Re-mapear inventarios existentes", key="btn_remap_inv"):
                engine = get_engine()
                if engine:
                    try:
                        df_all = run_query("SELECT id, producto FROM inventarios WHERE producto IS NOT NULL")
                        if df_all.empty:
                            st.info("No hay registros en inventarios.")
                        else:
                            updates = []
                            for _, row in df_all.iterrows():
                                prod = str(row['producto']).strip()
                                _, ctrl = calcular_total_kg(prod, 1)
                                updates.append({'id': int(row['id']), 'ctrl': ctrl})
                            with engine.connect() as conn:
                                for u in updates:
                                    conn.execute(
                                        text("UPDATE inventarios SET producto_control=:c WHERE id=:i"),
                                        {'c': u['ctrl'], 'i': u['id']}
                                    )
                                conn.commit()
                            st.success(f"✅ {len(updates)} registros actualizados (solo producto_control).")
                    except Exception as e:
                        st.error(f"Error en re-mapeo: {e}")

        with t6c:
            st.markdown("#### Compras No Registradas / Venta Inter-local")
            st.markdown("<div class='info-box'>Compras que no pasaron por el facturador ni la BD. Solo se consideran las cantidades con <b>Categoria Control</b> válida para el cálculo del informe de costos. <b>No se guardan en la tabla de compras.</b></div>", unsafe_allow_html=True)

            nr1, nr2 = st.columns([2,3])
            with nr1:
                periodo_nr = st.text_input("Período", key="nr_periodo", placeholder="ej: 2-8 Mar 2026")
            with nr2:
                f_nr = st.file_uploader("Archivo No Registrado (.xlsx)", type=["xlsx"], key="nr_file")

            if f_nr and periodo_nr:
                if st.button("💾 Cargar No Registrado", key="btn_nr"):
                    try:
                        df_nr = pd.read_excel(f_nr, header=0)
                        df_nr.columns = df_nr.columns.str.strip()
                        df_nr['cantidad'] = pd.to_numeric(df_nr['cantidad'], errors='coerce').fillna(0)
                        df_nr['Categoria Control'] = df_nr['Categoria Control'].fillna('').astype(str).str.strip()
                        df_nr['Local'] = df_nr['Local'].fillna('').astype(str).str.strip()
                        df_nr['fecha_dte'] = pd.to_datetime(df_nr['fecha_dte'], errors='coerce').dt.date

                        # Solo filas con Categoria Control válida
                        df_nr = df_nr[~df_nr['Categoria Control'].isin(['','0','nan','NaN'])]
                        df_nr['periodo'] = periodo_nr

                        # Normalizar local a title case
                        df_nr['Local'] = df_nr['Local'].str.title()

                        engine = get_engine()
                        with engine.connect() as conn:
                            conn.execute(text("DELETE FROM compras_no_registradas WHERE periodo=:p"),
                                         {'p': periodo_nr})
                            conn.commit()

                        cols_bd = ['fecha_dte','Local','producto','desc_producto','cantidad',
                                   'Categoria Control','Categoria Producto','periodo']
                        rename_map = {'Local':'local','producto':'nombre_producto',
                                      'desc_producto':'desc_producto',
                                      'Categoria Control':'producto_control',
                                      'Categoria Producto':'categoria_producto'}
                        df_save = df_nr[[c for c in cols_bd if c in df_nr.columns]].rename(columns=rename_map)
                        df_save.to_sql('compras_no_registradas', engine, if_exists='append', index=False)
                        st.success(f"✅ {len(df_save)} registros cargados — {df_save['local'].nunique()} locales · {periodo_nr}")
                    except Exception as e:
                        st.error(f"Error: {e}")
                        st.exception(e)

            # Vista de lo cargado
            df_nr_bd = run_query("SELECT periodo, local, COUNT(*) as registros, SUM(cantidad) as cant_total FROM compras_no_registradas GROUP BY periodo, local ORDER BY periodo DESC, local")
            if not df_nr_bd.empty:
                st.markdown("**No registrados en BD:**")
                st.dataframe(df_nr_bd, use_container_width=True, hide_index=True)

    with tab7:
        st.markdown("#### 🗂️ Clasificación de Productos")
        st.markdown("<div class='info-box'>Carga el archivo <b>Prod_Control.xlsx</b> con las hojas <b>Nomb Prod</b>, <b>Proveedor</b> y <b>Maestro SKU</b>. El sistema clasificará automáticamente todos los productos de compras usando 3 planes en cascada.</div>", unsafe_allow_html=True)

        f_clas = st.file_uploader("Archivo de Clasificación (.xlsx)", type=["xlsx"], key="clas_file")

        if f_clas:
            if st.button("💾 Cargar y Clasificar", key="btn_clas"):
                with st.spinner("Cargando tablas y clasificando compras..."):
                    try:
                        import io as _io5
                        raw_c = f_clas.read()
                        xls_c = pd.ExcelFile(_io5.BytesIO(raw_c))
                        engine = get_engine()

                        # ── Leer las 3 hojas ──────────────────────────────
                        df_np   = pd.read_excel(xls_c, 'Nomb Prod',   header=0)
                        df_prov = pd.read_excel(xls_c, 'Proveedor',   header=0)
                        df_msku = pd.read_excel(xls_c, 'Maestro SKU', header=0)

                        df_np.columns   = ['nombre_producto','categoria_control','categoria']
                        df_prov.columns = ['nombre_proveedor','categoria']
                        df_msku.columns = ['sku','descripcion']

                        # Normalizar
                        df_np['nombre_norm'] = df_np['nombre_producto'].fillna('').astype(str).str.strip().str.upper()
                        df_np['categoria_control'] = df_np['categoria_control'].fillna('').astype(str).str.strip()
                        df_np['categoria'] = df_np['categoria'].fillna('').astype(str).str.strip()
                        df_prov['prov_norm'] = df_prov['nombre_proveedor'].fillna('').astype(str).str.strip().str.upper()
                        df_prov['categoria'] = df_prov['categoria'].fillna('').astype(str).str.strip()
                        df_msku['sku_norm']  = df_msku['sku'].fillna('').astype(str).str.strip().str.upper()
                        df_msku['desc_norm'] = df_msku['descripcion'].fillna('').astype(str).str.strip().str.upper()

                        # ── Guardar tablas en BD ──────────────────────────
                        df_np_save = df_np[['nombre_producto','categoria_control','categoria']].copy()
                        df_np_save.columns = ['nombre_producto','categoria_control','categoria']
                        df_np_save = df_np_save[df_np_save['nombre_producto'].astype(str).str.strip() != '']

                        df_prov_save = df_prov[['nombre_proveedor','categoria']].copy()
                        df_prov_save = df_prov_save[df_prov_save['nombre_proveedor'].astype(str).str.strip() != '']

                        df_msku_save = df_msku[['sku','descripcion']].copy()
                        df_msku_save = df_msku_save[df_msku_save['sku'].astype(str).str.strip() != '']

                        with engine.connect() as conn:
                            conn.execute(text("DROP TABLE IF EXISTS clas_nomb_prod"))
                            conn.execute(text("DROP TABLE IF EXISTS clas_proveedor"))
                            conn.execute(text("DROP TABLE IF EXISTS clas_maestro_sku"))
                            conn.commit()

                        df_np_save.to_sql('clas_nomb_prod',   engine, if_exists='replace', index=False)
                        df_prov_save.to_sql('clas_proveedor', engine, if_exists='replace', index=False)
                        df_msku_save.to_sql('clas_maestro_sku', engine, if_exists='replace', index=False)

                        st.info(f"📚 Tablas guardadas: {len(df_np_save)} nombres · {len(df_prov_save)} proveedores · {len(df_msku_save)} SKUs")

                        # ── Clasificar compras en cascada ─────────────────
                        # Diccionarios de lookup
                        map_nombre = dict(zip(df_np['nombre_norm'],
                                              zip(df_np['categoria_control'], df_np['categoria'])))
                        map_prov   = dict(zip(df_prov['prov_norm'], df_prov['categoria']))
                        map_msku   = dict(zip(df_msku['sku_norm'], df_msku['desc_norm']))

                        # Cargar compras sin clasificar o con clasificación vacía
                        df_comp = run_query("""
                            SELECT id, nombre_producto, nombre_proveedor, sku,
                                   categoria_producto, subcat
                            FROM compras
                            WHERE nombre_producto IS NOT NULL
                            LIMIT 100000
                        """)

                        if df_comp.empty:
                            st.warning("No hay compras en BD para clasificar.")
                        else:
                            df_comp['nombre_norm'] = df_comp['nombre_producto'].fillna('').astype(str).str.strip().str.upper()
                            df_comp['prov_norm']   = df_comp['nombre_proveedor'].fillna('').astype(str).str.strip().str.upper()
                            df_comp['sku_norm']    = df_comp['sku'].fillna('').astype(str).str.strip().str.upper()

                            resultados = []
                            plan_a = plan_b = plan_c = sin_match = 0

                            for _, row in df_comp.iterrows():
                                cat_ctrl = ''
                                cat      = ''
                                plan     = ''

                                # Plan A: nombre producto
                                if row['nombre_norm'] in map_nombre:
                                    cat_ctrl, cat = map_nombre[row['nombre_norm']]
                                    plan = 'A'
                                    plan_a += 1

                                # Plan B: proveedor
                                elif row['prov_norm'] in map_prov:
                                    cat = map_prov[row['prov_norm']]
                                    plan = 'B'
                                    plan_b += 1

                                # Plan C: SKU → descripcion en maestro → buscar en Nomb Prod
                                elif row['sku_norm'] in map_msku:
                                    desc = map_msku[row['sku_norm']]
                                    if desc in map_nombre:
                                        cat_ctrl, cat = map_nombre[desc]
                                        plan = 'C'
                                        plan_c += 1
                                    else:
                                        plan = 'C-parcial'
                                        sin_match += 1
                                else:
                                    plan = 'sin_match'
                                    sin_match += 1

                                resultados.append({
                                    'id': row['id'],
                                    'categoria_control': cat_ctrl if cat_ctrl and cat_ctrl != 'nan' else None,
                                    'categoria_clasificada': cat if cat and cat != 'nan' else None,
                                    'plan_clasificacion': plan,
                                })

                            df_res = pd.DataFrame(resultados)

                            # Guardar resultados en tabla de clasificación
                            with engine.connect() as conn:
                                conn.execute(text("DROP TABLE IF EXISTS compras_clasificacion"))
                                conn.commit()
                            df_res.to_sql('compras_clasificacion', engine, if_exists='replace', index=False)

                            # Mostrar resumen
                            total = len(df_res)
                            clasificados = total - sin_match
                            st.success(f"✅ {clasificados:,}/{total:,} registros clasificados ({clasificados/total*100:.1f}%)")

                            cc1, cc2, cc3, cc4 = st.columns(4)
                            cc1.metric("Plan A (Nombre)", f"{plan_a:,}", f"{plan_a/total*100:.1f}%")
                            cc2.metric("Plan B (Proveedor)", f"{plan_b:,}", f"{plan_b/total*100:.1f}%")
                            cc3.metric("Plan C (SKU)", f"{plan_c:,}", f"{plan_c/total*100:.1f}%")
                            cc4.metric("Sin match", f"{sin_match:,}", f"{sin_match/total*100:.1f}%")

                            # Vista de sin match para revisión
                            if sin_match > 0:
                                df_sin = df_comp[df_res['plan_clasificacion'].isin(['sin_match','C-parcial'])][
                                    ['nombre_producto','nombre_proveedor','sku']
                                ].drop_duplicates('nombre_producto').head(50)
                                with st.expander(f"⚠️ {sin_match:,} sin clasificar — revisar"):
                                    st.dataframe(df_sin, use_container_width=True, hide_index=True)

                    except Exception as e:
                        st.error(f"Error: {e}")
                        st.exception(e)

        # Vista resumen de clasificación existente
        df_clas_res = run_query("""
            SELECT plan_clasificacion, COUNT(*) as registros
            FROM compras_clasificacion
            GROUP BY plan_clasificacion ORDER BY registros DESC
        """) if run_query("SELECT to_regclass('compras_clasificacion') as t")['t'].iloc[0] else pd.DataFrame()
        if not df_clas_res.empty:
            st.markdown("**Clasificación actual en BD:**")
            st.dataframe(df_clas_res, use_container_width=True, hide_index=True)

# ============================================================
# MÓDULO: EXPLOSIÓN MRP
# ============================================================
elif modulo.startswith("🧮"):
    st.markdown(f"""
    <div style="margin-bottom:1.5rem">
        <div style="font-size:0.72rem;text-transform:uppercase;letter-spacing:0.12em;color:#555;margin-bottom:4px">Módulo</div>
        <div style="font-family:'DM Serif Display',serif;font-size:2rem;color:#f0ede8;letter-spacing:-0.02em;line-height:1.1">
            🧮 Explosión MRP
        </div>
        <div style="width:40px;height:2px;background:#d4a853;margin-top:8px;border-radius:2px"></div>
    </div>
    """, unsafe_allow_html=True)
    st.markdown("<div class='info-box'>Sube el Excel con las hojas <b>Ventas</b>, <b>Directos</b> y <b>Procesados</b>. La lógica de cálculo es la versión validada.</div>", unsafe_allow_html=True)

    file_mrp = st.file_uploader("Archivo Excel MRP (.xlsx)", type="xlsx")

    if file_mrp:
        try:
            xls = pd.ExcelFile(file_mrp)
            res = process_bom(
                pd.read_excel(xls, 'Ventas'),
                pd.read_excel(xls, 'Directos'),
                pd.read_excel(xls, 'Procesados')
            )

            col_a, col_b, col_c = st.columns(3)
            col_a.metric("Insumos únicos", len(res))
            col_b.metric("Registros explotados", len(res))

            st.markdown("#### 📋 Resultado de la explosión")
            st.dataframe(
                res.style.format({"Total Kg/L/Un": "{:,.3f}"}),
                use_container_width=True,
                hide_index=True
            )

            buf = io.BytesIO()
            with pd.ExcelWriter(buf, engine='openpyxl') as w:
                res.to_excel(w, index=False)
            st.download_button(
                "📥 Descargar MRP (.xlsx)",
                buf.getvalue(),
                "MRP_Explosion.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
        except Exception as e:
            st.error(f"Error al procesar: {e}")


# ============================================================
# MÓDULO: INFORMES
# ============================================================
elif modulo.startswith("📊"):

    # Derivar informe activo desde subitem del menú
    if "Rentabilidad" in modulo:
        informe_sel = "Informe 1"
    elif "Desviación" in modulo:
        informe_sel = "Informe 2"
    elif "Variación Precio Compras" in modulo:
        informe_sel = "Informe 3"
    elif "Informe de Costos" in modulo:
        informe_sel = "Informe 4"
    else:
        informe_sel = "Informe 1"  # default

    # Título elegante según informe
    titulos = {
        "Informe 1": ("💰", "Rentabilidad por Producto"),
        "Informe 2": ("📉", "Desviación Real vs Teórico"),
        "Informe 3": ("🔀", "Variación Precio Compras"),
        "Informe 4": ("📋", "Informe de Costos"),
    }
    icono, titulo_txt = titulos.get(informe_sel, ("📊", "Informes"))
    st.markdown(f"""
    <div style="margin-bottom:1.5rem">
        <div style="font-size:0.72rem;text-transform:uppercase;letter-spacing:0.12em;color:#555;margin-bottom:4px">Informes</div>
        <div style="font-family:'DM Serif Display',serif;font-size:2rem;color:#f0ede8;letter-spacing:-0.02em;line-height:1.1">
            {icono} {titulo_txt}
        </div>
        <div style="width:40px;height:2px;background:#d4a853;margin-top:8px;border-radius:2px"></div>
    </div>
    """, unsafe_allow_html=True)

    # ----------------------------------------------------------
    # INFORME 1
    # ----------------------------------------------------------
    if "Informe 1" in informe_sel:
        st.markdown("### 💰 Rentabilidad por Producto / Categoría")
        st.markdown(f"<div class='info-box'>Período: <b>{f_inicio}</b> → <b>{f_fin}</b> · Local: <b>{f_local}</b><br><b>Rent. Teórica</b>: último precio de compra por SKU · <b>Rent. Período</b>: MUC ponderado del período (fallback: última compra).</div>", unsafe_allow_html=True)

        if st.button("▶ Generar Informe 1"):
            with st.spinner("Calculando rentabilidad..."):
                df_inf1 = informe_rentabilidad(f_inicio, f_fin, f_local)

            if not df_inf1.empty:
                venta_total = df_inf1['venta'].sum()
                costo_total = df_inf1['costo_total'].sum()
                rent_total  = df_inf1['rentabilidad'].sum()
                margen_gral = (rent_total / venta_total * 100) if venta_total > 0 else 0

                venta_total      = df_inf1['venta'].sum()
                costo_teo_total  = df_inf1['costo_total_teorico'].sum()
                costo_per_total  = df_inf1['costo_total_periodo'].sum()
                rent_teo_total   = df_inf1['rentabilidad_teorica'].sum()
                rent_per_total   = df_inf1['rentabilidad_periodo'].sum()
                margen_teo       = (rent_teo_total / venta_total * 100) if venta_total > 0 else 0
                margen_per       = (rent_per_total / venta_total * 100) if venta_total > 0 else 0

                m1, m2, m3, m4, m5, m6 = st.columns(6)
                m1.metric("💰 Venta Total",          f"${venta_total:,.0f}")
                m2.metric("📦 Costo Teórico",         f"${costo_teo_total:,.0f}")
                m3.metric("📦 Costo Período",         f"${costo_per_total:,.0f}")
                m4.metric("📈 Rent. Teórica",         f"${rent_teo_total:,.0f}")
                m5.metric("📈 Rent. Período",         f"${rent_per_total:,.0f}")
                m6.metric("🎯 Margen Período",        f"{margen_per:.1f}%", delta=f"{margen_per-margen_teo:+.1f}% vs teórico")

                st.markdown("<br>", unsafe_allow_html=True)

                # --- Helpers badge ---
                def badge_margen(val):
                    if pd.isna(val): return '<span style="color:#555">—</span>'
                    if val >= 60:
                        return f'<span style="background:#1a3a2a;color:#4caf7d;padding:2px 8px;border-radius:12px;font-size:0.78rem;font-weight:600">{val:.1f}%</span>'
                    elif val >= 40:
                        return f'<span style="background:#3a2a1a;color:#e89c45;padding:2px 8px;border-radius:12px;font-size:0.78rem;font-weight:600">{val:.1f}%</span>'
                    return f'<span style="background:#3a1a1a;color:#e84545;padding:2px 8px;border-radius:12px;font-size:0.78rem;font-weight:600">{val:.1f}%</span>'

                def fmt_rent(val):
                    if val >= 0:
                        return f'<span style="color:#4caf7d;font-weight:600">${val:,.0f}</span>'
                    return f'<span style="color:#e84545;font-weight:600">${val:,.0f}</span>'

                # --- Tabla detalle por producto ---
                rows_html = ''
                cols_show = ['sku_producto', 'categoria_menu', 'nombre_producto', 'cant',
                             'venta',
                             'costo_total_teorico', 'rentabilidad_teorica', 'margen_teorico',
                             'costo_total_periodo', 'rentabilidad_periodo', 'margen_periodo']
                for _, r in df_inf1.iterrows():
                    mg_teo = r.get('margen_teorico', 0)
                    mg_per = r.get('margen_periodo', 0)
                    bg = '#121e14' if mg_per >= 60 else '#1e1a12' if mg_per >= 40 else '#1e1212'
                    rows_html += (
                        f'<tr style="border-bottom:1px solid #1e1e1e;background:{bg}">'
                        f'<td style="padding:9px 12px;color:#666;font-size:0.74rem;font-family:monospace">{r.get("sku_producto","")}</td>'
                        f'<td style="padding:9px 12px;color:#555;font-size:0.78rem">{r.get("categoria_menu","")}</td>'
                        f'<td style="padding:9px 12px;font-weight:500;color:#e8e4de">{r.get("nombre_producto","")}</td>'
                        f'<td style="padding:9px 12px;text-align:right;color:#aaa">{r.get("cant",0):,.0f}</td>'
                        f'<td style="padding:9px 12px;text-align:right;color:#ccc">${r.get("venta",0):,.0f}</td>'
                        f'<td style="padding:9px 12px;text-align:right;color:#666">${r.get("costo_total_teorico",0):,.0f}</td>'
                        f'<td style="padding:9px 12px;text-align:right">{fmt_rent(r.get("rentabilidad_teorica",0))}</td>'
                        f'<td style="padding:9px 12px;text-align:center">{badge_margen(mg_teo)}</td>'
                        f'<td style="padding:9px 12px;text-align:right;color:#777">${r.get("costo_total_periodo",0):,.0f}</td>'
                        f'<td style="padding:9px 12px;text-align:right">{fmt_rent(r.get("rentabilidad_periodo",0))}</td>'
                        f'<td style="padding:9px 12px;text-align:center">{badge_margen(mg_per)}</td>'
                        f'</tr>'
                    )

                hs  = 'padding:10px 12px;font-size:0.68rem;text-transform:uppercase;letter-spacing:0.09em;font-weight:600;color:#444;border-bottom:1px solid #2a2a2a'
                hs2 = hs + ';background:#0d1a0d'  # header teórico
                hs3 = hs + ';background:#0a0a1a'  # header período
                tabla_html = (
                    '<div style="overflow-x:auto;border-radius:14px;border:1px solid #1e1e1e;margin-top:0.5rem;background:#0d0d0d">'
                    '<table style="width:100%;border-collapse:collapse;font-family:DM Sans,sans-serif;font-size:0.82rem">'
                    '<thead>'
                    '<tr style="background:#111">'
                    f'<th colspan="5" style="{hs};text-align:left;border-right:1px solid #2a2a2a"></th>'
                    f'<th colspan="3" style="{hs2};text-align:center;border-right:1px solid #2a2a2a;color:#4caf7d">RENTABILIDAD TEÓRICA</th>'
                    f'<th colspan="3" style="{hs3};text-align:center;color:#d4a853">RENTABILIDAD PERÍODO</th>'
                    f'</tr>'
                    '<tr style="background:#111">'
                    f'<th style="{hs};text-align:left">SKU</th>'
                    f'<th style="{hs};text-align:left">Categoría</th>'
                    f'<th style="{hs};text-align:left">Producto</th>'
                    f'<th style="{hs};text-align:right">Cant.</th>'
                    f'<th style="{hs};text-align:right;border-right:1px solid #2a2a2a">Venta</th>'
                    f'<th style="{hs2};text-align:right">Costo</th>'
                    f'<th style="{hs2};text-align:right">Rent.</th>'
                    f'<th style="{hs2};text-align:center;border-right:1px solid #2a2a2a">Margen</th>'
                    f'<th style="{hs3};text-align:right">Costo</th>'
                    f'<th style="{hs3};text-align:right">Rent.</th>'
                    f'<th style="{hs3};text-align:center">Margen</th>'
                    f'</tr></thead><tbody>{rows_html}</tbody></table></div>'
                )
                st.markdown("#### Detalle por Producto")
                st.markdown(tabla_html, unsafe_allow_html=True)

                # --- Resumen por Categoría ---
                st.markdown("---")
                st.markdown("#### Resumen por Categoría")
                cat = df_inf1.groupby('categoria_menu').agg(
                    venta=('venta','sum'),
                    costo_teo=('costo_total_teorico','sum'),
                    rent_teo=('rentabilidad_teorica','sum'),
                    costo_per=('costo_total_periodo','sum'),
                    rent_per=('rentabilidad_periodo','sum'),
                    productos=('sku_producto','count')
                ).reset_index()
                cat['margen_teo'] = cat.apply(lambda r: r['rent_teo']/r['venta']*100 if r['venta']>0 else 0, axis=1).round(1)
                cat['margen_per'] = cat.apply(lambda r: r['rent_per']/r['venta']*100 if r['venta']>0 else 0, axis=1).round(1)
                cat = cat.sort_values('rent_per', ascending=False)

                cat_rows = ''
                for _, r in cat.iterrows():
                    cat_rows += (
                        f'<tr style="border-bottom:1px solid #1e1e1e">'
                        f'<td style="padding:9px 12px;font-weight:500;color:#e8e4de">{r["categoria_menu"]}</td>'
                        f'<td style="padding:9px 12px;text-align:right;color:#aaa">{r["productos"]:,.0f}</td>'
                        f'<td style="padding:9px 12px;text-align:right;color:#ccc">${r["venta"]:,.0f}</td>'
                        f'<td style="padding:9px 12px;text-align:right;color:#666">${r["costo_teo"]:,.0f}</td>'
                        f'<td style="padding:9px 12px;text-align:right">{fmt_rent(r["rent_teo"])}</td>'
                        f'<td style="padding:9px 12px;text-align:center;border-right:1px solid #2a2a2a">{badge_margen(r["margen_teo"])}</td>'
                        f'<td style="padding:9px 12px;text-align:right;color:#777">${r["costo_per"]:,.0f}</td>'
                        f'<td style="padding:9px 12px;text-align:right">{fmt_rent(r["rent_per"])}</td>'
                        f'<td style="padding:9px 12px;text-align:center">{badge_margen(r["margen_per"])}</td>'
                        f'</tr>'
                    )

                cat_html = (
                    '<div style="overflow-x:auto;border-radius:14px;border:1px solid #1e1e1e;margin-top:0.5rem;background:#0d0d0d">'
                    '<table style="width:100%;border-collapse:collapse;font-family:DM Sans,sans-serif;font-size:0.82rem">'
                    '<thead>'
                    '<tr style="background:#111">'
                    f'<th colspan="3" style="{hs};text-align:left;border-right:1px solid #2a2a2a"></th>'
                    f'<th colspan="3" style="{hs2};text-align:center;border-right:1px solid #2a2a2a;color:#4caf7d">TEÓRICA</th>'
                    f'<th colspan="3" style="{hs3};text-align:center;color:#d4a853">PERÍODO</th>'
                    '</tr>'
                    '<tr style="background:#111">'
                    f'<th style="{hs};text-align:left">Categoría</th>'
                    f'<th style="{hs};text-align:right">Productos</th>'
                    f'<th style="{hs};text-align:right;border-right:1px solid #2a2a2a">Venta</th>'
                    f'<th style="{hs2};text-align:right">Costo</th>'
                    f'<th style="{hs2};text-align:right">Rent.</th>'
                    f'<th style="{hs2};text-align:center;border-right:1px solid #2a2a2a">Margen</th>'
                    f'<th style="{hs3};text-align:right">Costo</th>'
                    f'<th style="{hs3};text-align:right">Rent.</th>'
                    f'<th style="{hs3};text-align:center">Margen</th>'
                    f'</tr></thead><tbody>{cat_rows}</tbody></table></div>'
                )
                st.markdown(cat_html, unsafe_allow_html=True)

                # Descarga
                buf2 = io.BytesIO()
                with pd.ExcelWriter(buf2, engine='openpyxl') as w:
                    cols_excel = ['sku_producto','categoria_menu','nombre_producto','cant','venta',
                                     'costo_total_teorico','rentabilidad_teorica','margen_teorico',
                                     'costo_total_periodo','rentabilidad_periodo','margen_periodo']
                    cols_excel = [c for c in cols_excel if c in df_inf1.columns]
                    df_inf1[cols_excel].to_excel(w, sheet_name='Rentabilidad', index=False)
                    cat.to_excel(w, sheet_name='Por Categoria', index=False)
                st.download_button("📥 Descargar Informe 1", buf2.getvalue(), "Informe1_Rentabilidad.xlsx")

    # ----------------------------------------------------------
    # INFORME 2
    # ----------------------------------------------------------
    elif "Informe 2" in informe_sel:
        st.markdown("### 📉 Informe de Desviación")
        st.markdown(f"<div class='info-box'>Período: <b>{f_inicio}</b> → <b>{f_fin}</b> · Local: <b>{f_local}</b><br>Consumo teórico = ventas × CantReal. Comprado real = cant_conv de facturas. Variación % = (Comprado - Teórico) / Teórico × 100.</div>", unsafe_allow_html=True)

        if st.button("▶ Generar Informe 2"):
            with st.spinner("Calculando desviaciones..."):
                df_inf2 = informe_desviacion(f_inicio, f_fin, f_local)

            if not df_inf2.empty:
                # Calcular variación %
                df_inf2['variacion_pct'] = df_inf2.apply(
                    lambda r: ((r['cant_real_comprada'] - r['consumo_teorico']) / r['consumo_teorico'] * 100)
                    if r['consumo_teorico'] > 0 else None, axis=1
                )

                perdida_total  = df_inf2[df_inf2['desviacion_dinero'] > 0]['desviacion_dinero'].sum()
                ahorro_total   = df_inf2[df_inf2['desviacion_dinero'] < 0]['desviacion_dinero'].sum()
                items_exceso   = (df_inf2['desviacion_dinero'] > 0).sum()
                items_ok       = (df_inf2['desviacion_dinero'] <= 0).sum()

                # Métricas superiores
                m1, m2, m3, m4 = st.columns(4)
                m1.metric("🔴 Exceso comprado", f"${perdida_total:,.0f}", f"{items_exceso} ítems")
                m2.metric("🟢 Bajo lo teórico",  f"${abs(ahorro_total):,.0f}", f"{items_ok} ítems")
                m3.metric("📦 Total ítems", f"{len(df_inf2)}")
                desv_neta = perdida_total + ahorro_total
                m4.metric("⚖️ Desviación neta", f"${desv_neta:,.0f}")

                st.markdown("<br>", unsafe_allow_html=True)

                # Semáforos
                def semaforo_desv(val):
                    if pd.isna(val): return ''
                    if val > 0:   return 'background-color: #3a1a1a; color: #e84545'
                    elif val < 0: return 'background-color: #1a3a2a; color: #4caf7d'
                    return ''

                def semaforo_pct(val):
                    if pd.isna(val): return 'color: #555'
                    if val > 20:    return 'background-color: #3a1a1a; color: #e84545; font-weight:600'
                    elif val > 5:   return 'background-color: #3a2a1a; color: #e89c45; font-weight:600'
                    elif val < -5:  return 'background-color: #1a3a2a; color: #4caf7d; font-weight:600'
                    return 'color: #aaa'

                cols_show2 = ['sku_ingrediente', 'nombre_ingrediente', 'subcat',
                              'consumo_teorico', 'cant_real_comprada',
                              'desviacion_cant', 'variacion_pct', 'desviacion_dinero']
                existing_cols = [c for c in cols_show2 if c in df_inf2.columns]

                def badge_pct(val):
                    if val is None or (isinstance(val, float) and pd.isna(val)):
                        return '<span style="color:#555">—</span>'
                    if val > 20:
                        return f'<span style="background:#3a1a1a;color:#e84545;padding:2px 8px;border-radius:12px;font-size:0.78rem;font-weight:600">{val:+.1f}%</span>'
                    elif val > 5:
                        return f'<span style="background:#3a2a1a;color:#e89c45;padding:2px 8px;border-radius:12px;font-size:0.78rem;font-weight:600">{val:+.1f}%</span>'
                    elif val < -5:
                        return f'<span style="background:#1a3a2a;color:#4caf7d;padding:2px 8px;border-radius:12px;font-size:0.78rem;font-weight:600">{val:+.1f}%</span>'
                    return f'<span style="color:#aaa;font-size:0.78rem">{val:+.1f}%</span>'

                def fmt_dinero_html(val):
                    if val > 0:
                        return f'<span style="color:#e84545;font-weight:600">${val:,.0f}</span>'
                    elif val < 0:
                        return f'<span style="color:#4caf7d;font-weight:600">${val:,.0f}</span>'
                    return f'<span style="color:#aaa">${val:,.0f}</span>'

                rows_html = ''
                for _, r in df_inf2.iterrows():
                    pct    = r.get('variacion_pct', None)
                    dinero = r.get('desviacion_dinero', 0)
                    bg     = '#1e1212' if dinero > 0 else '#121e14' if dinero < 0 else ''
                    rows_html += (
                        f'<tr style="border-bottom:1px solid #1e1e1e;background:{bg};transition:background 0.15s">'
                        f'<td style="padding:10px 14px;color:#666;font-size:0.76rem;font-family:monospace;white-space:nowrap">{r.get("sku_ingrediente","")}</td>'
                        f'<td style="padding:10px 14px;font-weight:500;color:#e8e4de">{r.get("nombre_ingrediente","")}</td>'
                        f'<td style="padding:10px 14px;color:#555;font-size:0.8rem">{r.get("subcat","")}</td>'
                        f'<td style="padding:10px 14px;text-align:right;color:#777;font-variant-numeric:tabular-nums">{r.get("consumo_teorico",0):,.2f}</td>'
                        f'<td style="padding:10px 14px;text-align:right;color:#ccc;font-variant-numeric:tabular-nums;font-weight:500">{r.get("cant_real_comprada",0):,.2f}</td>'
                        f'<td style="padding:10px 14px;text-align:right;color:#777;font-variant-numeric:tabular-nums">{r.get("desviacion_cant",0):,.2f}</td>'
                        f'<td style="padding:10px 14px;text-align:center">{badge_pct(pct)}</td>'
                        f'<td style="padding:10px 14px;text-align:right;font-variant-numeric:tabular-nums">{fmt_dinero_html(dinero)}</td>'
                        f'</tr>'
                    )

                hs = 'padding:11px 14px;font-size:0.7rem;text-transform:uppercase;letter-spacing:0.09em;font-weight:600;color:#444;border-bottom:1px solid #2a2a2a'
                tabla_html = (
                    '<div style="overflow-x:auto;border-radius:14px;border:1px solid #1e1e1e;margin-top:0.5rem;background:#0d0d0d">'
                    '<table style="width:100%;border-collapse:collapse;font-family:DM Sans,sans-serif;font-size:0.84rem">'
                    '<thead><tr style="background:#111">'
                    f'<th style="{hs};text-align:left">SKU</th>'
                    f'<th style="{hs};text-align:left">Ingrediente</th>'
                    f'<th style="{hs};text-align:left">Cat.</th>'
                    f'<th style="{hs};text-align:right">Teórico</th>'
                    f'<th style="{hs};text-align:right">Comprado</th>'
                    f'<th style="{hs};text-align:right">Δ Cant.</th>'
                    f'<th style="{hs};text-align:center">Δ %</th>'
                    f'<th style="{hs};text-align:right">Δ $</th>'
                    f'</tr></thead><tbody>{rows_html}</tbody></table></div>'
                )
                st.markdown(tabla_html, unsafe_allow_html=True)

                # Resumen por subcategoría
                if 'subcat' in df_inf2.columns:
                    st.markdown("---")
                    st.markdown("#### Resumen por Categoría")
                    sub = df_inf2.groupby('subcat').agg(
                        Teórico=('consumo_teorico','sum'),
                        Comprado=('cant_real_comprada','sum'),
                        Δ_dinero=('desviacion_dinero','sum'),
                        Items=('sku_ingrediente','count')
                    ).reset_index().sort_values('Δ_dinero', ascending=False)
                    sub['Δ %'] = ((sub['Comprado'] - sub['Teórico']) / sub['Teórico'].replace(0,1) * 100).round(1)
                    sub = sub.rename(columns={'subcat':'Categoría','Δ_dinero':'Δ $'})
                    st.dataframe(
                        sub.style
                            .applymap(semaforo_desv, subset=['Δ $'])
                            .applymap(semaforo_pct,  subset=['Δ %'])
                            .format({'Teórico':'{:,.2f}','Comprado':'{:,.2f}','Δ $':'${:,.0f}','Δ %':'{:+.1f}%'}),
                        use_container_width=True, hide_index=True
                    )

                st.markdown("<br>", unsafe_allow_html=True)
                buf3 = io.BytesIO()
                export_cols = existing_cols
                with pd.ExcelWriter(buf3, engine='openpyxl') as w:
                    df_inf2[export_cols].to_excel(w, sheet_name='Desviacion', index=False)
                st.download_button("📥 Descargar Excel", buf3.getvalue(), "Informe2_Desviacion.xlsx")

    # ----------------------------------------------------------
    # INFORME 3 — IMPACTO DE PRECIOS SOBRE CANASTA DE INGREDIENTES
    # ----------------------------------------------------------
    elif "Informe 3" in informe_sel:

        # Selectores de mes
        meses_disp3 = run_query("""
            SELECT DISTINCT DATE_TRUNC('month', fecha_dte::timestamp)::date as mes
            FROM compras WHERE subcat IN ('Directo','Indirecto') ORDER BY 1
        """)

        if meses_disp3.empty:
            st.warning("No hay datos de compras disponibles.")
        else:
            meses_list3 = pd.to_datetime(meses_disp3['mes']).tolist()
            meses_fmt3  = [m.strftime('%B %Y').capitalize() for m in meses_list3]

            mc1, mc2, mc3 = st.columns([2, 2, 2])
            with mc1:
                mes_base_idx3 = st.selectbox("Mes muestra (canasta)", range(len(meses_fmt3)),
                                             format_func=lambda i: meses_fmt3[i],
                                             index=0, key='inf3_base')
            with mc2:
                mes_comp_idx3 = st.selectbox("Mes comparación (precios)", range(len(meses_fmt3)),
                                             format_func=lambda i: meses_fmt3[i],
                                             index=len(meses_list3)-1, key='inf3_comp')
            with mc3:
                cat3_q = run_query("SELECT DISTINCT categoria_producto FROM compras WHERE categoria_producto IS NOT NULL AND subcat IN ('Directo','Indirecto') ORDER BY 1")
                cats3  = ['Todos'] + cat3_q['categoria_producto'].tolist() if not cat3_q.empty else ['Todos']
                cat3_sel = st.selectbox("Categoría", cats3, key='inf3_cat')

            mes_base3     = meses_list3[mes_base_idx3]
            mes_comp3     = meses_list3[mes_comp_idx3]
            mes_base3_str = mes_base3.strftime('%B %Y').capitalize()
            mes_comp3_str = mes_comp3.strftime('%B %Y').capitalize()

            # Filtro texto + ordenamiento
            fc1, fc2, fc3 = st.columns([3, 2, 1])
            with fc1:
                filtro_texto3 = st.text_input("🔍 Buscar SKU o producto",
                                              placeholder="Ej: AL-AF-276 o papas...",
                                              key='inf3_buscar')
            with fc2:
                ord3_col_sel = st.selectbox("Ordenar por", [
                    'Producto', f'Cant. {mes_base3_str}',
                    f'Costo {mes_base3_str}', f'Costo {mes_comp3_str}',
                    'Δ$ Precio', 'Δ% Precio'
                ], key='ord3_col')
            with fc3:
                ord3_dir = st.selectbox("Dir.", ['↓', '↑'], key='ord3_dir')

            if st.button("▶ Generar Informe 3"):
                base_i = mes_base3.strftime('%Y-%m-01')
                base_f = (mes_base3 + pd.offsets.MonthEnd(1)).strftime('%Y-%m-%d')
                comp_i = mes_comp3.strftime('%Y-%m-01')
                comp_f = (mes_comp3 + pd.offsets.MonthEnd(1)).strftime('%Y-%m-%d')
                filtro_cat3 = f"AND categoria_producto = '{cat3_sel}'" if cat3_sel != 'Todos' else ""

                q_ing = f"""
                    WITH equiv AS (
                        SELECT sku_compra, sku_receta FROM sku_equivalencias
                    ),
                    base AS (
                        SELECT
                            COALESCE(e.sku_receta, c.sku)                                               AS sku,
                            MIN(c.nombre_producto)                                                      AS nombre,
                            MIN(c.nombre_proveedor)                                                     AS proveedor,
                            MIN(c.categoria_producto)                                                   AS categoria,
                            MAX(c.formato)                                                              AS formato,
                            SUM(c.cant_conv)                                                            AS cant_base,
                            SUM(c.costo_realfinal) / NULLIF(SUM(c.costo_realfinal / NULLIF(c.muc,0)),0) AS precio_base
                        FROM compras c
                        LEFT JOIN equiv e ON c.sku = e.sku_compra
                        WHERE c.fecha_dte::date BETWEEN '{base_i}' AND '{base_f}'
                          AND c.subcat IN ('Directo','Indirecto')
                          AND c.costo_realfinal > 0
                          AND c.monto_real > 0
                          AND c.muc > 0
                          {filtro_cat3}
                        GROUP BY 1
                    ),
                    comp AS (
                        SELECT
                            COALESCE(e.sku_receta, c.sku)                                               AS sku,
                            SUM(c.costo_realfinal) / NULLIF(SUM(c.costo_realfinal / NULLIF(c.muc,0)),0) AS precio_comp
                        FROM compras c
                        LEFT JOIN equiv e ON c.sku = e.sku_compra
                        WHERE c.fecha_dte::date BETWEEN '{comp_i}' AND '{comp_f}'
                          AND c.subcat IN ('Directo','Indirecto')
                          AND c.costo_realfinal > 0
                          AND c.monto_real > 0
                          AND c.muc > 0
                        GROUP BY 1
                    )
                    SELECT
                        b.sku, b.nombre, b.proveedor, b.categoria,
                        b.formato, b.cant_base, b.precio_base,
                        c.precio_comp,
                        b.cant_base * b.precio_base * b.formato                          AS impacto_base,
                        b.cant_base * COALESCE(c.precio_comp, b.precio_base) * b.formato AS impacto_comp
                    FROM base b
                    LEFT JOIN comp c ON b.sku = c.sku
                    ORDER BY b.nombre
                """
                df3 = run_query(q_ing)

                if df3.empty:
                    st.warning("Sin datos para el mes seleccionado.")
                else:
                    df3['precio_base']  = pd.to_numeric(df3['precio_base'],  errors='coerce').fillna(0)
                    df3['precio_comp']  = pd.to_numeric(df3['precio_comp'],  errors='coerce').fillna(df3['precio_base'])
                    df3['cant_base']    = pd.to_numeric(df3['cant_base'],    errors='coerce').fillna(0)
                    df3['formato']      = pd.to_numeric(df3['formato'],      errors='coerce').fillna(1)
                    df3['impacto_base'] = pd.to_numeric(df3['impacto_base'], errors='coerce').fillna(0)
                    df3['impacto_comp'] = df3['cant_base'] * df3['precio_comp'] * df3['formato']
                    df3['delta_dinero'] = df3['impacto_comp'] - df3['impacto_base']
                    df3['delta_pct']    = df3.apply(
                        lambda r: (r['delta_dinero'] / r['impacto_base'] * 100) if r['impacto_base'] > 0 else None, axis=1
                    )
                    df3['sin_precio_comp'] = df3['precio_comp'] == df3['precio_base']
                    st.session_state['inf3_df']     = df3
                    st.session_state['inf3_labels'] = (mes_base3_str, mes_comp3_str)
                    st.session_state['inf3_fechas'] = (base_i, base_f, comp_i, comp_f)
                    st.session_state['inf3_local_label'] = f"Cadena — {cat3_sel}" if cat3_sel != 'Todos' else 'Cadena Completa'

            if 'inf3_df' in st.session_state:
                df3 = st.session_state['inf3_df'].copy()
                mes_base3_str, mes_comp3_str = st.session_state['inf3_labels']

                # Ordenar
                asc3 = ord3_dir == '↑'
                sort_map3 = {
                    'Producto':                 ('nombre',       asc3),
                    f'Cant. {mes_base3_str}':   ('cant_base',    asc3),
                    f'Costo {mes_base3_str}':   ('impacto_base', asc3),
                    f'Costo {mes_comp3_str}':   ('impacto_comp', asc3),
                    'Δ$ Precio':               ('delta_dinero', asc3),
                    'Δ% Precio':               ('delta_pct',    asc3),
                }
                if ord3_col_sel in sort_map3:
                    col_s, asc_s = sort_map3[ord3_col_sel]
                    df3 = df3.sort_values(col_s, ascending=asc_s, na_position='last')

                # Filtro de texto
                if filtro_texto3:
                    mask3 = (
                        df3['sku'].str.contains(filtro_texto3, case=False, na=False) |
                        df3['nombre'].str.contains(filtro_texto3, case=False, na=False)
                    )
                    df3 = df3[mask3]

                # Métricas
                tot_base = df3['impacto_base'].sum()
                tot_comp = df3['impacto_comp'].sum()
                tot_delta = tot_comp - tot_base
                tot_pct   = (tot_delta / tot_base * 100) if tot_base > 0 else 0
                sin_precio = df3['sin_precio_comp'].sum()

                mm1, mm2, mm3, mm4 = st.columns(4)
                mm1.metric(f"Canasta {mes_base3_str}",     f"${tot_base:,.0f}")
                mm2.metric(f"Canasta a precios {mes_comp3_str}", f"${tot_comp:,.0f}")
                mm3.metric("Δ$ impacto precio",            f"${tot_delta:,.0f}")
                mm4.metric("Δ% total",                     f"{tot_pct:+.1f}%")
                if sin_precio > 0:
                    st.info(f"ℹ️ {int(sin_precio)} ingrediente(s) sin precio en mes de comparación — se usó precio del mes muestra.")

                st.markdown("<br>", unsafe_allow_html=True)

                def badge3(val):
                    if val is None or (isinstance(val, float) and pd.isna(val)):
                        return '<span style="color:#444">—</span>'
                    if val > 10:
                        return f'<span style="background:#3a1a1a;color:#e84545;padding:2px 8px;border-radius:12px;font-size:0.78rem;font-weight:600">{val:+.1f}%</span>'
                    elif val > 3:
                        return f'<span style="background:#3a2a1a;color:#e89c45;padding:2px 8px;border-radius:12px;font-size:0.78rem;font-weight:600">{val:+.1f}%</span>'
                    elif val < -3:
                        return f'<span style="background:#1a3a2a;color:#4caf7d;padding:2px 8px;border-radius:12px;font-size:0.78rem;font-weight:600">{val:+.1f}%</span>'
                    return f'<span style="color:#aaa;font-size:0.75rem">{val:+.1f}%</span>'

                def fmt_d3(val):
                    if val > 0: return f'<span style="color:#e84545;font-weight:600">${val:,.0f}</span>'
                    if val < 0: return f'<span style="color:#4caf7d;font-weight:600">${val:,.0f}</span>'
                    return f'<span style="color:#aaa">${val:,.0f}</span>'

                rows3 = ''
                for _, r in df3.iterrows():
                    bg = '#1e1212' if (r['delta_dinero'] or 0) > 0 else '#121e14' if (r['delta_dinero'] or 0) < 0 else ''
                    sin_p = r.get('sin_precio_comp', False)
                    row_bg = bg if bg else ('rgba(13,30,60,0.6)' if sin_p else '')
                    icono_cell = '<span style="color:#4a9eda;font-size:0.75rem">ℹ️ </span>' if sin_p else ''
                    precio_comp_color = '#4a9eda' if sin_p else '#ccc'
                    rows3 += (
                        f'<tr style="border-bottom:1px solid #1e1e1e;background:{row_bg}">'
                        f'<td style="padding:10px 14px;color:#666;font-family:monospace;font-size:0.76rem">{r.get("sku","")}</td>'
                        f'<td style="padding:10px 14px;font-weight:500;color:{"#4a9eda" if sin_p else "#e8e4de"}">{icono_cell}{r.get("nombre","")}</td>'
                        f'<td style="padding:10px 14px;color:#555;font-size:0.8rem">{r.get("categoria","")}</td>'
                        f'<td style="padding:10px 14px;color:#666;font-size:0.78rem">{r.get("proveedor","")}</td>'
                        f'<td style="padding:10px 14px;text-align:right;color:#aaa;font-variant-numeric:tabular-nums">{r.get("cant_base",0):,.2f}</td>'
                        f'<td style="padding:10px 14px;text-align:right;color:#888;font-variant-numeric:tabular-nums">${r.get("precio_base",0):,.2f}</td>'
                        f'<td style="padding:10px 14px;text-align:right;color:{precio_comp_color};font-variant-numeric:tabular-nums">${r.get("precio_comp",0):,.2f}</td>'
                        f'<td style="padding:10px 14px;text-align:right;color:#777;font-variant-numeric:tabular-nums">${r.get("impacto_base",0):,.0f}</td>'
                        f'<td style="padding:10px 14px;text-align:right;color:#e8e4de;font-variant-numeric:tabular-nums">${r.get("impacto_comp",0):,.0f}</td>'
                        f'<td style="padding:10px 14px;text-align:right">{fmt_d3(r.get("delta_dinero",0))}</td>'
                        f'<td style="padding:10px 14px;text-align:center">{badge3(r.get("delta_pct",None))}</td>'
                        f'</tr>'
                    )

                hs3 = 'padding:11px 14px;font-size:0.7rem;text-transform:uppercase;letter-spacing:0.09em;font-weight:600;color:#444;border-bottom:1px solid #2a2a2a'
                hdrs3 = ['SKU', 'Producto', 'Categoría', 'Proveedor',
                          f'Cant. {mes_base3_str}',
                          f'P. Unit {mes_base3_str}', f'P. Unit {mes_comp3_str}',
                          f'Total {mes_base3_str}', f'Total {mes_comp3_str}',
                          'Δ$', 'Δ%']
                tabla3 = (
                    '<div style="overflow-x:auto;border-radius:14px;border:1px solid #1e1e1e;margin-top:0.5rem;background:#0d0d0d">'
                    '<table style="width:100%;border-collapse:collapse;font-family:DM Sans,sans-serif;font-size:0.84rem">'
                    '<thead><tr style="background:#111">'
                    + ''.join([f'<th style="{hs3};text-align:{"left" if i<4 else "right"}">{h}</th>' for i, h in enumerate(hdrs3)])
                    + f'</tr></thead><tbody>{rows3}</tbody></table></div>'
                )
                st.markdown(tabla3, unsafe_allow_html=True)

                st.markdown("<br>", unsafe_allow_html=True)
                d1, d2, d3 = st.columns(3)

                # ── Botón 1: Excel ────────────────────────────────────
                with d1:
                    buf_inf3 = io.BytesIO()
                    with pd.ExcelWriter(buf_inf3, engine='openpyxl') as w:
                        df3[['sku','nombre','categoria','proveedor','cant_base',
                              'precio_base','precio_comp','impacto_base',
                              'impacto_comp','delta_dinero','delta_pct']].to_excel(w, sheet_name='Canasta', index=False)
                    st.download_button("📥 Excel", buf_inf3.getvalue(),
                        "Informe3_Canasta.xlsx", use_container_width=True)

                # ── Botón 2: PDF resumen de lo que está en pantalla (1 página) ──
                with d2:
                    _label_pdf = st.session_state.get('inf3_local_label', 'Cadena Completa')
                    _pdf_actual = generar_pdf_variacion(df3, mes_base3_str, mes_comp3_str, _label_pdf)
                    st.download_button("📄 PDF — Resumen cadena", _pdf_actual,
                        f"Variacion_{_label_pdf.replace(' ','_')}_{mes_base3_str}_vs_{mes_comp3_str}.pdf",
                        mime="application/pdf", use_container_width=True)

                # ── Botón 3: PDF con 1 página por cada local (10 páginas) ──
                with d3:
                    if st.button("📄 PDF — Por local", key="pdf_todos", use_container_width=True):
                        if 'inf3_fechas' not in st.session_state:
                            st.error("Primero genera el informe con ▶")
                        else:
                            with st.spinner("Generando resumen por local..."):
                                from pypdf import PdfWriter as PdfW, PdfReader as PdfR
                                _bi, _bf, _ci, _cf = st.session_state['inf3_fechas']
                                writer_all = PdfW()

                                for loc in [l for l in get_locales() if l not in ('Todos', None)]:
                                    try:
                                        _fl = f"AND UPPER(c.local) = UPPER('{loc}')"
                                        q_loc = f"""
                                            WITH equiv AS (SELECT sku_compra, sku_receta FROM sku_equivalencias),
                                            base AS (
                                                SELECT COALESCE(e.sku_receta,c.sku) AS sku,
                                                       MIN(c.nombre_producto) AS nombre,
                                                       MIN(c.nombre_proveedor) AS proveedor,
                                                       MIN(c.categoria_producto) AS categoria,
                                                       MAX(c.formato) AS formato,
                                                       SUM(c.cant_conv) AS cant_base,
                                                       SUM(c.costo_realfinal)/NULLIF(SUM(c.costo_realfinal/NULLIF(c.muc,0)),0) AS precio_base
                                                FROM compras c LEFT JOIN equiv e ON c.sku=e.sku_compra
                                                WHERE c.fecha_dte::date BETWEEN '{_bi}' AND '{_bf}'
                                                  AND c.subcat IN ('Directo','Indirecto')
                                                  AND c.costo_realfinal>0 AND c.monto_real>0 AND c.muc>0
                                                  {_fl} GROUP BY 1),
                                            comp AS (
                                                SELECT COALESCE(e.sku_receta,c.sku) AS sku,
                                                       SUM(c.costo_realfinal)/NULLIF(SUM(c.costo_realfinal/NULLIF(c.muc,0)),0) AS precio_comp
                                                FROM compras c LEFT JOIN equiv e ON c.sku=e.sku_compra
                                                WHERE c.fecha_dte::date BETWEEN '{_ci}' AND '{_cf}'
                                                  AND c.subcat IN ('Directo','Indirecto')
                                                  AND c.costo_realfinal>0 AND c.monto_real>0 AND c.muc>0
                                                  {_fl} GROUP BY 1)
                                            SELECT b.sku,b.nombre,b.proveedor,b.categoria,b.formato,b.cant_base,
                                                   b.precio_base, c.precio_comp,
                                                   b.cant_base*b.precio_base*b.formato AS impacto_base,
                                                   b.cant_base*COALESCE(c.precio_comp,b.precio_base)*b.formato AS impacto_comp
                                            FROM base b LEFT JOIN comp c ON b.sku=c.sku ORDER BY b.nombre
                                        """
                                        df_loc = run_query(q_loc)
                                        if df_loc.empty: continue
                                        df_loc['precio_base']  = pd.to_numeric(df_loc['precio_base'],  errors='coerce').fillna(0)
                                        df_loc['precio_comp']  = pd.to_numeric(df_loc['precio_comp'],  errors='coerce').fillna(df_loc['precio_base'])
                                        df_loc['cant_base']    = pd.to_numeric(df_loc['cant_base'],    errors='coerce').fillna(0)
                                        df_loc['formato']      = pd.to_numeric(df_loc['formato'],      errors='coerce').fillna(1)
                                        df_loc['impacto_base'] = pd.to_numeric(df_loc['impacto_base'], errors='coerce').fillna(0)
                                        df_loc['impacto_comp'] = df_loc['cant_base'] * df_loc['precio_comp'] * df_loc['formato']
                                        df_loc['delta_dinero'] = df_loc['impacto_comp'] - df_loc['impacto_base']
                                        df_loc['delta_pct']    = df_loc.apply(
                                            lambda r: (r['delta_dinero']/r['impacto_base']*100) if r['impacto_base']>0 else None, axis=1)
                                        pdf_loc = generar_pdf_variacion(df_loc, mes_base3_str, mes_comp3_str, loc)
                                        for pg in PdfR(io.BytesIO(pdf_loc)).pages:
                                            writer_all.add_page(pg)
                                    except Exception:
                                        continue

                                buf_all = io.BytesIO()
                                writer_all.write(buf_all)
                                st.session_state['pdf_locales_bytes']  = buf_all.getvalue()
                                st.session_state['pdf_locales_nombre'] = f"Variacion_PorLocal_{mes_base3_str}_vs_{mes_comp3_str}.pdf"

                # Botón descarga aparece debajo al estar listo
                if 'pdf_locales_bytes' in st.session_state:
                    st.download_button("⬇️ Descargar PDF — Por local",
                        st.session_state['pdf_locales_bytes'],
                        st.session_state['pdf_locales_nombre'],
                        mime="application/pdf", key="pdf_locales_dl", use_container_width=True)



    # ----------------------------------------------------------
    # INFORME 4 — INFORME DE COSTOS
    # ----------------------------------------------------------
    elif "Informe 4" in informe_sel:
        st.markdown("### 📋 Informe de Costos")

        # ── Controles ────────────────────────────────────────────
        _MESES_ES_IC = {1:'Ene',2:'Feb',3:'Mar',4:'Abr',5:'May',6:'Jun',
                        7:'Jul',8:'Ago',9:'Sep',10:'Oct',11:'Nov',12:'Dic'}
        ic1, ic2, ic3, ic4, ic5 = st.columns(5)
        with ic1:
            ic_fecha_i = st.date_input("Inicio semana", key="ic_fi", value=None)
        with ic2:
            ic_fecha_f = st.date_input("Fin semana", key="ic_ff", value=None)
        with ic3:
            locales_ic_q = run_query("SELECT DISTINCT local FROM inventarios WHERE local IS NOT NULL ORDER BY 1")
            locales_ic = ["Todos"] + locales_ic_q['local'].tolist() if not locales_ic_q.empty else ["Todos"]
            local_ic = st.selectbox("Local", locales_ic, key="ic_local")
        with ic4:
            st.markdown("<div style='height:28px'></div>", unsafe_allow_html=True)
            generar_ic = st.button("▶ Generar", key="btn_ic", use_container_width=True)

        if ic_fecha_i and ic_fecha_f:
            if ic_fecha_i.month == ic_fecha_f.month:
                periodo_ic = f"{ic_fecha_i.day}-{ic_fecha_f.day} {_MESES_ES_IC[ic_fecha_i.month]} {ic_fecha_i.year}"
            else:
                periodo_ic = f"{ic_fecha_i.day} {_MESES_ES_IC[ic_fecha_i.month]}-{ic_fecha_f.day} {_MESES_ES_IC[ic_fecha_f.month]} {ic_fecha_i.year}"
            fecha_ic_i = ic_fecha_i
            fecha_ic_f = ic_fecha_f
            st.caption(f"Período: **{periodo_ic}** · Compras/ventas: {ic_fecha_i} → {ic_fecha_f}")
        else:
            periodo_ic = None
            fecha_ic_i = None
            fecha_ic_f = None

        if not ic_fecha_i or not ic_fecha_f:
            st.info("Selecciona el inicio y fin de la semana del inventario")

        if generar_ic and periodo_ic and fecha_ic_i and fecha_ic_f:
            with st.spinner("Calculando informe de costos..."):
                engine = get_engine()

                locales_q = run_query("SELECT DISTINCT local FROM inventarios WHERE local IS NOT NULL ORDER BY 1")
                todos_locales = locales_q['local'].tolist() if not locales_q.empty else []
                locales_sel = todos_locales if local_ic == "Todos" else [local_ic]

                # ── Funciones de cálculo ──────────────────────────────
                def get_inv(periodo, tipo, locales):
                    lf = "AND LOWER(TRIM(local))=ANY(:ls)" if locales else ""
                    q = f"""
                        SELECT LOWER(TRIM(local)) as local,
                               UPPER(TRIM(producto_control)) as producto_control,
                               SUM(total_kg) as kg
                        FROM inventarios
                        WHERE TRIM(periodo)=:p AND tipo_inventario=:t {lf}
                        GROUP BY LOWER(TRIM(local)), UPPER(TRIM(producto_control))
                    """
                    params = {'p': periodo.strip(), 't': tipo}
                    if locales: params['ls'] = [l.lower().strip() for l in locales]
                    return run_query(q, params)

                def get_uso(periodo, locales):
                    lf = "AND LOWER(TRIM(local))=ANY(:ls)" if locales else ""
                    q = f"""
                        SELECT LOWER(TRIM(local)) as local,
                               UPPER(TRIM(nombre_ingrediente)) as producto_control,
                               SUM(cantidad) as kg, SUM(costo) as costo
                        FROM uso_ingredientes
                        WHERE TRIM(periodo)=:p {lf}
                        GROUP BY LOWER(TRIM(local)), UPPER(TRIM(nombre_ingrediente))
                    """
                    params = {'p': periodo.strip()}
                    if locales: params['ls'] = [l.lower().strip() for l in locales]
                    return run_query(q, params)

                def get_compras_kg(fecha_i, fecha_f, locales):
                    """Compras en KG por producto_control usando SKU→nombre mapeado"""
                    lf = "AND UPPER(c.local)=ANY(:ls)" if locales else ""
                    q = f"""
                        SELECT c.local,
                               UPPER(c.nombre_producto) as producto_control,
                               SUM(c.cant_conv) as kg,
                               SUM(c.costo_realfinal) as costo
                        FROM compras c
                        WHERE c.fecha_dte::date BETWEEN :i AND :f
                          AND c.subcat NOT IN ('COLACION','ADMINISTRACION','ART. LIMPIEZA','DESECHABLES')
                          {lf}
                        GROUP BY c.local, UPPER(c.nombre_producto)
                    """
                    params = {'i': str(fecha_i), 'f': str(fecha_f)}
                    if locales: params['ls'] = [l.upper() for l in locales]
                    return run_query(q, params)

                def get_no_registrado(periodo, locales):
                    """Compras no registradas — solo cantidad, agrupada por local + producto_control"""
                    lf = "AND LOWER(TRIM(local))=ANY(:ls)" if locales else ""
                    q = f"""
                        SELECT TRIM(local) as local,
                               UPPER(TRIM(producto_control)) as producto_control,
                               SUM(cantidad) as kg
                        FROM compras_no_registradas
                        WHERE TRIM(periodo)=:p
                          AND producto_control IS NOT NULL
                          AND TRIM(producto_control) != '' {lf}
                        GROUP BY TRIM(local), UPPER(TRIM(producto_control))
                    """
                    params = {'p': periodo}
                    if locales: params['ls'] = [l.lower() for l in locales]
                    return run_query(q, params)

                def get_ventas_ic(fecha_i, fecha_f, locales):
                    lf = "AND UPPER(local)=ANY(:ls)" if locales else ""
                    q = f"""
                        SELECT local,
                               SUM(CASE WHEN origen IS NULL OR origen='' THEN monto_venta_real ELSE 0 END) as venta_salon,
                               SUM(CASE WHEN origen IS NOT NULL AND origen!='' THEN monto_venta_real ELSE 0 END) as venta_delivery,
                               SUM(monto_venta_real) as venta_total,
                               SUM(CASE WHEN categoria_menu ILIKE '%cerveza%' OR categoria_menu ILIKE '%bebida%'
                                        OR categoria_menu ILIKE '%coctele%' OR categoria_menu ILIKE '%vino%'
                                        OR categoria_menu ILIKE '%pisco%' OR categoria_menu ILIKE '%vodka%'
                                        OR categoria_menu ILIKE '%whisky%' OR categoria_menu ILIKE '%ron%'
                                        OR categoria_menu ILIKE '%espumante%' OR categoria_menu ILIKE '%bajativo%'
                                        OR categoria_menu ILIKE '%jugo%' OR categoria_menu ILIKE '%cafeter%'
                                        OR categoria_menu ILIKE '%dulce%'
                                   THEN monto_venta_real ELSE 0 END) as venta_bar
                        FROM ventas
                        WHERE fecha_venta BETWEEN :i AND :f {lf}
                        GROUP BY local
                    """
                    params = {'i': str(fecha_i), 'f': str(fecha_f)}
                    if locales: params['ls'] = [l.upper() for l in locales]
                    return run_query(q, params)

                def get_compras_cat(fecha_i, fecha_f, locales):
                    lf = "AND UPPER(local)=ANY(:ls)" if locales else ""
                    q = f"""
                        SELECT local, categoria_producto,
                               SUM(costo_realfinal) as compra_total
                        FROM compras
                        WHERE fecha_dte::date BETWEEN :i AND :f {lf}
                        GROUP BY local, categoria_producto
                    """
                    params = {'i': str(fecha_i), 'f': str(fecha_f)}
                    if locales: params['ls'] = [l.upper() for l in locales]
                    return run_query(q, params)

                def get_bar_ventas(fecha_i, fecha_f, locales):
                    lf = "AND UPPER(local)=ANY(:ls)" if locales else ""
                    q = f"""
                        SELECT local, categoria_menu as producto,
                               SUM(monto_venta_real) as venta
                        FROM ventas
                        WHERE fecha_venta BETWEEN :i AND :f
                          AND (categoria_menu ILIKE '%cerveza%' OR categoria_menu ILIKE '%bebida%'
                               OR categoria_menu ILIKE '%coctel%' OR categoria_menu ILIKE '%vino%'
                               OR categoria_menu ILIKE '%pisco%' OR categoria_menu ILIKE '%vodka%'
                               OR categoria_menu ILIKE '%whisky%' OR categoria_menu ILIKE '%ron%'
                               OR categoria_menu ILIKE '%espumante%' OR categoria_menu ILIKE '%bajativo%'
                               OR categoria_menu ILIKE '%jugo%' OR categoria_menu ILIKE '%cafeter%'
                               OR categoria_menu ILIKE '%dulce%') {lf}
                        GROUP BY local, categoria_menu
                        ORDER BY local, venta DESC
                    """
                    params = {'i': str(fecha_i), 'f': str(fecha_f)}
                    if locales: params['ls'] = [l.upper() for l in locales]
                    return run_query(q, params)

                # ── Fechas del período — inputs explícitos ────────────
                from datetime import date as _date
                st.session_state['ic_needs_dates'] = True

                # Cargar datos — ventas y compras usan rango de fechas explícito
                df_inv_ini = get_inv(periodo_ic, 'Inicial', locales_sel)
                df_inv_fin = get_inv(periodo_ic, 'Final',   locales_sel)
                df_uso_ic  = get_uso(periodo_ic, locales_sel)
                df_ventas_ic   = get_ventas_ic(fecha_ic_i, fecha_ic_f, locales_sel)
                df_compras_cat = get_compras_cat(fecha_ic_i, fecha_ic_f, locales_sel)
                df_bar_ven     = get_bar_ventas(fecha_ic_i, fecha_ic_f, locales_sel)
                df_no_reg      = get_no_registrado(periodo_ic, locales_sel)

                # Compras KG por producto_control usando clasificación
                _lf_ckr = "AND LOWER(c.local)=ANY(:ls)" if locales_sel else ""
                _q_ckr = f"""
                    SELECT c.local,
                           COALESCE(NULLIF(TRIM(cn.categoria_control),''), UPPER(c.nombre_producto)) as producto_control,
                           SUM(c.cant_conv) as kg,
                           SUM(c.costo_realfinal) as costo
                    FROM compras c
                    LEFT JOIN clas_nomb_prod cn
                           ON UPPER(TRIM(c.nombre_producto)) = UPPER(TRIM(cn.nombre_producto))
                    WHERE c.fecha_dte::date BETWEEN :i AND :f
                      AND c.categoria_producto IN ('ALIMENTOS','VERDURAS','BAR')
                      AND c.cant_conv > 0
                      {_lf_ckr}
                    GROUP BY c.local, COALESCE(NULLIF(TRIM(cn.categoria_control),''), UPPER(c.nombre_producto))
                """
                _p_ckr = {'i': str(fecha_ic_i), 'f': str(fecha_ic_f)}
                if locales_sel: _p_ckr['ls'] = [l.lower() for l in locales_sel]
                df_compras_kg_raw = run_query(_q_ckr, _p_ckr)

                # ── Categorías de control ────────────────────────────
                cat_labels = {
                    'CARNES ROJAS':       ['POSTA','FILETE','PLATEADA','LOMO LISO','LOMO VETADO','GRASA DE WAGYU'],
                    'CARNES BLANCAS':     ['PECHUGA DE POLLO','COSTILLAS','CHULETA KASSLER','LOMO DE CENTRO','PERNIL','JAMÓN','TOCINO AHUMADO','PANCETA LAMINADA'],
                    'VERDURAS':           ['PALTA','TOMATE','LECHUGA'],
                    'PESCADOS Y MARISCOS':['FILETE SALMON','SALMON SLICE LAMINADO','CAMARON','CAMARON APANADO','ATUN','LOCOS','ERIZOS'],
                    'OTROS':              ['QUESO RANCO','QUESO CHEDDAR','QUESO PARMESANO','PAPAS FRITAS'],
                    'PAN':                ['FRICA 14 CMS','MOLDE BANQUETE','MOLDE BANQUETE INTEGRAL','PAN FRICA 12 CM','PAN FRICA N8','HOT - DOG 19 CM.'],
                    'BAR':                ['SCHOP','JUGOS'],
                }

                st.session_state['ic_data'] = {
                    'periodo': periodo_ic,
                    'locales': locales_sel,
                    'df_ventas': df_ventas_ic,
                    'df_compras_cat': df_compras_cat,
                    'df_compras_kg': df_compras_kg_raw,
                    'df_bar_ven': df_bar_ven,
                    'df_inv_ini': df_inv_ini,
                    'df_inv_fin': df_inv_fin,
                    'df_uso': df_uso_ic,
                    'df_no_reg': df_no_reg,
                    'cat_labels': cat_labels,
                    'fecha_i': fecha_ic_i,
                    'fecha_f': fecha_ic_f,
                }

            st.success(f"✅ Datos cargados para {len(locales_sel)} local(es) — período {periodo_ic}")

        # ── Mostrar informe si hay datos ─────────────────────────
        if 'ic_data' in st.session_state:
            d = st.session_state['ic_data']
            df_v   = d['df_ventas']
            df_cc  = d['df_compras_cat']
            df_bv  = d['df_bar_ven']
            df_nr  = d.get('df_no_reg', pd.DataFrame())
            df_ckr = d.get('df_compras_kg', pd.DataFrame())
            cat_labels = d.get('cat_labels', {})
            df_ini = d['df_inv_ini']
            df_fin = d['df_inv_fin']
            df_uso = d['df_uso']
            locales_show = d['locales']
            periodo_show = d['periodo']

            hs = 'padding:8px 12px;font-size:0.68rem;text-transform:uppercase;letter-spacing:0.08em;font-weight:600;color:#444;border-bottom:1px solid #2a2a2a;background:#111'
            hs2 = hs + ';color:#4caf7d'
            hsr = hs + ';color:#e84545'

            def fmt_clp(v):
                try: return f"${float(v):,.0f}"
                except: return "—"
            def fmt_pct(v):
                try: return f"{float(v)*100:.1f}%"
                except: return "—"
            def fmt_kg(v):
                try: return f"{float(v):.2f}"
                except: return "—"

            for local_show in locales_show:
                st.markdown(f"---")
                st.markdown(f"### 📍 {local_show} — {periodo_show}")

                # ── Filtrar por local ─────────────────────────────
                def filt(df, col='local'):
                    try:
                        if df is None or not isinstance(df, pd.DataFrame) or df.empty: return pd.DataFrame()
                        if col not in df.columns: return pd.DataFrame()
                        return df[df[col].astype(str).str.strip().str.lower() == local_show.strip().lower()].copy()
                    except: return pd.DataFrame()

                nr   = filt(df_nr)
                ckr  = filt(df_ckr)

                vv  = filt(df_v)
                cc  = filt(df_cc)
                bv  = filt(df_bv)
                ini = filt(df_ini)
                fin = filt(df_fin)
                uso = filt(df_uso)

                # ── Métricas principales ──────────────────────────
                v_salon    = float(vv['venta_salon'].sum())    if not vv.empty else 0
                v_delivery = float(vv['venta_delivery'].sum()) if not vv.empty else 0
                v_total    = float(vv['venta_total'].sum())    if not vv.empty else 0
                v_bar      = float(vv['venta_bar'].sum())      if not vv.empty else 0
                compra_tot = float(cc['compra_total'].sum())   if not cc.empty else 0
                pct_compra = compra_tot / v_total if v_total > 0 else 0

                # Compra por categoría
                cat_map = {'ALIMENTOS': 0, 'VERDURAS': 0, 'BAR': 0, 'ART. LIMPIEZA': 0, 'DESECHABLES': 0}
                if not cc.empty:
                    for _, row in cc.iterrows():
                        cat = str(row.get('categoria_producto','')).strip().upper()
                        val = float(row.get('compra_total', 0) or 0)
                        if cat in cat_map: cat_map[cat] += val

                # Uso por categoría de control
                cat_uso = {}
                cat_desv = {}
                cat_labels = {
                    'CARNES ROJAS': ['POSTA','FILETE','PLATEADA','LOMO LISO','LOMO VETADO','GRASA DE WAGYU'],
                    'CARNES BLANCAS': ['PECHUGA DE POLLO','COSTILLAS','CHULETA KASSLER','LOMO DE CENTRO','PERNIL','JAMÓN','TOCINO AHUMADO','PANCETA LAMINADA'],
                    'VERDURAS': ['PALTA','TOMATE','LECHUGA'],
                    'PESCADOS Y MARISCOS': ['FILETE SALMON','SALMON SLICE LAMINADO','CAMARON','CAMARON APANADO','ATUN','LOCOS','ERIZOS'],
                    'OTROS': ['QUESO RANCO','QUESO CHEDDAR','QUESO PARMESANO','PAPAS FRITAS'],
                    'PAN': ['FRICA 14 CMS','MOLDE BANQUETE','MOLDE BANQUETE INTEGRAL','PAN FRICA 12 CM','PAN FRICA N8','HOT - DOG 19 CM.'],
                }
                for cat, prods in cat_labels.items():
                    mask = uso['producto_control'].str.upper().isin([p.upper() for p in prods]) if not uso.empty else pd.Series(dtype=bool)
                    cat_uso[cat]  = float(uso[mask]['costo'].sum()) if not uso.empty and mask.any() else 0
                    cat_desv[cat] = 0  # se calculará en el control de productos

                # ── SECCIÓN 1: Análisis de costo ──────────────────
                c1, c2, c3 = st.columns([1.2, 1.2, 1.6])

                with c1:
                    st.markdown("**1. Análisis de Costo**")
                    cats_compra = ['ALIMENTOS', 'VERDURAS', 'BAR', 'ART. LIMPIEZA', 'DESECHABLES']
                    compra_tot = sum(cat_map.get(c, 0) for c in cats_compra)
                    pct_compra = compra_tot / v_total if v_total > 0 else 0
                    rows1 = [
                        ('VENTA SALÓN',    fmt_clp(v_salon),    fmt_pct(v_salon/v_total if v_total else 0)),
                        ('VENTA DELIVERY', fmt_clp(v_delivery), fmt_pct(v_delivery/v_total if v_total else 0)),
                        ('VENTA TOTAL',    fmt_clp(v_total),    '100%'),
                        ('VENTA BAR',      fmt_clp(v_bar),      fmt_pct(v_bar/v_total if v_total else 0)),
                        ('',              '',                   ''),
                        ('COMPRA TOTAL',   fmt_clp(compra_tot), fmt_pct(pct_compra)),
                        ('',              '',                   ''),
                        ('ALIMENTOS',      fmt_clp(cat_map['ALIMENTOS']), fmt_pct(cat_map['ALIMENTOS']/compra_tot if compra_tot else 0)),
                        ('VERDURAS',       fmt_clp(cat_map['VERDURAS']),  fmt_pct(cat_map['VERDURAS']/compra_tot if compra_tot else 0)),
                        ('BAR',            fmt_clp(cat_map['BAR']),       fmt_pct(cat_map['BAR']/compra_tot if compra_tot else 0)),
                        ('ART. LIMPIEZA',  fmt_clp(cat_map['ART. LIMPIEZA']), fmt_pct(cat_map['ART. LIMPIEZA']/compra_tot if compra_tot else 0)),
                        ('DESECHABLES',    fmt_clp(cat_map['DESECHABLES']), fmt_pct(cat_map['DESECHABLES']/compra_tot if compra_tot else 0)),
                    ]
                    r1_html = ''.join([
                        f'<tr style="border-bottom:1px solid #1a1a1a;{"background:#0a1a0a" if r[0]=="VENTA TOTAL" else "background:#111" if r[0]=="COMPRA TOTAL" else ""}">'
                        f'<td style="padding:6px 10px;color:#888;font-size:0.75rem">{r[0]}</td>'
                        f'<td style="padding:6px 10px;text-align:right;color:#ccc;font-size:0.78rem">{r[1]}</td>'
                        f'<td style="padding:6px 10px;text-align:right;color:#666;font-size:0.75rem">{r[2]}</td>'
                        f'</tr>'
                        for r in rows1 if r[0]
                    ])
                    st.markdown(
                        f'<div style="border:1px solid #1e1e1e;border-radius:10px;overflow:hidden;background:#0d0d0d">'
                        f'<table style="width:100%;border-collapse:collapse;font-family:DM Sans,sans-serif">'
                        f'<thead><tr style="background:#111"><th style="{hs};text-align:left">ÍTEM</th>'
                        f'<th style="{hs};text-align:right">$ CLP</th><th style="{hs};text-align:right">%</th></tr></thead>'
                        f'<tbody>{r1_html}</tbody></table></div>',
                        unsafe_allow_html=True)

                with c2:
                    st.markdown("**2. Vista General Bar**")
                    if not bv.empty:
                        bv_s = bv.sort_values('venta', ascending=False).head(14)
                        r2_html = ''.join([
                            f'<tr style="border-bottom:1px solid #1a1a1a">'
                            f'<td style="padding:5px 10px;color:#888;font-size:0.7rem;text-align:right">{i+2}</td>'
                            f'<td style="padding:5px 10px;color:#ccc;font-size:0.75rem">{r["producto"]}</td>'
                            f'<td style="padding:5px 10px;text-align:right;color:#d4a853;font-size:0.78rem">{fmt_clp(r["venta"])}</td>'
                            f'</tr>'
                            for i, (_, r) in enumerate(bv_s.iterrows())
                        ])
                        comp_bar = float(cat_map['BAR'])
                        pct_bar  = comp_bar / v_bar if v_bar > 0 else 0
                        r2_html += (
                            f'<tr style="background:#0d1a0d;border-top:1px solid #2a2a2a">'
                            f'<td colspan="2" style="padding:6px 10px;color:#4caf7d;font-size:0.75rem;font-weight:600">Total Venta</td>'
                            f'<td style="padding:6px 10px;text-align:right;color:#4caf7d;font-weight:600;font-size:0.78rem">{fmt_clp(v_bar)}</td></tr>'
                            f'<tr style="background:#0d1a0d"><td colspan="2" style="padding:4px 10px;color:#888;font-size:0.73rem">Total Compra Bar</td>'
                            f'<td style="padding:4px 10px;text-align:right;color:#aaa;font-size:0.75rem">{fmt_clp(comp_bar)}</td></tr>'
                            f'<tr style="background:#0d1a0d"><td colspan="2" style="padding:4px 10px;color:#888;font-size:0.73rem">% Compra</td>'
                            f'<td style="padding:4px 10px;text-align:right;color:#d4a853;font-size:0.75rem">{fmt_pct(pct_bar)}</td></tr>'
                        )
                        st.markdown(
                            f'<div style="border:1px solid #1e1e1e;border-radius:10px;overflow:hidden;background:#0d0d0d">'
                            f'<table style="width:100%;border-collapse:collapse;font-family:DM Sans,sans-serif">'
                            f'<thead><tr style="background:#111"><th style="{hs};text-align:right">#</th>'
                            f'<th style="{hs};text-align:left">PRODUCTO</th><th style="{hs};text-align:right">$ CLP</th></tr></thead>'
                            f'<tbody>{r2_html}</tbody></table></div>',
                            unsafe_allow_html=True)
                    else:
                        st.caption("Sin datos de ventas bar para el período.")

                with c3:
                    st.markdown("**3. Resumen General**")
                    cats_res = list(cat_labels.keys())
                    total_uso_clp  = sum(cat_uso.values())
                    r3_html = ''
                    for cat in cats_res:
                        ut  = cat_uso.get(cat, 0)
                        dsv = cat_desv.get(cat, 0)
                        pct_dsv = dsv / ut if ut > 0 else 0
                        r3_html += (
                            f'<tr style="border-bottom:1px solid #1a1a1a">'
                            f'<td style="padding:5px 10px;color:#ccc;font-size:0.75rem">{cat}</td>'
                            f'<td style="padding:5px 10px;text-align:right;color:#aaa;font-size:0.75rem">{fmt_clp(ut)}</td>'
                            f'<td style="padding:5px 10px;text-align:right;color:{"#e84545" if dsv>0 else "#4caf7d"};font-size:0.75rem">{fmt_clp(dsv)}</td>'
                            f'<td style="padding:5px 10px;text-align:right;color:#888;font-size:0.73rem">{fmt_pct(pct_dsv)}</td>'
                            f'</tr>'
                        )
                    r3_html += (
                        f'<tr style="background:#111;border-top:1px solid #2a2a2a">'
                        f'<td style="padding:6px 10px;color:#d4a853;font-weight:600;font-size:0.75rem">DESVIACIÓN TOTAL</td>'
                        f'<td style="padding:6px 10px;text-align:right;color:#d4a853;font-weight:600">{fmt_clp(total_uso_clp)}</td>'
                        f'<td style="padding:6px 10px;text-align:right;color:#d4a853;font-weight:600">—</td>'
                        f'<td style="padding:6px 10px;text-align:right;color:#d4a853;font-weight:600">—</td></tr>'
                    )
                    st.markdown(
                        f'<div style="border:1px solid #1e1e1e;border-radius:10px;overflow:hidden;background:#0d0d0d">'
                        f'<table style="width:100%;border-collapse:collapse;font-family:DM Sans,sans-serif">'
                        f'<thead><tr style="background:#111">'
                        f'<th style="{hs};text-align:left">CATEGORÍA</th>'
                        f'<th style="{hs};text-align:right">UTILIZADO $</th>'
                        f'<th style="{hs};text-align:right">DESVIACIÓN $</th>'
                        f'<th style="{hs};text-align:right">DESV %</th></tr></thead>'
                        f'<tbody>{r3_html}</tbody></table></div>',
                        unsafe_allow_html=True)

                # ── SECCIÓN 5: Control de productos críticos ──────
                st.markdown(f"**5. Control de Productos Críticos**")

                def _getkg(df, prod):
                    try:
                        if df is None or not isinstance(df, pd.DataFrame): return 0.0
                        if df.empty or 'producto_control' not in df.columns: return 0.0
                        m = df['producto_control'].astype(str).str.upper().str.strip() == prod.upper().strip()
                        return float(df.loc[m, 'kg'].sum() or 0)
                    except: return 0.0

                def _getcosto(df, prod):
                    try:
                        if df is None or not isinstance(df, pd.DataFrame): return 0.0
                        if df.empty or 'producto_control' not in df.columns: return 0.0
                        m = df['producto_control'].astype(str).str.upper().str.strip() == prod.upper().strip()
                        col = 'costo' if 'costo' in df.columns else None
                        return float(df.loc[m, col].sum() or 0) if col else 0.0
                    except: return 0.0

                for cat_nombre, prods in cat_labels.items():
                    filas_ctrl = []
                    for prod in prods:
                        ini_kg  = _getkg(ini,  prod)   # inventario inicial KG
                        fin_kg  = _getkg(fin,  prod)   # inventario final KG
                        uso_kg  = _getkg(uso,  prod)   # uso teórico recetario (Toteat) KG
                        nr_kg   = _getkg(nr,   prod)   # no registrado con signo
                        comp_kg = _getkg(ckr,  prod)   # compras reales KG del período
                        costo_u = _getcosto(ckr, prod) # costo real compras período

                        # Si no hay compras clasificadas disponibles, estimar por balance
                        if comp_kg == 0:
                            comp_kg = max(0.0, fin_kg - ini_kg + uso_kg - nr_kg)

                        # Costo: si no hay de compras, usar uso_ingredientes
                        if costo_u == 0:
                            costo_u = _getcosto(uso, prod)

                        # Real Utilizado = Inv.Ini + Compras + No Reg - Inv.Fin
                        real_ut  = ini_kg + comp_kg + nr_kg - fin_kg

                        # Desviación = Real - Recetario
                        desv_kg  = real_ut - uso_kg
                        desv_pct = desv_kg / uso_kg if uso_kg > 0 else 0
                        precio_u = costo_u / max(comp_kg, 0.001)
                        costo_desv = desv_kg * precio_u

                        if ini_kg == 0 and fin_kg == 0 and uso_kg == 0 and comp_kg == 0:
                            continue
                        filas_ctrl.append((prod, costo_u, ini_kg, fin_kg, comp_kg, real_ut, uso_kg, desv_kg, desv_pct, costo_desv))

                    if not filas_ctrl:
                        continue

                    um_label = 'LT' if cat_nombre == 'BAR' else 'UN' if cat_nombre == 'PAN' else 'KG'
                    ctrl_rows = ''
                    for f in filas_ctrl:
                        prod, ct, ini_v, fin_, cp, ru, uc, dk, dp, cd = f
                        color_desv = '#e84545' if dp > 0.1 else '#e89c45' if dp > 0.05 else '#4caf7d'
                        ctrl_rows += (
                            f'<tr style="border-bottom:1px solid #1a1a1a">'
                            f'<td style="padding:6px 10px;color:#ccc;font-size:0.75rem">{prod}</td>'
                            f'<td style="padding:6px 10px;text-align:right;color:#888;font-size:0.73rem">{fmt_clp(ct)}</td>'
                            f'<td style="padding:6px 10px;text-align:right;color:#777;font-size:0.73rem">{fmt_kg(ini_v)}</td>'
                            f'<td style="padding:6px 10px;text-align:right;color:#777;font-size:0.73rem">{fmt_kg(fin_)}</td>'
                            f'<td style="padding:6px 10px;text-align:right;color:#aaa;font-size:0.73rem">{fmt_kg(cp)}</td>'
                            f'<td style="padding:6px 10px;text-align:right;color:#aaa;font-size:0.73rem">{fmt_kg(ru)}</td>'
                            f'<td style="padding:6px 10px;text-align:right;color:#888;font-size:0.73rem">{fmt_kg(uc)}</td>'
                            f'<td style="padding:6px 10px;text-align:right;color:{color_desv};font-size:0.73rem">{fmt_kg(dk)}</td>'
                            f'<td style="padding:6px 10px;text-align:right;color:{color_desv};font-size:0.73rem">{fmt_pct(dp)}</td>'
                            f'<td style="padding:6px 10px;text-align:right;color:{"#e84545" if cd>0 else "#4caf7d"};font-size:0.73rem">{fmt_clp(cd)}</td>'
                            f'<td style="padding:6px 10px;text-align:right;color:#555;font-size:0.73rem">0</td>'
                            f'</tr>'
                        )
                    # Fila total
                    tot_ct  = sum(f[1] for f in filas_ctrl)
                    tot_ini = sum(f[2] for f in filas_ctrl)
                    tot_fin = sum(f[3] for f in filas_ctrl)
                    tot_cp  = sum(f[4] for f in filas_ctrl)
                    tot_ru  = sum(f[5] for f in filas_ctrl)
                    tot_uc  = sum(f[6] for f in filas_ctrl)
                    tot_dk  = sum(f[7] for f in filas_ctrl)
                    tot_dp  = tot_dk / tot_uc if tot_uc > 0 else 0
                    tot_cd  = sum(f[9] for f in filas_ctrl)
                    ctrl_rows += (
                        f'<tr style="background:#111;border-top:1px solid #2a2a2a">'
                        f'<td style="padding:6px 10px;color:#d4a853;font-weight:600;font-size:0.75rem">TOTAL</td>'
                        f'<td style="padding:6px 10px;text-align:right;color:#d4a853;font-weight:600;font-size:0.73rem">{fmt_clp(tot_ct)}</td>'
                        f'<td style="padding:6px 10px;text-align:right;color:#888;font-size:0.73rem">{fmt_kg(tot_ini)}</td>'
                        f'<td style="padding:6px 10px;text-align:right;color:#888;font-size:0.73rem">{fmt_kg(tot_fin)}</td>'
                        f'<td style="padding:6px 10px;text-align:right;color:#aaa;font-size:0.73rem">{fmt_kg(tot_cp)}</td>'
                        f'<td style="padding:6px 10px;text-align:right;color:#aaa;font-size:0.73rem">{fmt_kg(tot_ru)}</td>'
                        f'<td style="padding:6px 10px;text-align:right;color:#888;font-size:0.73rem">{fmt_kg(tot_uc)}</td>'
                        f'<td style="padding:6px 10px;text-align:right;color:#d4a853;font-weight:600;font-size:0.73rem">{fmt_kg(tot_dk)}</td>'
                        f'<td style="padding:6px 10px;text-align:right;color:#d4a853;font-weight:600;font-size:0.73rem">{fmt_pct(tot_dp)}</td>'
                        f'<td style="padding:6px 10px;text-align:right;color:#d4a853;font-weight:600;font-size:0.73rem">{fmt_clp(tot_cd)}</td>'
                        f'<td style="padding:6px 10px;text-align:right;color:#555;font-size:0.73rem">0</td></tr>'
                    )
                    st.markdown(
                        f'<div style="margin-top:0.8rem;border:1px solid #1e1e1e;border-radius:10px;overflow:hidden;background:#0d0d0d">'
                        f'<div style="background:#111;padding:8px 12px;font-size:0.68rem;text-transform:uppercase;letter-spacing:0.1em;color:#d4a853;font-weight:600">CONTROL {cat_nombre}</div>'
                        f'<div style="overflow-x:auto"><table style="width:100%;border-collapse:collapse;font-family:DM Sans,sans-serif">'
                        f'<thead><tr style="background:#0d0d0d">'
                        f'<th style="{hs};text-align:left">PRODUCTO</th>'
                        f'<th style="{hs};text-align:right">COMPRA $</th>'
                        f'<th style="{hs};text-align:right">INV.INI {um_label}</th>'
                        f'<th style="{hs};text-align:right">INV.FIN {um_label}</th>'
                        f'<th style="{hs};text-align:right">COMPRAS {um_label}</th>'
                        f'<th style="{hs};text-align:right">REAL UT. {um_label}</th>'
                        f'<th style="{hs};text-align:right">REC. {um_label}</th>'
                        f'<th style="{hs2};text-align:right">DESV. {um_label}</th>'
                        f'<th style="{hs2};text-align:right">DESV %</th>'
                        f'<th style="{hsr};text-align:right">COSTO DESV.</th>'
                        f'<th style="{hs};text-align:right">MERMA</th>'
                        f'</tr></thead><tbody>{ctrl_rows}</tbody></table></div></div>',
                        unsafe_allow_html=True)

                # ── Botón imprimir ────────────────────────────────
                st.markdown("---")
                if st.button("🖨️ Preparar para imprimir", key=f"btn_print_{local_show}"):
                    st.info("Usa Ctrl+P (o Cmd+P en Mac) para imprimir o guardar como PDF. El informe está optimizado para impresión.")

# ============================================================
# MÓDULO: AUDITOR DE CATEGORÍAS
# ============================================================
elif modulo.startswith("🔬"):
    from datetime import date as _date2

    st.markdown("""
    <div style="margin-bottom:1.5rem">
        <div style="font-size:0.72rem;text-transform:uppercase;letter-spacing:0.12em;color:#555;margin-bottom:4px">Herramientas</div>
        <div style="font-family:'DM Serif Display',serif;font-size:2rem;color:#f0ede8;letter-spacing:-0.02em;line-height:1.1">
            🔬 Auditor de Categorías
        </div>
        <div style="width:40px;height:2px;background:#d4a853;margin-top:8px;border-radius:2px"></div>
    </div>
    """, unsafe_allow_html=True)

    st.markdown("""<div class='info-box'>
    Revisa y corrige el mapeo de productos a <b>Categoría Control</b>. 
    Agrupa por nombre de producto (igual que el auditor MUC). 
    Filtra por SKU, nombre, categoría o rango de fechas.
    </div>""", unsafe_allow_html=True)

    # ── Filtros ─────────────────────────────────────────────────
    fa1, fa2, fa3, fa4 = st.columns([2, 1.2, 1.2, 2])
    with fa1:
        busq_ac = st.text_input("🔍 Buscar nombre o SKU", key="ac_busq", placeholder="ej: POSTA, AL-CA-010...")
    with fa2:
        fi_ac = st.date_input("Desde", key="ac_fi", value=f_inicio)
    with fa3:
        ff_ac = st.date_input("Hasta", key="ac_ff", value=f_fin)
    with fa4:
        cats_q = run_query("SELECT DISTINCT categoria_producto FROM compras WHERE categoria_producto IS NOT NULL ORDER BY 1")
        cats_ac = ["Todas"] + cats_q['categoria_producto'].tolist() if not cats_q.empty else ["Todas"]
        cat_ac = st.selectbox("Categoría", cats_ac, key="ac_cat")

    fa5, fa6 = st.columns([2, 2])
    with fa5:
        solo_sin_ctrl = st.toggle("⚠️ Solo sin Categoría Control", key="ac_solo_sin")
    with fa6:
        st.caption("Haz clic en un grupo para editar su Categoría Control")

    if st.button("🔎 Buscar", key="btn_ac_buscar", type="primary"):
        with st.spinner("Cargando datos..."):
            # Construir query con filtros
            where = ["c.fecha_dte::date BETWEEN :fi AND :ff"]
            params_ac = {'fi': str(fi_ac), 'ff': str(ff_ac)}

            if busq_ac.strip():
                where.append("(UPPER(c.nombre_producto) LIKE :busq OR UPPER(c.sku) LIKE :busq)")
                params_ac['busq'] = f'%{busq_ac.strip().upper()}%'

            if cat_ac != "Todas":
                where.append("c.categoria_producto = :cat")
                params_ac['cat'] = cat_ac

            where_sql = " AND ".join(where)

            df_ac = run_query(f"""
                SELECT
                    c.sku,
                    c.nombre_producto,
                    c.categoria_producto,
                    c.subcat,
                    cn.categoria_control,
                    cn.categoria as cat_clasificada,
                    COUNT(*)           as n_registros,
                    SUM(c.cant_conv)   as cant_total,
                    SUM(c.costo_realfinal) as costo_total,
                    MIN(c.fecha_dte::date) as primera_compra,
                    MAX(c.fecha_dte::date) as ultima_compra
                FROM compras c
                LEFT JOIN clas_nomb_prod cn
                       ON UPPER(c.nombre_producto) = UPPER(cn.nombre_producto)
                WHERE {where_sql}
                GROUP BY c.sku, c.nombre_producto, c.categoria_producto, c.subcat,
                         cn.categoria_control, cn.categoria
                ORDER BY c.nombre_producto, c.sku
            """, params_ac)

            if solo_sin_ctrl:
                df_ac = df_ac[df_ac['categoria_control'].isna() | (df_ac['categoria_control'] == '')]

            st.session_state['ac_data'] = df_ac
            st.session_state['ac_params'] = params_ac

    # ── Mostrar resultados ────────────────────────────────────────
    if 'ac_data' in st.session_state:
        df_ac = st.session_state['ac_data']

        if df_ac.empty:
            st.info("No se encontraron productos con los filtros aplicados.")
        else:
            # Métricas resumen
            n_prods   = df_ac['nombre_producto'].nunique()
            n_skus    = df_ac['sku'].nunique()
            sin_ctrl  = df_ac['categoria_control'].isna().sum()
            con_ctrl  = df_ac['categoria_control'].notna().sum()

            m1, m2, m3, m4 = st.columns(4)
            m1.metric("Productos únicos", f"{n_prods:,}")
            m2.metric("SKUs únicos", f"{n_skus:,}")
            m3.metric("✅ Con cat. control", f"{con_ctrl:,}", f"{con_ctrl/len(df_ac)*100:.0f}%")
            m4.metric("⚠️ Sin cat. control", f"{sin_ctrl:,}", f"-{sin_ctrl/len(df_ac)*100:.0f}%")

            st.markdown("---")

            # Selector de grupos (igual que auditor MUC)
            opciones_ac = []
            for _, r in df_ac.iterrows():
                ctrl  = r.get('categoria_control','') or '⚠️ SIN CONTROL'
                ctrl  = ctrl if ctrl and str(ctrl) != 'nan' else '⚠️ SIN CONTROL'
                label = f"{r['sku']} | {r['nombre_producto'][:40]} | Ctrl: {ctrl} | Cat: {r.get('categoria_producto','')}"
                opciones_ac.append(label)

            sel_ac = st.multiselect(
                "Selecciona productos para reclasificar",
                opciones_ac,
                key="ac_multisel",
                placeholder="Busca por SKU, nombre o categoría control..."
            )

            # Panel de edición si hay selección
            if sel_ac:
                sel_idx = [opciones_ac.index(l) for l in sel_ac if l in opciones_ac]
                df_sel  = df_ac.iloc[sel_idx].reset_index(drop=True)

                st.markdown(f"**⚙️ {len(df_sel)} producto(s) seleccionado(s)**")

                ed1, ed2, ed3 = st.columns([2, 2, 1])
                with ed1:
                    # Opciones de categoría control
                    ctrl_opts = ['POSTA','FILETE','PLATEADA','LOMO LISO','LOMO VETADO','GRASA DE WAGYU',
                                 'PECHUGA DE POLLO','COSTILLAS','CHULETA KASSLER','LOMO DE CENTRO',
                                 'PERNIL','JAMÓN','TOCINO AHUMADO','PANCETA LAMINADA',
                                 'PALTA','TOMATE','LECHUGA',
                                 'FILETE SALMON','SALMON SLICE LAMINADO','CAMARON','CAMARON APANADO',
                                 'ATUN','LOCOS','ERIZOS',
                                 'QUESO RANCO','QUESO CHEDDAR','QUESO PARMESANO','PAPAS FRITAS',
                                 'PAN','SCHOP','JUGOS','ACEITE FREIR','ACEITE MAYONESA',
                                 'ACEITE DE OLIVA','ACEITE SESAMO','ACETO BALSAMICO','LIMÓN',
                                 'HIELO','(sin categoría control)']
                    nueva_ctrl = st.selectbox("Nueva Categoría Control", ctrl_opts, key="ac_nueva_ctrl")
                with ed2:
                    cat_opts2 = ['ALIMENTOS','VERDURAS','BAR','ART. LIMPIEZA','DESECHABLES','ADMINISTRACION']
                    nueva_cat = st.selectbox("Nueva Categoría", cat_opts2, key="ac_nueva_cat")
                with ed3:
                    st.markdown("<div style='height:28px'></div>", unsafe_allow_html=True)
                    if st.button("💾 Aplicar", key="btn_ac_apply", use_container_width=True):
                        try:
                            engine = get_engine()
                            ctrl_val = None if nueva_ctrl == '(sin categoría control)' else nueva_ctrl

                            # Actualizar clas_nomb_prod para cada nombre de producto seleccionado
                            nombres_sel = df_sel['nombre_producto'].unique().tolist()
                            with engine.connect() as conn:
                                for nombre in nombres_sel:
                                    # Verificar si ya existe en la tabla
                                    existe = pd.read_sql(
                                        text("SELECT COUNT(*) as n FROM clas_nomb_prod WHERE UPPER(nombre_producto)=UPPER(:n)"),
                                        conn, params={'n': nombre})
                                    if existe['n'].iloc[0] > 0:
                                        conn.execute(text(
                                            "UPDATE clas_nomb_prod SET categoria_control=:ctrl, categoria=:cat "
                                            "WHERE UPPER(nombre_producto)=UPPER(:n)"),
                                            {'ctrl': ctrl_val, 'cat': nueva_cat, 'n': nombre})
                                    else:
                                        conn.execute(text(
                                            "INSERT INTO clas_nomb_prod (nombre_producto, categoria_control, categoria) "
                                            "VALUES (:n, :ctrl, :cat)"),
                                            {'n': nombre, 'ctrl': ctrl_val, 'cat': nueva_cat})
                                conn.commit()

                            st.success(f"✅ {len(nombres_sel)} producto(s) actualizados en clasificación")
                            # Limpiar cache para que el informe tome los nuevos datos
                            if 'ic_data' in st.session_state:
                                del st.session_state['ic_data']
                            del st.session_state['ac_data']
                            st.rerun()
                        except Exception as e:
                            st.error(f"Error: {e}")
                            st.exception(e)

            # ── Tabla HTML ───────────────────────────────────────
            st.markdown("---")
            hs_ac = 'padding:8px 12px;font-size:0.67rem;text-transform:uppercase;letter-spacing:0.09em;font-weight:600;color:#444;border-bottom:1px solid #2a2a2a'

            rows_ac = ''
            for i, (_, r) in enumerate(df_ac.iterrows()):
                ctrl = r.get('categoria_control','') or ''
                ctrl = ctrl if ctrl and str(ctrl) != 'nan' else ''
                tiene_ctrl = bool(ctrl)
                bg = '#0d0d0d' if i % 2 == 0 else '#111'
                badge_ctrl = (
                    f'<span style="background:#1a3a2a;color:#4caf7d;padding:2px 8px;border-radius:10px;font-size:0.72rem;font-weight:600">{ctrl}</span>'
                    if tiene_ctrl else
                    '<span style="background:#3a1a1a;color:#e84545;padding:2px 7px;border-radius:10px;font-size:0.71rem">⚠️ sin control</span>'
                )
                costo_f = f"${float(r.get('costo_total',0) or 0):,.0f}"
                cant_f  = f"{float(r.get('cant_total',0) or 0):,.2f}"
                rows_ac += (
                    f'<tr style="border-bottom:1px solid #1a1a1a;background:{bg}">'
                    f'<td style="padding:8px 12px;color:#666;font-family:monospace;font-size:0.72rem">{r.get("sku","")}</td>'
                    f'<td style="padding:8px 12px;color:#e8e4de;font-size:0.8rem;font-weight:500">{r.get("nombre_producto","")}</td>'
                    f'<td style="padding:8px 12px;color:#888;font-size:0.75rem">{r.get("categoria_producto","")}</td>'
                    f'<td style="padding:8px 12px;color:#666;font-size:0.75rem">{r.get("subcat","")}</td>'
                    f'<td style="padding:8px 12px">{badge_ctrl}</td>'
                    f'<td style="padding:8px 12px;text-align:right;color:#aaa;font-size:0.75rem">{r.get("n_registros",0):,}</td>'
                    f'<td style="padding:8px 12px;text-align:right;color:#888;font-size:0.75rem">{cant_f}</td>'
                    f'<td style="padding:8px 12px;text-align:right;color:#d4a853;font-size:0.75rem">{costo_f}</td>'
                    f'<td style="padding:8px 12px;text-align:center;color:#555;font-size:0.71rem">{str(r.get("ultima_compra",""))[:10]}</td>'
                    f'</tr>'
                )

            tabla_ac = (
                '<div style="overflow-x:auto;border-radius:14px;border:1px solid #1e1e1e;margin-top:0.5rem;background:#0d0d0d">'
                '<table style="width:100%;border-collapse:collapse;font-family:DM Sans,sans-serif;font-size:0.82rem">'
                '<thead><tr style="background:#111">'
                f'<th style="{hs_ac};text-align:left">SKU</th>'
                f'<th style="{hs_ac};text-align:left">Producto</th>'
                f'<th style="{hs_ac};text-align:left">Categoría</th>'
                f'<th style="{hs_ac};text-align:left">Subcat</th>'
                f'<th style="{hs_ac};text-align:left">Cat. Control</th>'
                f'<th style="{hs_ac};text-align:right"># Reg.</th>'
                f'<th style="{hs_ac};text-align:right">Cant. Conv.</th>'
                f'<th style="{hs_ac};text-align:right">Costo Total</th>'
                f'<th style="{hs_ac};text-align:center">Última Compra</th>'
                '</tr></thead>'
                f'<tbody>{rows_ac}</tbody></table></div>'
            )
            st.markdown(tabla_ac, unsafe_allow_html=True)

            # Descarga Excel
            st.markdown("<div style='height:8px'></div>", unsafe_allow_html=True)
            buf_ac = io.BytesIO()
            cols_xl = ['sku','nombre_producto','categoria_producto','subcat',
                       'categoria_control','n_registros','cant_total','costo_total',
                       'primera_compra','ultima_compra']
            cols_xl = [c for c in cols_xl if c in df_ac.columns]
            with pd.ExcelWriter(buf_ac, engine='openpyxl') as w:
                df_ac[cols_xl].to_excel(w, sheet_name='Clasificacion', index=False)
            st.download_button("📥 Exportar clasificación", buf_ac.getvalue(),
                               "Auditoria_Categorias.xlsx", use_container_width=False)

# ============================================================
# MÓDULO: TENDENCIAS BAR
# ============================================================
# ============================================================
# MÓDULO: TENDENCIAS BAR
# ============================================================
if modulo.startswith("🍹"):
    st.markdown("""
    <div style="margin-bottom:1.5rem">
        <div style="font-size:0.72rem;text-transform:uppercase;letter-spacing:0.12em;color:#555;margin-bottom:4px">Módulo</div>
        <div style="font-family:'DM Serif Display',serif;font-size:2rem;color:#f0ede8;letter-spacing:-0.02em;line-height:1.1">
            🍹 Tendencias Bar
        </div>
        <div style="font-size:0.82rem;color:#666;margin-top:4px">Análisis completo de compras · Todos los productos · Resumen + Detalle</div>
    </div>
    """, unsafe_allow_html=True)

    # ── Helpers ───────────────────────────────────────────────
    def _bar_local_filter(local):
        if local and local != "Todos":
            return f"AND UPPER(local) = UPPER('{local}')"
        return ""

    def _mes_lbl(y, m):
        meses = ["Ene","Feb","Mar","Abr","May","Jun","Jul","Ago","Sep","Oct","Nov","Dic"]
        return f"{meses[m-1]} {str(y)[-2:]}"

    # ── Controles globales del módulo ─────────────────────────
    ca, cb, cc, cd = st.columns([2, 3, 2, 2])
    with ca:
        bar_local = st.selectbox("Local", ["Todos"] + [l for l in get_locales() if l != "Todos"], key="bar_local")
    with cb:
        bar_texto = st.text_input("Filtrar SKU / Producto", key="bar_texto", placeholder="Ej: RON, VODKA, CERVEZA…")
    with cc:
        st.markdown("<div style='height:1.6rem'></div>", unsafe_allow_html=True)
        if st.button("▶ Cargar análisis Bar", key="bar_run", use_container_width=True):
            for k in ['bar_resumen_df','bar_vol_df','bar_gasto_df','bar_freq_df']:
                st.session_state.pop(k, None)
    with cd:
        st.markdown("<div style='height:1.6rem'></div>", unsafe_allow_html=True)
        exportar_base = st.button("📥 Exportar Base", key="bar_export_base", use_container_width=True)

    filtro_local_bar = _bar_local_filter(bar_local)

    # ── Export base mensual/semanal ───────────────────────────
    if exportar_base:
        sql_base_mensual = f"""
            SELECT
                local,
                sku,
                MODE() WITHIN GROUP (ORDER BY nombre_producto) AS producto,
                MODE() WITHIN GROUP (ORDER BY nombre_proveedor) AS proveedor,
                TO_CHAR(DATE_TRUNC('month', fecha_dte::timestamp), 'YYYY-MM') AS mes,
                MAX(conversion)                                AS conversion,
                MAX(formato)                                   AS formato,
                ROUND(SUM(cant_conv)::numeric, 2)              AS compra_q,
                ROUND(SUM(costo_realfinal)::numeric, 0)        AS compra_pesos
            FROM compras
            WHERE UPPER(categoria_producto) LIKE '%BAR%'
              AND cant_conv > 0
              AND costo_realfinal > 0
              AND tipo_dte != '61'
              {filtro_local_bar}
            GROUP BY local, sku, DATE_TRUNC('month', fecha_dte::timestamp)
            ORDER BY local, sku, mes
        """
        sql_base_semanal = f"""
            SELECT
                local,
                sku,
                MODE() WITHIN GROUP (ORDER BY nombre_producto) AS producto,
                MODE() WITHIN GROUP (ORDER BY nombre_proveedor) AS proveedor,
                TO_CHAR(DATE_TRUNC('week', fecha_dte::timestamp), 'IYYY-IW') AS semana,
                DATE_TRUNC('week', fecha_dte::timestamp)::date  AS inicio_semana,
                MAX(conversion)                                 AS conversion,
                MAX(formato)                                    AS formato,
                ROUND(SUM(cant_conv)::numeric, 2)               AS compra_q,
                ROUND(SUM(costo_realfinal)::numeric, 0)         AS compra_pesos
            FROM compras
            WHERE UPPER(categoria_producto) LIKE '%BAR%'
              AND cant_conv > 0
              AND costo_realfinal > 0
              AND tipo_dte != '61'
              {filtro_local_bar}
            GROUP BY local, sku, DATE_TRUNC('week', fecha_dte::timestamp)
            ORDER BY local, sku, semana
        """
        with st.spinner("Generando base de datos…"):
            try:
                df_base_m = run_query(sql_base_mensual)
                df_base_s = run_query(sql_base_semanal)
                df_base_m.columns = ['Local','SKU','Producto','Proveedor','Mes','Conversion','Formato','Compra Q','Compra $']
                df_base_s.columns = ['Local','SKU','Producto','Proveedor','Semana','Inicio Semana','Conversion','Formato','Compra Q','Compra $']
                buf_base = io.BytesIO()
                with pd.ExcelWriter(buf_base, engine='openpyxl') as w:
                    df_base_m.to_excel(w, sheet_name='Mensual', index=False)
                    df_base_s.to_excel(w, sheet_name='Semanal', index=False)
                st.download_button(
                    "⬇️ Descargar Excel",
                    buf_base.getvalue(),
                    file_name=f"Bar_Base_Compras{'_'+bar_local if bar_local != 'Todos' else ''}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    key="bar_export_dl"
                )
            except Exception as e:
                st.error(f"Error generando base: {e}")

    # ════════════════════════════════════════════════════════════
    # QUERY RESUMEN — Una fila por SKU con todos los indicadores
    # ════════════════════════════════════════════════════════════
    sql_resumen = f"""
        WITH nombres AS (
            SELECT sku,
                   MODE() WITHIN GROUP (ORDER BY nombre_producto) AS nombre,
                   MODE() WITHIN GROUP (ORDER BY nombre_proveedor) AS proveedor
            FROM compras
            WHERE UPPER(categoria_producto) LIKE '%BAR%'
              AND tipo_dte != '61'
            GROUP BY sku
        ),
        base AS (
            SELECT
                sku,
                DATE_TRUNC('month', fecha_dte::timestamp)::date           AS mes,
                fecha_dte::timestamp::date                                AS fecha_compra,
                SUM(cant_conv)                                            AS vol_mes,
                SUM(costo_realfinal)                                      AS gasto_mes,
                AVG(muc)                                                  AS muc_mes
            FROM compras
            WHERE UPPER(categoria_producto) LIKE '%BAR%'
              AND cant_conv > 0
              AND costo_realfinal > 0
              AND tipo_dte != '61'
              {filtro_local_bar}
            GROUP BY sku, DATE_TRUNC('month', fecha_dte::timestamp), fecha_dte::timestamp::date
        ),
        por_mes AS (
            SELECT sku,
                   mes,
                   SUM(vol_mes)   AS vol_total_mes,
                   SUM(gasto_mes) AS gasto_total_mes,
                   AVG(muc_mes)   AS muc_promedio_mes
            FROM base
            GROUP BY sku, mes
        ),
        stats AS (
            SELECT
                sku,
                COUNT(DISTINCT mes)                                        AS n_meses,
                ROUND(AVG(vol_total_mes)::numeric, 2)                     AS vol_prom_mes,
                ROUND(STDDEV(vol_total_mes)::numeric, 2)                  AS vol_std,
                ROUND(SUM(vol_total_mes)::numeric, 2)                     AS vol_total,
                ROUND(AVG(gasto_total_mes)::numeric, 0)                   AS gasto_prom_mes,
                ROUND(SUM(gasto_total_mes)::numeric, 0)                   AS gasto_total,
                ROUND(AVG(muc_promedio_mes)::numeric, 4)                  AS muc_actual,
                -- Tendencia: promedio últimos 2 meses vs 2 anteriores
                ROUND(AVG(CASE WHEN mes >= (SELECT MAX(mes) - INTERVAL '1 month' FROM por_mes p2 WHERE p2.sku = por_mes.sku)
                               THEN vol_total_mes END)::numeric, 2)       AS vol_ult2m,
                ROUND(AVG(CASE WHEN mes < (SELECT MAX(mes) - INTERVAL '1 month' FROM por_mes p2 WHERE p2.sku = por_mes.sku)
                               AND mes >= (SELECT MAX(mes) - INTERVAL '3 months' FROM por_mes p2 WHERE p2.sku = por_mes.sku)
                               THEN vol_total_mes END)::numeric, 2)       AS vol_ant2m
            FROM por_mes
            GROUP BY sku
        ),
        ultima_compra AS (
            SELECT sku,
                   MAX(fecha_compra) AS ultima_compra,
                   (CURRENT_DATE - MAX(fecha_compra))  AS dias_sin_comprar
            FROM base
            GROUP BY sku
        ),
        ciclo AS (
            SELECT sku,
                   CASE WHEN COUNT(DISTINCT fecha_compra) > 1
                        THEN ROUND((MAX(fecha_compra) - MIN(fecha_compra))::numeric
                             / NULLIF(COUNT(DISTINCT fecha_compra) - 1, 0), 1)
                        ELSE NULL END AS dias_entre_compras,
                   COUNT(DISTINCT fecha_compra) AS n_compras
            FROM base
            GROUP BY sku
        )
        SELECT
            s.sku, n.nombre, n.proveedor,
            s.n_meses, s.vol_prom_mes, s.vol_std, s.vol_total,
            s.gasto_prom_mes, s.gasto_total, s.muc_actual,
            s.vol_ult2m, s.vol_ant2m,
            CASE WHEN s.vol_ant2m > 0
                 THEN ROUND(((s.vol_ult2m - s.vol_ant2m) / s.vol_ant2m * 100)::numeric, 1)
                 ELSE NULL END                                             AS tendencia_pct,
            CASE WHEN s.vol_prom_mes > 0
                 THEN ROUND((s.vol_std / s.vol_prom_mes * 100)::numeric, 1)
                 ELSE NULL END                                             AS cv_pct,
            CASE WHEN c.dias_entre_compras IS NOT NULL AND s.vol_prom_mes > 0
                 THEN ROUND(((s.vol_prom_mes + 1.65 * COALESCE(s.vol_std, 0))
                              / 30.0 * c.dias_entre_compras)::numeric, 2)
                 ELSE NULL END                                             AS stock_seg_sugerido,
            c.dias_entre_compras,
            c.n_compras,
            u.ultima_compra,
            u.dias_sin_comprar
        FROM stats s
        JOIN nombres n      ON s.sku = n.sku
        JOIN ultima_compra u ON s.sku = u.sku
        JOIN ciclo c         ON s.sku = c.sku
        ORDER BY s.gasto_total DESC
    """

    # ── QUERY VOLUMEN PIVOT ────────────────────────────────────
    sql_vol = f"""
        SELECT
            sku,
            MODE() WITHIN GROUP (ORDER BY nombre_producto)                              AS nombre,
            DATE_TRUNC('month', fecha_dte::timestamp)::date   AS mes,
            ROUND(SUM(cant_conv)::numeric, 2)                 AS vol_total
        FROM compras
        WHERE UPPER(categoria_producto) LIKE '%BAR%'
          AND cant_conv > 0
          AND tipo_dte != '61'
          {filtro_local_bar}
        GROUP BY sku, DATE_TRUNC('month', fecha_dte::timestamp)
        ORDER BY mes, sku
    """

    # ── QUERY GASTO PIVOT ─────────────────────────────────────
    sql_gasto = f"""
        SELECT
            sku,
            MODE() WITHIN GROUP (ORDER BY nombre_producto)                              AS nombre,
            DATE_TRUNC('month', fecha_dte::timestamp)::date   AS mes,
            ROUND(SUM(costo_realfinal)::numeric, 0)           AS gasto_total
        FROM compras
        WHERE UPPER(categoria_producto) LIKE '%BAR%'
          AND costo_realfinal > 0
          AND tipo_dte != '61'
          {filtro_local_bar}
        GROUP BY sku, DATE_TRUNC('month', fecha_dte::timestamp)
        ORDER BY mes, sku
    """

    # ── QUERY FRECUENCIA ──────────────────────────────────────
    sql_freq = f"""
        WITH fechas AS (
            SELECT sku, MODE() WITHIN GROUP (ORDER BY nombre_producto) AS nombre, local,
                   fecha_dte::timestamp::date AS fecha_compra,
                   SUM(cant_conv)             AS vol_dia,
                   SUM(costo_realfinal)       AS gasto_dia
            FROM compras
            WHERE UPPER(categoria_producto) LIKE '%BAR%'
              AND cant_conv > 0
              AND tipo_dte != '61'
              {filtro_local_bar}
            GROUP BY sku, local, fecha_dte::timestamp::date
        )
        SELECT
            sku, MAX(nombre) AS nombre, local,
            COUNT(*)                                                          AS n_compras,
            MIN(fecha_compra)                                                 AS primera_compra,
            MAX(fecha_compra)                                                 AS ultima_compra,
            (CURRENT_DATE - MAX(fecha_compra))                                AS dias_sin_comprar,
            CASE WHEN COUNT(*) > 1
                 THEN ROUND((MAX(fecha_compra) - MIN(fecha_compra))::numeric
                      / NULLIF(COUNT(*) - 1, 0), 1)
                 ELSE NULL END                                                AS dias_entre_compras,
            ROUND(AVG(vol_dia)::numeric, 2)                                  AS vol_promedio,
            ROUND(AVG(gasto_dia)::numeric, 0)                                AS gasto_promedio,
            ROUND(STDDEV(vol_dia)::numeric, 2)                               AS vol_std
        FROM fechas
        GROUP BY sku, local
        ORDER BY dias_entre_compras ASC NULLS LAST, sku
    """

    # ── Carga automática ──────────────────────────────────────
    if 'bar_resumen_df' not in st.session_state:
        with st.spinner("Cargando análisis completo del bar…"):
            try:
                st.session_state['bar_resumen_df'] = run_query(sql_resumen)
                st.session_state['bar_vol_df']     = run_query(sql_vol)
                st.session_state['bar_gasto_df']   = run_query(sql_gasto)
                st.session_state['bar_freq_df']    = run_query(sql_freq)
            except Exception as e:
                st.error(f"Error cargando datos bar: {e}")
                st.stop()

    df_res  = st.session_state.get('bar_resumen_df', pd.DataFrame())
    df_vol  = st.session_state.get('bar_vol_df',     pd.DataFrame())
    df_gasto= st.session_state.get('bar_gasto_df',   pd.DataFrame())
    df_freq = st.session_state.get('bar_freq_df',    pd.DataFrame())

    # ── Filtro texto (aplica a todos los tabs) ────────────────
    def _filtrar_texto(df, texto, cols=('sku','nombre')):
        if texto and not df.empty:
            mask = pd.Series([False]*len(df), index=df.index)
            for c in cols:
                if c in df.columns:
                    mask |= df[c].astype(str).str.contains(texto, case=False, na=False)
            return df[mask]
        return df

    df_res_f   = _filtrar_texto(df_res,   bar_texto)
    df_vol_f   = _filtrar_texto(df_vol,   bar_texto)
    df_gasto_f = _filtrar_texto(df_gasto, bar_texto)
    df_freq_f  = _filtrar_texto(df_freq,  bar_texto)

    # ════════════════════════════════════════════════════════════
    # TABS
    # ════════════════════════════════════════════════════════════
    tb_res, tb_vol, tb_gasto, tb_freq = st.tabs([
        "📋 Resumen Ejecutivo",
        "📦 Volumen por Mes",
        "💰 Gasto por Mes",
        "🔄 Frecuencia de Compra"
    ])

    # ════════════════════════════════════════════════════════════
    # TAB RESUMEN EJECUTIVO
    # ════════════════════════════════════════════════════════════
    with tb_res:
        st.markdown("<div style='height:0.5rem'></div>", unsafe_allow_html=True)

        if df_res_f is not None and not df_res_f.empty:
            # KPIs globales
            k1, k2, k3, k4, k5 = st.columns(5)
            k1.metric("SKUs Bar", str(len(df_res_f)))
            k2.metric("Gasto Total", f"${df_res_f['gasto_total'].sum():,.0f}")
            k3.metric("Vol. Total (kg/lt)", f"{df_res_f['vol_total'].sum():,.1f}")
            k4.metric("Prom. días entre compras",
                      f"{df_res_f['dias_entre_compras'].median():.0f}d"
                      if not df_res_f['dias_entre_compras'].isna().all() else "—")
            n_urgente = (df_res_f['dias_sin_comprar'] >= df_res_f['dias_entre_compras'].fillna(999)).sum()
            k5.metric("⚠️ Próximos a pedir", str(int(n_urgente)))

            st.markdown("<div style='height:0.75rem'></div>", unsafe_allow_html=True)

            # Helpers visuales
            def tend_badge(pct):
                if pd.isna(pct):
                    return '<span style="color:#444">—</span>'
                if pct > 10:
                    return f'<span style="color:#4cdd8a;font-weight:600">▲ {pct:.1f}%</span>'
                elif pct < -10:
                    return f'<span style="color:#e84545;font-weight:600">▼ {pct:.1f}%</span>'
                else:
                    return f'<span style="color:#888">→ {pct:.1f}%</span>'

            def cv_badge(cv):
                if pd.isna(cv):
                    return '<span style="color:#444">—</span>'
                if cv <= 20:
                    return f'<span style="background:#1a3a2a;color:#4cdd8a;padding:2px 8px;border-radius:10px;font-size:0.74rem">{cv:.0f}%</span>'
                elif cv <= 50:
                    return f'<span style="background:#2a2a1a;color:#e8c14a;padding:2px 8px;border-radius:10px;font-size:0.74rem">{cv:.0f}%</span>'
                else:
                    return f'<span style="background:#2a1a1a;color:#e84545;padding:2px 8px;border-radius:10px;font-size:0.74rem">{cv:.0f}%</span>'

            def urgencia_cell(dias_sin, ciclo):
                if pd.isna(ciclo) or pd.isna(dias_sin):
                    return f'<span style="color:#555;font-size:0.78rem">{int(dias_sin) if not pd.isna(dias_sin) else "—"}d</span>'
                ratio = dias_sin / ciclo
                if ratio >= 0.9:
                    return f'<span style="background:#3a1a1a;color:#ff6b6b;padding:2px 8px;border-radius:10px;font-size:0.74rem;font-weight:700">🔴 {int(dias_sin)}d</span>'
                elif ratio >= 0.6:
                    return f'<span style="background:#2a2a1a;color:#e8c14a;padding:2px 8px;border-radius:10px;font-size:0.74rem">🟡 {int(dias_sin)}d</span>'
                else:
                    return f'<span style="color:#555;font-size:0.78rem">{int(dias_sin)}d</span>'

            hs = 'padding:10px 12px;font-size:0.67rem;text-transform:uppercase;letter-spacing:0.09em;font-weight:600;color:#444;border-bottom:1px solid #1e1e1e;white-space:nowrap'
            hdrs_r = ['SKU','Producto','Proveedor','Meses','Vol.Prom/mes','CV%','Tendencia','MUC actual','Ciclo(d)','Sin comprar','Stock Seg.','Gasto Prom/mes']
            thead_r = '<tr style="background:#111">' + ''.join(
                [f'<th style="{hs};text-align:left">{h}</th>' if i < 3
                 else f'<th style="{hs};text-align:right">{h}</th>'
                 for i, h in enumerate(hdrs_r)]
            ) + '</tr>'

            rows_r = ''
            for _, r in df_res_f.iterrows():
                rows_r += (
                    f'<tr style="border-bottom:1px solid #161616">'
                    f'<td style="padding:9px 12px;color:#666;font-family:monospace;font-size:0.74rem;white-space:nowrap">{r["sku"]}</td>'
                    f'<td style="padding:9px 12px;color:#ccc;max-width:200px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="{r["nombre"]}">{str(r["nombre"])[:38]}</td>'
                    f'<td style="padding:9px 12px;color:#777;font-size:0.78rem;white-space:nowrap;max-width:140px;overflow:hidden;text-overflow:ellipsis">{str(r["proveedor"])[:22]}</td>'
                    f'<td style="padding:9px 12px;text-align:right;color:#aaa">{int(r["n_meses"])}</td>'
                    f'<td style="padding:9px 12px;text-align:right;color:#7ab8e8;font-variant-numeric:tabular-nums">{r["vol_prom_mes"]:,.2f}</td>'
                    f'<td style="padding:9px 12px;text-align:right">{cv_badge(r["cv_pct"])}</td>'
                    f'<td style="padding:9px 12px;text-align:right">{tend_badge(r["tendencia_pct"])}</td>'
                    f'<td style="padding:9px 12px;text-align:right;color:#d4a853;font-variant-numeric:tabular-nums">{r["muc_actual"]:.4f}</td>'
                    f'<td style="padding:9px 12px;text-align:right;color:#888">'
                    + (f'{r["dias_entre_compras"]:.0f}d' if not pd.isna(r["dias_entre_compras"]) else '—') +
                    f'</td>'
                    f'<td style="padding:9px 12px;text-align:right">{urgencia_cell(r["dias_sin_comprar"], r["dias_entre_compras"])}</td>'
                    f'<td style="padding:9px 12px;text-align:right;color:#b8e8c0;font-variant-numeric:tabular-nums">'
                    + (f'{r["stock_seg_sugerido"]:,.2f}' if not pd.isna(r["stock_seg_sugerido"]) else '—') +
                    f'</td>'
                    f'<td style="padding:9px 12px;text-align:right;color:#d4a853;font-variant-numeric:tabular-nums">${r["gasto_prom_mes"]:,.0f}</td>'
                    f'</tr>'
                )

            tabla_r = (
                '<div style="overflow-x:auto;border-radius:14px;border:1px solid #1e1e1e;background:#0d0d0d">'
                '<table style="width:100%;border-collapse:collapse;font-family:DM Sans,sans-serif;font-size:0.83rem">'
                f'<thead>{thead_r}</thead><tbody>{rows_r}</tbody></table></div>'
            )
            st.markdown(tabla_r, unsafe_allow_html=True)

            st.markdown("""
            <div style='margin-top:0.75rem;font-size:0.75rem;color:#555'>
            <b style='color:#666'>CV%</b>: Coeficiente de variación mensual — 
            <span style='color:#4cdd8a'>≤20% estable</span> · 
            <span style='color:#e8c14a'>21-50% variable</span> · 
            <span style='color:#e84545'>>50% errático</span> &nbsp;|&nbsp;
            <b style='color:#666'>Tendencia</b>: últimos 2 meses vs 2 anteriores &nbsp;|&nbsp;
            <b style='color:#666'>Stock Seg.</b>: kg/lt recomendados al momento de pedir (95% nivel servicio)
            </div>
            """, unsafe_allow_html=True)

            st.markdown("<div style='height:0.75rem'></div>", unsafe_allow_html=True)
            buf_r = io.BytesIO()
            export_r = df_res_f[['sku','nombre','proveedor','n_meses','vol_prom_mes','cv_pct',
                                  'tendencia_pct','muc_actual','dias_entre_compras',
                                  'dias_sin_comprar','stock_seg_sugerido','gasto_prom_mes','gasto_total']].copy()
            export_r.columns = ['SKU','Producto','Proveedor','Meses','Vol.Prom/mes','CV%',
                                 'Tendencia%','MUC actual','Ciclo(días)',
                                 'Días sin comprar','Stock Seg.','Gasto Prom/mes','Gasto Total']
            with pd.ExcelWriter(buf_r, engine='openpyxl') as w:
                export_r.to_excel(w, sheet_name='Resumen_Bar', index=False)
            st.download_button("📥 Exportar Resumen", buf_r.getvalue(), "Bar_Resumen.xlsx")
        else:
            st.info("Presiona ▶ Cargar análisis Bar para ejecutar.")

    # ════════════════════════════════════════════════════════════
    # TAB VOLUMEN POR MES
    # ════════════════════════════════════════════════════════════
    with tb_vol:
        st.markdown("<div style='height:0.5rem'></div>", unsafe_allow_html=True)
        if df_vol_f is not None and not df_vol_f.empty:
            df_vol_f['mes_lbl'] = df_vol_f['mes'].apply(lambda x: _mes_lbl(x.year, x.month))
            pivot_v = df_vol_f.pivot_table(index=['sku','nombre'], columns='mes_lbl',
                                            values='vol_total', aggfunc='sum').reset_index()
            meses_ord = sorted(df_vol_f['mes'].unique())
            col_m = [_mes_lbl(m.year, m.month) for m in meses_ord]
            col_m = [c for c in col_m if c in pivot_v.columns]
            pivot_v['_prom'] = pivot_v[col_m].mean(axis=1, skipna=True)
            pivot_v['_total'] = pivot_v[col_m].sum(axis=1, skipna=True)
            pivot_v = pivot_v.sort_values('_total', ascending=False)

            mv1, mv2, mv3 = st.columns(3)
            mv1.metric("SKUs", str(len(pivot_v)))
            mv2.metric("Vol. Total (kg/lt)", f"{pivot_v['_total'].sum():,.1f}")
            mv3.metric("Meses", str(len(col_m)))
            st.markdown("<div style='height:0.5rem'></div>", unsafe_allow_html=True)

            def hm(val, rmax, rmin):
                if pd.isna(val) or rmax == rmin:
                    return '<td style="padding:9px 12px;text-align:right;color:#333">—</td>'
                ratio = (val - rmin) / (rmax - rmin)
                if ratio > 0.75:   bg,fg = '#1a3a2a','#4cdd8a'
                elif ratio > 0.4:  bg,fg = '#1a2a3a','#4aaded'
                elif ratio > 0.1:  bg,fg = '#2a2a1a','#c8b94a'
                else:              bg,fg = '#1a1a1a','#555'
                return f'<td style="padding:9px 12px;text-align:right;background:{bg};color:{fg};font-weight:600;font-variant-numeric:tabular-nums">{val:,.2f}</td>'

            hs = 'padding:10px 12px;font-size:0.67rem;text-transform:uppercase;letter-spacing:0.09em;font-weight:600;color:#444;border-bottom:1px solid #1e1e1e;white-space:nowrap'
            hdrs_v = ['SKU','Producto'] + col_m + ['Prom/mes','Total']
            thead_v = '<tr style="background:#111">' + ''.join(
                [f'<th style="{hs};text-align:left">{h}</th>' if i<2 else f'<th style="{hs};text-align:right">{h}</th>'
                 for i,h in enumerate(hdrs_v)]) + '</tr>'
            rows_v = ''
            for _, r in pivot_v.iterrows():
                vals = [r.get(c, float('nan')) for c in col_m]
                vnum = [v for v in vals if not pd.isna(v)]
                rmx,rmn = (max(vnum),min(vnum)) if vnum else (1,0)
                rows_v += (
                    f'<tr style="border-bottom:1px solid #161616">'
                    f'<td style="padding:9px 12px;color:#666;font-family:monospace;font-size:0.74rem">{r["sku"]}</td>'
                    f'<td style="padding:9px 12px;color:#ccc;max-width:220px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">{str(r["nombre"])[:44]}</td>'
                    + ''.join([hm(v,rmx,rmn) for v in vals])
                    + f'<td style="padding:9px 12px;text-align:right;color:#7ab8e8;font-variant-numeric:tabular-nums">{r["_prom"]:,.2f}</td>'
                    + f'<td style="padding:9px 12px;text-align:right;color:#d4a853;font-weight:700;font-variant-numeric:tabular-nums">{r["_total"]:,.1f}</td>'
                    f'</tr>'
                )
            st.markdown(
                '<div style="overflow-x:auto;border-radius:14px;border:1px solid #1e1e1e;background:#0d0d0d">'
                '<table style="width:100%;border-collapse:collapse;font-family:DM Sans,sans-serif;font-size:0.83rem">'
                f'<thead>{thead_v}</thead><tbody>{rows_v}</tbody></table></div>',
                unsafe_allow_html=True)
            st.markdown("<div style='height:0.75rem'></div>", unsafe_allow_html=True)
            buf_v = io.BytesIO()
            exp_v = pivot_v[['sku','nombre']+col_m+['_prom','_total']].copy()
            exp_v.columns = ['SKU','Producto']+col_m+['Prom/mes','Total']
            with pd.ExcelWriter(buf_v, engine='openpyxl') as w:
                exp_v.to_excel(w, sheet_name='Volumen_Bar', index=False)
            st.download_button("📥 Exportar Excel", buf_v.getvalue(), "Bar_Volumen.xlsx")
        else:
            st.info("Presiona ▶ Cargar análisis Bar para ejecutar.")

    # ════════════════════════════════════════════════════════════
    # TAB GASTO POR MES
    # ════════════════════════════════════════════════════════════
    with tb_gasto:
        st.markdown("<div style='height:0.5rem'></div>", unsafe_allow_html=True)
        if df_gasto_f is not None and not df_gasto_f.empty:
            df_gasto_f['mes_lbl'] = df_gasto_f['mes'].apply(lambda x: _mes_lbl(x.year, x.month))
            pivot_g = df_gasto_f.pivot_table(index=['sku','nombre'], columns='mes_lbl',
                                              values='gasto_total', aggfunc='sum').reset_index()
            meses_ord_g = sorted(df_gasto_f['mes'].unique())
            col_mg = [_mes_lbl(m.year, m.month) for m in meses_ord_g]
            col_mg = [c for c in col_mg if c in pivot_g.columns]
            pivot_g['_prom'] = pivot_g[col_mg].mean(axis=1, skipna=True)
            pivot_g['_total'] = pivot_g[col_mg].sum(axis=1, skipna=True)
            pivot_g = pivot_g.sort_values('_total', ascending=False)

            mg1, mg2, mg3 = st.columns(3)
            mg1.metric("SKUs", str(len(pivot_g)))
            mg2.metric("Gasto Total", f"${pivot_g['_total'].sum():,.0f}")
            mg3.metric("Meses", str(len(col_mg)))
            st.markdown("<div style='height:0.5rem'></div>", unsafe_allow_html=True)

            def hmg(val, rmax, rmin):
                if pd.isna(val) or rmax == rmin:
                    return '<td style="padding:9px 12px;text-align:right;color:#333">—</td>'
                ratio = (val - rmin) / (rmax - rmin)
                if ratio > 0.75:   bg,fg = '#3a1a1a','#ff8c8c'
                elif ratio > 0.4:  bg,fg = '#2a1a2a','#c87ae8'
                elif ratio > 0.1:  bg,fg = '#1a2a2a','#4ae8c8'
                else:              bg,fg = '#1a1a1a','#555'
                return f'<td style="padding:9px 12px;text-align:right;background:{bg};color:{fg};font-weight:600;font-variant-numeric:tabular-nums">${val:,.0f}</td>'

            hs = 'padding:10px 12px;font-size:0.67rem;text-transform:uppercase;letter-spacing:0.09em;font-weight:600;color:#444;border-bottom:1px solid #1e1e1e;white-space:nowrap'
            hdrs_g = ['SKU','Producto'] + col_mg + ['Prom/mes','Total']
            thead_g = '<tr style="background:#111">' + ''.join(
                [f'<th style="{hs};text-align:left">{h}</th>' if i<2 else f'<th style="{hs};text-align:right">{h}</th>'
                 for i,h in enumerate(hdrs_g)]) + '</tr>'
            rows_g = ''
            for _, r in pivot_g.iterrows():
                vals = [r.get(c, float('nan')) for c in col_mg]
                vnum = [v for v in vals if not pd.isna(v)]
                rmx,rmn = (max(vnum),min(vnum)) if vnum else (1,0)
                rows_g += (
                    f'<tr style="border-bottom:1px solid #161616">'
                    f'<td style="padding:9px 12px;color:#666;font-family:monospace;font-size:0.74rem">{r["sku"]}</td>'
                    f'<td style="padding:9px 12px;color:#ccc;max-width:220px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">{str(r["nombre"])[:44]}</td>'
                    + ''.join([hmg(v,rmx,rmn) for v in vals])
                    + f'<td style="padding:9px 12px;text-align:right;color:#c87ae8;font-variant-numeric:tabular-nums">${r["_prom"]:,.0f}</td>'
                    + f'<td style="padding:9px 12px;text-align:right;color:#d4a853;font-weight:700;font-variant-numeric:tabular-nums">${r["_total"]:,.0f}</td>'
                    f'</tr>'
                )
            st.markdown(
                '<div style="overflow-x:auto;border-radius:14px;border:1px solid #1e1e1e;background:#0d0d0d">'
                '<table style="width:100%;border-collapse:collapse;font-family:DM Sans,sans-serif;font-size:0.83rem">'
                f'<thead>{thead_g}</thead><tbody>{rows_g}</tbody></table></div>',
                unsafe_allow_html=True)
            st.markdown("<div style='height:0.75rem'></div>", unsafe_allow_html=True)
            buf_g = io.BytesIO()
            exp_g = pivot_g[['sku','nombre']+col_mg+['_prom','_total']].copy()
            exp_g.columns = ['SKU','Producto']+col_mg+['Prom/mes','Total']
            with pd.ExcelWriter(buf_g, engine='openpyxl') as w:
                exp_g.to_excel(w, sheet_name='Gasto_Bar', index=False)
            st.download_button("📥 Exportar Excel", buf_g.getvalue(), "Bar_Gasto.xlsx")
        else:
            st.info("Presiona ▶ Cargar análisis Bar para ejecutar.")

    # ════════════════════════════════════════════════════════════
    # TAB FRECUENCIA DE COMPRA
    # ════════════════════════════════════════════════════════════
    with tb_freq:
        st.markdown("<div style='height:0.5rem'></div>", unsafe_allow_html=True)
        if df_freq_f is not None and not df_freq_f.empty:
            mf1, mf2, mf3, mf4 = st.columns(4)
            med_dias = df_freq_f['dias_entre_compras'].median()
            mf1.metric("Mediana ciclo", f"{med_dias:.0f}d" if not pd.isna(med_dias) else "—")
            mf2.metric("🟢 ≤7 días", str(int((df_freq_f['dias_entre_compras']<=7).sum())))
            mf3.metric("🟡 8–15 días", str(int(((df_freq_f['dias_entre_compras']>7)&(df_freq_f['dias_entre_compras']<=15)).sum())))
            mf4.metric("🔴 >15 días", str(int((df_freq_f['dias_entre_compras']>15).sum())))
            st.markdown("<div style='height:0.5rem'></div>", unsafe_allow_html=True)

            def badge_c(dias):
                if pd.isna(dias):
                    return '<span style="color:#444;font-size:0.75rem">1 compra</span>'
                if dias<=7:
                    return f'<span style="background:#1a3a2a;color:#4cdd8a;padding:2px 9px;border-radius:12px;font-size:0.75rem;font-weight:600">cada {dias:.0f}d</span>'
                elif dias<=15:
                    return f'<span style="background:#2a2a1a;color:#e8c14a;padding:2px 9px;border-radius:12px;font-size:0.75rem;font-weight:600">cada {dias:.0f}d</span>'
                else:
                    return f'<span style="background:#2a1a1a;color:#e84545;padding:2px 9px;border-radius:12px;font-size:0.75rem;font-weight:600">cada {dias:.0f}d</span>'

            def urg_b(dias_sin, ciclo):
                if pd.isna(ciclo) or pd.isna(dias_sin): return f'<span style="color:#555">{int(dias_sin) if not pd.isna(dias_sin) else "—"}d</span>'
                ratio = dias_sin/ciclo
                if ratio>=0.9: return f'<span style="background:#3a1a1a;color:#ff6b6b;padding:2px 8px;border-radius:10px;font-size:0.74rem;font-weight:700">🔴 {int(dias_sin)}d</span>'
                elif ratio>=0.6: return f'<span style="background:#2a2a1a;color:#e8c14a;padding:2px 8px;border-radius:10px;font-size:0.74rem">🟡 {int(dias_sin)}d</span>'
                return f'<span style="color:#555;font-size:0.78rem">{int(dias_sin)}d</span>'

            hs = 'padding:10px 12px;font-size:0.67rem;text-transform:uppercase;letter-spacing:0.09em;font-weight:600;color:#444;border-bottom:1px solid #1e1e1e;white-space:nowrap'
            hdrs_f = ['SKU','Producto','Local','# Compras','Ciclo','Sin comprar','Vol.Prom(kg/lt)','Gasto Prom $','Primera','Última']
            thead_f = '<tr style="background:#111">' + ''.join(
                [f'<th style="{hs};text-align:left">{h}</th>' if i<3 else f'<th style="{hs};text-align:right">{h}</th>'
                 for i,h in enumerate(hdrs_f)]) + '</tr>'
            rows_f = ''
            for _, r in df_freq_f.iterrows():
                rows_f += (
                    f'<tr style="border-bottom:1px solid #161616">'
                    f'<td style="padding:9px 12px;color:#666;font-family:monospace;font-size:0.74rem">{r["sku"]}</td>'
                    f'<td style="padding:9px 12px;color:#ccc;max-width:200px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">{str(r["nombre"])[:42]}</td>'
                    f'<td style="padding:9px 12px;color:#888;font-size:0.8rem">{r["local"]}</td>'
                    f'<td style="padding:9px 12px;text-align:right;color:#aaa">{int(r["n_compras"])}</td>'
                    f'<td style="padding:9px 12px;text-align:right">{badge_c(r["dias_entre_compras"])}</td>'
                    f'<td style="padding:9px 12px;text-align:right">{urg_b(r["dias_sin_comprar"], r["dias_entre_compras"])}</td>'
                    f'<td style="padding:9px 12px;text-align:right;color:#7ab8e8;font-variant-numeric:tabular-nums">{r["vol_promedio"]:,.2f}</td>'
                    f'<td style="padding:9px 12px;text-align:right;color:#d4a853;font-variant-numeric:tabular-nums">${r["gasto_promedio"]:,.0f}</td>'
                    f'<td style="padding:9px 12px;text-align:right;color:#555;font-size:0.78rem">{str(r["primera_compra"])[:10]}</td>'
                    f'<td style="padding:9px 12px;text-align:right;color:#888;font-size:0.78rem">{str(r["ultima_compra"])[:10]}</td>'
                    f'</tr>'
                )
            st.markdown(
                '<div style="overflow-x:auto;border-radius:14px;border:1px solid #1e1e1e;background:#0d0d0d">'
                '<table style="width:100%;border-collapse:collapse;font-family:DM Sans,sans-serif;font-size:0.83rem">'
                f'<thead>{thead_f}</thead><tbody>{rows_f}</tbody></table></div>',
                unsafe_allow_html=True)
            st.markdown("<div style='height:0.75rem'></div>", unsafe_allow_html=True)
            buf_f = io.BytesIO()
            with pd.ExcelWriter(buf_f, engine='openpyxl') as w:
                df_freq_f[['sku','nombre','local','n_compras','dias_entre_compras',
                            'dias_sin_comprar','vol_promedio','gasto_promedio',
                            'primera_compra','ultima_compra']].to_excel(
                    w, sheet_name='Frecuencia_Bar', index=False)
            st.download_button("📥 Exportar Excel", buf_f.getvalue(), "Bar_Frecuencia.xlsx")
        else:
            st.info("Presiona ▶ Cargar análisis Bar para ejecutar.")
