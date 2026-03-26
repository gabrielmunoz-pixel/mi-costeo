"""
mobile_adapter.py
=================
Módulo de utilidades Mobile-First para MRP Gastro.
Importa esto al inicio de app.py y llama a inject_mobile_css() justo
después de st.set_page_config().

Uso:
    from mobile_adapter import inject_mobile_css, is_mobile, metric_card, kpi_row, tabla_mobile

Compatibilidad total con Streamlit ≥ 1.32.
"""

import streamlit as st
import pandas as pd


# ─────────────────────────────────────────────────────────────────────────────
# 1. CSS GLOBAL MOBILE-FIRST
# ─────────────────────────────────────────────────────────────────────────────

def inject_mobile_css():
    """
    Inyecta el CSS de adaptación mobile.
    Llama a esta función UNA SOLA VEZ, justo después de st.set_page_config().
    """
    st.markdown("""
    <style>
    /* ══════════════════════════════════════════════════════════════
       FUENTES
    ══════════════════════════════════════════════════════════════ */
    @import url('https://fonts.googleapis.com/css2?family=DM+Sans:wght@300;400;500;600&family=DM+Serif+Display&display=swap');

    /* ══════════════════════════════════════════════════════════════
       VARIABLES
    ══════════════════════════════════════════════════════════════ */
    :root {
        --gold:      #d4a853;
        --gold-dim:  #a07830;
        --bg-0:      #0a0a0a;
        --bg-1:      #111111;
        --bg-2:      #1a1a1a;
        --bg-3:      #222222;
        --border:    #2a2a2a;
        --txt-hi:    #f0ede8;
        --txt-md:    #c8c4be;
        --txt-lo:    #666666;
        --green:     #4caf7d;
        --red:       #e84545;
        --amber:     #e89c45;
        --blue:      #4a9eda;
        --radius-sm: 8px;
        --radius-md: 12px;
        --radius-lg: 16px;
    }

    /* ══════════════════════════════════════════════════════════════
       LAYOUT BASE — aplica siempre
    ══════════════════════════════════════════════════════════════ */
    .main .block-container {
        padding-left:  1rem !important;
        padding-right: 1rem !important;
        padding-top:   1rem !important;
        max-width: 100% !important;
    }

    /* ══════════════════════════════════════════════════════════════
       SIDEBAR — ajuste de ancho y tap targets
    ══════════════════════════════════════════════════════════════ */
    section[data-testid="stSidebar"] {
        min-width: 220px !important;
        max-width: 240px !important;
    }
    section[data-testid="stSidebar"] button {
        min-height: 44px !important;   /* tap target mínimo recomendado */
        font-size: 0.9rem !important;
    }

    /* ══════════════════════════════════════════════════════════════
       INPUTS — ancho 100% en mobile, altura cómoda
    ══════════════════════════════════════════════════════════════ */
    @media (max-width: 768px) {

        /* Ocultar sidebar por defecto en mobile
           (el usuario lo abre con el hamburger nativo de Streamlit) */
        section[data-testid="stSidebar"] {
            display: none;
        }
        [data-testid="collapsedControl"] {
            display: flex !important;
        }

        /* Contenedor principal sin padding lateral en pantalla pequeña */
        .main .block-container {
            padding-left:  0.5rem !important;
            padding-right: 0.5rem !important;
            padding-top:   0.75rem !important;
        }

        /* Columnas de Streamlit → stack vertical en mobile */
        [data-testid="column"] {
            width: 100% !important;
            flex: 1 1 100% !important;
            min-width: 100% !important;
        }

        /* st.metric — más compacto y tap-friendly */
        [data-testid="stMetric"] {
            background:    var(--bg-2) !important;
            border:        1px solid var(--border) !important;
            border-radius: var(--radius-md) !important;
            padding:       0.75rem 1rem !important;
            margin-bottom: 0.5rem !important;
        }
        [data-testid="stMetricLabel"] {
            font-size: 0.7rem !important;
            color: var(--txt-lo) !important;
            text-transform: uppercase !important;
            letter-spacing: 0.08em !important;
        }
        [data-testid="stMetricValue"] {
            font-size: 1.35rem !important;
            font-weight: 600 !important;
            color: var(--txt-hi) !important;
        }

        /* st.tabs — scrollable horizontal */
        [data-testid="stTabs"] > div:first-child {
            overflow-x: auto !important;
            -webkit-overflow-scrolling: touch !important;
            scrollbar-width: none !important;
            flex-wrap: nowrap !important;
        }
        [data-testid="stTabs"] > div:first-child::-webkit-scrollbar {
            display: none !important;
        }
        button[data-baseweb="tab"] {
            min-width: fit-content !important;
            white-space: nowrap !important;
            padding: 8px 14px !important;
            font-size: 0.82rem !important;
        }

        /* Inputs 100% ancho */
        [data-testid="stTextInput"] input,
        [data-testid="stSelectbox"] > div,
        [data-testid="stDateInput"] input {
            width: 100% !important;
            min-height: 44px !important;
        }

        /* Botones — más grandes y tap-friendly */
        [data-testid="stButton"] > button {
            width: 100% !important;
            min-height: 48px !important;
            font-size: 0.95rem !important;
            border-radius: var(--radius-md) !important;
        }

        /* st.radio — vertical en mobile */
        [data-testid="stRadio"] > div {
            flex-direction: column !important;
            gap: 4px !important;
        }

        /* Tablas HTML — scroll horizontal obligatorio */
        div[data-testid="stMarkdownContainer"] > div {
            max-width: 100% !important;
            overflow-x: auto !important;
        }

        /* Título del informe — tamaño reducido */
        .informe-titulo {
            font-size: 1.4rem !important;
        }

        /* Ocultar columnas verbose en tablas mobile */
        .hide-mobile { display: none !important; }

        /* Download buttons — full width */
        [data-testid="stDownloadButton"] > button {
            width: 100% !important;
            min-height: 48px !important;
        }
    }

    /* ══════════════════════════════════════════════════════════════
       COMPONENTES CUSTOM — funcionan en desktop Y mobile
    ══════════════════════════════════════════════════════════════ */

    /* KPI Card */
    .kpi-card {
        background:    var(--bg-2);
        border:        1px solid var(--border);
        border-radius: var(--radius-md);
        padding:       1rem 1.25rem;
        margin-bottom: 0.5rem;
    }
    .kpi-label {
        font-size:      0.68rem;
        text-transform: uppercase;
        letter-spacing: 0.1em;
        color:          var(--txt-lo);
        margin-bottom:  4px;
    }
    .kpi-value {
        font-size:   1.5rem;
        font-weight: 700;
        color:       var(--txt-hi);
        font-variant-numeric: tabular-nums;
    }
    .kpi-delta {
        font-size:    0.75rem;
        margin-top:   3px;
    }
    .kpi-delta.up   { color: var(--red);   }
    .kpi-delta.down { color: var(--green); }
    .kpi-delta.neu  { color: var(--txt-lo); }

    /* Card genérica (para filas de tabla mobile) */
    .row-card {
        background:    var(--bg-1);
        border:        1px solid var(--border);
        border-radius: var(--radius-sm);
        padding:       0.85rem 1rem;
        margin-bottom: 0.4rem;
        display:       flex;
        flex-direction: column;
        gap:           6px;
    }
    .row-card-header {
        display:         flex;
        justify-content: space-between;
        align-items:     center;
    }
    .row-card-nombre {
        font-weight: 500;
        color:       var(--txt-hi);
        font-size:   0.88rem;
        flex:        1;
    }
    .row-card-sku {
        font-family: monospace;
        font-size:   0.7rem;
        color:       var(--txt-lo);
    }
    .row-card-body {
        display:               grid;
        grid-template-columns: 1fr 1fr;
        gap:                   4px 12px;
    }
    .row-card-kv {
        display:       flex;
        flex-direction: column;
    }
    .row-card-kv .k {
        font-size: 0.62rem;
        color:     var(--txt-lo);
        text-transform: uppercase;
        letter-spacing: 0.06em;
    }
    .row-card-kv .v {
        font-size:    0.82rem;
        font-weight:  500;
        color:        var(--txt-md);
        font-variant-numeric: tabular-nums;
    }

    /* Badge de margen */
    .badge-green { background:#1a3a2a; color:#4caf7d; padding:2px 8px; border-radius:12px; font-size:0.75rem; font-weight:600; }
    .badge-amber { background:#3a2a1a; color:#e89c45; padding:2px 8px; border-radius:12px; font-size:0.75rem; font-weight:600; }
    .badge-red   { background:#3a1a1a; color:#e84545; padding:2px 8px; border-radius:12px; font-size:0.75rem; font-weight:600; }
    .badge-gray  { background:#1e1e1e; color:#666;    padding:2px 8px; border-radius:12px; font-size:0.75rem; font-weight:600; }

    /* Info box */
    .info-box {
        background:    #111a11;
        border-left:   3px solid var(--gold);
        border-radius: 0 var(--radius-sm) var(--radius-sm) 0;
        padding:       0.6rem 1rem;
        font-size:     0.8rem;
        color:         var(--txt-md);
        margin-bottom: 1rem;
    }

    /* Barra de acceso rápido (Mobile Bottom Nav) */
    .mobile-bottom-nav {
        display: none;
    }
    @media (max-width: 768px) {
        .mobile-bottom-nav {
            display:          flex;
            position:         fixed;
            bottom:           0;
            left:             0;
            right:            0;
            background:       #111;
            border-top:       1px solid #2a2a2a;
            padding:          8px 0 env(safe-area-inset-bottom, 8px);
            z-index:          9999;
            justify-content:  space-around;
        }
        .mobile-bottom-nav a {
            display:        flex;
            flex-direction: column;
            align-items:    center;
            gap:            2px;
            font-size:      0.6rem;
            color:          #666;
            text-decoration: none;
            padding:        4px 8px;
        }
        .mobile-bottom-nav a.active { color: var(--gold); }
        .mobile-bottom-nav a span.icon { font-size: 1.2rem; }

        /* Empujar contenido sobre el bottom nav */
        .main .block-container {
            padding-bottom: 70px !important;
        }
    }

    /* ══════════════════════════════════════════════════════════════
       PRINT — sin cambios
    ══════════════════════════════════════════════════════════════ */
    @media print {
        section[data-testid="stSidebar"] { display: none !important; }
        .mobile-bottom-nav { display: none !important; }
    }
    </style>
    """, unsafe_allow_html=True)


# ─────────────────────────────────────────────────────────────────────────────
# 2. HELPERS DE DETECCIÓN
# ─────────────────────────────────────────────────────────────────────────────

def is_mobile() -> bool:
    """
    Heurística: si el viewport es ≤ 768px Streamlit no lo expone directamente,
    pero podemos usar st.session_state para que el usuario lo indique,
    o simplemente retornar False y dejar que el CSS haga el trabajo.
    Para lógica condicional en Python, el usuario puede forzarlo con
    ?mobile=1 en la URL o via st.query_params.
    """
    params = st.query_params
    return params.get("mobile", "0") == "1"


# ─────────────────────────────────────────────────────────────────────────────
# 3. COMPONENTE: FILA DE KPIs ADAPTATIVA
# ─────────────────────────────────────────────────────────────────────────────

def kpi_row(kpis: list[dict]):
    """
    Renderiza una fila de KPIs que en desktop usa st.columns
    y en mobile usa cards verticales apiladas.

    kpis: lista de dicts con keys:
        label (str), value (str), delta (str|None), delta_type ('up'|'down'|'neu')

    Ejemplo:
        kpi_row([
            {"label": "💰 Venta Total",  "value": "$1.234.567", "delta": None,     "delta_type": "neu"},
            {"label": "📈 MC Total",     "value": "$890.234",   "delta": "+12.3%", "delta_type": "down"},
            {"label": "🎯 Margen",       "value": "72.1%",      "delta": None,     "delta_type": "neu"},
        ])
    """
    # Renderizado con cards HTML (funciona en desktop y mobile via CSS)
    cols_html = ""
    for kpi in kpis:
        delta_html = ""
        if kpi.get("delta"):
            dt = kpi.get("delta_type", "neu")
            delta_html = f'<div class="kpi-delta {dt}">{kpi["delta"]}</div>'

        cols_html += f"""
        <div class="kpi-card">
            <div class="kpi-label">{kpi["label"]}</div>
            <div class="kpi-value">{kpi["value"]}</div>
            {delta_html}
        </div>
        """

    # Wrapper: grid en desktop, columna en mobile (via CSS)
    st.markdown(f"""
    <style>
    .kpi-row-grid {{
        display: grid;
        grid-template-columns: repeat({len(kpis)}, 1fr);
        gap: 0.75rem;
        margin-bottom: 1.25rem;
    }}
    @media (max-width: 768px) {{
        .kpi-row-grid {{
            grid-template-columns: 1fr 1fr;
        }}
    }}
    @media (max-width: 480px) {{
        .kpi-row-grid {{
            grid-template-columns: 1fr;
        }}
    }}
    </style>
    <div class="kpi-row-grid">
        {cols_html}
    </div>
    """, unsafe_allow_html=True)


# ─────────────────────────────────────────────────────────────────────────────
# 4. COMPONENTE: TABLA ADAPTATIVA (desktop table / mobile cards)
# ─────────────────────────────────────────────────────────────────────────────

def tabla_mobile(
    filas: list[dict],
    columnas_desktop: list[dict],
    columnas_mobile_header: list[str],
    columnas_mobile_body: list[str],
    color_fn=None,
    badge_fn=None,
):
    """
    Renderiza una tabla responsiva:
    - Desktop (>768px): tabla HTML clásica con overflow-x:auto
    - Mobile (≤768px):  cards verticales, una por fila

    Parámetros
    ----------
    filas : list[dict]
        Cada dict es una fila. Keys = nombres de columna.

    columnas_desktop : list[dict]
        [{"key": "nombre", "label": "Producto", "align": "left", "fmt": lambda v: v}, ...]
        - key:   key del dict fila
        - label: encabezado
        - align: "left" | "right" | "center"
        - fmt:   función opcional de formato (recibe valor, devuelve HTML string)
        - hide:  True = ocultar en mobile (clase CSS hide-mobile)

    columnas_mobile_header : list[str]
        Lista de keys que aparecen en el header de la card mobile
        (máx 2: izq y der).

    columnas_mobile_body : list[str]
        Lista de keys que aparecen en el grid 2-col del cuerpo de la card.

    color_fn : callable(fila_dict) → str (color CSS background) | None
    badge_fn : callable(key, valor) → html_str | None
    """
    hs = ("padding:10px 14px;font-size:0.68rem;text-transform:uppercase;"
          "letter-spacing:0.09em;font-weight:600;color:#444;"
          "border-bottom:1px solid #2a2a2a;white-space:nowrap")

    # ── Desktop table ────────────────────────────────────────────
    hdrs_html = ""
    for col in columnas_desktop:
        hide_cls = ' class="hide-mobile"' if col.get("hide") else ""
        align = col.get("align", "left")
        hdrs_html += f'<th style="{hs};text-align:{align}"{hide_cls}>{col["label"]}</th>'

    rows_html = ""
    for fila in filas:
        bg = color_fn(fila) if color_fn else ""
        row = f'<tr style="border-bottom:1px solid #1e1e1e;background:{bg}">'
        for col in columnas_desktop:
            hide_cls = ' class="hide-mobile"' if col.get("hide") else ""
            align = col.get("align", "left")
            val = fila.get(col["key"], "")
            fmt = col.get("fmt")
            cell = fmt(val) if fmt else str(val) if val is not None else "—"
            row += f'<td style="padding:10px 14px;text-align:{align};font-size:0.83rem"{hide_cls}>{cell}</td>'
        row += "</tr>"
        rows_html += row

    tabla_html = (
        '<div style="overflow-x:auto;border-radius:14px;border:1px solid #1e1e1e;'
        'margin-bottom:1rem;background:#0d0d0d">'
        '<table style="width:100%;border-collapse:collapse;font-family:\'DM Sans\',sans-serif">'
        f'<thead><tr style="background:#111">{hdrs_html}</tr></thead>'
        f'<tbody>{rows_html}</tbody></table></div>'
    )

    # ── Mobile cards ─────────────────────────────────────────────
    # Busca los labels de cada key
    label_map = {col["key"]: col.get("label", col["key"]) for col in columnas_desktop}

    cards_html = '<div class="mobile-cards">'
    for fila in filas:
        bg = color_fn(fila) if color_fn else "var(--bg-1)"

        # Header de la card
        header_items = columnas_mobile_header[:2]
        h_left  = str(fila.get(header_items[0], "")) if len(header_items) > 0 else ""
        h_right = str(fila.get(header_items[1], "")) if len(header_items) > 1 else ""

        # Busca el fmt para el campo derecho del header
        fmt_right = None
        for col in columnas_desktop:
            if col["key"] == (header_items[1] if len(header_items) > 1 else ""):
                fmt_right = col.get("fmt")
                break
        if fmt_right and len(header_items) > 1:
            h_right_html = fmt_right(fila.get(header_items[1], ""))
        else:
            h_right_html = h_right

        # Body de la card (grid 2 col)
        body_html = '<div class="row-card-body">'
        for key in columnas_mobile_body:
            lbl = label_map.get(key, key)
            val = fila.get(key, "")
            fmt_fn = None
            for col in columnas_desktop:
                if col["key"] == key:
                    fmt_fn = col.get("fmt")
                    break
            val_html = fmt_fn(val) if fmt_fn else (str(val) if val is not None else "—")
            body_html += (
                f'<div class="row-card-kv">'
                f'<span class="k">{lbl}</span>'
                f'<span class="v">{val_html}</span>'
                f'</div>'
            )
        body_html += "</div>"

        cards_html += (
            f'<div class="row-card" style="border-left:3px solid {bg}">'
            f'<div class="row-card-header">'
            f'<span class="row-card-nombre">{h_left}</span>'
            f'<span>{h_right_html}</span>'
            f'</div>'
            f'{body_html}'
            f'</div>'
        )
    cards_html += "</div>"

    # ── CSS que alterna entre table y cards ──────────────────────
    st.markdown(f"""
    <style>
    .mobile-cards {{ display: none; }}
    @media (max-width: 768px) {{
        .desktop-table {{ display: none !important; }}
        .mobile-cards  {{ display: block; }}
    }}
    </style>
    <div class="desktop-table">{tabla_html}</div>
    {cards_html}
    """, unsafe_allow_html=True)


# ─────────────────────────────────────────────────────────────────────────────
# 5. FILTROS COMPACTOS MOBILE
# ─────────────────────────────────────────────────────────────────────────────

def filtros_compactos(key_prefix: str, opciones_mes: list[str], opciones_cat: list[str] = None):
    """
    Renderiza los filtros de fecha/mes en 1 columna en mobile, 3 en desktop.
    Retorna (mes_base_idx, mes_comp_idx, cat_sel).
    Si opciones_cat es None, no muestra selectbox de categoría.
    """
    if opciones_cat:
        c1, c2, c3 = st.columns([1, 1, 1])
    else:
        c1, c2 = st.columns([1, 1])
        c3 = None

    with c1:
        idx_base = st.selectbox(
            "Mes muestra",
            range(len(opciones_mes)),
            format_func=lambda i: opciones_mes[i],
            key=f"{key_prefix}_base"
        )
    with c2:
        idx_comp = st.selectbox(
            "Mes comparación",
            range(len(opciones_mes)),
            format_func=lambda i: opciones_mes[i],
            index=len(opciones_mes) - 1,
            key=f"{key_prefix}_comp"
        )
    cat_sel = "Todos"
    if c3 and opciones_cat:
        with c3:
            cat_sel = st.selectbox("Categoría", opciones_cat, key=f"{key_prefix}_cat")

    return idx_base, idx_comp, cat_sel


# ─────────────────────────────────────────────────────────────────────────────
# 6. BADGE HELPERS (reutilizables)
# ─────────────────────────────────────────────────────────────────────────────

def badge_margen(val) -> str:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return '<span class="badge-gray">—</span>'
    if val >= 60:
        return f'<span class="badge-green">{val:.2f}%</span>'
    elif val >= 40:
        return f'<span class="badge-amber">{val:.2f}%</span>'
    return f'<span class="badge-red">{val:.2f}%</span>'


def badge_delta_pct(val, umbral_alto=10, umbral_bajo=3) -> str:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return '<span class="badge-gray">—</span>'
    if val > umbral_alto:
        return f'<span class="badge-red">{val:+.2f}%</span>'
    elif val > umbral_bajo:
        return f'<span class="badge-amber">{val:+.2f}%</span>'
    elif val < -umbral_bajo:
        return f'<span class="badge-green">{val:+.2f}%</span>'
    return f'<span style="color:#aaa;font-size:0.75rem">{val:+.2f}%</span>'


def fmt_dinero(val, positivo_malo=True) -> str:
    """Si positivo_malo=True, positivo = rojo (exceso gasto), negativo = verde."""
    if val is None:
        return '<span style="color:#555">—</span>'
    val = float(val)
    if positivo_malo:
        color = "#e84545" if val > 0 else "#4caf7d" if val < 0 else "#aaa"
    else:
        color = "#4caf7d" if val > 0 else "#e84545" if val < 0 else "#aaa"
    return f'<span style="color:{color};font-weight:600;font-variant-numeric:tabular-nums">${val:,.0f}</span>'
