"""
Dashboard de Ventas — VentasAnalisisDB
KPIs: Ventas por mes, categoría, top clientes, trimestre, productos, día laboral/fds.

Ejecución:
    streamlit run dashboard/app.py
"""

import sys
from pathlib import Path

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import pyodbc

sys.path.insert(0, str(Path(__file__).parent.parent))
from config import Settings

# ─── CONFIGURACIÓN DE PÁGINA ──────────────────────────────────────────
st.set_page_config(
    page_title="Dashboard de Ventas | VentasAnalisisDB",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ─── ESTILOS PERSONALIZADOS ───────────────────────────────────────────
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;600;700&display=swap');

    html, body, [class*="css"] { font-family: 'Inter', sans-serif; }

    .main { background: #0f1117; }

    /* Tarjetas KPI */
    [data-testid="metric-container"] {
        background: linear-gradient(135deg, #1e2130 0%, #252a3d 100%);
        border: 1px solid rgba(99,102,241,0.25);
        border-radius: 12px;
        padding: 1rem 1.2rem;
        box-shadow: 0 4px 20px rgba(0,0,0,0.3);
        transition: transform 0.2s ease;
    }
    [data-testid="metric-container"]:hover { transform: translateY(-2px); }
    [data-testid="metric-container"] label { color: #a5b4fc !important; font-size: 0.78rem !important; }
    [data-testid="metric-container"] [data-testid="stMetricValue"] { color: #f8fafc !important; font-size: 1.6rem !important; font-weight: 700; }

    /* Sidebar */
    [data-testid="stSidebar"] {
        background: linear-gradient(180deg, #141727 0%, #0f1117 100%);
        border-right: 1px solid rgba(99,102,241,0.2);
    }

    /* Sección header */
    .section-header {
        font-size: 1rem;
        font-weight: 600;
        color: #a5b4fc;
        letter-spacing: 0.05em;
        text-transform: uppercase;
        margin-bottom: 0.5rem;
    }

    /* Tabla */
    [data-testid="stDataFrame"] { border-radius: 8px; overflow: hidden; }

    /* Divider */
    hr { border-color: rgba(99,102,241,0.2) !important; }

    /* Botón de refresco */
    .stButton > button {
        background: linear-gradient(135deg, #6366f1, #8b5cf6);
        color: white;
        border: none;
        border-radius: 8px;
        font-weight: 600;
        padding: 0.5rem 1.5rem;
        width: 100%;
    }
    .stButton > button:hover { opacity: 0.9; }
</style>
""", unsafe_allow_html=True)

# ─── PALETA DE COLORES ────────────────────────────────────────────────
COLORS = px.colors.qualitative.Set3
ACCENT = "#6366f1"
BG_PLOT = "#1e2130"
PAPER_BG = "#1e2130"
FONT_COLOR = "#e2e8f0"
GRID_COLOR = "rgba(255,255,255,0.07)"

PLOTLY_LAYOUT = dict(
    plot_bgcolor=BG_PLOT,
    paper_bgcolor=PAPER_BG,
    font=dict(color=FONT_COLOR, family="Inter"),
    xaxis=dict(showgrid=True, gridcolor=GRID_COLOR, zeroline=False),
    yaxis=dict(showgrid=True, gridcolor=GRID_COLOR, zeroline=False),
    margin=dict(l=10, r=10, t=40, b=10),
    legend=dict(bgcolor="rgba(0,0,0,0)", font=dict(color=FONT_COLOR)),
)


# ─── CONEXIÓN Y CONSULTAS ─────────────────────────────────────────────
@st.cache_resource
def get_connection():
    settings = Settings()
    return pyodbc.connect(settings.dw_connection_string, timeout=30)


@st.cache_data(ttl=300)
def run_query(sql: str) -> pd.DataFrame:
    conn = get_connection()
    return pd.read_sql(sql, conn)


# ─── SIDEBAR ──────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("## 📊 Dashboard Ventas")
    st.markdown("**Data Warehouse** — VentasAnalisisDB")
    st.markdown("Modelo Estrella (Star Schema)")
    st.divider()

    st.markdown("### 🔍 Filtros")

    anios_df = run_query("SELECT DISTINCT Anio FROM DimTiempo ORDER BY Anio")
    anios = ["Todos"] + [str(a) for a in anios_df["Anio"].tolist()]
    filtro_anio = st.selectbox("Año", anios)

    categorias_df = run_query("SELECT DISTINCT NombreCategoria FROM DimCategoria ORDER BY NombreCategoria")
    cats = ["Todas"] + categorias_df["NombreCategoria"].tolist()
    filtro_cat = st.selectbox("Categoría", cats)

    st.divider()
    if st.button("🔄 Refrescar datos"):
        st.cache_data.clear()
        st.rerun()

    st.divider()
    st.caption("🕐 Datos en caché por 5 min")
    st.caption("Motor: SQL Server LocalDB")


# ─── CABECERA ─────────────────────────────────────────────────────────
st.markdown("""
<div style="background:linear-gradient(135deg,#1e2130,#252a3d);
            border:1px solid rgba(99,102,241,0.3);
            border-radius:16px; padding:1.5rem 2rem; margin-bottom:1.5rem;">
  <h1 style="margin:0;font-size:1.8rem;color:#f8fafc;">
    📊 Tablero de Ventas
  </h1>
  <p style="margin:0.3rem 0 0;color:#94a3b8;font-size:0.9rem;">
    Indicadores analíticos conectados a <strong style="color:#a5b4fc;">VentasAnalisisDB</strong>
    — Modelo Estrella (Star Schema)
  </p>
</div>
""", unsafe_allow_html=True)


# ─── CLÁUSULA DE FILTRO ───────────────────────────────────────────────
def where_anio(alias_tiempo: str = "t") -> str:
    if filtro_anio != "Todos":
        return f"AND {alias_tiempo}.Anio = {filtro_anio}"
    return ""

def where_cat(alias_cat: str = "c") -> str:
    if filtro_cat != "Todas":
        cat_safe = filtro_cat.replace("'", "''")
        return f"AND {alias_cat}.NombreCategoria = '{cat_safe}'"
    return ""


try:
    # ══════════════════════════════════════════════════════════════════
    # BLOQUE 1 — KPIs PRINCIPALES
    # ══════════════════════════════════════════════════════════════════
    kpi_sql = f"""
        SELECT
            COUNT(DISTINCT f.idHecho)      AS TotalVentas,
            COUNT(DISTINCT f.idCliente)    AS TotalClientes,
            COUNT(DISTINCT f.idProducto)   AS TotalProductos,
            ISNULL(SUM(f.TotalVenta), 0)   AS IngresoTotal,
            ISNULL(AVG(f.TotalVenta), 0)   AS TicketPromedio,
            ISNULL(SUM(f.Cantidad), 0)     AS UnidadesVendidas
        FROM FactVentas f
        INNER JOIN DimTiempo   t ON f.idTiempo   = t.idTiempo
        INNER JOIN DimCategoria c ON f.idCategoria = c.idCategoria
        WHERE 1=1 {where_anio()} {where_cat()}
    """
    kpi = run_query(kpi_sql).iloc[0]

    k1, k2, k3, k4, k5, k6 = st.columns(6)
    k1.metric("🛒 Transacciones",    f"{int(kpi['TotalVentas']):,}")
    k2.metric("👥 Clientes Únicos",  f"{int(kpi['TotalClientes']):,}")
    k3.metric("📦 Productos",        f"{int(kpi['TotalProductos']):,}")
    k4.metric("💰 Ingreso Total",    f"${float(kpi['IngresoTotal']):,.2f}")
    k5.metric("🎫 Ticket Promedio",  f"${float(kpi['TicketPromedio']):,.2f}")
    k6.metric("📈 Unidades",         f"{int(kpi['UnidadesVendidas']):,}")

    st.divider()

    # ══════════════════════════════════════════════════════════════════
    # BLOQUE 2 — Ventas por Mes | Ventas por Categoría
    # ══════════════════════════════════════════════════════════════════
    col1, col2 = st.columns(2)

    with col1:
        st.markdown('<p class="section-header">📅 KPI 1 — Ingresos por Mes</p>', unsafe_allow_html=True)
        df_mes = run_query(f"""
            SELECT t.Anio, t.Mes, t.NombreMes,
                   COUNT(f.idHecho)  AS CantidadVentas,
                   SUM(f.TotalVenta) AS TotalIngresos,
                   AVG(f.TotalVenta) AS TicketPromedio
            FROM FactVentas f
            INNER JOIN DimTiempo t    ON f.idTiempo    = t.idTiempo
            INNER JOIN DimCategoria c ON f.idCategoria = c.idCategoria
            WHERE 1=1 {where_anio()} {where_cat()}
            GROUP BY t.Anio, t.Mes, t.NombreMes
            ORDER BY t.Anio, t.Mes
        """)
        if not df_mes.empty:
            df_mes["Periodo"] = df_mes["NombreMes"] + " " + df_mes["Anio"].astype(str)
            fig = go.Figure()
            fig.add_trace(go.Bar(
                x=df_mes["Periodo"], y=df_mes["TotalIngresos"],
                name="Ingresos", marker_color=ACCENT,
                hovertemplate="<b>%{x}</b><br>Ingresos: $%{y:,.2f}<extra></extra>",
            ))
            fig.add_trace(go.Scatter(
                x=df_mes["Periodo"], y=df_mes["CantidadVentas"],
                name="Ventas", yaxis="y2", mode="lines+markers",
                line=dict(color="#f472b6", width=2),
                marker=dict(size=6),
            ))
            fig.update_layout(
                **{**PLOTLY_LAYOUT, "legend": dict(orientation="h", yanchor="bottom", y=1.02, bgcolor="rgba(0,0,0,0)", font=dict(color=FONT_COLOR))},
                yaxis2=dict(overlaying="y", side="right", showgrid=False, color="#f472b6"),
                hovermode="x unified",
            )
            st.plotly_chart(fig, use_container_width=True)
            with st.expander("Ver datos"):
                st.dataframe(df_mes[["Periodo","CantidadVentas","TotalIngresos","TicketPromedio"]], use_container_width=True)
        else:
            st.info("Sin datos para los filtros seleccionados.")

    with col2:
        st.markdown('<p class="section-header">🏷️ KPI 3 — Ventas por Categoría</p>', unsafe_allow_html=True)
        df_cat = run_query(f"""
            SELECT c.NombreCategoria AS Categoria,
                   COUNT(f.idHecho)       AS CantidadVentas,
                   SUM(f.Cantidad)        AS UnidadesVendidas,
                   SUM(f.TotalVenta)      AS TotalIngresos,
                   AVG(f.PrecioUnitario)  AS PrecioPromedio
            FROM FactVentas f
            INNER JOIN DimCategoria c ON f.idCategoria = c.idCategoria
            INNER JOIN DimTiempo    t ON f.idTiempo    = t.idTiempo
            WHERE 1=1 {where_anio()} {where_cat()}
            GROUP BY c.NombreCategoria
            ORDER BY TotalIngresos DESC
        """)
        if not df_cat.empty:
            fig2 = px.pie(
                df_cat, names="Categoria", values="TotalIngresos",
                color_discrete_sequence=COLORS,
                hole=0.45,
            )
            fig2.update_traces(
                textposition="inside", textinfo="percent+label",
                hovertemplate="<b>%{label}</b><br>$%{value:,.2f}<extra></extra>",
            )
            fig2.update_layout(**PLOTLY_LAYOUT)
            st.plotly_chart(fig2, use_container_width=True)
            with st.expander("Ver datos"):
                st.dataframe(df_cat, use_container_width=True)
        else:
            st.info("Sin datos de categorías.")

    st.divider()

    # ══════════════════════════════════════════════════════════════════
    # BLOQUE 3 — Top 10 Clientes | Ventas por Trimestre
    # ══════════════════════════════════════════════════════════════════
    col3, col4 = st.columns(2)

    with col3:
        st.markdown('<p class="section-header">🏆 KPI 2 — Top 10 Clientes por Ingreso</p>', unsafe_allow_html=True)
        df_cli = run_query(f"""
            SELECT TOP 10
                cl.NombreCompleto AS Cliente,
                cl.Pais,
                cl.Segmento,
                COUNT(f.idHecho)  AS CantidadCompras,
                SUM(f.TotalVenta) AS TotalGastado
            FROM FactVentas f
            INNER JOIN DimCliente cl ON f.idCliente  = cl.idCliente
            INNER JOIN DimTiempo  t  ON f.idTiempo   = t.idTiempo
            INNER JOIN DimCategoria c ON f.idCategoria = c.idCategoria
            WHERE 1=1 {where_anio()} {where_cat()}
            GROUP BY cl.NombreCompleto, cl.Pais, cl.Segmento
            ORDER BY TotalGastado DESC
        """)
        if not df_cli.empty:
            fig3 = px.bar(
                df_cli, x="TotalGastado", y="Cliente",
                orientation="h", color="Segmento",
                color_discrete_sequence=COLORS,
                text_auto=".2s",
            )
            fig3.update_traces(
                hovertemplate="<b>%{y}</b><br>Total: $%{x:,.2f}<extra></extra>",
                textfont_size=11,
            )
            fig3.update_layout(**PLOTLY_LAYOUT, yaxis_categoryorder="total ascending")
            st.plotly_chart(fig3, use_container_width=True)
            with st.expander("Ver datos"):
                st.dataframe(df_cli, use_container_width=True)
        else:
            st.info("Sin datos de clientes.")

    with col4:
        st.markdown('<p class="section-header">📊 KPI 4 — Ventas por Trimestre</p>', unsafe_allow_html=True)
        df_trim = run_query(f"""
            SELECT t.Anio, t.Trimestre,
                   COUNT(f.idHecho)  AS CantidadVentas,
                   SUM(f.TotalVenta) AS TotalIngresos,
                   SUM(f.Cantidad)   AS UnidadesTrimestral
            FROM FactVentas f
            INNER JOIN DimTiempo   t ON f.idTiempo    = t.idTiempo
            INNER JOIN DimCategoria c ON f.idCategoria = c.idCategoria
            WHERE 1=1 {where_anio()} {where_cat()}
            GROUP BY t.Anio, t.Trimestre
            ORDER BY t.Anio, t.Trimestre
        """)
        if not df_trim.empty:
            df_trim["Periodo"] = "Q" + df_trim["Trimestre"].astype(str) + " " + df_trim["Anio"].astype(str)
            fig4 = px.bar(
                df_trim, x="Periodo", y="TotalIngresos",
                color="Periodo", color_discrete_sequence=COLORS,
                text_auto=".2s",
            )
            fig4.update_traces(
                hovertemplate="<b>%{x}</b><br>$%{y:,.2f}<extra></extra>",
            )
            fig4.update_layout(**PLOTLY_LAYOUT, showlegend=False)
            st.plotly_chart(fig4, use_container_width=True)
            with st.expander("Ver datos"):
                st.dataframe(df_trim[["Periodo","CantidadVentas","TotalIngresos","UnidadesTrimestral"]], use_container_width=True)
        else:
            st.info("Sin datos por trimestre.")

    st.divider()

    # ══════════════════════════════════════════════════════════════════
    # BLOQUE 4 — Top 10 Productos | Laboral vs Fin de Semana
    # ══════════════════════════════════════════════════════════════════
    col5, col6 = st.columns(2)

    with col5:
        st.markdown('<p class="section-header">📦 KPI 5 — Top 10 Productos Más Vendidos</p>', unsafe_allow_html=True)
        df_prod = run_query(f"""
            SELECT TOP 10
                p.NombreProducto AS Producto,
                p.Categoria,
                SUM(f.Cantidad)    AS UnidadesVendidas,
                SUM(f.TotalVenta)  AS TotalIngresos,
                AVG(f.PrecioUnitario) AS PrecioPromedio
            FROM FactVentas f
            INNER JOIN DimProducto  p ON f.idProducto  = p.idProducto
            INNER JOIN DimTiempo    t ON f.idTiempo    = t.idTiempo
            INNER JOIN DimCategoria c ON f.idCategoria = c.idCategoria
            WHERE 1=1 {where_anio()} {where_cat()}
            GROUP BY p.NombreProducto, p.Categoria
            ORDER BY UnidadesVendidas DESC
        """)
        if not df_prod.empty:
            fig5 = px.bar(
                df_prod, x="UnidadesVendidas", y="Producto",
                orientation="h", color="Categoria",
                color_discrete_sequence=COLORS,
                text_auto=True,
            )
            fig5.update_traces(
                hovertemplate="<b>%{y}</b><br>Unidades: %{x:,}<extra></extra>",
            )
            fig5.update_layout(**PLOTLY_LAYOUT, yaxis_categoryorder="total ascending")
            st.plotly_chart(fig5, use_container_width=True)
            with st.expander("Ver datos"):
                st.dataframe(df_prod, use_container_width=True)
        else:
            st.info("Sin datos de productos.")

    with col6:
        st.markdown('<p class="section-header">📆 KPI 6 — Laboral vs Fin de Semana</p>', unsafe_allow_html=True)
        df_fds = run_query(f"""
            SELECT
                CASE WHEN t.EsFinDeSemana = 1 THEN 'Fin de Semana' ELSE 'Día Laboral' END AS TipoDia,
                COUNT(f.idHecho)  AS CantidadVentas,
                SUM(f.TotalVenta) AS TotalIngresos,
                SUM(f.Cantidad)   AS UnidadesVendidas
            FROM FactVentas f
            INNER JOIN DimTiempo   t ON f.idTiempo    = t.idTiempo
            INNER JOIN DimCategoria c ON f.idCategoria = c.idCategoria
            WHERE 1=1 {where_anio()} {where_cat()}
            GROUP BY t.EsFinDeSemana
        """)
        if not df_fds.empty:
            fig6 = px.pie(
                df_fds, names="TipoDia", values="TotalIngresos",
                color_discrete_sequence=["#6366f1", "#f472b6"],
                hole=0.45,
            )
            fig6.update_traces(
                textposition="inside", textinfo="percent+label",
                hovertemplate="<b>%{label}</b><br>$%{value:,.2f} (%{percent})<extra></extra>",
            )
            fig6.update_layout(**PLOTLY_LAYOUT)
            st.plotly_chart(fig6, use_container_width=True)
            with st.expander("Ver datos"):
                st.dataframe(df_fds, use_container_width=True)
        else:
            st.info("Sin datos de tipo de día.")

    st.divider()

    # ══════════════════════════════════════════════════════════════════
    # BLOQUE 5 — Evolución de ingresos (tendencia)
    # ══════════════════════════════════════════════════════════════════
    st.markdown('<p class="section-header">📉 Tendencia de Ingresos Diarios</p>', unsafe_allow_html=True)
    df_tend = run_query(f"""
        SELECT t.Fecha,
               SUM(f.TotalVenta) AS TotalDiario,
               COUNT(f.idHecho)  AS VentasDiarias
        FROM FactVentas f
        INNER JOIN DimTiempo   t ON f.idTiempo    = t.idTiempo
        INNER JOIN DimCategoria c ON f.idCategoria = c.idCategoria
        WHERE 1=1 {where_anio()} {where_cat()}
        GROUP BY t.Fecha
        ORDER BY t.Fecha
    """)
    if not df_tend.empty:
        fig7 = go.Figure()
        fig7.add_trace(go.Scatter(
            x=df_tend["Fecha"], y=df_tend["TotalDiario"],
            mode="lines", name="Ingresos Diarios",
            line=dict(color=ACCENT, width=2),
            fill="tozeroy",
            fillcolor="rgba(99,102,241,0.12)",
        ))
        fig7.update_layout(
            **PLOTLY_LAYOUT,
            hovermode="x unified",
            xaxis_title="Fecha",
            yaxis_title="Ingresos ($)",
        )
        st.plotly_chart(fig7, use_container_width=True)
    else:
        st.info("Sin datos de tendencia.")

    # ─── PIE DE PÁGINA ────────────────────────────────────────────────
    st.divider()
    st.caption("📊 Dashboard de Ventas — VentasAnalisisDB · Modelo Estrella (Star Schema) · ETL con Python")

except pyodbc.Error as e:
    st.error(f"⚠️ Error de conexión al Data Warehouse: {e}")
    st.info(
        "Asegúrate de:\n"
        "1. Tener SQL Server LocalDB instalado y en ejecución\n"
        "2. Haber creado la BD ejecutando `database/VentasAnalisis.sql`\n"
        "3. Haber ejecutado el ETL (`python main.py`) para cargar datos"
    )
except Exception as e:
    st.error(f"Error inesperado: {e}")
    st.exception(e)
