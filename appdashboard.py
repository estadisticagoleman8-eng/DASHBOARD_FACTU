import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import datetime
import os

# --- Configuración de la página ---
st.set_page_config(page_title="Dashboard de Productividad", page_icon="📊", layout="wide")

# --- 1. CONFIGURACIÓN DE FUENTE DE DATOS (GOOGLE SHEETS) ---
GSHEET_ID = "1rS3nZ-LBiACn_PIcIaGYCh6O6PROATCjLHMz0YED9fE"
GSHEET_URL_BASE = f"https://docs.google.com/spreadsheets/d/1FzDU1Bz_Cp5Rr0-YjFWdMmOv8G-Re_Uh2IB8wWimfks/edit?gid=0#gid=0"

PERSISTED_DATA_DIR = "persisted_data"
os.makedirs(PERSISTED_DATA_DIR, exist_ok=True)

FILES = {
    "PPL": os.path.join(PERSISTED_DATA_DIR, "df_ppl.parquet"),
    "Convenios": os.path.join(PERSISTED_DATA_DIR, "df_convenios.parquet"),
    "RIPS": os.path.join(PERSISTED_DATA_DIR, "df_rips.parquet"),
    "Facturacion": os.path.join(PERSISTED_DATA_DIR, "df_facturacion.parquet")
}


# --- 2. FUNCIONES DE CARGA ---
def load_sheet(sheet_name):
    try:
        return pd.read_csv(GSHEET_URL_BASE + sheet_name)
    except Exception:
        return None


def save_local(df, filepath):
    if df is not None and not df.empty:
        df.astype(str).to_parquet(filepath, index=False)


def load_local(filepath):
    return pd.read_parquet(filepath) if os.path.exists(filepath) else None


# --- 3. INICIALIZACIÓN ---
if 'initialized' not in st.session_state:
    st.session_state.df_ppl = load_local(FILES["PPL"])
    st.session_state.df_convenios = load_local(FILES["Convenios"])
    st.session_state.df_rips = load_local(FILES["RIPS"])
    st.session_state.df_facturacion = load_local(FILES["Facturacion"])
    st.session_state.initialized = True

# --- 4. BARRA LATERAL (CONTROL Y FILTROS GLOBALES) ---
st.sidebar.header("📥 Control de Datos")
if st.sidebar.button("🔄 Sincronizar Google Sheets", use_container_width=True):
    with st.spinner("Actualizando datos..."):
        st.session_state.df_ppl = load_sheet("PPL")
        st.session_state.df_convenios = load_sheet("Convenios")
        st.session_state.df_rips = load_sheet("RIPS")
        st.session_state.df_facturacion = load_sheet("Facturacion")
        for k, v in FILES.items():
            save_local(getattr(st.session_state, f"df_{k.lower()}"), v)
        st.rerun()

st.sidebar.markdown("---")
st.sidebar.subheader("Filtros de Análisis")

# Filtro de Tipo (Legalizaciones)
tipo_legalizacion = st.sidebar.multiselect("Tipo de Legalización", ["PPL", "Convenios"], default=["PPL", "Convenios"])

# Recolectar facturadores de todas las fuentes
facturadores_total = []
for df_name in ['df_ppl', 'df_convenios', 'df_rips', 'df_facturacion']:
    df_temp = getattr(st.session_state, df_name)
    if df_temp is not None:
        col_u = 'USUARIO' if 'USUARIO' in df_temp.columns else 'Usuario'
        if col_u in df_temp.columns:
            facturadores_total.extend(df_temp[col_u].dropna().unique())

sel_usuarios = st.sidebar.multiselect("Seleccionar Facturador", ['Todos'] + sorted(list(set(facturadores_total))),
                                      default=['Todos'])
start_date, end_date = st.sidebar.date_input("Rango de fechas", [datetime.date.today() - datetime.timedelta(days=30),
                                                                 datetime.date.today()])


# --- 5. FUNCIÓN MAESTRA DE VISUALIZACIÓN ---
def procesar_y_graficar(df, titulo, es_legalizacion=False):
    if df is None or df.empty:
        st.info(f"No hay datos cargados para la sección de {titulo}")
        return

    # Normalización de columnas de Usuario y Fecha
    col_u = 'USUARIO' if 'USUARIO' in df.columns else 'Usuario'
    col_f = next((c for c in ['FECHA_REAL', 'FECHA_FACTURA', 'FECHA', 'Fecha'] if c in df.columns), None)

    # Filtro por tipo (solo legalizaciones)
    if es_legalizacion and 'Tipo_Leg' in df.columns:
        df = df[df['Tipo_Leg'].isin(tipo_legalizacion)]

    # Filtro de Fecha
    if col_f:
        df[col_f] = pd.to_datetime(df[col_f], errors='coerce')
        df = df.dropna(subset=[col_f])
        df = df[(df[col_f].dt.date >= start_date) & (df[col_f].dt.date <= end_date)]

    # Determinar si el filtro de usuario está activo
    es_filtro_activo = 'Todos' not in sel_usuarios and len(sel_usuarios) > 0
    if es_filtro_activo:
        df = df[df[col_u].isin(sel_usuarios)]

    if df.empty:
        st.warning(f"No hay datos para mostrar en {titulo} con los filtros actuales.")
        return

    # Métricas superiores
    st.metric(f"Total Registros ({titulo})", f"{len(df):,}")

    if not es_filtro_activo:
        # --- MODO GENERAL: GRÁFICO DE BARRAS + TABLA % ---
        st.subheader(f"Productividad General: {titulo}")
        fig, ax = plt.subplots(figsize=(10, 6))
        counts = df[col_u].value_counts().reset_index()
        counts.columns = ['Usuario', 'Conteo']

        sns.barplot(data=counts, y='Usuario', x='Conteo', palette='viridis', ax=ax)
        for i, v in enumerate(counts['Conteo']):
            ax.text(v + 0.1, i, str(int(v)), color='black', va='center', fontweight='bold')

        ax.set_xlabel("Cantidad Total")
        st.pyplot(fig)

        st.subheader(f"Distribución Porcentual - {titulo}")
        counts['%'] = (counts['Conteo'] / counts['Conteo'].sum() * 100).round(2)
        st.table(counts.style.format({'%': '{:.2f}%'}))

    else:
        # --- MODO COMPARATIVO: LINEPLOT + TABLA RESUMEN ---
        st.subheader(f"Comparativa de Evolución Temporal ({titulo})")
        if col_f:
            df['Dia_Evolucion'] = df[col_f].dt.date
            evol = df.groupby(['Dia_Evolucion', col_u]).size().reset_index(name='Cuenta')

            fig2, ax2 = plt.subplots(figsize=(12, 5))
            sns.lineplot(data=evol, x='Dia_Evolucion', y='Cuenta', hue=col_u, marker='o', ax=ax2)
            ax2.grid(True, linestyle='--', alpha=0.6)  # Cuadrícula para facilitar lectura
            plt.xticks(rotation=45)
            ax2.set_ylabel("Productividad Diaria")
            st.pyplot(fig2)

        st.subheader(f"Resumen de Usuarios Seleccionados")
        resumen_sel = df[col_u].value_counts().reset_index()
        resumen_sel.columns = ['Usuario', 'Total Realizado']
        st.table(resumen_sel)


# --- 6. TABS (PÁGINAS) ---
tab_leg, tab_rips, tab_fact = st.tabs(["📁 Legalizaciones", "📄 RIPS", "💰 Facturación"])

with tab_leg:
    list_leg = []
    if st.session_state.df_ppl is not None:
        d_p = st.session_state.df_ppl.copy()
        d_p['Tipo_Leg'] = 'PPL'
        list_leg.append(d_p)
    if st.session_state.df_convenios is not None:
        d_c = st.session_state.df_convenios.copy()
        d_c['Tipo_Leg'] = 'Convenios'
        list_leg.append(d_c)

    if list_leg:
        procesar_y_graficar(pd.concat(list_leg, ignore_index=True), "Legalizaciones", es_legalizacion=True)
    else:
        st.info("Sincroniza los datos para visualizar Legalizaciones.")

with tab_rips:
    procesar_y_graficar(st.session_state.df_rips, "RIPS")

with tab_fact:

    procesar_y_graficar(st.session_state.df_facturacion, "Facturación")
