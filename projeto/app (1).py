
# -*- coding: utf-8 -*-
import os
from datetime import datetime
from io import BytesIO
import streamlit as st
import pandas as pd

from db import (
    init_db, ensure_db_writable, vacuum, DB_PATH,
    upsert_dataframe, read_all, count_all,
    find_registros_para_prefill, list_registros_base_all,
    delete_all_pacientes, upsert_paciente_single, delete_paciente_by_key,
    list_procedimento_tipos, set_procedimento_tipo_status, upsert_procedimento_tipo,
    list_cirurgia_situacoes, set_cirurgia_situacao_status, upsert_cirurgia_situacao,
    list_cirurgias, insert_or_update_cirurgia, delete_cirurgia,
    delete_cirurgia_by_key, delete_cirurgias_by_filter,
    delete_all_cirurgias, delete_all_catalogos, hard_reset_local_db,
)
from processing import process_uploaded_file
from export import to_formatted_excel_by_hospital, to_formatted_excel_cirurgias

# --- GitHub sync ---
try:
    from github_sync import (
        download_db_from_github,
        safe_upload_with_merge,
        upload_db_to_github,
        get_remote_sha,
    )
    GITHUB_SYNC_AVAILABLE = True
except Exception:
    GITHUB_SYNC_AVAILABLE = False

# ---- Config GitHub ----
GH_OWNER = st.secrets.get("GH_OWNER", "seu-usuario-ou-org")
GH_REPO  = st.secrets.get("GH_REPO", "seu-repo")
GH_BRANCH = st.secrets.get("GH_BRANCH", "main")
GH_PATH_IN_REPO = st.secrets.get("GH_DB_PATH", "data/exemplo.db")
GITHUB_TOKEN_OK = bool(st.secrets.get("GITHUB_TOKEN", ""))

APP_CACHE_VERSION = "v1.0.0"

st.set_page_config(page_title="Gestão de Pacientes e Cirurgias", layout="wide")

# =========================
# Função helper para garantir colunas fixas
# =========================
def ensure_columns(df: pd.DataFrame, cols: list) -> pd.DataFrame:
    for col in cols:
        if col not in df.columns:
            df[col] = ""
    return df

# Boot cleanup do cache
if not st.session_state.get("_boot_cleanup_done"):
    st.cache_data.clear()
    st.session_state["_boot_cleanup_done"] = True

def try_vacuum_safely():
    try:
        ensure_db_writable()
        vacuum()
        st.cache_data.clear()
        st.caption("VACUUM + checkpoint executados.")
    except Exception as e:
        msg = str(e).lower()
        if "readonly" in msg or "read-only" in msg:
            st.warning("VACUUM não pôde ser executado (banco read-only agora). Prosseguindo sem VACUUM.")
        else:
            st.info("Não foi possível executar VACUUM agora.")
            st.exception(e)

# Inicializa DB
init_db()

# Tabs
HOSPITAL_OPCOES = ["Hospital Santa Lucia Sul", "Hospital Santa Lucia Norte", "Hospital Maria Auxiliadora"]
tabs = st.tabs(["📥 Importação & Pacientes", "🩺 Cirurgias", "📚 Cadastro (Tipos & Situações)", "📄 Tipos (Lista)"])

# ====================================================================================
# Aba Cirurgias corrigida
# ====================================================================================
with tabs[1]:
    st.subheader("Cadastrar / Editar Cirurgias")

    hosp_cad = st.selectbox("Hospital", options=HOSPITAL_OPCOES, index=0)
    ano_cad = st.number_input("Ano", min_value=2000, max_value=2100, value=datetime.now().year)
    mes_cad = st.number_input("Mês", min_value=1, max_value=12, value=datetime.now().month)

    sit_filter_nomes = st.multiselect("Filtrar por Situação", options=[], default=[])
    sit_filter_ids = []  # Simulação

    ignorar_periodo_por_situacao = len(sit_filter_ids) > 0

    ano_mes_str = None if ignorar_periodo_por_situacao else f"{int(ano_cad)}-{int(mes_cad):02d}"

    rows_cir = list_cirurgias(hospital=hosp_cad, ano_mes=ano_mes_str, prestador=None)
    df_cir = pd.DataFrame(rows_cir, columns=[
        "id", "Hospital", "Atendimento", "Paciente", "Prestador", "Data_Cirurgia",
        "Convenio", "Procedimento_Tipo_ID", "Situacao_ID",
        "Guia_AMHPTISS", "Guia_AMHPTISS_Complemento", "Fatura",
        "Observacoes", "created_at", "updated_at"
    ])

    df_base_mapped = pd.DataFrame({
        "id": [], "Hospital": [], "Atendimento": [], "Paciente": [], "Prestador": [], "Data_Cirurgia": [],
        "Convenio": [], "Procedimento_Tipo_ID": [], "Situacao_ID": [],
        "Guia_AMHPTISS": [], "Guia_AMHPTISS_Complemento": [], "Fatura": [],
        "Observacoes": [], "created_at": [], "updated_at": [],
        "Fonte": [], "Tipo (nome)": [], "Situação (nome)": [],
        "_old_source": [], "_old_h": [], "_old_att": [], "_old_pac": [], "_old_pre": [], "_old_data": []
    })

    # Colunas fixas
    COLS_CIRURGIAS = [
        "id", "Hospital", "Atendimento", "Paciente", "Prestador", "Data_Cirurgia",
        "Convenio", "Procedimento_Tipo_ID", "Situacao_ID",
        "Guia_AMHPTISS", "Guia_AMHPTISS_Complemento", "Fatura",
        "Observacoes", "created_at", "updated_at",
        "Fonte", "Tipo (nome)", "Situação (nome)",
        "_old_source", "_old_h", "_old_att", "_old_pac", "_old_pre", "_old_data"
    ]

    df_cir = ensure_columns(df_cir, COLS_CIRURGIAS)
    df_base_mapped = ensure_columns(df_base_mapped, COLS_CIRURGIAS)

    df_union = pd.concat([df_cir[COLS_CIRURGIAS], df_base_mapped[COLS_CIRURGIAS]], ignore_index=True)

    st.dataframe(df_union, use_container_width=True)
