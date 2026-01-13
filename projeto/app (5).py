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

GH_OWNER = st.secrets.get("GH_OWNER", "seu-usuario-ou-org")
GH_REPO  = st.secrets.get("GH_REPO", "seu-repo")
GH_BRANCH = st.secrets.get("GH_BRANCH", "main")
GH_PATH_IN_REPO = st.secrets.get("GH_DB_PATH", "data/exemplo.db")
GITHUB_TOKEN_OK = bool(st.secrets.get("GITHUB_TOKEN", ""))

APP_CACHE_VERSION = "v1.0.1"  # versão corrigida

st.set_page_config(page_title="Gestão de Pacientes e Cirurgias", layout="wide")

def _boot_session_cleanup():
    st.cache_data.clear()

if not st.session_state.get("_boot_cleanup_done"):
    _boot_session_cleanup()
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

def _should_bootstrap_from_github(db_path: str, size_threshold_bytes: int = 10_000) -> bool:
    if not os.path.exists(db_path):
        return True
    try:
        return os.path.getsize(db_path) < size_threshold_bytes
    except Exception:
        return True

if GITHUB_SYNC_AVAILABLE and GITHUB_TOKEN_OK:
    needs_bootstrap = _should_bootstrap_from_github(DB_PATH, size_threshold_bytes=10_000)
    if needs_bootstrap and not st.session_state.get("gh_db_fetched"):
        try:
            downloaded, remote_sha = download_db_from_github(
                owner=GH_OWNER,
                repo=GH_REPO,
                path_in_repo=GH_PATH_IN_REPO,
                branch=GH_BRANCH,
                local_db_path=DB_PATH,
                return_sha=True
            )
            if downloaded:
                st.cache_data.clear()
                st.session_state["gh_sha"] = remote_sha
                st.session_state["gh_db_fetched"] = True
                st.success("Banco baixado do GitHub na inicialização (bootstrap).")
                st.rerun()
            else:
                st.info("Banco ainda não existe no GitHub. Um novo será criado localmente ao salvar.")
        except Exception as e:
            st.error("Erro ao sincronizar inicialização com GitHub.")
            st.exception(e)
            st.session_state["gh_db_fetched"] = True

with st.sidebar:
    st.markdown("### Sincronização GitHub")
    if GITHUB_SYNC_AVAILABLE and GITHUB_TOKEN_OK:
        if st.button("🔽 Baixar banco do GitHub (manual)"):
            try:
                downloaded, remote_sha = download_db_from_github(
                    owner=GH_OWNER,
                    repo=GH_REPO,
                    path_in_repo=GH_PATH_IN_REPO,
                    branch=GH_BRANCH,
                    local_db_path=DB_PATH,
                    return_sha=True
                )
                if downloaded:
                    st.cache_data.clear()
                    st.success("Banco baixado do GitHub (manual).")
                    st.session_state["gh_sha"] = remote_sha
                    st.session_state["gh_db_fetched"] = True
                    st.rerun()
                else:
                    st.info("Arquivo não existe no repositório.")
            except Exception as e:
                st.error("Falha ao baixar do GitHub.")
                st.exception(e)
    else:
        st.info("GitHub sync desativado (sem token).")

# ... (mantém toda lógica anterior até a aba Cirurgias)

# CORREÇÃO NA ABA CIRURGIAS
# Antes do concat, vamos mesclar df_base_mapped com df_cir para preservar colunas

# Dentro do bloco try onde monta df_union:
# Substituir trecho original por:

# Preenche candidatos da base com dados existentes de cirurgias
if not df_base.empty:
    df_base_mapped = pd.DataFrame({
        "id": [None]*len(df_base),
        "Hospital": df_base["Hospital"],
        "Atendimento": df_base["Atendimento"],
        "Paciente": df_base["Paciente"],
        "Prestador": df_base["Prestador"],
        "Data_Cirurgia": df_base["Data"],
        "Convenio": df_base["Convenio"],
        "Procedimento_Tipo_ID": [None]*len(df_base),
        "Situacao_ID": [None]*len(df_base),
        "Guia_AMHPTISS": ["" for _ in range(len(df_base))],
        "Guia_AMHPTISS_Complemento": ["" for _ in range(len(df_base))],
        "Fatura": ["" for _ in range(len(df_base))],
        "Observacoes": ["" for _ in range(len(df_base))],
        "created_at": [None]*len(df_base),
        "updated_at": [None]*len(df_base),
        "Fonte": ["Base"]*len(df_base),
        "Tipo (nome)": ["" for _ in range(len(df_base))],
        "Situação (nome)": ["" for _ in range(len(df_base))],
        "_old_source": ["Base"]*len(df_base),
        "_old_h": df_base["Hospital"].astype(str),
        "_old_att": df_base["Atendimento"].astype(str),
        "_old_pac": df_base["Paciente"].astype(str),
        "_old_pre": df_base["Prestador"].astype(str),
        "_old_data": df_base["Data"].astype(str),
    })

    # Merge para preencher colunas se já existe cirurgia
    df_base_mapped = df_base_mapped.merge(
        df_cir[["Hospital", "Atendimento", "Paciente", "Prestador", "Data_Cirurgia",
                "Tipo (nome)", "Situação (nome)", "Observacoes"]],
        on=["Hospital", "Atendimento", "Paciente", "Prestador", "Data_Cirurgia"],
        how="left",
        suffixes=("", "_cirurgia")
    )
    for col in ["Tipo (nome)", "Situação (nome)", "Observacoes"]:
        df_base_mapped[col] = df_base_mapped[col].combine_first(df_base_mapped[f"{col}_cirurgia"])
        df_base_mapped.drop(columns=[f"{col}_cirurgia"], inplace=True)

# Depois, concat permanece igual:
df_union = pd.concat([df_cir, df_base_mapped], ignore_index=True)

# ... (restante do código igual)
