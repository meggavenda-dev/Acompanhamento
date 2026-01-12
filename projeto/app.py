import os
import streamlit as st
import pandas as pd
from datetime import datetime

# Importações locais
from db import (
    init_db, upsert_dataframe, read_all, DB_PATH, count_all,
    delete_all_pacientes, delete_all_cirurgias, delete_all_catalogos, 
    vacuum, dispose_engine, reset_db_file
)
from processing import process_uploaded_file
from export import to_formatted_excel_by_hospital

# Tenta carregar sincronização GitHub
GITHUB_SYNC_AVAILABLE = False
try:
    from github_sync import download_db_from_github, upload_db_to_github
    GITHUB_SYNC_AVAILABLE = True
except:
    pass

st.set_page_config(page_title="Gestão Hospitalar", layout="wide")
init_db()

# --- Config GitHub (Secrets) ---
GH_OWNER = st.secrets.get("GH_OWNER", "")
GH_REPO = st.secrets.get("GH_REPO", "")
GITHUB_TOKEN_OK = bool(st.secrets.get("GITHUB_TOKEN", ""))

# --- Sidebar: Sincronização e Área de Risco ---
with st.sidebar:
    st.title("Configurações")
    
    # Sincronização GitHub
    if GITHUB_SYNC_AVAILABLE and GITHUB_TOKEN_OK:
        if st.button("🔽 Baixar do GitHub"):
            download_db_from_github(owner=GH_OWNER, repo=GH_REPO, local_db_path=DB_PATH)
            st.rerun()

    st.markdown("---")
    st.markdown("### 🧨 Área de Risco")
    confirmar = st.checkbox("Habilitar botões de exclusão")
    reset_txt = st.text_input("Digite RESET para confirmar")
    pode_apagar = confirmar and reset_txt == "RESET"

    def _sync_gh(msg):
        if GITHUB_SYNC_AVAILABLE and GITHUB_TOKEN_OK:
            upload_db_to_github(owner=GH_OWNER, repo=GH_REPO, local_db_path=DB_PATH, commit_message=msg)

    if st.button("Apagar PACIENTES", disabled=not pode_apagar):
        delete_all_pacientes()
        vacuum()
        _sync_gh("Limpeza de pacientes")
        st.success("Tabela de pacientes limpa!")
        st.rerun()

    if st.button("Apagar CIRURGIAS", disabled=not pode_apagar):
        delete_all_cirurgias()
        vacuum()
        _sync_gh("Limpeza de cirurgias")
        st.success("Tabela de cirurgias limpa!")
        st.rerun()

    if st.button("🗑️ RESET TOTAL (Deletar .db)", type="primary", disabled=not pode_apagar):
        reset_db_file()
        _sync_gh("Reset total do banco")
        st.warning("Banco de dados reiniciado do zero!")
        st.rerun()

# --- Abas Principais ---
tabs = st.tabs(["📥 Importação", "🩺 Cirurgias", "📚 Cadastro"])

with tabs[0]:
    st.subheader("Importar Planilha")
    selected_hospital = st.selectbox("Hospital", ["Hospital Santa Lucia Sul", "Hospital Santa Lucia Norte"])
    uploaded_file = st.file_uploader("Arquivo", type=["csv", "xlsx"])
    
    if uploaded_file:
        df_processado = process_uploaded_file(uploaded_file, ["MEDICO EXEMPLO"], selected_hospital)
        st.dataframe(df_processado)
        if st.button("Salvar no Banco"):
            upsert_dataframe(df_processado)
            st.success("Dados salvos!")
            _sync_gh("Novo upload de dados")

with tabs[1]:
    st.subheader("Gestão de Cirurgias")
    # Aqui entraria a lógica de listagem e edição de cirurgias do seu código original

with tabs[2]:
    st.subheader("Catálogos")
    # Aqui entraria a lógica de tipos e situações do seu código original
