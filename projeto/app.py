import os
import streamlit as st
import pandas as pd
from datetime import datetime

# Importações de banco de dados e utilitários
from db import (
    init_db, upsert_dataframe, read_all, DB_PATH, count_all,
    delete_all_pacientes, delete_all_cirurgias, delete_all_catalogos, 
    vacuum, dispose_engine, reset_db_file,
    list_procedimento_tipos, list_cirurgia_situacoes, list_cirurgias,
    insert_or_update_cirurgia, find_registros_para_prefill, delete_cirurgia,
    upsert_procedimento_tipo, set_procedimento_tipo_status,
    upsert_cirurgia_situacao, set_cirurgia_situacao_status
)
from processing import process_uploaded_file
from export import to_formatted_excel_by_hospital, to_formatted_excel_cirurgias

# --- Sincronização GitHub ---
GITHUB_SYNC_AVAILABLE = False
try:
    from github_sync import download_db_from_github, upload_db_to_github
    GITHUB_SYNC_AVAILABLE = True
except:
    pass

# Configurações de Segredos (Streamlit Cloud Secrets)
GH_OWNER = st.secrets.get("GH_OWNER", "")
GH_REPO = st.secrets.get("GH_REPO", "")
GH_BRANCH = st.secrets.get("GH_BRANCH", "main")
GH_PATH_IN_REPO = st.secrets.get("GH_DB_PATH", "data/exemplo.db")
GITHUB_TOKEN_OK = bool(st.secrets.get("GITHUB_TOKEN", ""))

# Configuração da Página
st.set_page_config(page_title="Gestão de Pacientes e Cirurgias", layout="wide")
init_db()

# --- Funções de Apoio ---
def _sync_gh(message):
    """Encapsula a lógica de upload para o GitHub."""
    if GITHUB_SYNC_AVAILABLE and GITHUB_TOKEN_OK:
        try:
            success = upload_db_to_github(
                owner=GH_OWNER, repo=GH_REPO, branch=GH_BRANCH,
                path_in_repo=GH_PATH_IN_REPO, local_db_path=DB_PATH,
                commit_message=message
            )
            if success: st.toast(f"✅ Sincronizado: {message}")
        except Exception as e:
            st.error(f"Erro na sincronização: {e}")

# --- Sidebar: Configurações e Área de Risco ---
with st.sidebar:
    st.title("⚙️ Painel de Controle")
    
    if GITHUB_SYNC_AVAILABLE and GITHUB_TOKEN_OK:
        st.subheader("Sincronização")
        if st.button("🔽 Baixar Banco (Nuvem → Local)"):
            if download_db_from_github(owner=GH_OWNER, repo=GH_REPO, local_db_path=DB_PATH):
                st.success("Banco atualizado!")
                st.rerun()

    st.markdown("---")
    st.subheader("🧨 Área de Risco")
    st.caption("Ações irreversíveis. Use com cautela.")
    
    confirmar_permissao = st.checkbox("Liberar botões de exclusão")
    input_reset = st.text_input("Digite **RESET** para confirmar")
    pode_executar = confirmar_permissao and input_reset == "RESET"

    if st.button("Limpar Tabela Pacientes", disabled=not pode_executar):
        delete_all_pacientes()
        vacuum()
        _sync_gh("Limpeza manual: Pacientes")
        st.rerun()

    if st.button("Limpar Tabela Cirurgias", disabled=not pode_executar):
        delete_all_cirurgias()
        vacuum()
        _sync_gh("Limpeza manual: Cirurgias")
        st.rerun()

    if st.button("🗑️ RESET TOTAL (Deletar .db)", type="primary", disabled=not pode_executar):
        reset_db_file()
        _sync_gh("Reset total do sistema")
        st.rerun()

# --- Interface Principal ---
st.title("Gestão de Pacientes e Cirurgias")

tabs = st.tabs(["📥 Importação & Pacientes", "🩺 Cirurgias", "📚 Cadastro (Catálogos)"])

# === ABA 1: IMPORTAÇÃO ===
with tabs[0]:
    st.header("Processamento de Planilhas")
    
    col1, col2 = st.columns(2)
    with col1:
        prestadores_text = st.text_area("Prestadores Alvo (um por linha)", value="JOSE ADORNO\nCASSIO CESAR")
        prestadores_lista = [p.strip() for p in prestadores_text.splitlines() if p.strip()]
    
    with col2:
        hospital_selecionado = st.selectbox("Hospital de Origem", [
            "Hospital Santa Lucia Sul", "Hospital Santa Lucia Norte", "Hospital Maria Auxiliadora"
        ])
        uploaded_file = st.file_uploader("Upload CSV ou Excel", type=["csv", "xlsx"])

    if uploaded_file:
        df_result = process_uploaded_file(uploaded_file, prestadores_lista, hospital_selecionado)
        
        if not df_result.empty:
            st.subheader("Dados Processados")
            # Editor para correções de última hora nos nomes
            df_editado = st.data_editor(df_result, use_container_width=True, hide_index=True)
            
            if st.button("💾 Salvar Pacientes no Banco de Dados"):
                upsert_dataframe(df_editado)
                _sync_gh(f"Importação: {len(df_editado)} registros - {hospital_selecionado}")
                st.success("Dados persistidos com sucesso!")
        else:
            st.warning("Nenhum dado encontrado para os prestadores informados.")

# === ABA 2: CIRURGIAS ===
with tabs[1]:
    st.header("Registro de Cirurgias")
    
    # Filtros para carregar pacientes da base
    f_col1, f_col2, f_col3 = st.columns(3)
    with f_col1: h_filtro = st.selectbox("Filtrar Hospital", ["Hospital Santa Lucia Sul", "Hospital Santa Lucia Norte"], key="h_f")
    with f_col2: ano_f = st.number_input("Ano", value=datetime.now().year)
    with f_col3: mes_f = st.number_input("Mês", value=datetime.now().month, min_value=1, max_value=12)

    # Buscar dados para o editor
    tipos_cat = {r[1]: r[0] for r in list_procedimento_tipos(only_active=True)}
    sits_cat = {r[1]: r[0] for r in list_cirurgia_situacoes(only_active=True)}
    
    base_pacientes = find_registros_para_prefill(h_filtro, ano_f, mes_f)
    
    if base_pacientes:
        df_cirurgias = pd.DataFrame(base_pacientes, columns=["Hospital", "Data", "Atendimento", "Paciente", "Convenio", "Prestador"])
        
        # Adiciona colunas de cirurgia
        df_cirurgias["Tipo (nome)"] = ""
        df_cirurgias["Situação (nome)"] = ""
        df_cirurgias["Guia/Fatura"] = ""

        df_final_cir = st.data_editor(
            df_cirurgias,
            column_config={
                "Tipo (nome)": st.column_config.SelectboxColumn("Procedimento", options=list(tipos_cat.keys())),
                "Situação (nome)": st.column_config.SelectboxColumn("Status", options=list(sits_cat.keys())),
            },
            use_container_width=True, hide_index=True
        )

        if st.button("💾 Salvar/Atualizar Cirurgias"):
            for _, row in df_final_cir.iterrows():
                if row["Tipo (nome)"] != "" and row["Situação (nome)"] != "":
                    payload = {
                        "Hospital": row["Hospital"], "Atendimento": row["Atendimento"],
                        "Paciente": row["Paciente"], "Prestador": row["Prestador"],
                        "Data_Cirurgia": row["Data"], "Convenio": row["Convenio"],
                        "Procedimento_Tipo_ID": tipos_cat.get(row["Tipo (nome)"]),
                        "Situacao_ID": sits_cat.get(row["Situação (nome)"]),
                        "Fatura": row["Guia/Fatura"]
                    }
                    insert_or_update_cirurgia(payload)
            _sync_gh("Atualização em massa de cirurgias")
            st.success("Cirurgias atualizadas!")

# === ABA 3: CADASTRO (CATÁLOGOS) ===
with tabs[2]:
    st.header("Configuração de Catálogos")
    c1, c2 = st.columns(2)
    
    with c1:
        st.subheader("Tipos de Procedimento")
        novo_tipo = st.text_input("Novo Tipo (Ex: Colecistectomia)")
        if st.button("Adicionar Tipo") and novo_tipo:
            upsert_procedimento_tipo(novo_tipo, 1, 0)
            st.rerun()
            
        tipos_db = pd.DataFrame(list_procedimento_tipos(only_active=False), columns=["ID", "Nome", "Ativo", "Ordem"])
        st.data_editor(tipos_db, use_container_width=True, hide_index=True, key="ed_tipos")

    with c2:
        st.subheader("Situações")
        nova_sit = st.text_input("Nova Situação (Ex: Realizada)")
        if st.button("Adicionar Situação") and nova_sit:
            upsert_cirurgia_situacao(nova_sit, 1, 0)
            st.rerun()
            
        sits_db = pd.DataFrame(list_cirurgia_situacoes(only_active=False), columns=["ID", "Nome", "Ativo", "Ordem"])
        st.data_editor(sits_db, use_container_width=True, hide_index=True, key="ed_sits")

# Footer com estatísticas
st.divider()
st.caption(f"📊 Total de registros no banco: {count_all()} | Local do Banco: {DB_PATH}")
