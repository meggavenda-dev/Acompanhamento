
# app.py
import os
import streamlit as st
import pandas as pd
from db import init_db, upsert_dataframe, read_all, DB_PATH, count_all
from processing import process_uploaded_file
from export import to_formatted_excel_by_hospital

# --- GitHub sync (baixar/subir o .db) ---
try:
    from github_sync import download_db_from_github, upload_db_to_github
    GITHUB_SYNC_AVAILABLE = True
except Exception:
    GITHUB_SYNC_AVAILABLE = False

# ---- Config GitHub (usa st.secrets; sem UI) ----
GH_OWNER = st.secrets.get("GH_OWNER", "seu-usuario-ou-org")
GH_REPO = st.secrets.get("GH_REPO", "seu-repo")
GH_BRANCH = st.secrets.get("GH_BRANCH", "main")
GH_PATH_IN_REPO = st.secrets.get("GH_DB_PATH", "data/exemplo.db")  # deve coincidir com DB_PATH em db.py
GITHUB_TOKEN_OK = bool(st.secrets.get("GITHUB_TOKEN", ""))

st.set_page_config(page_title="Pacientes por Dia, Prestador e Hospital", layout="wide")

st.title("Pacientes únicos por data, prestador e hospital")
st.caption("Download automático do banco no GitHub → Upload → herança/filtragem/deduplicação → Hospital (lista) → editar Paciente → salvar → exportar → commit automático no GitHub")

# 1) Baixa o DB do GitHub (se existir) antes de inicializar tabelas
if GITHUB_SYNC_AVAILABLE and GITHUB_TOKEN_OK:
    try:
        downloaded = download_db_from_github(
            owner=GH_OWNER,
            repo=GH_REPO,
            path_in_repo=GH_PATH_IN_REPO,
            branch=GH_BRANCH,
            local_db_path=DB_PATH
        )
        if downloaded:
            st.success("Banco baixado do GitHub.")
        else:
            st.info("Banco não encontrado no GitHub (primeiro uso). Será criado localmente ao salvar.")
    except Exception as e:
        st.warning("Não foi possível baixar o banco do GitHub. Verifique token/permissões em st.secrets.")
        st.exception(e)

# Inicializa DB (cria tabela/índices se necessário)
init_db()

# ---------------- Configuração dos Prestadores ----------------
st.subheader("Prestadores alvo")
prestadores_default = ["JOSE.ADORNO", "CASSIO CESAR", "FERNANDO AND", "SIMAO.MATOS"]
prestadores_text = st.text_area(
    "Informe os prestadores (um por linha)",
    value="\n".join(prestadores_default),
    height=120,
    help="A lista é usada para filtrar os registros. A comparação é case-insensitive."
)
prestadores_lista = [p.strip() for p in prestadores_text.splitlines() if p.strip()]

# ---------------- Hospital do arquivo (lista fixa) ----------------
st.subheader("Hospital deste arquivo")
hospital_opcoes = [
    "Hospital Santa Lucia Sul",
    "Hospital Santa Lucia Norte",
    "Hospital Maria Auxiliadora",
]
selected_hospital = st.selectbox(
    "Selecione o Hospital referente à planilha enviada",
    options=hospital_opcoes,
    index=0,
    help="O hospital selecionado será aplicado a todas as linhas processadas deste arquivo."
)

# ---------------- Upload de Arquivo ----------------
st.subheader("Upload de planilha (CSV ou Excel)")
uploaded_file = st.file_uploader(
    "Escolha o arquivo",
    type=["csv", "xlsx", "xls"],
    help="Aceita CSV 'bruto' (sem cabeçalho padronizado) ou planilhas estruturadas."
)

# ---- Estado para manter DF e controle de uploads ----
if "df_final" not in st.session_state:
    st.session_state.df_final = None
if "last_upload_id" not in st.session_state:
    st.session_state.last_upload_id = None
if "editor_key" not in st.session_state:
    st.session_state.editor_key = "editor_pacientes_initial"

# Gera um ID único do upload (arquivo + hospital) para detectar nova importação
def _make_upload_id(file, hospital: str) -> str:
    name = getattr(file, "name", "sem_nome")
    size = getattr(file, "size", 0)
    # hospital influencia o processamento; trocando hospital também deve resetar
    return f"{name}-{size}-{hospital.strip()}"

# Botão para limpar e recomeçar (opcional)
col_reset1, col_reset2 = st.columns(2)
with col_reset1:
    if st.button("🧹 Limpar tabela / reset"):
        st.session_state.df_final = None
        st.session_state.last_upload_id = None
        st.session_state.editor_key = "editor_pacientes_reset"
        st.success("Tabela limpa. Faça novo upload para reprocessar.")

# Processamento (com reset automático do editor em nova importação)
if uploaded_file is not None:
    current_upload_id = _make_upload_id(uploaded_file, selected_hospital)

    # Se for uma nova importação (arquivo/hospital diferente), zera o DF e editor
    if st.session_state.last_upload_id != current_upload_id:
        st.session_state.df_final = None
        st.session_state.editor_key = f"editor_pacientes_{current_upload_id}"
        st.session_state.last_upload_id = current_upload_id

    with st.spinner("Processando arquivo com a lógica consolidada..."):
        try:
            df_final = process_uploaded_file(uploaded_file, prestadores_lista, selected_hospital.strip())
            if df_final is None or len(df_final) == 0:
                st.warning("Nenhuma linha após processamento. Verifique a lista de prestadores e o conteúdo do arquivo.")
                st.session_state.df_final = None
            else:
                st.session_state.df_final = df_final
        except Exception as e:
            st.error("Falha ao processar o arquivo. Verifique o formato da planilha/CSV.")
            st.exception(e)

# ---------------- Revisão / Edição ----------------
if st.session_state.df_final is not None and len(st.session_state.df_final) > 0:
    st.success(f"Processamento concluído! Linhas: {len(st.session_state.df_final)}")

    st.subheader("Revisar e editar nomes de Paciente (opcional)")
    st.caption("Edite apenas a coluna 'Paciente' se necessário. As demais estão bloqueadas para evitar alterações acidentais.")

    # Editor com restrição: somente 'Paciente' editável
    df_to_edit = st.session_state.df_final.sort_values(
        ["Hospital", "Ano", "Mes", "Dia", "Paciente", "Prestador"]
    ).reset_index(drop=True)

    edited_df = st.data_editor(
        df_to_edit,
        use_container_width=True,
        num_rows="fixed",  # não permite adicionar linhas
        column_config={
            "Hospital": st.column_config.TextColumn(disabled=True),
            "Ano": st.column_config.NumberColumn(disabled=True),
            "Mes": st.column_config.NumberColumn(disabled=True),
            "Dia": st.column_config.NumberColumn(disabled=True),
            "Data": st.column_config.TextColumn(disabled=True),
            "Atendimento": st.column_config.TextColumn(disabled=True),
            "Aviso": st.column_config.TextColumn(disabled=True),
            "Convenio": st.column_config.TextColumn(disabled=True),
            "Prestador": st.column_config.TextColumn(disabled=True),
            "Quarto": st.column_config.TextColumn(disabled=True),
            # Paciente permanece editável
            "Paciente": st.column_config.TextColumn(help="Clique para editar o nome do paciente."),
        },
        hide_index=True,
        key=st.session_state.editor_key  # chave única por importação
    )

    # Atualiza o estado com as edições realizadas
    st.session_state.df_final = edited_df

    # ---------------- Gravar no Banco + commit automático no GitHub ----------------
    st.subheader("Persistência")
    if st.button("Salvar no banco (exemplo.db)"):
        try:
            # 1) UPSERT local
            upsert_dataframe(st.session_state.df_final)

            # 2) Contagem para feedback
            total = count_all()
            st.success(f"Dados salvos com sucesso em exemplo.db. Total de linhas no banco: {total}")

            # 3) Commit/push automático para GitHub
            if GITHUB_SYNC_AVAILABLE and GITHUB_TOKEN_OK:
                try:
                    ok = upload_db_to_github(
                        owner=GH_OWNER,
                        repo=GH_REPO,
                        path_in_repo=GH_PATH_IN_REPO,
                        branch=GH_BRANCH,
                        local_db_path=DB_PATH,
                        commit_message="Atualiza banco SQLite via app (salvar no banco)"
                    )
                    if ok:
                        st.success("Sincronização automática com GitHub concluída.")
                except Exception as e:
                    st.error("Falha ao sincronizar com GitHub (commit automático).")
                    st.exception(e)

            # 4) Limpa DF e editor para nova importação
            st.session_state.df_final = None
            st.session_state.editor_key = "editor_pacientes_after_save"

        except Exception as e:
            st.error("Falha ao salvar no banco. Veja detalhes abaixo:")
            st.exception(e)

    # ---------------- Exportar Excel (por Hospital) ----------------
    st.subheader("Exportar Excel (multi-aba por Hospital)")
    excel_bytes = to_formatted_excel_by_hospital(st.session_state.df_final)
    st.download_button(
        label="Baixar Excel por Hospital",
        data=excel_bytes,
        file_name="Pacientes_por_dia_prestador_hospital.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )

# ---------------- Conteúdo atual do banco ----------------
st.divider()
st.subheader("Conteúdo atual do banco (exemplo.db)")
rows = read_all()
if rows:
    cols = ["Hospital", "Ano", "Mes", "Dia", "Data", "Atendimento", "Paciente", "Aviso", "Convenio", "Prestador", "Quarto"]
    db_df = pd.DataFrame(rows, columns=cols)
    st.dataframe(
        db_df.sort_values(["Hospital", "Ano", "Mes", "Dia", "Paciente", "Prestador"]),
        use_container_width=True
    )

    # Exportar direto do banco também (multi-aba por hospital)
    st.subheader("Exportar Excel por Hospital (dados do banco)")
    excel_bytes_db = to_formatted_excel_by_hospital(db_df)
    st.download_button(
        label="Baixar Excel (Banco)",
        data=excel_bytes_db,
        file_name="Pacientes_por_dia_prestador_hospital_banco.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )
else:
    st.info("Banco ainda sem dados. Faça o upload e clique em 'Salvar no banco'.")
