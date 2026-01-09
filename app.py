
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

# ---- Config GitHub (pode pegar defaults de st.secrets) ----
GH_OWNER_DEFAULT = st.secrets.get("GH_OWNER", "seu-usuario-ou-org")
GH_REPO_DEFAULT = st.secrets.get("GH_REPO", "seu-repo")
GH_BRANCH_DEFAULT = st.secrets.get("GH_BRANCH", "main")
GH_PATH_DEFAULT = st.secrets.get("GH_DB_PATH", "data/exemplo.db")  # deve coincidir com DB_PATH

st.set_page_config(page_title="Pacientes por Dia, Prestador e Hospital", layout="wide")

st.title("Pacientes únicos por data, prestador e hospital")
st.caption("Download do banco no GitHub → Upload → herança/filtragem/deduplicação → informar Hospital (lista) → revisar/editar Paciente → Ano/Mes/Dia → salvar → exportar → sincronizar com GitHub")

# 1) Baixa o DB do GitHub (se existir) antes de inicializar tabelas
with st.spinner("Verificando banco no GitHub..."):
    if GITHUB_SYNC_AVAILABLE:
        try:
            downloaded = download_db_from_github(
                owner=GH_OWNER_DEFAULT,
                repo=GH_REPO_DEFAULT,
                path_in_repo=GH_PATH_DEFAULT,
                branch=GH_BRANCH_DEFAULT,
                local_db_path=DB_PATH
            )
            if downloaded:
                st.success("Banco baixado do GitHub.")
            else:
                st.info("Banco não encontrado no GitHub (primeiro uso). Será criado localmente ao salvar.")
        except Exception as e:
            st.warning("Não foi possível baixar o banco do GitHub. Verifique token/permissões.")
            st.exception(e)
    else:
        st.info("Sincronização com GitHub indisponível (módulo github_sync não encontrado).")

# Inicializa DB (cria tabela/índices se necessário)
init_db()

# Info de persistência
with st.expander("ℹ️ Informações de persistência"):
    st.write("Este app salva os dados em um arquivo SQLite local e pode sincronizar com o GitHub.")
    st.code(f"DB_PATH = {DB_PATH}", language="text")
    try:
        if os.path.exists(DB_PATH):
            size_mb = os.path.getsize(DB_PATH) / (1024 * 1024)
            st.caption(f"Arquivo do banco: {DB_PATH} ({size_mb:.2f} MB)")
            with open(DB_PATH, "rb") as f:
                st.download_button(
                    "Baixar arquivo do banco (SQLite)",
                    data=f.read(),
                    file_name="exemplo.db",
                    mime="application/x-sqlite3",
                    help="Baixe o arquivo e versione no GitHub se quiser manter histórico."
                )
        else:
            st.info("O arquivo do banco ainda não existe. Salve alguma carga para gerar o arquivo.")
    except Exception as _e:
        pass

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

    # ---------------- Gravar no Banco ----------------
    st.subheader("Persistência")
    if st.button("Salvar no banco (exemplo.db)"):
        try:
            upsert_dataframe(st.session_state.df_final)
            total = count_all()
            st.success(f"Dados salvos com sucesso em exemplo.db. Total de linhas no banco: {total}")
            try:
                if os.path.exists(DB_PATH):
                    size_mb = os.path.getsize(DB_PATH) / (1024 * 1024)
                    st.caption(f"Arquivo do banco: {DB_PATH} ({size_mb:.2f} MB)")
            except Exception:
                pass
            # Após salvar, limpar DF e editor para nova importação
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
try:
    if os.path.exists(DB_PATH):
        size_mb = os.path.getsize(DB_PATH) / (1024 * 1024)
        st.caption(f"Arquivo do banco: {DB_PATH} ({size_mb:.2f} MB)")
    else:
        st.caption(f"Arquivo do banco não encontrado em: {DB_PATH}")
except Exception:
    pass

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
    try:
        total_now = count_all()
        st.caption(f"Contagem direta via SQL: {total_now} linha(s).")
    except Exception:
        pass

# ---------------- Sincronização com GitHub ----------------
with st.expander("🔄 Sincronização com GitHub"):
    if not GITHUB_SYNC_AVAILABLE:
        st.warning("Sincronização indisponível: módulo github_sync.py não encontrado.")
        st.stop()

    st.caption("Faça commit/push do arquivo ./data/exemplo.db no repositório para manter persistência entre reinícios.")
    owner = st.text_input("Owner/Org", GH_OWNER_DEFAULT)
    repo = st.text_input("Repository", GH_REPO_DEFAULT)
    branch = st.text_input("Branch", GH_BRANCH_DEFAULT)
    path_in_repo = st.text_input("Caminho no repo", GH_PATH_DEFAULT)
    commit_message = st.text_input("Mensagem de commit", "Atualiza banco SQLite via app")

    col_sync1, col_sync2 = st.columns(2)
    with col_sync1:
        if st.button("⬆️ Subir banco para GitHub (commit)"):
            try:
                ok = upload_db_to_github(owner, repo, path_in_repo, branch, DB_PATH, commit_message)
                if ok:
                    st.success("Sincronizado com GitHub com sucesso.")
            except Exception as e:
                st.error("Falha ao sincronizar: verifique GITHUB_TOKEN/permite repo/write, owner/repo/branch/caminho.")
                st.exception(e)

    with col_sync2:
        if st.button("⬇️ Baixar banco do GitHub (pull)"):
            try:
                downloaded = download_db_from_github(owner, repo, path_in_repo, branch, DB_PATH)
                if downloaded:
                    st.success("Banco baixado do GitHub e sobrescrito localmente. Recarregando app...")
                    # Para garantir que o engine do SQLite recarregue o arquivo, peça um rerun
                    st.rerun()
                else:
                    st.info("Arquivo ainda não existe no repositório.")
            except Exception as e:
                st.error("Falha ao baixar do GitHub.")
                st.exception(e)
