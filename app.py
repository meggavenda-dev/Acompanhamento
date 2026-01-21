
# -*- coding: utf-8 -*-
import os
from datetime import datetime
from io import BytesIO
import streamlit as st
import pandas as pd

from db import (
    # Básico / manutenção
    init_db, ensure_db_writable, vacuum, DB_PATH,

    # Base (pacientes)
    upsert_dataframe, read_all, count_all,
    find_registros_para_prefill, list_registros_base_all,
    delete_all_pacientes, upsert_paciente_single, delete_paciente_by_key,

    # Catálogos
    list_procedimento_tipos, set_procedimento_tipo_status, upsert_procedimento_tipo,
    list_cirurgia_situacoes, set_cirurgia_situacao_status, upsert_cirurgia_situacao,

    # Cirurgias
    list_cirurgias, insert_or_update_cirurgia, delete_cirurgia,
    delete_cirurgia_by_key, delete_cirurgias_by_filter,

    # Resets
    delete_all_cirurgias, delete_all_catalogos, hard_reset_local_db,
)
from processing import process_uploaded_file
from export import to_formatted_excel_by_hospital, to_formatted_excel_cirurgias

# --- GitHub sync (baixar/subir o .db) ---
try:
    from github_sync import (
        download_db_from_github,
        safe_upload_with_merge,
        upload_db_to_github,
        get_remote_sha,  # usado para atualizar SHA após upload
    )
    GITHUB_SYNC_AVAILABLE = True
except Exception:
    GITHUB_SYNC_AVAILABLE = False

# ---- Config GitHub (usa st.secrets; sem UI) ----
GH_OWNER = st.secrets.get("GH_OWNER", "seu-usuario-ou-org")
GH_REPO  = st.secrets.get("GH_REPO", "seu-repo")
GH_BRANCH = st.secrets.get("GH_BRANCH", "main")
GH_PATH_IN_REPO = st.secrets.get("GH_DB_PATH", "data/exemplo.db")
GITHUB_TOKEN_OK = bool(st.secrets.get("GITHUB_TOKEN", ""))

# ---- Versão de cache do app (mude quando alterar a lógica de processamento) ----
APP_CACHE_VERSION = "v1.0.0"

st.set_page_config(page_title="Gestão de Pacientes e Cirurgias", layout="wide")

# ---------------------------
# Boot cleanup do cache de dados (uma vez por sessão)
# ---------------------------
def _boot_session_cleanup():
    st.cache_data.clear()

if not st.session_state.get("_boot_cleanup_done"):
    _boot_session_cleanup()
    st.session_state["_boot_cleanup_done"] = True

# ---------------------------
# Helper: VACUUM seguro + limpeza de cache pós-manutenção
# ---------------------------
def try_vacuum_safely():
    """Tenta executar VACUUM; se DB estiver read-only, não interrompe a UI. Limpa cache de dados após sucesso."""
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

# =========================
# Startup: baixar .db se não existir ou se parecer "vazio" (apenas schema)
# =========================
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

# =======================
# Sidebar: Sincronização + Diagnóstico
# =======================
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

    st.markdown("---")
    st.caption("Diagnóstico de versão")
    st.write(f"Versão atual (SHA): `{st.session_state.get('gh_sha', 'desconhecida')}`")
    if os.path.exists(DB_PATH):
        try:
            st.write(f"Tamanho do .db: {os.path.getsize(DB_PATH)} bytes")
            db_dir = os.path.dirname(DB_PATH) or "."
            st.caption(f"DB_DIR: {db_dir}")
            st.caption(f"Diretório gravável: {os.access(db_dir, os.W_OK)}")
        except Exception:
            pass

# =======================
# 🧨 Área de risco (Reset)
# =======================
with st.sidebar:
    st.markdown("---")
    st.markdown("### 🧨 Área de risco (Reset)")
    st.caption("Atenção: ações destrutivas. Exporte o Excel para backup antes.")

    confirmar = st.checkbox("Eu entendo que isso **não pode ser desfeito**.")
    confirma_texto = st.text_input("Digite **RESET** para confirmar:", value="")

    def _sync_after_reset(commit_message: str):
        """Para resets: sobe a versão local para o GitHub com detalhes."""
        if GITHUB_SYNC_AVAILABLE and GITHUB_TOKEN_OK:
            try:
                ok, new_sha, status, msg = upload_db_to_github(
                    owner=GH_OWNER,
                    repo=GH_REPO,
                    path_in_repo=GH_PATH_IN_REPO,
                    branch=GH_BRANCH,
                    local_db_path=DB_PATH,
                    commit_message=commit_message,
                    prev_sha=st.session_state.get("gh_sha"),
                    _return_details=True
                )
                if ok:
                    st.session_state["gh_sha"] = new_sha or st.session_state.get("gh_sha")
                    st.success("Sincronização automática com GitHub concluída.")
                else:
                    st.error(f"Falha ao sincronizar com GitHub (status={status}). {msg}")
            except Exception as e:
                st.error("Falha ao sincronizar com GitHub.")
                st.exception(e)

    can_execute = confirmar and (confirma_texto.strip().upper() == "RESET")

    col_r1, col_r2 = st.columns(2)
    with col_r1:
        if st.button("Apagar **PACIENTES** (tabela base)", type="secondary", disabled=not can_execute):
            try:
                ensure_db_writable()
                apagados = delete_all_pacientes()
                st.cache_data.clear()
                try_vacuum_safely()
                st.success(f"✅ {apagados} paciente(s) apagado(s) do banco.")
                _sync_after_reset(f"Reset: apaga {apagados} pacientes")
                st.rerun()
            except Exception as e:
                st.error("Falha ao apagar pacientes.")
                st.exception(e)

    with col_r2:
        if st.button("Apagar **CIRURGIAS**", type="secondary", disabled=not can_execute):
            try:
                ensure_db_writable()
                apagadas = delete_all_cirurgias()
                st.cache_data.clear()
                try_vacuum_safely()
                st.session_state.pop("editor_lista_cirurgias_union", None)
                st.success(f"✅ {apagadas} cirurgia(s) apagada(s) do banco.")
                _sync_after_reset(f"Reset: apaga {apagadas} cirurgias")
                st.rerun()
            except Exception as e:
                st.error("Falha ao apagar cirurgias.")
                st.exception(e)

    col_r3, col_r4 = st.columns(2)
    with col_r3:
        if st.button("Apagar **CATÁLOGOS** (Tipos/Situações)", type="secondary", disabled=not can_execute):
            try:
                ensure_db_writable()
                apagados = delete_all_catalogos()
                st.cache_data.clear()
                try_vacuum_safely()
                st.success(f"✅ {apagados} registro(s) apagado(s) dos catálogos.")
                _sync_after_reset(f"Reset: apaga {apagados} catálogos")
                st.rerun()
            except Exception as e:
                st.error("Falha ao apagar catálogos.")
                st.exception(e)

    with col_r4:
        if st.button("🗑️ **RESET TOTAL** (apaga arquivo .db)", type="primary", disabled=not can_execute):
            try:
                hard_reset_local_db()
                st.cache_data.clear()
                st.success("Banco recriado vazio (local).")
                _sync_after_reset("Reset total: recria .db vazio")
                st.session_state["gh_db_fetched"] = True
                st.rerun()
            except Exception as e:
                st.error("Falha no reset total.")
                st.exception(e)

# Inicializa DB
init_db()

# -----------------------------------------------------------------
# Cache de dados (com TTL + versão) para processamento de arquivo
# -----------------------------------------------------------------
@st.cache_data(ttl=900, show_spinner=False)
def _process_file_cached(file_bytes: bytes, file_name: str, prestadores_lista: list, hospital: str,
                         upload_id: str, app_cache_version: str) -> pd.DataFrame:
    bio = BytesIO(file_bytes)
    try:
        bio.name = file_name or "upload.bin"
    except Exception:
        pass
    df = process_uploaded_file(bio, prestadores_lista, hospital)
    return pd.DataFrame(df) if df is not None else pd.DataFrame()

# Diagnóstico rápido do arquivo .db
with st.expander("🔎 Diagnóstico do arquivo .db (local)", expanded=False):
    exists = os.path.exists(DB_PATH)
    size = os.path.getsize(DB_PATH) if exists else 0
    st.caption(f"Caminho: `{DB_PATH}` | Existe: {exists} | Tamanho: {size} bytes | Linhas (pacientes): {count_all()}")

# Lista única de hospitais
HOSPITAL_OPCOES = [
    "Hospital Santa Lucia Sul",
    "Hospital Santa Lucia Norte",
    "Hospital Maria Auxiliadora",
]

# ---------------- Abas ----------------
tabs = st.tabs([
    "📥 Importação & Pacientes",
    "🩺 Cirurgias",
    "📚 Cadastro (Tipos & Situações)",
    "📄 Tipos (Lista)"
])


# =======================
# Inicializa DB
# =======================
init_db()

# -----------------------------------------------------------------
# Cache de dados (com TTL + versão) para processamento de arquivo
# -----------------------------------------------------------------
@st.cache_data(ttl=900, show_spinner=False)
def _process_file_cached(file_bytes: bytes, file_name: str, prestadores_lista: list, hospital: str,
                         upload_id: str, app_cache_version: str) -> pd.DataFrame:
    bio = BytesIO(file_bytes)
    try:
        bio.name = file_name or "upload.bin"
    except Exception:
        pass
    df = process_uploaded_file(bio, prestadores_lista, hospital)
    return pd.DataFrame(df) if df is not None else pd.DataFrame()

# Diagnóstico rápido do arquivo .db
with st.expander("🔎 Diagnóstico do arquivo .db (local)", expanded=False):
    exists = os.path.exists(DB_PATH)
    size = os.path.getsize(DB_PATH) if exists else 0
    st.caption(f"Caminho: `{DB_PATH}` | Existe: {exists} | Tamanho: {size} bytes | Linhas (pacientes): {count_all()}")

# Lista única de hospitais
HOSPITAL_OPCOES = [
    "Hospital Santa Lucia Sul",
    "Hospital Santa Lucia Norte",
    "Hospital Maria Auxiliadora",
]

# ---------------- Abas ----------------
tabs = st.tabs([
    "📥 Importação & Pacientes",
    "🩺 Cirurgias",
    "📚 Cadastro (Tipos & Situações)",
    "📄 Tipos (Lista)"
])

# ====================================================================================
# 📥 Aba 1: Importação & Pacientes
# ====================================================================================
with tabs[0]:
    st.subheader("Pacientes únicos por data, prestador e hospital")
    st.caption("Upload → processamento cacheado (TTL) → Revisar/Editar/Selecionar → Salvar apenas selecionados → Exportar → sync GitHub")

    st.markdown("#### Prestadores alvo")
    prestadores_default = ["JOSE.ADORNO", "CASSIO CESAR", "FERNANDO AND", "SIMAO.MATOS"]
    prestadores_text = st.text_area(
        "Informe os prestadores (um por linha)",
        value="\n".join(prestadores_default),
        height=120,
        help="A lista é usada para filtrar os registros. A comparação é case-insensitive."
    )
    prestadores_lista = [p.strip() for p in prestadores_text.splitlines() if p.strip()]

    st.markdown("#### Hospital deste arquivo")
    selected_hospital = st.selectbox(
        "Selecione o Hospital referente à planilha enviada",
        options=HOSPITAL_OPCOES,
        index=0,
        help="Aplicado a todas as linhas processadas deste arquivo."
    )

    st.markdown("#### Upload de planilha (CSV ou Excel)")
    uploaded_file = st.file_uploader(
        "Escolha o arquivo",
        type=["csv", "xlsx", "xls"],
        help="Aceita CSV 'bruto' ou planilhas estruturadas."
    )

    if "pacientes_select_default" not in st.session_state:
        st.session_state["pacientes_select_default"] = True

    if "df_final" not in st.session_state:
        st.session_state.df_final = None
    if "last_upload_id" not in st.session_state:
        st.session_state.last_upload_id = None
    if "editor_key" not in st.session_state:
        st.session_state.editor_key = "editor_pacientes_initial"

    def _make_upload_id(file, hospital: str) -> str:
        name = getattr(file, "name", "sem_nome")
        size = getattr(file, "size", 0)
        return f"{name}-{size}-{hospital.strip()}"

    col_reset1, col_reset2 = st.columns(2)
    with col_reset1:
        if st.button("🧹 Limpar tabela / reset"):
            st.session_state.df_final = None
            st.session_state.last_upload_id = None
            st.session_state.editor_key = "editor_pacientes_reset"
            st.success("Tabela limpa. Faça novo upload para reprocessar.")
    with col_reset2:
        col_sel_all, col_sel_none = st.columns(2)
        with col_sel_all:
            if st.button("✅ Selecionar todos"):
                st.session_state["pacientes_select_default"] = True
                st.success("Todos marcados para importar.")
                st.rerun()
        with col_sel_none:
            if st.button("❌ Desmarcar todos"):
                st.session_state["pacientes_select_default"] = False
                st.info("Todos desmarcados; escolha manualmente quem importar.")
                st.rerun()

    if uploaded_file is not None:
        current_upload_id = _make_upload_id(uploaded_file, selected_hospital)
        if st.session_state.last_upload_id != current_upload_id:
            st.cache_data.clear()
            st.session_state.df_final = None
            st.session_state.editor_key = f"editor_pacientes_{current_upload_id}"
            st.session_state.last_upload_id = current_upload_id

        with st.spinner("Processando arquivo (cacheado com TTL)..."):
            try:
                file_name = getattr(uploaded_file, "name", "upload.bin")
                file_bytes = uploaded_file.getvalue()
                df_final = _process_file_cached(
                    file_bytes=file_bytes,
                    file_name=file_name,
                    prestadores_lista=prestadores_lista,
                    hospital=selected_hospital.strip(),
                    upload_id=current_upload_id,
                    app_cache_version=APP_CACHE_VERSION
                )
                if df_final is None or len(df_final) == 0:
                    st.warning("Nenhuma linha após processamento. Verifique a lista de prestadores e o conteúdo do arquivo.")
                    st.session_state.df_final = None
                else:
                    st.session_state.df_final = df_final
            except Exception as e:
                st.error("Falha ao processar o arquivo.")
                st.exception(e)

    if st.session_state.df_final is not None and len(st.session_state.df_final) > 0:
        st.success(f"Processamento concluído! Linhas: {len(st.session_state.df_final)}")

        st.markdown("#### Revisar, editar e selecionar pacientes")
        df_to_edit = st.session_state.df_final.sort_values(
            ["Hospital", "Ano", "Mes", "Dia", "Paciente", "Prestador"]
        ).reset_index(drop=True)

        default_select = bool(st.session_state.get("pacientes_select_default", True))
        if "Selecionar" not in df_to_edit.columns:
            df_to_edit["Selecionar"] = default_select
        else:
            df_to_edit["Selecionar"] = df_to_edit["Selecionar"].fillna(default_select).astype(bool)

        edited_df = st.data_editor(
            df_to_edit,
            use_container_width=True,
            num_rows="fixed",
            column_config={
                "Selecionar": st.column_config.CheckboxColumn(help="Marque para importar"),
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
                "Paciente": st.column_config.TextColumn(help="Clique para editar o nome do paciente."),
            },
            hide_index=True,
            key=st.session_state.editor_key
        )
        edited_df = pd.DataFrame(edited_df)

        df_selecionados = edited_df[edited_df["Selecionar"] == True].copy()

        st.markdown("#### Persistência (apenas selecionados)")
        if st.button("💾 Salvar apenas selecionados no banco"):
            try:
                if df_selecionados.empty:
                    st.warning("Nenhum paciente selecionado para importação.")
                else:
                    ensure_db_writable()
                    salvos, ignoradas = upsert_dataframe(df_selecionados.drop(columns=["Selecionar"], errors="ignore"))
                    st.cache_data.clear()
                    total = count_all()
                    st.success(f"Importação concluída: {salvos} salvos, {ignoradas} ignorados (chave incompleta). Total no banco: {total}")

                    try_vacuum_safely()

                    if GITHUB_SYNC_AVAILABLE and GITHUB_TOKEN_OK:
                        try:
                            ok, status, msg = safe_upload_with_merge(
                                owner=GH_OWNER,
                                repo=GH_REPO,
                                path_in_repo=GH_PATH_IN_REPO,
                                branch=GH_BRANCH,
                                local_db_path=DB_PATH,
                                commit_message=f"Atualiza banco SQLite via app (salvar pacientes selecionados: {salvos})",
                                prev_sha=st.session_state.get("gh_sha"),
                                _return_details=True
                            )
                            if ok:
                                new_sha = get_remote_sha(GH_OWNER, GH_REPO, GH_PATH_IN_REPO, GH_BRANCH)
                                if new_sha:
                                    st.session_state["gh_sha"] = new_sha
                                st.success("Sincronização automática com GitHub concluída.")
                            else:
                                st.error(f"Falha ao sincronizar com GitHub (status={status}). {msg}")
                        except Exception as e:
                            st.error("Falha ao sincronizar com GitHub.")
                            st.exception(e)

                    st.session_state.editor_key = "editor_pacientes_after_save"

            except PermissionError as pe:
                st.error(f"Diretório/arquivo do DB não é gravável. Ajuste 'DB_DIR' ou permissões. Detalhe: {pe}")
            except Exception as e:
                st.error("Falha ao salvar no banco.")
                st.exception(e)

        st.markdown("#### Exportar Excel (multi-aba por Hospital)")
        df_for_export = pd.DataFrame(edited_df.drop(columns=["Selecionar"], errors="ignore"))
        excel_bytes = to_formatted_excel_by_hospital(df_for_export)
        st.download_button(
            label="Baixar Excel por Hospital (arquivo atual)",
            data=excel_bytes,
            file_name="Pacientes_por_dia_prestador_hospital.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

    st.divider()
    st.markdown("#### Conteúdo atual do banco (exemplo.db)")
    rows = read_all()
    if rows:
        cols = ["Hospital", "Ano", "Mes", "Dia", "Data", "Atendimento", "Paciente", "Aviso", "Convenio", "Prestador", "Quarto"]
        db_df = pd.DataFrame(rows, columns=cols)
        st.dataframe(
            db_df.sort_values(["Hospital", "Ano", "Mes", "Dia", "Paciente", "Prestador"]),
            use_container_width=True
        )
        st.markdown("##### Exportar Excel (dados do banco)")
        excel_bytes_db = to_formatted_excel_by_hospital(db_df)
        st.download_button(
            label="Baixar Excel (Banco)",
            data=excel_bytes_db,
            file_name="Pacientes_por_dia_prestador_hospital_banco.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
    else:
        st.info("Banco ainda sem dados. Faça o upload, selecione e clique em 'Salvar apenas selecionados no banco'.")





# ====================================================================================
# 🩺 Aba 2: Cirurgias — União (ocultando colunas técnicas) + salvar mover/atualizar
# ====================================================================================
with tabs[1]:
    st.subheader("Cadastrar / Editar Cirurgias")

    # 1. Filtros de carregamento
    st.markdown("#### Filtros para carregar pacientes")
    colF0, colF1, colF2, colF3 = st.columns([1, 1, 1, 1])
    with colF0:
        usar_periodo = st.checkbox("Filtrar por Ano/Mês", value=True)
    with colF1:
        hosp_cad = st.selectbox("Hospital", options=HOSPITAL_OPCOES, index=0)
    
    now = datetime.now()
    with colF2:
        ano_cad = st.number_input("Ano", min_value=2000, value=now.year, disabled=not usar_periodo)
    with colF3:
        mes_cad = st.number_input("Mês", min_value=1, max_value=12, value=now.month, disabled=not usar_periodo)

    prestadores_filtro = st.text_input("Filtrar Prestadores (separar por ; )", value="")
    prestadores_lista_filtro = [p.strip() for p in prestadores_filtro.split(";") if p.strip()]

    # 2. Carregamento de Catálogos (Mapeamentos)
    tipos_rows_all = list_procedimento_tipos(only_active=False)
    df_tipos_all = pd.DataFrame(tipos_rows_all, columns=["id", "nome", "ativo", "ordem"])
    tipo_id2nome = dict(zip(df_tipos_all["id"], df_tipos_all["nome"]))
    tipo_nome2id = {str(r["nome"]).strip(): r["id"] for _, r in df_tipos_all.iterrows() if r["ativo"] == 1}
    tipo_nome_list = sorted(tipo_nome2id.keys())

    sits_rows_all = list_cirurgia_situacoes(only_active=False)
    df_sits_all = pd.DataFrame(sits_rows_all, columns=["id", "nome", "ativo", "ordem"])
    sit_id2nome = dict(zip(df_sits_all["id"], df_sits_all["nome"]))
    sit_nome2id = {str(r["nome"]).strip(): r["id"] for _, r in df_sits_all.iterrows() if r["ativo"] == 1}
    sit_nome_list = sorted(sit_nome2id.keys())

    # 3. Filtro de Situação
    sit_filter_nomes = st.multiselect("Filtrar por Situação (ignora candidatos da Base)", options=sit_nome_list)
    sit_filter_ids = [sit_nome2id[n] for n in sit_filter_nomes if n in sit_nome2id]
    ignorar_base = len(sit_filter_ids) > 0

    # 4. Construção da União (Union)
    try:
        ano_mes_str = f"{int(ano_cad)}-{int(mes_cad):02d}" if (usar_periodo and not ignorar_base) else None
        rows_cir = list_cirurgias(hospital=hosp_cad, ano_mes=ano_mes_str, situacoes=sit_filter_ids if ignorar_base else None)
        
        df_cir = pd.DataFrame(rows_cir, columns=[
            "id", "Hospital", "Atendimento", "Paciente", "Prestador", "Data_Cirurgia",
            "Convenio", "Procedimento_Tipo_ID", "Situacao_ID", "Guia_AMHPTISS", 
            "Guia_AMHPTISS_Complemento", "Fatura", "Observacoes", "created_at", "updated_at"
        ])

        df_base_mapped = pd.DataFrame()
        if not ignorar_base:
            base_rows = find_registros_para_prefill(hosp_cad, ano=int(ano_cad) if usar_periodo else None, 
                                                   mes=int(mes_cad) if usar_periodo else None, 
                                                   prestadores=prestadores_lista_filtro)
            if base_rows:
                df_base = pd.DataFrame(base_rows, columns=["Hospital", "Data", "Atendimento", "Paciente", "Convenio", "Prestador"])
                df_base_mapped = pd.DataFrame({
                    "Hospital": df_base["Hospital"], "Atendimento": df_base["Atendimento"],
                    "Paciente": df_base["Paciente"], "Prestador": df_base["Prestador"],
                    "Data_Cirurgia": df_base["Data"], "Convenio": df_base["Convenio"],
                    "Fonte": "Base"
                })

        df_union = pd.concat([df_cir, df_base_mapped], ignore_index=True)
        df_union["has_id"] = df_union["id"].notna()
        df_union = df_union.sort_values("has_id", ascending=False).drop_duplicates(
            subset=["Hospital", "Atendimento", "Paciente", "Prestador", "Data_Cirurgia"], keep="first"
        )

        df_union["Tipo (nome)"] = df_union["Procedimento_Tipo_ID"].map(tipo_id2nome).fillna("")
        df_union["Situação (nome)"] = df_union["Situacao_ID"].map(sit_id2nome).fillna("")

        # 5. Recuperação de Snapshot
        if "cirurgias_editadas_snapshot" in st.session_state:
            snap = st.session_state["cirurgias_editadas_snapshot"]
            keys = ["Hospital", "Atendimento", "Paciente", "Prestador", "Data_Cirurgia"]
            df_union = df_union.merge(snap, on=keys, how="left", suffixes=("", "_snap"))
            
            for col in ["Tipo (nome)", "Situação (nome)", "Convenio", "Guia_AMHPTISS", "Fatura", "Observacoes"]:
                if f"{col}_snap" in df_union.columns:
                    df_union[col] = df_union[f"{col}_snap"].combine_first(df_union[col].astype(str))
            df_union = df_union.drop(columns=[c for c in df_union.columns if c.endswith("_snap")])

        # 6. Grid de Edição
        st.markdown("#### Grid de Edição")
        cols_view = ["Hospital", "Atendimento", "Paciente", "Prestador", "Data_Cirurgia", "Convenio", 
                     "Tipo (nome)", "Situação (nome)", "Guia_AMHPTISS", "Fatura", "Observacoes"]
        
        edited_df = st.data_editor(
            df_union[cols_view],
            use_container_width=True,
            column_config={
                "Tipo (nome)": st.column_config.SelectboxColumn(options=[""] + tipo_nome_list),
                "Situação (nome)": st.column_config.SelectboxColumn(options=[""] + sit_nome_list),
            },
            key="editor_cirurgias_final"
        )
        st.session_state["cirurgias_editadas_snapshot"] = edited_df

        # --- CORREÇÃO AQUI: DEFINIÇÃO DAS COLUNAS PARA OS BOTÕES ---
        col_btn_save, col_btn_export = st.columns(2)

        with col_btn_save:
            if st.button("💾 Salvar Alterações", use_container_width=True):
                ensure_db_writable()
                for _, r in edited_df.iterrows():
                    if r["Tipo (nome)"] or r["Situação (nome)"]:
                        payload = {
                            "Hospital": r["Hospital"], "Atendimento": r["Atendimento"],
                            "Paciente": r["Paciente"], "Prestador": r["Prestador"],
                            "Data_Cirurgia": r["Data_Cirurgia"], "Convenio": r["Convenio"],
                            "Procedimento_Tipo_ID": tipo_nome2id.get(r["Tipo (nome)"]),
                            "Situacao_ID": sit_nome2id.get(r["Situação (nome)"]),
                            "Guia_AMHPTISS": r["Guia_AMHPTISS"], "Fatura": r["Fatura"],
                            "Observacoes": r["Observacoes"]
                        }
                        insert_or_update_cirurgia(payload)
                st.success("Dados salvos e sincronizados!")
                st.cache_data.clear()
                st.rerun()

        with col_btn_export:
            if st.button("📊 Preparar Excel para Download", use_container_width=True):
                try:
                    df_export = edited_df.copy()
                    excel_data = to_formatted_excel_cirurgias(df_export)
                    st.download_button(
                        label="⬇️ Clique para Baixar Planilha",
                        data=excel_data,
                        file_name=f"Cirurgias_{datetime.now().strftime('%d_%m_%Y')}.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        use_container_width=True
                    )
                except Exception as e:
                    st.error(f"Erro ao gerar Excel: {e}")
       
        with st.expander("🗑️ Exclusão em lote (por filtros)", expanded=False):
            st.caption("Hospital é obrigatório. Demais filtros opcionais; se todos vazios, nada será apagado.")
            hosp_del = st.selectbox("Hospital", options=HOSPITAL_OPCOES, index=0, key="del_hosp")
            atts_raw = st.text_area("Atendimentos (um por linha)", value="", height=120, key="del_atts")
            prests_raw = st.text_area("Prestadores (um por linha)", value="", height=120, key="del_prests")
            datas_raw = st.text_area("Datas de Cirurgia (um por linha, ex.: 10/10/2025)", value="", height=120, key="del_datas")

            def _to_list(raw: str):
                return [ln.strip() for ln in raw.splitlines() if ln.strip()]

            if st.button("Apagar por filtros (lote)"):
                try:
                    ensure_db_writable()
                    total_apagadas = delete_cirurgias_by_filter(
                        hospital=hosp_del,
                        atendimentos=_to_list(atts_raw),
                        prestadores=_to_list(prests_raw),
                        datas=_to_list(datas_raw)
                    )
                    st.cache_data.clear()
                    try_vacuum_safely()
                    st.success(f"{total_apagadas} cirurgia(s) apagada(s) com sucesso.")

                    if GITHUB_SYNC_AVAILABLE and GITHUB_TOKEN_OK:
                        ok, status, msg = safe_upload_with_merge(
                            owner=GH_OWNER, repo=GH_REPO, path_in_repo=GH_PATH_IN_REPO,
                            branch=GH_BRANCH, local_db_path=DB_PATH,
                            commit_message=f"Exclusão em lote de cirurgias ({total_apagadas} apagadas)",
                            prev_sha=st.session_state.get("gh_sha"),
                            _return_details=True
                        )
                        if ok:
                            new_sha = get_remote_sha(GH_OWNER, GH_REPO, GH_PATH_IN_REPO, GH_BRANCH)
                            if new_sha: st.session_state["gh_sha"] = new_sha
                            st.success("Sincronização automática com GitHub concluída.")
                        else:
                            st.error(f"Falha ao sincronizar com GitHub (status={status}). {msg}")

                    st.rerun()
                except Exception as e:
                    st.error("Falha na exclusão em lote.")
                    st.exception(e)

        with st.expander("🔎 Diagnóstico rápido (ver primeiros registros da base)", expanded=False):
            if st.button("Ver todos (limite 500)"):
                try:
                    rows_all = list_registros_base_all(500)
                    df_all = pd.DataFrame(rows_all, columns=["Hospital", "Data", "Atendimento", "Paciente", "Convenio", "Prestador"])
                    st.dataframe(df_all, use_container_width=True, height=300)
                except Exception as e:
                    st.error("Erro ao listar registros base.")
                    st.exception(e)

    except Exception as e:
        st.error("Erro ao montar a lista de cirurgias.")
        st.exception(e)
# ====================================================================================
# 📚 Aba 3: Cadastro (Tipos & Situações)
# ====================================================================================
with tabs[2]:
    st.subheader("Catálogos de Tipos de Procedimento e Situações da Cirurgia")

    st.markdown("#### Tipos de Procedimento")
    colA, colB = st.columns([2, 1])

    if "tipo_form_reset" not in st.session_state:
        st.session_state["tipo_form_reset"] = 0
    if "tipo_bulk_reset" not in st.session_state:
        st.session_state["tipo_bulk_reset"] = 0

    df_tipos_cached = st.session_state.get("df_tipos_cached")
    if df_tipos_cached is None:
        tipos_all = list_procedimento_tipos(only_active=False)
        if tipos_all:
            df_tipos_cached = pd.DataFrame(tipos_all, columns=["id", "nome", "ativo", "ordem"])
        else:
            df_tipos_cached = pd.DataFrame(columns=["id", "nome", "ativo", "ordem"])
        st.session_state["df_tipos_cached"] = df_tipos_cached

    def _next_ordem_from_cache(df: pd.DataFrame) -> int:
        if df.empty or "ordem" not in df.columns:
            return 1
        try:
            return int(pd.to_numeric(df["ordem"], errors="coerce").max() or 0) + 1
        except Exception:
            return 1

    next_tipo_ordem = _next_ordem_from_cache(df_tipos_cached)

    def _upload_db_catalogo(commit_msg: str):
        if GITHUB_SYNC_AVAILABLE and GITHUB_TOKEN_OK:
            try:
                try_vacuum_safely()
                ok, status, msg = safe_upload_with_merge(
                    owner=GH_OWNER,
                    repo=GH_REPO,
                    path_in_repo=GH_PATH_IN_REPO,
                    branch=GH_BRANCH,
                    local_db_path=DB_PATH,
                    commit_message=commit_msg,
                    prev_sha=st.session_state.get("gh_sha"),
                    _return_details=True
                )
                if ok:
                    new_sha = get_remote_sha(GH_OWNER, GH_REPO, GH_PATH_IN_REPO, GH_BRANCH)
                    if new_sha:
                        st.session_state["gh_sha"] = new_sha
                    st.success("Sincronização automática com GitHub concluída.")
                else:
                    st.error(f"Falha ao sincronizar com GitHub (status={status}). {msg}")
            except Exception as e:
                st.error("Falha ao sincronizar com GitHub.")
                st.exception(e)

    def _save_tipo_and_reset():
        try:
            suffix = st.session_state["tipo_form_reset"]
            tipo_nome = (st.session_state.get(f"tipo_nome_input_{suffix}") or "").strip()
            if not tipo_nome:
                st.warning("Informe um nome de Tipo antes de salvar.")
                return
            tipo_ordem = int(st.session_state.get(f"tipo_ordem_input_{suffix}", next_tipo_ordem))
            tipo_ativo = bool(st.session_state.get(f"tipo_ativo_input_{suffix}", True))

            ensure_db_writable()
            tid = upsert_procedimento_tipo(tipo_nome, int(tipo_ativo), int(tipo_ordem))
            st.cache_data.clear()
            st.success(f"Tipo salvo (id={tid}).")

            tipos_all2 = list_procedimento_tipos(only_active=False)
            df2 = pd.DataFrame(tipos_all2, columns=["id", "nome", "ativo", "ordem"]) if tipos_all2 else pd.DataFrame(columns=["id", "nome", "ativo", "ordem"])
            st.session_state["df_tipos_cached"] = df2

            prox_id = (df2["id"].max() + 1) if not df2.empty else 1
            st.info(f"Próximo ID previsto: {prox_id}")

            _upload_db_catalogo("Atualiza catálogo de Tipos (salvar individual)")
        except PermissionError as pe:
            st.error(f"Diretório/arquivo do DB não é gravável. Ajuste 'DB_DIR' ou permissões. Detalhe: {pe}")
        except Exception as e:
            msg = str(e).lower()
            if "readonly" in msg or "read-only" in msg:
                st.error("Banco está em modo somente leitura. Mova o .db para diretório gravável (DB_DIR) ou ajuste permissões.")
            else:
                st.error("Falha ao salvar tipo.")
                st.exception(e)
        finally:
            st.session_state["tipo_form_reset"] += 1

    with colA:
        suffix = st.session_state["tipo_form_reset"]
        st.text_input("Novo tipo / atualizar por nome", placeholder="Ex.: Colecistectomia", key=f"tipo_nome_input_{suffix}")
        st.number_input("Ordem (para ordenar listagem)", min_value=0, value=next_tipo_ordem, step=1, key=f"tipo_ordem_input_{suffix}")
        st.checkbox("Ativo", value=True, key=f"tipo_ativo_input_{suffix}")
        st.button("Salvar tipo de procedimento", on_click=_save_tipo_and_reset)

        st.markdown("##### Cadastrar vários tipos (em lote)")
        bulk_suffix = st.session_state["tipo_bulk_reset"]
        st.caption("Informe um tipo por linha. Ex.: Consulta\nECG\nRaio-X")
        st.text_area("Tipos (um por linha)", height=120, key=f"tipo_bulk_input_{bulk_suffix}")
        st.number_input("Ordem inicial (auto-incrementa)", min_value=0, value=next_tipo_ordem, step=1, key=f"tipo_bulk_ordem_{bulk_suffix}")
        st.checkbox("Ativo (padrão)", value=True, key=f"tipo_bulk_ativo_{bulk_suffix}")

        def _save_tipos_bulk_and_reset():
            try:
                suffix = st.session_state["tipo_bulk_reset"]
                raw_text = st.session_state.get(f"tipo_bulk_input_{suffix}", "") or ""
                start_ordem = int(st.session_state.get(f"tipo_bulk_ordem_{suffix}", next_tipo_ordem))
                ativo_padrao = bool(st.session_state.get(f"tipo_bulk_ativo_{suffix}", True))

                linhas = [ln.strip() for ln in raw_text.splitlines()]
                nomes = [ln for ln in linhas if ln]
                if not nomes:
                    st.warning("Nada a cadastrar: informe ao menos um nome de tipo.")
                    return

                ensure_db_writable()
                num_new, num_skip = 0, 0
                vistos = set()
                for i, nome in enumerate(nomes):
                    if nome.lower() in vistos:
                        num_skip += 1
                        continue
                    vistos.add(nome.lower())
                    try:
                        upsert_procedimento_tipo(nome, int(ativo_padrao), start_ordem + i)
                        num_new += 1
                    except Exception:
                        num_skip += 1

                st.cache_data.clear()

                tipos_all3 = list_procedimento_tipos(only_active=False)
                df3 = pd.DataFrame(tipos_all3, columns=["id", "nome", "ativo", "ordem"]) if tipos_all3 else pd.DataFrame(columns=["id", "nome", "ativo", "ordem"])
                st.session_state["df_tipos_cached"] = df3

                st.success(f"Cadastro em lote concluído. Criados/atualizados: {num_new} | ignorados: {num_skip}")
                prox_id = (df3["id"].max() + 1) if not df3.empty else 1
                st.info(f"Próximo ID previsto: {prox_id}")

                _upload_db_catalogo("Atualiza catálogo de Tipos (cadastro em lote)")
            except PermissionError as pe:
                st.error(f"Diretório/arquivo do DB não é gravável. Ajuste 'DB_DIR' ou permissões. Detalhe: {pe}")
            except Exception as e:
                st.error("Falha no cadastro em lote de tipos.")
                st.exception(e)
            finally:
                st.session_state["tipo_bulk_reset"] += 1

        st.button("Salvar tipos em lote", on_click=_save_tipos_bulk_and_reset)

    with colB:
        st.markdown("##### Ações rápidas (Tipos)")
        col_btn_tipos, _ = st.columns([1.5, 2.5])
        with col_btn_tipos:
            if st.button("🔄 Recarregar catálogos de Tipos"):
                try:
                    tipos_allX = list_procedimento_tipos(only_active=False)
                    dfX = pd.DataFrame(tipos_allX, columns=["id", "nome", "ativo", "ordem"]) if tipos_allX else pd.DataFrame(columns=["id", "nome", "ativo", "ordem"])
                    st.session_state["df_tipos_cached"] = dfX
                    st.success("Tipos recarregados com sucesso.")
                except Exception as e:
                    st.error("Falha ao recarregar tipos.")
                    st.exception(e)

        try:
            df_tipos = st.session_state.get("df_tipos_cached", pd.DataFrame(columns=["id", "nome", "ativo", "ordem"]))
            if not df_tipos.empty:
                st.data_editor(
                    df_tipos,
                    use_container_width=True,
                    column_config={
                        "id": st.column_config.NumberColumn(disabled=True),
                        "nome": st.column_config.TextColumn(disabled=True),
                        "ordem": st.column_config.NumberColumn(),
                        "ativo": st.column_config.CheckboxColumn(),
                    },
                    key="editor_tipos_proc"
                )
                if st.button("Aplicar alterações nos tipos"):
                    try:
                        ensure_db_writable()
                        for _, r in df_tipos.iterrows():
                            set_procedimento_tipo_status(int(r["id"]), int(r["ativo"]))
                        st.cache_data.clear()

                        st.success("Tipos atualizados.")

                        tipos_all3 = list_procedimento_tipos(only_active=False)
                        df3 = pd.DataFrame(tipos_all3, columns=["id", "nome", "ativo", "ordem"]) if tipos_all3 else pd.DataFrame(columns=["id", "nome", "ativo", "ordem"])
                        st.session_state["df_tipos_cached"] = df3

                        prox_id = (df3["id"].max() + 1) if not df3.empty else 1
                        st.info(f"Próximo ID previsto: {prox_id}")

                        _upload_db_catalogo("Atualiza catálogo de Tipos (aplicar alterações)")
                    except PermissionError as pe:
                        st.error(f"Diretório/arquivo do DB não é gravável. Ajuste 'DB_DIR' ou permissões. Detalhe: {pe}")
                    except Exception as e:
                        st.error("Falha ao aplicar alterações nos tipos.")
                        st.exception(e)
            else:
                st.info("Nenhum tipo cadastrado ainda.")
        except Exception as e:
            st.error("Erro ao listar/editar tipos.")
            st.exception(e)

    # --------- Situações da Cirurgia -----------
    st.markdown("#### Situações da Cirurgia")
    colC, colD = st.columns([2, 1])

    if "sit_form_reset" not in st.session_state:
        st.session_state["sit_form_reset"] = 0

    df_sits_cached = st.session_state.get("df_sits_cached")
    if df_sits_cached is None:
        sits_all = list_cirurgia_situacoes(only_active=False)
        if sits_all:
            df_sits_cached = pd.DataFrame(sits_all, columns=["id", "nome", "ativo", "ordem"])
        else:
            df_sits_cached = pd.DataFrame(columns=["id", "nome", "ativo", "ordem"])
        st.session_state["df_sits_cached"] = df_sits_cached

    def _next_sit_ordem_from_cache(df: pd.DataFrame) -> int:
        if df.empty or "ordem" not in df.columns:
            return 1
        try:
            return int(pd.to_numeric(df["ordem"], errors="coerce").max() or 0) + 1
        except Exception:
            return 1

    next_sit_ordem = _next_sit_ordem_from_cache(df_sits_cached)

    def _upload_db_situacao(commit_msg: str):
        if GITHUB_SYNC_AVAILABLE and GITHUB_TOKEN_OK:
            try_vacuum_safely()
            try:
                ok, status, msg = safe_upload_with_merge(
                    owner=GH_OWNER,
                    repo=GH_REPO,
                    path_in_repo=GH_PATH_IN_REPO,
                    branch=GH_BRANCH,
                    local_db_path=DB_PATH,
                    commit_message=commit_msg,
                    prev_sha=st.session_state.get("gh_sha"),
                    _return_details=True
                )
                if ok:
                    new_sha = get_remote_sha(GH_OWNER, GH_REPO, GH_PATH_IN_REPO, GH_BRANCH)
                    if new_sha:
                        st.session_state["gh_sha"] = new_sha
                    st.success("Sincronização automática com GitHub concluída.")
                else:
                    st.error(f"Falha ao sincronizar com GitHub (status={status}). {msg}")
            except Exception as e:
                st.error("Falha ao sincronizar com GitHub.")
                st.exception(e)

    def _save_sit_and_reset():
        try:
            suffix = st.session_state["sit_form_reset"]
            sit_nome = (st.session_state.get(f"sit_nome_input_{suffix}") or "").strip()
            if not sit_nome:
                st.warning("Informe um nome de Situação antes de salvar.")
                return
            sit_ordem = int(st.session_state.get(f"sit_ordem_input_{suffix}", next_sit_ordem))
            sit_ativo = bool(st.session_state.get(f"sit_ativo_input_{suffix}", True))

            ensure_db_writable()
            sid = upsert_cirurgia_situacao(sit_nome, int(sit_ativo), int(sit_ordem))
            st.cache_data.clear()
            st.success(f"Situação salva (id={sid}).")

            sits_all2 = list_cirurgia_situacoes(only_active=False)
            df2 = pd.DataFrame(sits_all2, columns=["id", "nome", "ativo", "ordem"]) if sits_all2 else pd.DataFrame(columns=["id", "nome", "ativo", "ordem"])
            st.session_state["df_sits_cached"] = df2

            prox_id_s = (df2["id"].max() + 1) if not df2.empty else 1
            st.info(f"Próximo ID previsto: {prox_id_s}")

            _upload_db_situacao("Atualiza catálogo de Situações (salvar individual)")
        except PermissionError as pe:
            st.error(f"Diretório/arquivo do DB não é gravável. Ajuste 'DB_DIR' ou permissões. Detalhe: {pe}")
        except Exception as e:
            msg = str(e).lower()
            if "readonly" in msg or "read-only" in msg:
                st.error("Banco está em modo somente leitura. Mova o .db para diretório gravável (DB_DIR) ou ajuste permissões.")
            else:
                st.error("Falha ao salvar situação.")
                st.exception(e)
        finally:
            st.session_state["sit_form_reset"] += 1

    with colC:
        suffix = st.session_state["sit_form_reset"]
        st.text_input("Nova situação / atualizar por nome", placeholder="Ex.: Realizada, Cancelada, Adiada", key=f"sit_nome_input_{suffix}")
        st.number_input("Ordem (para ordenar listagem)", min_value=0, value=next_sit_ordem, step=1, key=f"sit_ordem_input_{suffix}")
        st.checkbox("Ativo", value=True, key=f"sit_ativo_input_{suffix}")
        st.button("Salvar situação", on_click=_save_sit_and_reset)

    with colD:
        st.markdown("##### Ações rápidas (Situações)")
        col_btn_sits, _ = st.columns([1.5, 2.5])
        with col_btn_sits:
            if st.button("🔄 Recarregar catálogos de Situações"):
                try:
                    sits_allX = list_cirurgia_situacoes(only_active=False)
                    dfX = pd.DataFrame(sits_allX, columns=["id", "nome", "ativo", "ordem"]) if sits_allX else pd.DataFrame(columns=["id", "nome", "ativo", "ordem"])
                    st.session_state["df_sits_cached"] = dfX
                    st.success("Situações recarregadas com sucesso.")
                except Exception as e:
                    st.error("Falha ao recarregar situações.")
                    st.exception(e)

        try:
            df_sits = st.session_state.get("df_sits_cached", pd.DataFrame(columns=["id", "nome", "ativo", "ordem"]))
            if not df_sits.empty:
                st.data_editor(
                    df_sits,
                    use_container_width=True,
                    column_config={
                        "id": st.column_config.NumberColumn(disabled=True),
                        "nome": st.column_config.TextColumn(disabled=True),
                        "ordem": st.column_config.NumberColumn(),
                        "ativo": st.column_config.CheckboxColumn(),
                    },
                    key="editor_situacoes"
                )
                if st.button("Aplicar alterações nas situações"):
                    try:
                        ensure_db_writable()
                        for _, r in df_sits.iterrows():
                            set_cirurgia_situacao_status(int(r["id"]), int(r["ativo"]))
                        st.cache_data.clear()

                        st.success("Situações atualizadas.")

                        sits_all3 = list_cirurgia_situacoes(only_active=False)
                        df3 = pd.DataFrame(sits_all3, columns=["id", "nome", "ativo", "ordem"]) if sits_all3 else pd.DataFrame(columns=["id", "nome", "ativo", "ordem"])
                        st.session_state["df_sits_cached"] = df3

                        prox_id_s = (df3["id"].max() + 1) if not df3.empty else 1
                        st.info(f"Próximo ID previsto: {prox_id_s}")

                        _upload_db_situacao("Atualiza catálogo de Situações (aplicar alterações)")
                    except PermissionError as pe:
                        st.error(f"Diretório/arquivo do DB não é gravável. Ajuste 'DB_DIR' ou permissões. Detalhe: {pe}")
                    except Exception as e:
                        st.error("Falha ao aplicar alterações nas situações.")
                        st.exception(e)
            else:
                st.info("Nenhuma situação cadastrada ainda.")
        except Exception as e:
            st.error("Erro ao listar/editar situações.")
            st.exception(e)

# ====================================================================================
# 📄 Aba 4: Tipos (Lista)
# ====================================================================================
with tabs[3]:
    st.subheader("Lista de Tipos de Procedimento")
    st.caption("Visualize, filtre, busque, ordene e exporte todos os tipos (ativos e inativos).")

    try:
        tipos_all = list_procedimento_tipos(only_active=False)
        df_tipos_full = pd.DataFrame(tipos_all, columns=["id", "nome", "ativo", "ordem"])
    except Exception as e:
        st.error("Erro ao carregar tipos do banco.")
        st.exception(e)
        df_tipos_full = pd.DataFrame(columns=["id", "nome", "ativo", "ordem"])

    colF1, colF2, colF3, colF4 = st.columns([1, 1, 1, 2])
    with colF1:
        filtro_status = st.selectbox("Status", options=["Todos", "Ativos", "Inativos"], index=0)
    with colF2:
        ordenar_por = st.selectbox("Ordenar por", options=["id", "nome", "ativo", "ordem"], index=3)
    with colF3:
        ordem_cresc = st.checkbox("Ordem crescente", value=True)
    with colF4:
        busca_nome = st.text_input("Buscar por nome (contém)", value="", placeholder="Ex.: ECG, Consulta...")

    df_view = df_tipos_full.copy()
    if filtro_status == "Ativos":
        df_view = df_view[df_view["ativo"] == 1]
    elif filtro_status == "Inativos":
        df_view = df_view[df_view["ativo"] == 0]
    if busca_nome.strip():
        termo = busca_nome.strip().lower()
        df_view = df_view[df_view["nome"].astype(str).str.lower().str.contains(termo)]
    df_view = df_view.sort_values(by=[ordenar_por], ascending=ordem_cresc, kind="mergesort")

    st.divider()
    st.markdown("#### Resultado")
    total_rows = len(df_view)
    per_page = st.number_input("Linhas por página", min_value=10, max_value=200, value=25, step=5)
    max_page = max(1, (total_rows + per_page - 1) // per_page)
    page = st.number_input("Página", min_value=1, max_value=max_page, value=1, step=1)
    start, end = (page - 1) * per_page, (page - 1) * per_page + per_page
    df_page = df_view.iloc[start:end].copy()
    st.caption(f"Exibindo {len(df_page)} de {total_rows} registro(s) — página {page}/{max_page}")
    st.dataframe(df_page, use_container_width=True)

    st.markdown("#### Exportar")
    colE1, colE2 = st.columns(2)
    with colE1:
        csv_bytes = df_view.to_csv(index=False).encode("utf-8")
        st.download_button(
            label="⬇️ Baixar CSV (filtros aplicados)",
            data=csv_bytes,
            file_name="tipos_de_procedimento.csv",
            mime="text/csv"
        )
    with colE2:
        try:
            output = BytesIO()
            with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
                df_view.to_excel(writer, sheet_name="Tipos", index=False)
                wb = writer.book
                ws = writer.sheets("Tipos") if hasattr(writer, "sheets") else writer.sheets["Tipos"]
                header_fmt = wb.add_format({"bold": True, "bg_color": "#DCE6F1", "border": 1})
                for col_num, value in enumerate(df_view.columns):
                    ws.write(0, col_num, value, header_fmt)
                last_row = max(len(df_view), 1)
                ws.autofilter(0, 0, last_row, max(0, len(df_view.columns) - 1))
                for i, col in enumerate(df_view.columns):
                    values = [str(x) for x in df_view[col].tolist()]
                    maxlen = max([len(str(col))] + [len(v) for v in values]) + 2
                    ws.set_column(i, i, max(14, min(maxlen, 60)))
            output.seek(0)
            st.download_button(
                label="⬇️ Baixar Excel (filtros aplicados)",
                data=output.getvalue(),
                file_name="tipos_de_procedimento.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
        except Exception as e:
            st.error("Falha ao gerar Excel.")
            st.exception(e)

    with st.expander("ℹ️ Ajuda / Diagnóstico", expanded=False):
        st.markdown("""
        - **Status**: escolha **Ativos** para ver apenas os que aparecem na Aba **Cirurgias**.
        - **Ordenação**: por padrão ordenamos por **ordem** e depois por **nome**.
        - **Busca**: digite parte do nome e pressione Enter.
        - **Paginação**: ajuste conforme necessário.
        - **Exportar**: baixa exatamente o que está filtrado/ordenado.
        """)
