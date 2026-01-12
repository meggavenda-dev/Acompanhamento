
# app.py
import os
from datetime import datetime
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

st.set_page_config(page_title="Gestão de Pacientes e Cirurgias", layout="wide")

# --- Header ---
st.title("Gestão de Pacientes e Cirurgias")
st.caption("Download do banco no GitHub (1x) → Importar/Processar → Revisar/Salvar → Exportar → Cirurgias (com catálogos) → Cadastro/Lista")

# Baixar DB do GitHub apenas 1x por sessão (ou se não existir localmente)
if GITHUB_SYNC_AVAILABLE and GITHUB_TOKEN_OK:
    if ("gh_db_fetched" not in st.session_state) or (not st.session_state["gh_db_fetched"]):
        if not os.path.exists(DB_PATH):
            try:
                downloaded = download_db_from_github(
                    owner=GH_OWNER,
                    repo=GH_REPO,
                    path_in_repo=GH_PATH_IN_REPO,
                    branch=GH_BRANCH,
                    local_db_path=DB_PATH
                )
                if downloaded:
                    st.success("Banco baixado do GitHub (primeira carga na sessão).")
                else:
                    st.info("Banco não encontrado no GitHub (primeiro uso). Será criado localmente ao salvar.")
            except Exception as e:
                st.warning("Não foi possível baixar o banco do GitHub. Verifique token/permissões em st.secrets.")
                st.exception(e)
        st.session_state["gh_db_fetched"] = True

# Botão opcional (sidebar) para re-download manual
with st.sidebar:
    st.markdown("### Sincronização GitHub")
    if GITHUB_SYNC_AVAILABLE and GITHUB_TOKEN_OK:
        if st.button("🔽 Baixar banco do GitHub (manual)"):
            try:
                downloaded = download_db_from_github(
                    owner=GH_OWNER,
                    repo=GH_REPO,
                    path_in_repo=GH_PATH_IN_REPO,
                    branch=GH_BRANCH,
                    local_db_path=DB_PATH
                )
                if downloaded:
                    st.success("Banco baixado do GitHub (manual).")
                else:
                    st.info("Arquivo não existe no repositório.")
            except Exception as e:
                st.error("Falha ao baixar do GitHub.")
                st.exception(e)
    else:
        st.info("GitHub sync desativado (sem token).")

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
        if GITHUB_SYNC_AVAILABLE and GITHUB_TOKEN_OK:
            try:
                ok = upload_db_to_github(
                    owner=GH_OWNER,
                    repo=GH_REPO,
                    path_in_repo=GH_PATH_IN_REPO,
                    branch=GH_BRANCH,
                    local_db_path=DB_PATH,
                    commit_message=commit_message
                )
                if ok:
                    st.success("Sincronização automática com GitHub concluída.")
            except Exception as e:
                st.error("Falha ao sincronizar com GitHub.")
                st.exception(e)

    can_execute = confirmar and (confirma_texto.strip().upper() == "RESET")

    col_r1, col_r2 = st.columns(2)
    with col_r1:
        if st.button("Apagar **PACIENTES** (tabela base)", type="secondary", disabled=not can_execute):
            try:
                from db import delete_all_pacientes, vacuum
                delete_all_pacientes()
                vacuum()
                st.success("Pacientes apagados (tabela base).")
                _sync_after_reset("Reset: apaga pacientes (tabela base)")
                st.rerun()
            except Exception as e:
                st.error("Falha ao apagar pacientes.")
                st.exception(e)

    with col_r2:
        if st.button("Apagar **CIRURGIAS**", type="secondary", disabled=not can_execute):
            try:
                from db import delete_all_cirurgias, vacuum
                delete_all_cirurgias()
                vacuum()
                st.success("Cirurgias apagadas.")
                _sync_after_reset("Reset: apaga cirurgias")
                st.rerun()
            except Exception as e:
                st.error("Falha ao apagar cirurgias.")
                st.exception(e)

    col_r3, col_r4 = st.columns(2)
    with col_r3:
        if st.button("Apagar **CATÁLOGOS** (Tipos/Situações)", type="secondary", disabled=not can_execute):
            try:
                from db import delete_all_catalogos, vacuum
                delete_all_catalogos()
                vacuum()
                st.success("Catálogos apagados (Tipos/Situações).")
                _sync_after_reset("Reset: apaga catálogos (tipos/situações)")
                st.rerun()
            except Exception as e:
                st.error("Falha ao apagar catálogos.")
                st.exception(e)

    with col_r4:
        if st.button("🗑️ **RESET TOTAL** (apaga arquivo .db)", type="primary", disabled=not can_execute):
            try:
                from db import dispose_engine, reset_db_file
                dispose_engine()
                reset_db_file()
                st.success("Banco recriado vazio.")
                _sync_after_reset("Reset total: recria .db vazio")
                st.rerun()
            except Exception as e:
                st.error("Falha no reset total.")
                st.exception(e)

# Inicializa DB
init_db()

# Lista única de hospitais (ajuste conforme necessário)
HOSPITAL_OPCOES = [
    "Hospital Santa Lucia Sul",
    "Hospital Santa Lucia Norte",
    "Hospital Maria Auxiliadora",
]

# ---------------- Abas ----------------
tabs = st.tabs([
    "📥 Importação &amp; Pacientes",
    "🩺 Cirurgias",
    "📚 Cadastro (Tipos &amp; Situações)",
    "📄 Tipos (Lista)"
])

# ====================================================================================
# 📥 Aba 1: Importação & Pacientes
# ====================================================================================
with tabs[0]:
    st.subheader("Pacientes únicos por data, prestador e hospital")
    st.caption("Upload → herança/filtragem/deduplicação → Hospital → editar Paciente → salvar → exportar → commit automático no GitHub")

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

    col_reset1, _ = st.columns(2)
    with col_reset1:
        if st.button("🧹 Limpar tabela / reset"):
            st.session_state.df_final = None
            st.session_state.last_upload_id = None
            st.session_state.editor_key = "editor_pacientes_reset"
            st.success("Tabela limpa. Faça novo upload para reprocessar.")

    if uploaded_file is not None:
        current_upload_id = _make_upload_id(uploaded_file, selected_hospital)
        if st.session_state.last_upload_id != current_upload_id:
            st.session_state.df_final = None
            st.session_state.editor_key = f"editor_pacientes_{current_upload_id}"
            st.session_state.last_upload_id = current_upload_id

        with st.spinner("Processando arquivo..."):
            try:
                df_final = process_uploaded_file(uploaded_file, prestadores_lista, selected_hospital.strip())
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

        st.markdown("#### Revisar e editar nomes de Paciente (opcional)")
        df_to_edit = st.session_state.df_final.sort_values(
            ["Hospital", "Ano", "Mes", "Dia", "Paciente", "Prestador"]
        ).reset_index(drop=True)

        edited_df = st.data_editor(
            df_to_edit,
            use_container_width=True,
            num_rows="fixed",
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
                "Paciente": st.column_config.TextColumn(help="Clique para editar o nome do paciente."),
            },
            hide_index=True,
            key=st.session_state.editor_key
        )
        # ✅ Garantir tipo correto após o editor
        edited_df = pd.DataFrame(edited_df)
        st.session_state.df_final = edited_df

        st.markdown("#### Persistência")
        if st.button("Salvar no banco (exemplo.db)"):
            try:
                upsert_dataframe(st.session_state.df_final)
                total = count_all()
                st.success(f"Dados salvos com sucesso. Total de linhas no banco: {total}")

                if GITHUB_SYNC_AVAILABLE and GITHUB_TOKEN_OK:
                    try:
                        ok = upload_db_to_github(
                            owner=GH_OWNER,
                            repo=GH_REPO,
                            path_in_repo=GH_PATH_IN_REPO,
                            branch=GH_BRANCH,
                            local_db_path=DB_PATH,
                            commit_message="Atualiza banco SQLite via app (salvar pacientes)"
                        )
                        if ok:
                            st.success("Sincronização automática com GitHub concluída.")
                    except Exception as e:
                        st.error("Falha ao sincronizar com GitHub.")
                        st.exception(e)

                st.session_state.df_final = None
                st.session_state.editor_key = "editor_pacientes_after_save"

            except Exception as e:
                st.error("Falha ao salvar no banco.")
                st.exception(e)

        st.markdown("#### Exportar Excel (multi-aba por Hospital)")
        # ✅ Garantir DataFrame na exportação
        df_for_export = pd.DataFrame(st.session_state.df_final)
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
        st.info("Banco ainda sem dados. Faça o upload e clique em 'Salvar no banco'.")

# ====================================================================================
# 🩺 Aba 2: Cirurgias
# ====================================================================================
with tabs[1]:
    st.subheader("Cadastrar / Editar Cirurgias (compartilha o mesmo banco)")
    from db import (
        find_registros_para_prefill,
        insert_or_update_cirurgia,
        list_procedimento_tipos,
        list_cirurgia_situacoes,
        list_cirurgias,
        delete_cirurgia,
        list_registros_base_all
    )
    from export import to_formatted_excel_cirurgias

    st.markdown("#### Filtros para carregar pacientes na Lista de Cirurgias")
    colF0, colF1, colF2, colF3 = st.columns([1, 1, 1, 1])
    with colF0:
        usar_periodo = st.checkbox(
            "Filtrar por Ano/Mês",
            value=True,
            help="Desmarque para carregar todos os pacientes do hospital, independente do período."
        )
    with colF1:
        hosp_cad = st.selectbox("Filtro Hospital (lista)", options=HOSPITAL_OPCOES, index=0)
    now = datetime.now()
    with colF2:
        ano_cad = st.number_input(
            "Ano (filtro base)", min_value=2000, max_value=2100,
            value=now.year, step=1, disabled=not usar_periodo
        )
    with colF3:
        mes_cad = st.number_input(
            "Mês (filtro base)", min_value=1, max_value=12,
            value=now.month, step=1, disabled=not usar_periodo
        )

    prestadores_filtro = st.text_input(
        "Prestadores (filtro base, separar por ; ) — deixe vazio para não filtrar",
        value=""
    )
    prestadores_lista_filtro = [p.strip() for p in prestadores_filtro.split(";") if p.strip()]

    # ---- Recarregar catálogos (Tipos/Situações) ----
    col_refresh, col_refresh_info = st.columns([1.5, 2.5])
    with col_refresh:
        if st.button("🔄 Recarregar catálogos (Tipos/Situações)"):
            st.session_state["catalog_refresh_ts"] = datetime.now().isoformat(timespec="seconds")
            st.success(f"Catálogos recarregados às {st.session_state['catalog_refresh_ts']}")

    with col_refresh_info:
        ts = st.session_state.get("catalog_refresh_ts")
        if ts:
            st.caption(f"Último recarregamento: {ts}")

    # -------- Carregar catálogos (para dropdowns do grid) --------
    tipos_rows = list_procedimento_tipos(only_active=True)
    df_tipos_cat = pd.DataFrame(tipos_rows, columns=["id", "nome", "ativo", "ordem"]) if tipos_rows else pd.DataFrame(columns=["id", "nome", "ativo", "ordem"])
    if not df_tipos_cat.empty:
        df_tipos_cat = df_tipos_cat.sort_values(["ordem", "nome"], kind="mergesort")
        tipo_nome_list = df_tipos_cat["nome"].tolist()
        tipo_nome2id = dict(zip(df_tipos_cat["nome"], df_tipos_cat["id"]))  # nome -> id
        tipo_id2nome = dict(zip(df_tipos_cat["id"], df_tipos_cat["nome"]))  # id -> nome
    else:
        tipo_nome_list = []
        tipo_nome2id = {}
        tipo_id2nome = {}

    sits_rows = list_cirurgia_situacoes(only_active=True)
    df_sits_cat = pd.DataFrame(sits_rows, columns=["id", "nome", "ativo", "ordem"]) if sits_rows else pd.DataFrame(columns=["id", "nome", "ativo", "ordem"])
    if not df_sits_cat.empty:
        df_sits_cat = df_sits_cat.sort_values(["ordem", "nome"], kind="mergesort")
        sit_nome_list = df_sits_cat["nome"].tolist()
        sit_nome2id = dict(zip(df_sits_cat["nome"], df_sits_cat["id"]))
        sit_id2nome = dict(zip(df_sits_cat["id"], df_sits_cat["nome"]))
    else:
        sit_nome_list = []
        sit_nome2id = {}
        sit_id2nome = {}

    if not tipo_nome_list:
        st.warning("Nenhum **Tipo de Procedimento** ativo encontrado. Cadastre na aba **📚 Cadastro (Tipos & Situações)** e marque como **Ativo**.")
    if not sit_nome_list:
        st.warning("Nenhuma **Situação da Cirurgia** ativa encontrada. Cadastre na aba **📚 Cadastro (Tipos & Situações)** e marque como **Ativo**.")

    try:
        rows_cir = list_cirurgias(hospital=hosp_cad, ano_mes=None, prestador=None)
        df_cir = pd.DataFrame(rows_cir, columns=[
            "id", "Hospital", "Atendimento", "Paciente", "Prestador", "Data_Cirurgia",
            "Convenio", "Procedimento_Tipo_ID", "Situacao_ID",
            "Guia_AMHPTISS", "Guia_AMHPTISS_Complemento", "Fatura",
            "Observacoes", "created_at", "updated_at"
        ])
        if df_cir.empty:
            df_cir = pd.DataFrame(columns=[
                "id", "Hospital", "Atendimento", "Paciente", "Prestador", "Data_Cirurgia",
                "Convenio", "Procedimento_Tipo_ID", "Situacao_ID",
                "Guia_AMHPTISS", "Guia_AMHPTISS_Complemento", "Fatura",
                "Observacoes", "created_at", "updated_at"
            ])

        df_cir["Fonte"] = "Cirurgia"
        df_cir["Tipo (nome)"] = df_cir["Procedimento_Tipo_ID"].map(tipo_id2nome).fillna("")
        df_cir["Situação (nome)"] = df_cir["Situacao_ID"].map(sit_id2nome).fillna("")

        base_rows = find_registros_para_prefill(
            hosp_cad,
            ano=int(ano_cad) if usar_periodo else None,
            mes=int(mes_cad) if usar_periodo else None,
            prestadores=prestadores_lista_filtro
        )
        df_base = pd.DataFrame(base_rows, columns=["Hospital", "Data", "Atendimento", "Paciente", "Convenio", "Prestador"])
        if df_base.empty:
            df_base = pd.DataFrame(columns=["Hospital", "Data", "Atendimento", "Paciente", "Convenio", "Prestador"])
        else:
            for col in ["Hospital", "Data", "Atendimento", "Paciente", "Convenio", "Prestador"]:
                df_base[col] = df_base[col].fillna("").astype(str)

        st.info(f"Cirurgias já salvas: {len(df_cir)} | Candidatos da base: {len(df_base)}")

        if df_base.empty:
            st.warning("Nenhum candidato carregado da base com os filtros atuais.")
            with st.expander("Diagnóstico do filtro", expanded=False):
                st.markdown("- Verifique o **Hospital** (coincide com Aba 1?).")
                st.markdown("- Ajuste **Ano/Mês** ou desmarque **Filtrar por Ano/Mês**.")
                st.markdown("- Deixe **Prestadores** vazio para não filtrar.")
                st.markdown("- O filtro aceita datas em `dd/MM/yyyy` e `YYYY-MM-DD`.")

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
            "Situação (nome)": ["" for _ in range(len(df_base))]
        })

        df_union = pd.concat([df_cir, df_base_mapped], ignore_index=True)
        df_union["_has_id"] = df_union["id"].notna().astype(int)
        df_union["_AttOrPac"] = df_union["Atendimento"].fillna("").astype(str).str.strip()
        empty_mask = df_union["_AttOrPac"] == ""
        df_union.loc[empty_mask, "_AttOrPac"] = df_union.loc[empty_mask, "Paciente"].fillna("").astype(str).str.strip()

        KEY_COLS = ["Hospital", "_AttOrPac", "Prestador", "Data_Cirurgia"]
        df_union = df_union.sort_values(KEY_COLS + ["_has_id"], ascending=[True, True, True, True, False])
        df_union = df_union.drop_duplicates(subset=KEY_COLS, keep="first")
        df_union.drop(columns=["_has_id", "_AttOrPac"], inplace=True)

        st.markdown("#### Lista de Cirurgias (com pacientes carregados da base)")
        st.caption("Edite diretamente no grid. Selecione **Tipo (nome)** e **Situação (nome)**; ao salvar, o app preenche os IDs correspondentes.")

        df_edit_view = df_union.drop(
            columns=["id", "Fonte", "Procedimento_Tipo_ID", "Situacao_ID", "created_at", "updated_at"],
            errors="ignore"
        )

        edited_df = st.data_editor(
            df_edit_view,
            use_container_width=True,
            num_rows="fixed",
            column_config={
                "Hospital": st.column_config.TextColumn(),
                "Atendimento": st.column_config.TextColumn(),
                "Paciente": st.column_config.TextColumn(),
                "Prestador": st.column_config.TextColumn(),
                "Data_Cirurgia": st.column_config.TextColumn(help="Formato livre, ex.: dd/MM/yyyy ou YYYY-MM-DD."),
                "Convenio": st.column_config.TextColumn(),
                "Tipo (nome)": st.column_config.SelectboxColumn(
                    options=[""] + tipo_nome_list,
                    help="Selecione o tipo de serviço cadastrado (apenas ativos)."
                ),
                "Situação (nome)": st.column_config.SelectboxColumn(
                    options=[""] + sit_nome_list,
                    help="Selecione a situação da cirurgia (apenas ativas)."
                ),
                "Guia_AMHPTISS": st.column_config.TextColumn(),
                "Guia_AMHPTISS_Complemento": st.column_config.TextColumn(),
                "Fatura": st.column_config.TextColumn(),
                "Observacoes": st.column_config.TextColumn(),
            },
            key="editor_lista_cirurgias_union"
        )
        # ✅ Garantir tipo correto após o editor
        edited_df = pd.DataFrame(edited_df)

        colG1, colG2, colG3 = st.columns([1.2, 1, 1.8])
        with colG1:
            if st.button("💾 Salvar alterações da Lista (UPSERT em massa)"):
                try:
                    edited_df = edited_df.copy()
                    edited_df["Procedimento_Tipo_ID"] = edited_df["Tipo (nome)"].map(lambda n: tipo_nome2id.get(n) if n else None)
                    edited_df["Situacao_ID"] = edited_df["Situação (nome)"].map(lambda n: sit_nome2id.get(n) if n else None)

                    num_ok, num_skip = 0, 0
                    for _, r in edited_df.iterrows():
                        h = str(r.get("Hospital", "")).strip()
                        att = str(r.get("Atendimento", "")).strip()
                        p = str(r.get("Prestador", "")).strip()
                        d = str(r.get("Data_Cirurgia", "")).strip()

                        if h and p and d and (att or str(r.get("Paciente", "")).strip()):
                            payload = {
                                "Hospital": h,
                                "Atendimento": att,
                                "Paciente": str(r.get("Paciente", "")).strip(),
                                "Prestador": p,
                                "Data_Cirurgia": d,
                                "Convenio": str(r.get("Convenio", "")).strip(),
                                "Procedimento_Tipo_ID": r.get("Procedimento_Tipo_ID"),
                                "Situacao_ID": r.get("Situacao_ID"),
                                "Guia_AMHPTISS": str(r.get("Guia_AMHPTISS", "")).strip(),
                                "Guia_AMHPTISS_Complemento": str(r.get("Guia_AMHPTISS_Complemento", "")).strip(),
                                "Fatura": str(r.get("Fatura", "")).strip(),
                                "Observacoes": str(r.get("Observacoes", "")).strip(),
                            }
                            insert_or_update_cirurgia(payload)
                            num_ok += 1
                        else:
                            num_skip += 1
                    st.success(f"UPSERT concluído. {num_ok} linha(s) salvas; {num_skip} ignorada(s) (chave incompleta).")

                    if GITHUB_SYNC_AVAILABLE and GITHUB_TOKEN_OK:
                        try:
                            ok = upload_db_to_github(
                                owner=GH_OWNER,
                                repo=GH_REPO,
                                path_in_repo=GH_PATH_IN_REPO,
                                branch=GH_BRANCH,
                                local_db_path=DB_PATH,
                                commit_message="Atualiza banco SQLite via app (salvar lista de cirurgias)"
                            )
                            if ok:
                                st.success("Sincronização automática com GitHub concluída.")
                        except Exception as e:
                            st.error("Falha ao sincronizar com GitHub.")
                            st.exception(e)

                except Exception as e:
                    st.error("Falha ao salvar alterações da lista.")
                    st.exception(e)

        with colG2:
            if st.button("⬇️ Exportar Excel (Lista atual)"):
                try:
                    export_df = edited_df.drop(columns=["Tipo (nome)", "Situação (nome)"], errors="ignore")
                    # ✅ Garantir DataFrame na exportação
                    export_df = pd.DataFrame(export_df)
                    excel_bytes = to_formatted_excel_cirurgias(export_df)
                    st.download_button(
                        label="Baixar Cirurgias.xlsx",
                        data=excel_bytes,
                        file_name="Cirurgias.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                    )
                except Exception as e:
                    st.error("Falha ao exportar Excel.")
                    st.exception(e)

        with colG3:
            del_id = st.number_input("Excluir cirurgia por id", min_value=0, step=1, value=0)
            if st.button("🗑️ Excluir cirurgia"):
                try:
                    delete_cirurgia(int(del_id))
                    st.success(f"Cirurgia id={int(del_id)} excluída.")
                    if GITHUB_SYNC_AVAILABLE and GITHUB_TOKEN_OK:
                        try:
                            ok = upload_db_to_github(
                                owner=GH_OWNER,
                                repo=GH_REPO,
                                path_in_repo=GH_PATH_IN_REPO,
                                branch=GH_BRANCH,
                                local_db_path=DB_PATH,
                                commit_message="Atualiza banco SQLite via app (excluir cirurgia)"
                            )
                            if ok:
                                st.success("Sincronização automática com GitHub concluída.")
                        except Exception as e:
                            st.error("Falha ao sincronizar com GitHub.")
                            st.exception(e)
                except Exception as e:
                    st.error("Falha ao excluir.")
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
# 📚 Aba 3: Cadastro (Tipos & Situações) — (sem mudanças funcionais aqui)
# ====================================================================================
with tabs[2]:
    # ... (seu código desta aba permanece igual)
    # Nenhuma alteração necessária para o problema reportado
    pass

# ====================================================================================
# 📄 Aba 4: Tipos (Lista) — (sem mudanças funcionais aqui)
# ====================================================================================
with tabs[3]:
    # ... (seu código desta aba permanece igual)
    pass

