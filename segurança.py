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
        excel_bytes = to_formatted_excel_by_hospital(st.session_state.df_final)
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
    # TIPOS: apenas ativos, ordenados por 'ordem' e 'nome'
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

    # SITUAÇÕES: apenas ativas, ordenadas por 'ordem' e 'nome'
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

    # Avisos se catálogos estiverem vazios
    if not tipo_nome_list:
        st.warning("Nenhum **Tipo de Procedimento** ativo encontrado. Cadastre na aba **📚 Cadastro (Tipos & Situações)** e marque como **Ativo**.")
    if not sit_nome_list:
        st.warning("Nenhuma **Situação da Cirurgia** ativa encontrada. Cadastre na aba **📚 Cadastro (Tipos & Situações)** e marque como **Ativo**.")

    # -------- Montar a Lista de Cirurgias com união (Cirurgias + Base) --------
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

        # Prepara nomes legíveis a partir dos IDs para linhas existentes
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

        # Mapeia candidatos da base para o esquema de cirurgias (com colunas legíveis)
        df_base_mapped = pd.DataFrame({
            "id": [None]*len(df_base),
            "Hospital": df_base["Hospital"],
            "Atendimento": df_base["Atendimento"],
            "Paciente": df_base["Paciente"],
            "Prestador": df_base["Prestador"],
            "Data_Cirurgia": df_base["Data"],
            "Convenio": df_base["Convenio"],
            "Procedimento_Tipo_ID": [None]*len(df_base),  # será preenchido ao salvar
            "Situacao_ID": [None]*len(df_base),           # idem
            "Guia_AMHPTISS": ["" for _ in range(len(df_base))],
            "Guia_AMHPTISS_Complemento": ["" for _ in range(len(df_base))],
            "Fatura": ["" for _ in range(len(df_base))],
            "Observacoes": ["" for _ in range(len(df_base))],
            "created_at": [None]*len(df_base),
            "updated_at": [None]*len(df_base),
            "Fonte": ["Base"]*len(df_base),
            "Tipo (nome)": ["" for _ in range(len(df_base))],       # edição por nome
            "Situação (nome)": ["" for _ in range(len(df_base))]    # edição por nome
        })

        # União preferindo registros já existentes (evita duplicar mesma chave)
        df_union = pd.concat([df_cir, df_base_mapped], ignore_index=True)
        df_union["_has_id"] = df_union["id"].notna().astype(int)

        # Chave resiliente: usa Atendimento; se vazio, usa Paciente
        df_union["_AttOrPac"] = df_union["Atendimento"].fillna("").astype(str).str.strip()
        empty_mask = df_union["_AttOrPac"] == ""
        df_union.loc[empty_mask, "_AttOrPac"] = df_union.loc[empty_mask, "Paciente"].fillna("").astype(str).str.strip()

        KEY_COLS = ["Hospital", "_AttOrPac", "Prestador", "Data_Cirurgia"]
        df_union = df_union.sort_values(KEY_COLS + ["_has_id"], ascending=[True, True, True, True, False])
        df_union = df_union.drop_duplicates(subset=KEY_COLS, keep="first")
        df_union.drop(columns=["_has_id", "_AttOrPac"], inplace=True)

        st.markdown("#### Lista de Cirurgias (com pacientes carregados da base)")
        st.caption("Edite diretamente no grid. Selecione **Tipo (nome)** e **Situação (nome)**; ao salvar, o app preenche os IDs correspondentes.")

        # 👇 Oculta colunas ID/Fonte, numéricas e auditoria na visão do editor
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

                # ✅ Dropdown com os Tipos de serviço (ativos e ordenados)
                "Tipo (nome)": st.column_config.SelectboxColumn(
                    options=[""] + tipo_nome_list,
                    help="Selecione o tipo de serviço cadastrado (apenas ativos)."
                ),
                # ✅ Dropdown com as Situações (ativas e ordenadas)
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

        colG1, colG2, colG3 = st.columns([1.2, 1, 1.8])
        with colG1:
            if st.button("💾 Salvar alterações da Lista (UPSERT em massa)"):
                try:
                    edited_df = edited_df.copy()

                    # Reconstroi IDs a partir dos nomes escolhidos
                    edited_df["Procedimento_Tipo_ID"] = edited_df["Tipo (nome)"].map(lambda n: tipo_nome2id.get(n) if n else None)
                    edited_df["Situacao_ID"] = edited_df["Situação (nome)"].map(lambda n: sit_nome2id.get(n) if n else None)

                    num_ok, num_skip = 0, 0
                    for _, r in edited_df.iterrows():
                        h = str(r.get("Hospital", "")).strip()
                        att = str(r.get("Atendimento", "")).strip()
                        p = str(r.get("Prestador", "")).strip()
                        d = str(r.get("Data_Cirurgia", "")).strip()

                        # Chave mínima para UPSERT (Atendimento ou Paciente + Hospital/Prestador/Data)
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
                    from export import to_formatted_excel_cirurgias
                    # Exporta sem as colunas de nomes de apoio
                    export_df = edited_df.drop(columns=["Tipo (nome)", "Situação (nome)"], errors="ignore")
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
# 📚 Aba 3: Cadastro (Tipos & Situações) — reset counter + ordem auto-incremental + lote
# ====================================================================================
with tabs[2]:
    st.subheader("Catálogos de Tipos de Procedimento e Situações da Cirurgia")

    st.markdown("#### Tipos de Procedimento")
    colA, colB = st.columns([2, 1])

    if "tipo_form_reset" not in st.session_state:
        st.session_state["tipo_form_reset"] = 0
    if "tipo_bulk_reset" not in st.session_state:
        st.session_state["tipo_bulk_reset"] = 0

    from db import list_procedimento_tipos
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
                ok = upload_db_to_github(
                    owner=GH_OWNER,
                    repo=GH_REPO,
                    path_in_repo=GH_PATH_IN_REPO,
                    branch=GH_BRANCH,
                    local_db_path=DB_PATH,
                    commit_message=commit_msg
                )
                if ok:
                    st.success("Sincronização automática com GitHub concluída.")
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

            from db import upsert_procedimento_tipo, list_procedimento_tipos
            tid = upsert_procedimento_tipo(tipo_nome, int(tipo_ativo), int(tipo_ordem))
            st.success(f"Tipo salvo (id={tid}).")

            tipos_all2 = list_procedimento_tipos(only_active=False)
            df2 = pd.DataFrame(tipos_all2, columns=["id", "nome", "ativo", "ordem"]) if tipos_all2 else pd.DataFrame(columns=["id", "nome", "ativo", "ordem"])
            st.session_state["df_tipos_cached"] = df2

            prox_id = (df2["id"].max() + 1) if not df2.empty else 1
            st.info(f"Próximo ID previsto: {prox_id}")

            _upload_db_catalogo("Atualiza catálogo de Tipos (salvar individual)")
        except Exception as e:
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

                from db import upsert_procedimento_tipo, list_procedimento_tipos
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

                tipos_all3 = list_procedimento_tipos(only_active=False)
                df3 = pd.DataFrame(tipos_all3, columns=["id", "nome", "ativo", "ordem"]) if tipos_all3 else pd.DataFrame(columns=["id", "nome", "ativo", "ordem"])
                st.session_state["df_tipos_cached"] = df3

                st.success(f"Cadastro em lote concluído. Criados/atualizados: {num_new} | ignorados: {num_skip}")
                prox_id = (df3["id"].max() + 1) if not df3.empty else 1
                st.info(f"Próximo ID previsto: {prox_id}")

                _upload_db_catalogo("Atualiza catálogo de Tipos (cadastro em lote)")
            except Exception as e:
                st.error("Falha no cadastro em lote de tipos.")
                st.exception(e)
            finally:
                st.session_state["tipo_bulk_reset"] += 1

        st.button("Salvar tipos em lote", on_click=_save_tipos_bulk_and_reset)

    with colB:
        # Botão de recarregar tipos (cache do grid)
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

        from db import set_procedimento_tipo_status
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
                        for _, r in df_tipos.iterrows():
                            set_procedimento_tipo_status(int(r["id"]), int(r["ativo"]))
                        st.success("Tipos atualizados.")

                        tipos_all3 = list_procedimento_tipos(only_active=False)
                        df3 = pd.DataFrame(tipos_all3, columns=["id", "nome", "ativo", "ordem"]) if tipos_all3 else pd.DataFrame(columns=["id", "nome", "ativo", "ordem"])
                        st.session_state["df_tipos_cached"] = df3

                        prox_id = (df3["id"].max() + 1) if not df3.empty else 1
                        st.info(f"Próximo ID previsto: {prox_id}")

                        _upload_db_catalogo("Atualiza catálogo de Tipos (aplicar alterações)")
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

    from db import list_cirurgia_situacoes
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
            try:
                ok = upload_db_to_github(
                    owner=GH_OWNER,
                    repo=GH_REPO,
                    path_in_repo=GH_PATH_IN_REPO,
                    branch=GH_BRANCH,
                    local_db_path=DB_PATH,
                    commit_message=commit_msg
                )
                if ok:
                    st.success("Sincronização automática com GitHub concluída.")
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

            from db import upsert_cirurgia_situacao, list_cirurgia_situacoes
            sid = upsert_cirurgia_situacao(sit_nome, int(sit_ativo), int(sit_ordem))
            st.success(f"Situação salva (id={sid}).")

            sits_all2 = list_cirurgia_situacoes(only_active=False)
            df2 = pd.DataFrame(sits_all2, columns=["id", "nome", "ativo", "ordem"]) if sits_all2 else pd.DataFrame(columns=["id", "nome", "ativo", "ordem"])
            st.session_state["df_sits_cached"] = df2

            prox_id_s = (df2["id"].max() + 1) if not df2.empty else 1
            st.info(f"Próximo ID previsto: {prox_id_s}")

            _upload_db_situacao("Atualiza catálogo de Situações (salvar individual)")
        except Exception as e:
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
        # Botão de recarregar situações (cache do grid)
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

        from db import set_cirurgia_situacao_status
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
                        for _, r in df_sits.iterrows():
                            set_cirurgia_situacao_status(int(r["id"]), int(r["ativo"]))
                        st.success("Situações atualizadas.")

                        sits_all3 = list_cirurgia_situacoes(only_active=False)
                        df3 = pd.DataFrame(sits_all3, columns=["id", "nome", "ativo", "ordem"]) if sits_all3 else pd.DataFrame(columns=["id", "nome", "ativo", "ordem"])
                        st.session_state["df_sits_cached"] = df3

                        prox_id_s = (df3["id"].max() + 1) if not df3.empty else 1
                        st.info(f"Próximo ID previsto: {prox_id_s}")

                        _upload_db_situacao("Atualiza catálogo de Situações (aplicar alterações)")
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

    from db import list_procedimento_tipos

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
            from io import BytesIO
            output = BytesIO()
            with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
                df_view.to_excel(writer, sheet_name="Tipos", index=False)
                wb = writer.book
                ws = writer.sheets["Tipos"]
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
        - **Status**: escolha **Ativos** para ver apenas os que aparecem na Aba **Cirurgias** (dropdown “Tipo (nome)”).
        - **Ordenação**: por padrão ordenamos por **ordem** e depois por **nome**.
        - **Busca**: digite parte do nome e pressione Enter.
        - **Paginação**: ajuste conforme necessário.
        - **Exportar**: baixa exatamente o que está filtrado/ordenado.
        """)  
# db.py
from __future__ import annotations

import os
import math
import tempfile
from typing import Optional, List, Dict, Any
from sqlalchemy import create_engine, text

# ---------------- Configuração de caminho persistente (writable) ----------------
def _pick_writable_dir() -> str:
    """
    Escolhe um diretório garantidamente gravável:
    1) st.secrets["DB_DIR"] se existir;
    2) env var DB_DIR se existir;
    3) /tmp (tempfile.gettempdir()) como fallback.
    """
    db_dir = os.environ.get("DB_DIR", "")
    # Tenta pegar do Streamlit secrets sem impor dependência forte
    try:
        import streamlit as st
        db_dir = st.secrets.get("DB_DIR", db_dir)
    except Exception:
        pass

    if not db_dir:
        db_dir = os.path.join(tempfile.gettempdir(), "acompanhamento_data")

    os.makedirs(db_dir, exist_ok=True)
    # Garante permissões de escrita no diretório
    try:
        os.chmod(db_dir, 0o777)
    except Exception:
        pass
    return db_dir

DATA_DIR = _pick_writable_dir()
DB_PATH = os.path.join(DATA_DIR, "exemplo.db")
DB_URI = f"sqlite:///{DB_PATH}"

# Engine singleton simples para evitar recriar a cada chamada (mais estável em Streamlit).
_ENGINE = None
def get_engine():
    """
    Retorna um engine SQLAlchemy para SQLite no arquivo local (writable).
    Usa check_same_thread=False para compat Streamlit.
    """
    global _ENGINE
    if _ENGINE is None:
        _ENGINE = create_engine(
            DB_URI,
            future=True,
            echo=False,
            connect_args={"check_same_thread": False}
        )
    return _ENGINE


def _ensure_db_file_writable():
    """
    Garante que o arquivo do DB exista e tenha permissão de escrita.
    """
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    if not os.path.exists(DB_PATH):
        # cria arquivo vazio para assegurar permissão
        open(DB_PATH, "a").close()
    try:
        os.chmod(DB_PATH, 0o666)
    except Exception:
        pass


def init_db():
    """
    Cria/atualiza a estrutura do banco:
    - Tabela original: pacientes_unicos_por_dia_prestador
    - Catálogo: procedimento_tipos
    - Catálogo: cirurgia_situacoes
    - Tabela: cirurgias
    """
    _ensure_db_file_writable()
    engine = get_engine()
    with engine.begin() as conn:
        # ---- Tabela original (mantida) ----
        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS pacientes_unicos_por_dia_prestador (
            Hospital    TEXT,
            Ano         INTEGER,
            Mes         INTEGER,
            Dia         INTEGER,
            Data        TEXT,
            Atendimento TEXT,
            Paciente    TEXT,
            Aviso       TEXT,
            Convenio    TEXT,
            Prestador   TEXT,
            Quarto      TEXT
        );
        """))

        conn.execute(text("""
        CREATE UNIQUE INDEX IF NOT EXISTS idx_unicidade
        ON pacientes_unicos_por_dia_prestador (Data, Paciente, Prestador, Hospital);
        """))

        conn.execute(text("""
        CREATE INDEX IF NOT EXISTS idx_hospital_calendario
        ON pacientes_unicos_por_dia_prestador (Hospital, Ano, Mes, Dia);
        """))

        conn.execute(text("""
        CREATE INDEX IF NOT EXISTS idx_hospital_calendario_prestador
        ON pacientes_unicos_por_dia_prestador (Hospital, Ano, Mes, Dia, Prestador);
        """))

        # ---- Catálogo: Tipos de Procedimento ----
        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS procedimento_tipos (
            id     INTEGER PRIMARY KEY AUTOINCREMENT,
            nome   TEXT NOT NULL UNIQUE,
            ativo  INTEGER NOT NULL DEFAULT 1,
            ordem  INTEGER DEFAULT 0
        );
        """))

        conn.execute(text("""
        CREATE INDEX IF NOT EXISTS idx_proc_tipos_ativo
        ON procedimento_tipos (ativo, ordem, nome);
        """))

        # ---- Catálogo: Situações da Cirurgia ----
        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS cirurgia_situacoes (
            id     INTEGER PRIMARY KEY AUTOINCREMENT,
            nome   TEXT NOT NULL UNIQUE,
            ativo  INTEGER NOT NULL DEFAULT 1,
            ordem  INTEGER DEFAULT 0
        );
        """))

        conn.execute(text("""
        CREATE INDEX IF NOT EXISTS idx_cir_sit_ativo
        ON cirurgia_situacoes (ativo, ordem, nome);
        """))

        # ---- Registro de Cirurgias ----
        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS cirurgias (
            id       INTEGER PRIMARY KEY AUTOINCREMENT,
            Hospital TEXT NOT NULL,
            Atendimento TEXT,
            Paciente TEXT,
            Prestador TEXT,
            Data_Cirurgia TEXT,         -- formato livre (ex.: dd/MM/yyyy ou YYYY-MM-DD)
            Convenio TEXT,
            Procedimento_Tipo_ID INTEGER,
            Situacao_ID INTEGER,
            Guia_AMHPTISS TEXT,
            Guia_AMHPTISS_Complemento TEXT,
            Fatura TEXT,
            Observacoes TEXT,
            created_at TEXT DEFAULT (datetime('now')),
            updated_at TEXT,
            FOREIGN KEY (Procedimento_Tipo_ID) REFERENCES procedimento_tipos(id),
            FOREIGN KEY (Situacao_ID) REFERENCES cirurgia_situacoes(id)
        );
        """))

        # Índices úteis para consultas
        conn.execute(text("""
        CREATE INDEX IF NOT EXISTS idx_cirurgias_hosp_data
        ON cirurgias (Hospital, Data_Cirurgia);
        """))

        conn.execute(text("""
        CREATE INDEX IF NOT EXISTS idx_cirurgias_atendimento
        ON cirurgias (Atendimento);
        """))

        conn.execute(text("""
        CREATE INDEX IF NOT EXISTS idx_cirurgias_paciente
        ON cirurgias (Paciente);
        """))

        # Evita duplicar mesma cirurgia com chave composta (ajuste conforme regra desejada)
        conn.execute(text("""
        CREATE UNIQUE INDEX IF NOT EXISTS idx_cirurgia_unica
        ON cirurgias (Hospital, Atendimento, Prestador, Data_Cirurgia);
        """))


def _safe_int(val, default: int = 0) -> int:
    """
    Converte em int com segurança (None/NaN/strings vazias viram default).
    """
    if val is None:
        return default
    if isinstance(val, float):
        try:
            if math.isnan(val):
                return default
        except Exception:
            pass
    s = str(val).strip()
    if s == "":
        return default
    try:
        return int(float(s))
    except Exception:
        return default


def _safe_str(val, default: str = "") -> str:
    """
    Converte em str com segurança (None/NaN viram default) e trim.
    """
    if val is None:
        return default
    if isinstance(val, float):
        try:
            if math.isnan(val):
                return default
        except Exception:
            pass
    return str(val).strip()


# ---------------- UPSERT original (pacientes_unicos_por_dia_prestador) ----------------
def upsert_dataframe(df):
    """
    UPSERT (INSERT OR REPLACE) por (Data, Paciente, Prestador, Hospital).
    """
    if df is None or len(df) == 0:
        return

    if "Paciente" not in df.columns:
        raise ValueError("Coluna 'Paciente' não encontrada no DataFrame.")

    blank_mask = df["Paciente"].astype(str).str.strip() == ""
    num_blank = int(blank_mask.sum())
    if num_blank > 0:
        raise ValueError(
            f"Existem {num_blank} registro(s) com 'Paciente' vazio. "
            "Preencha todos os nomes antes de salvar."
        )

    engine = get_engine()
    with engine.begin() as conn:
        for _, row in df.iterrows():
            conn.execute(text("""
                INSERT OR REPLACE INTO pacientes_unicos_por_dia_prestador
                (Hospital, Ano, Mes, Dia, Data, Atendimento, Paciente, Aviso, Convenio, Prestador, Quarto)
                VALUES (:Hospital, :Ano, :Mes, :Dia, :Data, :Atendimento, :Paciente, :Aviso, :Convenio, :Prestador, :Quarto)
            """), {
                "Hospital":    _safe_str(row.get("Hospital", "")),
                "Ano":         _safe_int(row.get("Ano", 0)),
                "Mes":         _safe_int(row.get("Mes", 0)),
                "Dia":         _safe_int(row.get("Dia", 0)),
                "Data":        _safe_str(row.get("Data", "")),
                "Atendimento": _safe_str(row.get("Atendimento", "")),
                "Paciente":    _safe_str(row.get("Paciente", "")),
                "Aviso":       _safe_str(row.get("Aviso", "")),
                "Convenio":    _safe_str(row.get("Convenio", "")),
                "Prestador":   _safe_str(row.get("Prestador", "")),
                "Quarto":      _safe_str(row.get("Quarto", "")),
            })


def read_all():
    engine = get_engine()
    with engine.connect() as conn:
        rs = conn.execute(text("""
            SELECT Hospital, Ano, Mes, Dia, Data, Atendimento, Paciente, Aviso, Convenio, Prestador, Quarto
            FROM pacientes_unicos_por_dia_prestador
            ORDER BY Hospital, Ano, Mes, Dia, Paciente, Prestador
        """))
        rows = rs.fetchall()
    return rows


# ---------- Utilitários opcionais ----------
def read_by_hospital(hospital: str):
    engine = get_engine()
    with engine.connect() as conn:
        rs = conn.execute(text("""
            SELECT Hospital, Ano, Mes, Dia, Data, Atendimento, Paciente, Aviso, Convenio, Prestador, Quarto
            FROM pacientes_unicos_por_dia_prestador
            WHERE Hospital = :h
            ORDER BY Ano, Mes, Dia, Paciente, Prestador
        """), {"h": hospital})
        return rs.fetchall()


def read_by_hospital_period(hospital: str, ano: Optional[int] = None, mes: Optional[int] = None):
    engine = get_engine()
    where = ["Hospital = :h"]
    params = {"h": hospital}
    if ano is not None:
        where.append("Ano = :a"); params["a"] = int(ano)
    if mes is not None:
        where.append("Mes = :m"); params["m"] = int(mes)
    sql = f"""
        SELECT Hospital, Ano, Mes, Dia, Data, Atendimento, Paciente, Aviso, Convenio, Prestador, Quarto
        FROM pacientes_unicos_por_dia_prestador
        WHERE {' AND '.join(where)}
        ORDER BY Ano, Mes, Dia, Paciente, Prestador
    """
    with engine.connect() as conn:
        rs = conn.execute(text(sql), params)
        return rs.fetchall()


def delete_all():
    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM pacientes_unicos_por_dia_prestador"))


def count_all():
    engine = get_engine()
    with engine.connect() as conn:
        rs = conn.execute(text("SELECT COUNT(1) FROM pacientes_unicos_por_dia_prestador"))
        return rs.scalar_one()


# ---------------- Catálogos (Tipos / Situações) ----------------
def upsert_procedimento_tipo(nome: str, ativo: int = 1, ordem: int = 0) -> int:
    _ensure_db_file_writable()
    engine = get_engine()
    with engine.begin() as conn:
        rs = conn.execute(text("SELECT id FROM procedimento_tipos WHERE nome = :n"), {"n": nome.strip()})
        row = rs.fetchone()
        if row:
            conn.execute(text("""
                UPDATE procedimento_tipos SET ativo = :a, ordem = :o WHERE id = :id
            """), {"a": int(ativo), "o": int(ordem), "id": int(row[0])})
            return int(row[0])
        else:
            result = conn.execute(text("""
                INSERT INTO procedimento_tipos (nome, ativo, ordem) VALUES (:n, :a, :o)
            """), {"n": nome.strip(), "a": int(ativo), "o": int(ordem)})
            return int(result.lastrowid)


def list_procedimento_tipos(only_active: bool = True):
    engine = get_engine()
    with engine.connect() as conn:
        if only_active:
            rs = conn.execute(text("SELECT id, nome, ativo, ordem FROM procedimento_tipos WHERE ativo = 1 ORDER BY ordem, nome"))
        else:
            rs = conn.execute(text("SELECT id, nome, ativo, ordem FROM procedimento_tipos ORDER BY ativo DESC, ordem, nome"))
        return rs.fetchall()


def set_procedimento_tipo_status(id_: int, ativo: int):
    _ensure_db_file_writable()
    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(text("UPDATE procedimento_tipos SET ativo = :a WHERE id = :id"), {"a": int(ativo), "id": int(id_)})


def upsert_cirurgia_situacao(nome: str, ativo: int = 1, ordem: int = 0) -> int:
    _ensure_db_file_writable()
    engine = get_engine()
    with engine.begin() as conn:
        rs = conn.execute(text("SELECT id FROM cirurgia_situacoes WHERE nome = :n"), {"n": nome.strip()})
        row = rs.fetchone()
        if row:
            conn.execute(text("""
                UPDATE cirurgia_situacoes SET ativo = :a, ordem = :o WHERE id = :id
            """), {"a": int(ativo), "o": int(ordem), "id": int(row[0])})
            return int(row[0])
        else:
            result = conn.execute(text("""
                INSERT INTO cirurgia_situacoes (nome, ativo, ordem) VALUES (:n, :a, :o)
            """), {"n": nome.strip(), "a": int(ativo), "o": int(ordem)})
            return int(result.lastrowid)


def list_cirurgia_situacoes(only_active: bool = True):
    engine = get_engine()
    with engine.connect() as conn:
        if only_active:
            rs = conn.execute(text("SELECT id, nome, ativo, ordem FROM cirurgia_situacoes WHERE ativo = 1 ORDER BY ordem, nome"))
        else:
            rs = conn.execute(text("SELECT id, nome, ativo, ordem FROM cirurgia_situacoes ORDER BY ativo DESC, ordem, nome"))
        return rs.fetchall()


def set_cirurgia_situacao_status(id_: int, ativo: int):
    _ensure_db_file_writable()
    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(text("UPDATE cirurgia_situacoes SET ativo = :a WHERE id = :id"), {"a": int(ativo), "id": int(id_)})


# ---------------- Cirurgias (CRUD) ----------------
def insert_or_update_cirurgia(payload: Dict[str, Any]) -> int:
    """
    UPSERT por (Hospital, Atendimento, Prestador, Data_Cirurgia).
    Retorna id da cirurgia.
    """
    _ensure_db_file_writable()
    engine = get_engine()
    with engine.begin() as conn:
        rs = conn.execute(text("""
            SELECT id FROM cirurgias
            WHERE Hospital = :h AND Atendimento = :att AND Prestador = :p AND Data_Cirurgia = :d
        """), {
            "h": _safe_str(payload.get("Hospital")),
            "att": _safe_str(payload.get("Atendimento")),
            "p": _safe_str(payload.get("Prestador")),
            "d": _safe_str(payload.get("Data_Cirurgia"))
        })
        row = rs.fetchone()

        params = {
            "Hospital": _safe_str(payload.get("Hospital")),
            "Atendimento": _safe_str(payload.get("Atendimento")),
            "Paciente": _safe_str(payload.get("Paciente")),
            "Prestador": _safe_str(payload.get("Prestador")),
            "Data_Cirurgia": _safe_str(payload.get("Data_Cirurgia")),
            "Convenio": _safe_str(payload.get("Convenio")),
            "Procedimento_Tipo_ID": payload.get("Procedimento_Tipo_ID"),
            "Situacao_ID": payload.get("Situacao_ID"),
            "Guia_AMHPTISS": _safe_str(payload.get("Guia_AMHPTISS")),
            "Guia_AMHPTISS_Complemento": _safe_str(payload.get("Guia_AMHPTISS_Complemento")),
            "Fatura": _safe_str(payload.get("Fatura")),
            "Observacoes": _safe_str(payload.get("Observacoes")),
        }

        if row:
            conn.execute(text("""
                UPDATE cirurgias SET
                    Paciente = :Paciente,
                    Convenio = :Convenio,
                    Procedimento_Tipo_ID = :Procedimento_Tipo_ID,
                    Situacao_ID = :Situacao_ID,
                    Guia_AMHPTISS = :Guia_AMHPTISS,
                    Guia_AMHPTISS_Complemento = :Guia_AMHPTISS_Complemento,
                    Fatura = :Fatura,
                    Observacoes = :Observacoes,
                    updated_at = datetime('now')
                WHERE id = :id
            """), {**params, "id": int(row[0])})
            return int(row[0])
        else:
            result = conn.execute(text("""
                INSERT INTO cirurgias (
                    Hospital, Atendimento, Paciente, Prestador, Data_Cirurgia,
                    Convenio, Procedimento_Tipo_ID, Situacao_ID,
                    Guia_AMHPTISS, Guia_AMHPTISS_Complemento, Fatura, Observacoes
                ) VALUES (
                    :Hospital, :Atendimento, :Paciente, :Prestador, :Data_Cirurgia,
                    :Convenio, :Procedimento_Tipo_ID, :Situacao_ID,
                    :Guia_AMHPTISS, :Guia_AMHPTISS_Complemento, :Fatura, :Observacoes
                )
            """), params)
            return int(result.lastrowid)


def list_cirurgias(
    hospital: Optional[str] = None,
    ano_mes: Optional[str] = None,  # "YYYY-MM" ou "MM/YYYY"
    prestador: Optional[str] = None
):
    """
    Lista cirurgias com filtros simples.
    Obs.: Como Data_Cirurgia é TEXT, o filtro 'ano_mes' faz um LIKE na string.
    """
    engine = get_engine()
    where = []
    params = {}
    if hospital:
        where.append("Hospital = :h"); params["h"] = hospital
    if prestador:
        where.append("Prestador = :p"); params["p"] = prestador
    if ano_mes:
        where.append("Data_Cirurgia LIKE :dm"); params["dm"] = f"%{ano_mes}%"

    sql = f"""
        SELECT id, Hospital, Atendimento, Paciente, Prestador, Data_Cirurgia,
               Convenio, Procedimento_Tipo_ID, Situacao_ID,
               Guia_AMHPTISS, Guia_AMHPTISS_Complemento, Fatura,
               Observacoes, created_at, updated_at
        FROM cirurgias
        {('WHERE ' + ' AND '.join(where)) if where else ''}
        ORDER BY Hospital, Data_Cirurgia, Paciente
    """
    with engine.connect() as conn:
        rs = conn.execute(text(sql), params)
        return rs.fetchall()


def delete_cirurgia(id_: int):
    _ensure_db_file_writable()
    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM cirurgias WHERE id = :id"), {"id": int(id_)})


# ------- Helper para pré-preenchimento a partir da tabela original -------
def find_registros_para_prefill(
    hospital: str,
    ano: Optional[int] = None,
    mes: Optional[int] = None,
    prestadores: Optional[List[str]] = None
):
    """
    Retorna registros da tabela base (pacientes_unicos_por_dia_prestador) para servir de base na criação de cirurgias.

    Filtros:
      - Hospital (TRIM + UPPER)
      - Ano/Mês (opcionais)
        * Caso Ano/Mês na tabela estejam NULL/0, faz fallback por Data LIKE suportando:
          - dd/MM/yyyy  -> padrão com “/”
          - YYYY-MM-DD  -> padrão ISO com “-”
      - Prestadores (opcional) — filtrado em Python com normalização agressiva (sem acentos, sem espaços/pontuação, UPPER).
    """
    engine = get_engine()

    # ---- Normalizadores para filtro em Python ----
    import unicodedata
    def _strip_accents(s: str) -> str:
        return "".join(ch for ch in unicodedata.normalize("NFKD", s) if not unicodedata.combining(ch))

    def _normalize_name(s: Optional[str]) -> str:
        if s is None:
            return ""
        t = _strip_accents(str(s)).upper()
        for ch in (" ", ".", "-", "_", "/", "\\"):
            t = t.replace(ch, "")
        return t.strip()

    # ---- WHERE base: Hospital ----
    where = ["UPPER(TRIM(Hospital)) = UPPER(:h)"]
    params = {"h": hospital.strip()}

    # ---- Filtros de Ano/Mês com fallbacks robustos ----
    if ano is not None and mes is not None:
        params["a"] = int(ano)
        params["m"] = int(mes)
        # dd/MM/yyyy
        params["dm_like_slash"] = f"%/{int(mes):02d}/{int(ano)}%"
        # YYYY-MM-DD (ISO)
        params["dm_like_dash"] = f"{int(ano)}-{int(mes):02d}-%"

        where.append("(Ano = :a OR Ano IS NULL OR Ano = 0 OR Data LIKE :dm_like_slash OR Data LIKE :dm_like_dash)")
        where.append("(Mes = :m OR Mes IS NULL OR Mes = 0 OR Data LIKE :dm_like_slash OR Data LIKE :dm_like_dash)")

    elif ano is not None:
        params["a"] = int(ano)
        # dd/MM/yyyy: .../YYYY
        params["a_like_slash"] = f"%/{int(ano)}%"
        # YYYY-MM-DD: YYYY-
        params["a_like_dash"] = f"{int(ano)}-%"

        where.append("(Ano = :a OR Ano IS NULL OR Ano = 0 OR Data LIKE :a_like_slash OR Data LIKE :a_like_dash)")

    elif mes is not None:
        params["m"] = int(mes)
        # dd/MM/yyyy: /MM/
        params["m_like_slash"] = f"%/{int(mes):02d}/%"
        # YYYY-MM-DD: -MM-
        params["m_like_dash"] = f"%-{int(mes):02d}-%"

        where.append("(Mes = :m OR Mes IS NULL OR Mes = 0 OR Data LIKE :m_like_slash OR Data LIKE :m_like_dash)")

    sql = f"""
        SELECT Hospital, Data, Atendimento, Paciente, Convenio, Prestador
        FROM pacientes_unicos_por_dia_prestador
        WHERE {' AND '.join(where)}
        ORDER BY Ano, Mes, Dia, Paciente, Prestador
    """

    with engine.connect() as conn:
        rows = conn.execute(text(sql), params).fetchall()

    # ---- Filtro opcional por prestadores em Python ----
    prestadores = [p for p in (prestadores or []) if p and str(p).strip()]
    if not prestadores:
        return rows

    target_norm = {_normalize_name(p) for p in prestadores}
    filtered = []
    for (h, data, att, pac, conv, prest) in rows:
        if _normalize_name(prest) in target_norm:
            filtered.append((h, data, att, pac, conv, prest))

    return filtered


# ---------- (Opcional) Diagnóstico rápido ----------
def list_registros_base_all(limit: int = 500):
    engine = get_engine()
    with engine.connect() as conn:
        rs = conn.execute(text(f"""
            SELECT Hospital, Data, Atendimento, Paciente, Convenio, Prestador
            FROM pacientes_unicos_por_dia_prestador
            ORDER BY Hospital, Ano, Mes, Dia, Paciente, Prestador
            LIMIT {int(limit)}
        """))
        return rs.fetchall()  
# export.py
import io
import re
import pandas as pd

# ---------------- Helpers de formatação ----------------

_INVALID_SHEET_CHARS_RE = re.compile(r'[:\\/?*\[\]]')

def _sanitize_sheet_name(name: str, fallback: str = "Dados") -> str:
    """
    Limpa o nome da aba para atender restrições do Excel:
    - remove caracteres inválidos: : \ / ? * [ ]
    - limita a 31 caracteres
    - se vazio após limpeza, usa fallback
    """
    if not name:
        name = fallback

    name = str(name).strip()
    name = _INVALID_SHEET_CHARS_RE.sub("", name)

    if not name:
        name = fallback

    # Excel limita a 31 caracteres
    return name[:31]


def _write_sheet(writer: pd.ExcelWriter, sheet_name: str, df: pd.DataFrame):
    """
    Escreve o DataFrame com cabeçalho formatado, autofiltro e ajuste de larguras.
    - Mantém index=False para planilhas limpas.
    - Aplica estilo ao cabeçalho.
    - Cria autofiltro no range completo.
    - Ajusta larguras de coluna com base no maior conteúdo observado.
    """
    df = df.copy()

    # Converte colunas com objetos complexos em string para evitar erros de escrita
    for c in df.columns:
        # Garante que tudo que não é escalar/str vire string
        if df[c].dtype == "object":
            df[c] = df[c].apply(lambda x: "" if x is None else str(x))

    df.to_excel(writer, sheet_name=sheet_name, index=False)
    wb = writer.book
    ws = writer.sheets[sheet_name]

    # Cabeçalho
    header_fmt = wb.add_format({
        "bold": True,
        "bg_color": "#DCE6F1",
        "border": 1
    })

    for col_num, value in enumerate(df.columns.values):
        ws.write(0, col_num, value, header_fmt)

    # Autofiltro (range correto)
    last_row = max(len(df), 1)
    ws.autofilter(0, 0, last_row, max(0, len(df.columns) - 1))

    # Ajuste automático de largura com limites razoáveis
    for i, col in enumerate(df.columns):
        valores = [str(x) for x in df[col].tolist()]
        maxlen = max([len(str(col))] + [len(v) for v in valores]) + 2
        ws.set_column(i, i, max(14, min(maxlen, 60)))


# ---------------- Exportações (Pacientes) ----------------

def to_formatted_excel(
    df: pd.DataFrame,
    sheet_name: str = "Pacientes por dia e prestador"
) -> io.BytesIO:
    """
    Gera Excel em memória com:
    - Cabeçalho formatado
    - Autofiltro
    - Largura automática das colunas
    """
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        _write_sheet(writer, _sanitize_sheet_name(sheet_name), df)
    output.seek(0)
    return output


def to_formatted_excel_by_hospital(df: pd.DataFrame) -> io.BytesIO:
    """
    Gera um Excel com uma aba por Hospital.
    - Normaliza o nome do Hospital
    - Ordena abas alfabeticamente
    - Em cada aba, ordena por: Ano, Mes, Dia, Paciente, Prestador (quando existirem)
    """
    output = io.BytesIO()

    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        if "Hospital" not in df.columns:
            _write_sheet(writer, "Dados", df)
        else:
            df_aux = df.copy()
            df_aux["Hospital"] = (
                df_aux["Hospital"]
                .fillna("Sem_Hospital")
                .astype(str)
                .str.strip()
                .replace("", "Sem_Hospital")
            )

            order_cols = [c for c in ["Ano", "Mes", "Dia", "Paciente", "Prestador"] if c in df_aux.columns]

            # Ordena hospitais para gerar abas previsíveis
            for hosp in sorted(df_aux["Hospital"].unique()):
                dfh = df_aux[df_aux["Hospital"] == hosp].copy()
                if order_cols:
                    dfh = dfh.sort_values(order_cols, kind="mergesort")

                sheet_name = _sanitize_sheet_name(hosp, fallback="Sem_Hospital")
                _write_sheet(writer, sheet_name, dfh)

    output.seek(0)
    return output


# ---------------- Exportações (Cirurgias) ----------------

def to_formatted_excel_cirurgias(df: pd.DataFrame) -> io.BytesIO:
    """
    Exporta cirurgias em Excel.
    - Se houver coluna 'Hospital', cria multi-aba por hospital
    - Ordena cada aba por Data_Cirurgia e Paciente, quando existirem
    - Mantém cabeçalho formatado, autofiltro e largura automática
    """
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        if "Hospital" not in df.columns:
            _write_sheet(writer, "Cirurgias", df)
        else:
            df_aux = df.copy()
            df_aux["Hospital"] = (
                df_aux["Hospital"]
                .fillna("Sem_Hospital")
                .astype(str)
                .str.strip()
                .replace("", "Sem_Hospital")
            )

            # Ordena hospitais para gerar abas consistentes
            for hosp in sorted(df_aux["Hospital"].unique()):
                dfh = df_aux[df_aux["Hospital"] == hosp].copy()

                # Ordena colunas se existirem
                order_cols = [c for c in ["Data_Cirurgia", "Paciente"] if c in dfh.columns]
                if order_cols:
                    dfh = dfh.sort_values(order_cols, kind="mergesort")

                sheet_name = _sanitize_sheet_name(hosp, fallback="Sem_Hospital")
                _write_sheet(writer, sheet_name, dfh)

    output.seek(0)
    return output  
# github_sync.py
import base64
import os
import json
import requests
import streamlit as st

def _gh_headers():
    token = st.secrets.get("GITHUB_TOKEN", "")
    if not token:
        raise RuntimeError("GITHUB_TOKEN não encontrado em st.secrets.")
    return {
        "Authorization": f"token {token}",
        "Accept": "application/vnd.github+json"
    }

def _normalize_repo_path(path_in_repo: str) -> str:
    """
    Normaliza o 'path' exigido pela API /repos/{owner}/{repo}/contents/{path}:
    - não pode começar com '/', './' ou '.\\'
    - remove espaços nas pontas
    """
    if not path_in_repo:
        raise ValueError("path_in_repo vazio.")
    p = str(path_in_repo).strip()
    while p.startswith("/") or p.startswith("./") or p.startswith(".\\") or p.startswith("\\"):
        if p.startswith("./"):
            p = p[2:]
        elif p.startswith(".\\"):
            p = p[3:]
        else:
            p = p[1:]
    if p == "":
        raise ValueError("path_in_repo inválido após normalização.")
    return p

def download_db_from_github(owner: str, repo: str, path_in_repo: str, branch: str, local_db_path: str) -> bool:
    """
    Baixa (GET contents) o arquivo SQLite do GitHub e grava em local_db_path.
    Retorna True se baixou, False se não existe no repo.
    """
    path_norm = _normalize_repo_path(path_in_repo)
    url = f"https://api.github.com/repos/{owner}/{repo}/contents/{path_norm}?ref={branch}"
    headers = _gh_headers()
    r = requests.get(url, headers=headers)

    if r.status_code == 200:
        data = r.json()
        if "content" not in data:
            raise RuntimeError(f"Resposta inesperada da API do GitHub (sem 'content'): {json.dumps(data)[:300]}")
        content_b64 = data["content"].replace("\n", "")
        content_bytes = base64.b64decode(content_b64)

        os.makedirs(os.path.dirname(local_db_path), exist_ok=True)
        with open(local_db_path, "wb") as f:
            f.write(content_bytes)

        # garante permissão de escrita para o processo
        try:
            os.chmod(local_db_path, 0o666)
        except Exception:
            pass

        return True

    elif r.status_code == 404:
        return False
    else:
        raise RuntimeError(f"Falha ao baixar do GitHub: {r.status_code} - {r.text}")

def upload_db_to_github(owner: str, repo: str, path_in_repo: str, branch: str, local_db_path: str, commit_message: str = "Atualiza banco SQLite via app") -> bool:
    """
    Sobe (PUT contents) o arquivo SQLite para o GitHub (commit/push).
    Se o arquivo já existir, passa o 'sha' para atualizar.
    Retorna True em sucesso.
    """
    if not os.path.exists(local_db_path):
        raise FileNotFoundError(f"Arquivo local não encontrado: {local_db_path}")

    path_norm = _normalize_repo_path(path_in_repo)
    url = f"https://api.github.com/repos/{owner}/{repo}/contents/{path_norm}"
    headers = _gh_headers()

    r_get = requests.get(url, headers=headers, params={"ref": branch})
    sha = r_get.json().get("sha") if r_get.status_code == 200 else None

    with open(local_db_path, "rb") as f:
        content_bytes = f.read()
    content_b64 = base64.b64encode(content_bytes).decode("utf-8")

    payload = {
        "message": commit_message,
        "content": content_b64,
        "branch": branch,
        "committer": {"name": "Streamlit App", "email": "streamlit@app.local"},
    }
    if sha:
        payload["sha"] = sha

    r_put = requests.put(url, headers=headers, json=payload)
    if r_put.status_code in (200, 201):
        return True
    else:
        raise RuntimeError(f"Falha ao subir para GitHub: {r_put.status_code} - {r_put.text}")  
# processing.py
import io
import csv
import re
import unicodedata
import numpy as np
import pandas as pd
from dateutil import parser as dtparser  # reservado para futuras evoluções

# =========================
# Regex / Constantes
# =========================

TIME_RE = re.compile(r"^\d{1,2}:\d{2}$")
DATE_RE = re.compile(r"\b(\d{2}/\d{2}/\d{4})\b")
HAS_LETTER_RE = re.compile(r"[A-Za-zÁÉÍÓÚÃÕÇáéíóúãõç]")
SECTION_KEYWORDS = ["CENTRO CIRURGICO", "HEMODINAMICA", "CENTRO OBSTETRICO"]

EXPECTED_COLS = [
    "Centro", "Data", "Atendimento", "Paciente", "Aviso",
    "Hora_Inicio", "Hora_Fim", "Cirurgia", "Convenio", "Prestador",
    "Anestesista", "Tipo_Anestesia", "Quarto"
]

REQUIRED_COLS = [
    "Data", "Prestador", "Hora_Inicio",
    "Atendimento", "Paciente", "Aviso",
    "Convenio", "Quarto"
]

# Conjunto de "hints" que indicam texto de procedimento (não nome de paciente)
PROCEDURE_HINTS = {
    "HERNIA", "HERNIORRAFIA", "COLECISTECTOMIA", "APENDICECTOMIA",
    "ENDOMETRIOSE", "SINOVECTOMIA", "OSTEOCONDROPLASTIA", "ARTROPLASTIA",
    "ADENOIDECTOMIA", "AMIGDALECTOMIA", "ETMOIDECTOMIA", "SEPTOPLASTIA",
    "TURBINECTOMIA", "MIOMECTOMIA", "HISTEROSCOPIA", "HISTERECTOMIA",
    "ENXERTO", "TENOLISE", "MICRONEUROLISE", "URETERO", "NEFRECTOMIA",
    "LAPAROTOMIA", "LAPAROSCOPICA", "ROBOTICA", "BIOPSIA", "CRANIOTOMIA",
    "RETIRADA", "DRENAGEM", "FISTULECTOMIA", "HEMOSTA", "ARTRODESE",
    "OSTEOTOMIA", "SEPTOPLASTA", "CIRURGIA", "EXERESE", "RESSECCAO",
    "URETEROLITOTRIPSIA", "URETEROSCOPIA", "ENDOSCOPICA", "ENDOSCOPIA",
    "CATETER", "AMIGDALECTOMIA LINGUAL", "CERVICOTOMIA", "TIREOIDECTOMIA",
    "LINFADENECTOMIA", "RECONSTRUÇÃO", "RETOSSIGMOIDECTOMIA", "PLEUROSCOPIA",
}

def _is_probably_procedure_token(tok) -> bool:
    """
    Heurística para sinalizar que um token parece ser texto de procedimento (não paciente).
    Evita avaliar boolean de pd.NA.
    """
    if tok is None or pd.isna(tok):
        return False
    T = str(tok).upper().strip()
    # Sinais de procedimento/painel técnico
    if any(h in T for h in PROCEDURE_HINTS):
        return True
    # Muitos sinais de "frase técnica"
    if ("," in T) or ("/" in T) or ("(" in T) or (")" in T) or ("%" in T) or ("  " in T) or ("-" in T):
        return True
    # Muito longo para nome de pessoa
    if len(T) > 50:
        return True
    return False

def _strip_accents(s: str) -> str:
    """Remove acentos para comparações robustas (Prestador, etc.)."""
    if s is None or pd.isna(s):
        return ""
    s = str(s)
    return "".join(ch for ch in unicodedata.normalize("NFKD", s) if not unicodedata.combining(ch))


# =========================
# Normalização de colunas
# =========================

def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normaliza cabeçalhos para evitar KeyError:
    - remove BOM, espaços no início/fim
    - mapeia sinônimos/acento para nomes esperados
    """
    if df is None or df.empty:
        return df

    # strip + remove BOM
    df.columns = [str(c).replace("\ufeff", "").strip() for c in df.columns]

    # mapa de sinônimos -> nomes esperados
    col_map = {
        "Convênio": "Convenio",
        "Convênio*": "Convenio",
        "Tipo Anestesia": "Tipo_Anestesia",
        "Hora Inicio": "Hora_Inicio",
        "Hora Início": "Hora_Inicio",
        "Hora Fim": "Hora_Fim",
        "Centro Cirurgico": "Centro",
        "Centro Cirúrgico": "Centro",
    }
    df.rename(columns=col_map, inplace=True)
    return df


# =========================
# Parser de texto bruto
# =========================

def _parse_raw_text_to_rows(text: str) -> pd.DataFrame:
    """
    Parser robusto para CSV 'bruto' (relatórios exportados),
    lendo linha a linha em ordem original e extraindo campos.
    Corrigido para não confundir 'Paciente' com 'Cirurgia'.
    """
    rows = []
    current_section = None
    current_date_str = None
    ctx = {
        "atendimento": None, "paciente": None, "aviso": None,
        "hora_inicio": None, "hora_fim": None, "quarto": None
    }
    row_idx = 0

    for line in text.splitlines():
        # Detecta Data em qualquer linha
        m_date = DATE_RE.search(line)
        if m_date:
            current_date_str = m_date.group(1)

        # Tokeniza respeitando aspas
        tokens = next(csv.reader([line]))
        tokens = [t.strip() for t in tokens if t is not None]
        if not tokens:
            continue

        # Detecta seção (reinicia contexto)
        if "Centro Cirurgico" in line or "Centro Cirúrgico" in line:
            current_section = next((kw for kw in SECTION_KEYWORDS if kw in line), None)
            ctx = {k: None for k in ctx}
            continue

        # Ignora cabeçalhos/rodapés
        header_phrases = [
            "Hora", "Atendimento", "Paciente", "Convênio", "Prestador",
            "Anestesista", "Tipo Anestesia", "Total", "Total Geral"
        ]
        if any(h in line for h in header_phrases):
            continue

        # Procura horários
        time_idxs = [i for i, t in enumerate(tokens) if TIME_RE.match(t)]
        if time_idxs:
            h0 = time_idxs[0]
            h1 = h0 + 1 if (h0 + 1 < len(tokens) and TIME_RE.match(tokens[h0 + 1])) else None
            hora_inicio = tokens[h0]
            hora_fim = tokens[h1] if h1 is not None else None

            # Aviso imediatamente antes do primeiro horário (código 3+ dígitos)
            aviso = None
            if h0 - 1 >= 0 and re.fullmatch(r"\d{3,}", tokens[h0 - 1]):
                aviso = tokens[h0 - 1]

            # Atendimento e Paciente
            atendimento = None
            paciente = None

            # Procura atendimento (número 7-10 dígitos)
            for i, t in enumerate(tokens):
                if re.fullmatch(r"\d{7,10}", t):
                    atendimento = t
                    # Limita a busca do paciente ao intervalo antes do horário (h0 - 2), para não pegar 'Cirurgia'
                    upper_bound = (h0 - 2) if h0 is not None else len(tokens) - 1
                    if upper_bound >= i + 1:
                        for j in range(i + 1, upper_bound + 1):
                            tj = tokens[j]
                            # Deve ter letras, não ser horário e não "parecer" procedimento
                            if tj and HAS_LETTER_RE.search(tj) and not TIME_RE.match(tj) and not _is_probably_procedure_token(tj):
                                paciente = tj
                                break
                    break

            base_idx = h1 if h1 is not None else h0
            cirurgia     = tokens[base_idx + 1] if base_idx + 1 < len(tokens) else None
            convenio     = tokens[base_idx + 2] if base_idx + 2 < len(tokens) else None
            prestador    = tokens[base_idx + 3] if base_idx + 3 < len(tokens) else None
            anestesista  = tokens[base_idx + 4] if base_idx + 4 < len(tokens) else None
            tipo         = tokens[base_idx + 5] if base_idx + 5 < len(tokens) else None
            quarto       = tokens[base_idx + 6] if base_idx + 6 < len(tokens) else None

            rows.append({
                "Centro": current_section,
                "Data": current_date_str,
                "Atendimento": atendimento,
                "Paciente": paciente,
                "Aviso": aviso,
                "Hora_Inicio": hora_inicio,
                "Hora_Fim": hora_fim,
                "Cirurgia": cirurgia,
                "Convenio": convenio,
                "Prestador": prestador,
                "Anestesista": anestesista,
                "Tipo_Anestesia": tipo,
                "Quarto": quarto,
                "_row_idx": row_idx
            })

            # Atualiza contexto para eventuais linhas subsequentes sem horário
            ctx["atendimento"] = atendimento
            ctx["paciente"] = paciente
            ctx["aviso"] = aviso
            ctx["hora_inicio"] = hora_inicio
            ctx["hora_fim"] = hora_fim
            ctx["quarto"] = quarto

            row_idx += 1
            continue

        # Linhas sem horário (procedimentos adicionais) herdam contexto
        if current_section and any(tok for tok in tokens):
            nonempty = [t for t in tokens if t]
            if len(nonempty) >= 4:
                cirurgia     = nonempty[0]
                quarto       = nonempty[-1] if nonempty else None
                tipo         = nonempty[-2] if len(nonempty) >= 2 else None
                anestesista  = nonempty[-3] if len(nonempty) >= 3 else None
                prestador    = nonempty[-4] if len(nonempty) >= 4 else None
                convenio     = nonempty[-5] if len(nonempty) >= 5 else None

                rows.append({
                    "Centro": current_section,
                    "Data": current_date_str,
                    "Atendimento": ctx["atendimento"],
                    "Paciente": ctx["paciente"],
                    "Aviso": ctx["aviso"],
                    "Hora_Inicio": ctx["hora_inicio"],
                    "Hora_Fim": ctx["hora_fim"],
                    "Cirurgia": cirurgia,
                    "Convenio": convenio,
                    "Prestador": prestador,
                    "Anestesista": anestesista,
                    "Tipo_Anestesia": tipo,
                    "Quarto": quarto,
                    "_row_idx": row_idx
                })
                row_idx += 1

    return pd.DataFrame(rows)


# =========================
# Herança CONTROLADA
# =========================

def _herdar_por_data_ordem_original(df: pd.DataFrame) -> pd.DataFrame:
    """
    Herança linha-a-linha por Data, preservando ordem original do arquivo.

    Regras:
    - Aplica herança somente quando há Prestador na linha atual.
    - 'Atendimento' e 'Aviso' herdam sempre que estiverem vazios e houver valor anterior.
    - 'Paciente' só herda se o último paciente conhecido (no mesmo dia) NÃO estiver vazio;
      caso contrário, mantém 'Paciente' em branco para edição posterior.
    - Linhas que venham sem 'Paciente' permanecem em branco.
    """
    if df is None or df.empty:
        return df

    df = df.copy()
    df.replace({"": pd.NA}, inplace=True)

    if "_row_idx" not in df.columns:
        df["_row_idx"] = range(len(df))

    if "Data" not in df.columns:
        return df

    # Garante que Data exista em todas as linhas
    df["Data"] = df["Data"].ffill().bfill()

    # Varre dia a dia na ordem original
    for _, grp in df.groupby("Data", sort=False):
        last_att = pd.NA
        last_pac = pd.NA
        last_aviso = pd.NA

        for i in grp.sort_values("_row_idx").index:
            att = df.at[i, "Atendimento"] if "Atendimento" in df.columns else pd.NA
            pac = df.at[i, "Paciente"] if "Paciente" in df.columns else pd.NA
            av  = df.at[i, "Aviso"] if "Aviso" in df.columns else pd.NA

            # Atualiza memória com valores não vazios
            if pd.notna(att) and str(att).strip():
                last_att = att
            if pd.notna(pac) and str(pac).strip():
                last_pac = pac
            if pd.notna(av) and str(av).strip():
                last_aviso = av

            # Herança só se houver Prestador na linha atual
            has_prestador = (
                "Prestador" in df.columns and
                pd.notna(df.at[i, "Prestador"]) and
                str(df.at[i, "Prestador"]).strip() != ""
            )
            if not has_prestador:
                continue

            # Atendimento: herda se vazio
            if "Atendimento" in df.columns and (pd.isna(att) or str(att).strip() == "") and pd.notna(last_att):
                df.at[i, "Atendimento"] = last_att

            # Aviso: herda se vazio
            if "Aviso" in df.columns and (pd.isna(av) or str(av).strip() == "") and pd.notna(last_aviso):
                df.at[i, "Aviso"] = last_aviso

            # Paciente: herda somente se last_pac não estiver vazio; senão mantém blank
            if "Paciente" in df.columns and (pd.isna(pac) or str(pac).strip() == ""):
                if pd.notna(last_pac) and str(last_pac).strip() != "":
                    df.at[i, "Paciente"] = last_pac
                else:
                    df.at[i, "Paciente"] = pd.NA

    return df


# =========================
# Pipeline principal
# =========================

def process_uploaded_file(upload, prestadores_lista, selected_hospital: str):
    """
    Entrada:
      - upload: arquivo enviado pelo Streamlit (CSV/Excel/Texto)
      - prestadores_lista: lista de prestadores alvo (strings)
      - selected_hospital: nome do Hospital informado no app (aplicado a todas as linhas)

    Saída:
      DataFrame final com colunas:
        Hospital, Ano, Mes, Dia, Data, Atendimento, Paciente, Aviso, Convenio, Prestador, Quarto
    """
    name = upload.name.lower()

    # 1) Ler arquivo (CSV/Excel ou texto bruto)
    if name.endswith(".xlsx"):
        df_in = pd.read_excel(upload, engine="openpyxl")
    elif name.endswith(".xls"):
        df_in = pd.read_excel(upload, engine="xlrd")
    elif name.endswith(".csv"):
        try:
            df_in = pd.read_csv(upload, sep=",", encoding="utf-8")
            # Se não tem colunas suficientes, parseia como texto bruto
            if len(set(EXPECTED_COLS) & set(df_in.columns)) < 6:
                upload.seek(0)
                text = upload.read().decode("utf-8", errors="ignore")
                df_in = _parse_raw_text_to_rows(text)
        except Exception:
            upload.seek(0)
            text = upload.read().decode("utf-8", errors="ignore")
            df_in = _parse_raw_text_to_rows(text)
    else:
        text = upload.read().decode("utf-8", errors="ignore")
        df_in = _parse_raw_text_to_rows(text)

    # 1.1) Normaliza colunas e garante mínimas
    df_in = _normalize_columns(df_in)

    if "_row_idx" not in df_in.columns:
        df_in["_row_idx"] = range(len(df_in))

    for c in REQUIRED_COLS:
        if c not in df_in.columns:
            # cria coluna vazia com alinhamento de índice
            df_in[c] = pd.NA

    # >>> Guarda os valores CRUS pré-herança (usados na dedup híbrida e para refletir o relatório)
    df_in["__pac_raw"]   = df_in["Paciente"]
    df_in["__att_raw"]   = df_in["Atendimento"]
    df_in["__aviso_raw"] = df_in["Aviso"]

    # Sanitiza SOMENTE o __pac_raw (remove “paciente = cirurgia” / texto técnico)
    def _sanitize_one(pac_val, cir_val):
        pac = "" if pd.isna(pac_val) else str(pac_val).strip()
        cir = "" if pd.isna(cir_val) else str(cir_val).strip()
        if pac == "":
            return pd.NA
        if cir and pac.upper() == cir.upper():
            return pd.NA
        if _is_probably_procedure_token(pac):
            return pd.NA
        return pac

    df_in["__pac_raw"] = [
        _sanitize_one(p, c) for p, c in zip(
            df_in["__pac_raw"],
            df_in.get("Cirurgia", pd.Series(index=df_in.index))
        )
    ]

    # 2) Herança CONTROLADA (aplicada após salvar os CRUS)
    df = _herdar_por_data_ordem_original(df_in)

    # 3) Filtro de prestadores (case-insensitive + remoção de acentos)
    def norm(s):
        s = "" if (s is None or pd.isna(s)) else str(s)
        # remove acentos e normaliza
        s = _strip_accents(s)
        return s.strip().upper()

    target = [norm(p) for p in prestadores_lista]  # inclua "CASSIO CESAR" na chamada

    # Garante coluna Prestador
    if "Prestador" not in df.columns:
        df["Prestador"] = pd.NA

    df["Prestador_norm"] = df["Prestador"].apply(norm)
    df = df[df["Prestador_norm"].isin(target)].copy()

    # 4) start_key (ordenação temporal)
    hora_inicio = df["Hora_Inicio"] if "Hora_Inicio" in df.columns else pd.Series("", index=df.index)
    data_series = df["Data"] if "Data" in df.columns else pd.Series("", index=df.index)
    df["start_key"] = pd.to_datetime(
        data_series.fillna("").astype(str) + " " + hora_inicio.fillna("").astype(str),
        format="%d/%m/%Y %H:%M",
        errors="coerce"
    )

    # 4.1) DEDUP HÍBRIDA com VALORES CRUS (pré-herança) e regra PA/PV
    def _norm_blank(series: pd.Series) -> pd.Series:
        return series.fillna("").astype(str).str.strip().str.upper()

    P_raw  = _norm_blank(df["__pac_raw"])
    A_raw  = _norm_blank(df["__att_raw"])
    V_raw  = _norm_blank(df["__aviso_raw"])
    D      = _norm_blank(df["Data"])
    PR     = df["Prestador_norm"].fillna("").astype(str)

    # Prioriza PA (Paciente+Atendimento), depois PV (Paciente+Aviso), depois P, A, V e T (tempo)
    df["__dedup_tag"] = np.where((P_raw != "") & (A_raw != ""),
        "PA|" + D + "|" + P_raw + "|" + A_raw + "|" + PR,
        np.where((P_raw != "") & (V_raw != ""),
            "PV|" + D + "|" + P_raw + "|" + V_raw + "|" + PR,
            np.where(P_raw != "",
                "P|"  + D + "|" + P_raw + "|" + PR,
                np.where(A_raw != "",
                    "A|" + D + "|" + A_raw + "|" + PR,
                    np.where(V_raw != "",
                        "V|" + D + "|" + V_raw + "|" + PR,
                        "T|" + D + "|" + PR + "|" + df["start_key"].astype(str)
                    )
                )
            )
        )
    )

    df = df.sort_values(["Data", "Paciente", "Prestador_norm", "start_key"])
    df = df.drop_duplicates(subset=["__dedup_tag"], keep="first")

    # 🔧 Correção: usar o Paciente CRU (sanitizado) no resultado final (evita heranças indevidas)
    df["Paciente"] = df["__pac_raw"]

    # Limpeza de colunas técnicas
    df = df.drop(columns=["__dedup_tag", "__pac_raw", "__att_raw", "__aviso_raw"], errors="ignore")

    # 5) Hospital + Ano/Mes/Dia
    hosp = selected_hospital if (selected_hospital and not pd.isna(selected_hospital)) else ""
    hosp = hosp.strip() or "Hospital não informado"
    df["Hospital"] = hosp

    # Garante coluna Data antes de extrair Ano/Mes/Dia
    if "Data" not in df.columns:
        df["Data"] = pd.NA

    dt = pd.to_datetime(df["Data"], format="%d/%m/%Y", errors="coerce")
    df["Ano"] = dt.dt.year
    df["Mes"] = dt.dt.month
    df["Dia"] = dt.dt.day

    # 6) Seleção das colunas finais (organizado por ano/mês/dia)
    final_cols = [
        "Hospital", "Ano", "Mes", "Dia",
        "Data", "Atendimento", "Paciente", "Aviso",
        "Convenio", "Prestador", "Quarto"
    ]
    for c in final_cols:
        if c not in df.columns:
            df[c] = pd.NA

    out = df[final_cols].copy()

    # Ordenação para retorno
    out = out.sort_values(["Hospital", "Ano", "Mes", "Dia", "Paciente", "Prestador"]).reset_index(drop=True)
    return out
