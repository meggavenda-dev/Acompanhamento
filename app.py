
# app.py
import os
import sys
import streamlit as st
import pandas as pd

# =========================
# IMPORT SEGURO DO MÓDULO db
# =========================
try:
    import db as DBMOD  # importa o módulo inteiro local
except Exception as e:
    st.error("Falha ao importar o módulo local 'db'. Veja detalhes abaixo para diagnosticar.")
    st.write("sys.path:", sys.path)
    st.write("Arquivo atual (__file__):", __file__)
    st.exception(e)
    st.stop()

# Alias das funções/constantes usadas pelo app
init_db                          = getattr(DBMOD, "init_db", None)
upsert_dataframe                 = getattr(DBMOD, "upsert_dataframe", None)
read_all                         = getattr(DBMOD, "read_all", None)
DB_PATH                          = getattr(DBMOD, "DB_PATH", None)
count_all                        = getattr(DBMOD, "count_all", None)
upsert_autorizacoes              = getattr(DBMOD, "upsert_autorizacoes", None)
read_autorizacoes                = getattr(DBMOD, "read_autorizacoes", None)
count_autorizacoes               = getattr(DBMOD, "count_autorizacoes", None)
join_aut_por_atendimento         = getattr(DBMOD, "join_aut_por_atendimento", None)
sync_autorizacoes_from_pacientes = getattr(DBMOD, "sync_autorizacoes_from_pacientes", None)
# Equipe (filha)
read_equipes                     = getattr(DBMOD, "read_equipes", None)
upsert_equipes                   = getattr(DBMOD, "upsert_equipes", None)
distinct_prestadores_for_auth    = getattr(DBMOD, "distinct_prestadores_for_auth", None)
sync_equipes_from_pacientes      = getattr(DBMOD, "sync_equipes_from_pacientes", None)

_missing = [name for name, ref in [
    ("init_db", init_db),
    ("upsert_dataframe", upsert_dataframe),
    ("read_all", read_all),
    ("DB_PATH", DB_PATH),
    ("count_all", count_all),
    ("upsert_autorizacoes", upsert_autorizacoes),
    ("read_autorizacoes", read_autorizacoes),
    ("count_autorizacoes", count_autorizacoes),
    ("join_aut_por_atendimento", join_aut_por_atendimento),
    ("sync_autorizacoes_from_pacientes", sync_autorizacoes_from_pacientes),
    ("read_equipes", read_equipes),
    ("upsert_equipes", upsert_equipes),
    ("distinct_prestadores_for_auth", distinct_prestadores_for_auth),
    ("sync_equipes_from_pacientes", sync_equipes_from_pacientes),
] if ref is None]

if _missing:
    st.error("As seguintes funções/itens não foram encontradas em 'db.py': " + ", ".join(_missing))
    st.stop()

# =========================
# IMPORT DEMAIS MÓDULOS
# =========================
from processing import process_uploaded_file
from export import (
    to_formatted_excel_by_hospital,
    to_formatted_excel_by_status,
    to_formatted_excel_authorizations_with_team
)

# --- GitHub sync (baixar/subir o .db) ---
try:
    from github_sync import download_db_from_github, upload_db_to_github
    GITHUB_SYNC_AVAILABLE = True
except Exception:
    GITHUB_SYNC_AVAILABLE = False

# ---- Config GitHub (usa st.secrets; sem UI) ----
GH_OWNER  = st.secrets.get("GH_OWNER", "seu-usuario-ou-org")
GH_REPO   = st.secrets.get("GH_REPO", "seu-repo")
GH_BRANCH = st.secrets.get("GH_BRANCH", "main")

def _normalize_repo_path(p: str) -> str:
    """Evita erro 422 'path cannot start with a slash' na API de Contents do GitHub."""
    p = (p or "").strip()
    while p.startswith("/") or p.startswith("./") or p.startswith(".\\") or p.startswith("\\"):
        if p.startswith("./"):
            p = p[2:]
        elif p.startswith(".\\"):
            p = p[3:]
        else:
            p = p[1:]
    return p or "data/exemplo.db"

GH_PATH_IN_REPO  = _normalize_repo_path(st.secrets.get("GH_DB_PATH", "data/exemplo.db"))  # deve coincidir com DB_PATH
GITHUB_TOKEN_OK  = bool(st.secrets.get("GITHUB_TOKEN", ""))

# =========================
# CONFIGURAÇÃO APP
# =========================
st.set_page_config(page_title="Pacientes e Autorizações", layout="wide")
st.title("Pacientes únicos por data, prestador e hospital")
st.caption("Importa/edita pacientes e salva no banco → sincroniza autorizações a partir dos pacientes → acompanha status/equipe → exporta e comita no GitHub.")

# Flags de sessão
if "db_downloaded_shown" not in st.session_state:
    st.session_state.db_downloaded_shown = False
if "aut_sync_done" not in st.session_state:
    st.session_state.aut_sync_done = False
if "aut_sync_done_in_tab" not in st.session_state:
    st.session_state.aut_sync_done_in_tab = False

# --- helper para garantir que o arquivo/pasta são graváveis
def _ensure_writable(path: str):
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
    except Exception:
        pass
    try:
        with open(path, "ab"):
            pass
        os.chmod(path, 0o666)
    except Exception as _e:
        st.warning(f"Não foi possível garantir escrita em: {path}. Detalhes: {type(_e).__name__}")

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
        if downloaded and not st.session_state.db_downloaded_shown:
            st.success("Banco baixado do GitHub.")
            st.session_state.db_downloaded_shown = True
        elif not downloaded and not st.session_state.db_downloaded_shown:
            st.info("Banco não encontrado no GitHub (primeiro uso). Será criado localmente ao salvar.")
            st.session_state.db_downloaded_shown = True
    except Exception as e:
        st.warning("Não foi possível baixar o banco do GitHub. Verifique token/permissões em st.secrets.")
        st.exception(e)

# Garante permissão de escrita no DB (após download)
_ensure_writable(DB_PATH)

# Inicializa DB (cria tabela/índices se necessário)
init_db()

# --- Patch 1: Auto-sync de Autorizações uma vez por sessão ---
try:
    if not st.session_state.aut_sync_done:
        total_aut = count_autorizacoes()
        total_pac = count_all()
        if (total_pac > 0) and (total_aut == 0):
            novos, atualizados = sync_autorizacoes_from_pacientes(default_status="EM ANDAMENTO")
            st.session_state.aut_sync_done = True
            if novos or atualizados:
                st.info(f"Autorizações sincronizadas automaticamente: novos={novos}, atualizados={atualizados}.")
        else:
            st.session_state.aut_sync_done = True
except Exception as e:
    st.warning("Não foi possível realizar a auto-sincronização das autorizações nesta sessão.")
    st.exception(e)

# ---------------- Navegação por Abas ----------------
tab_pacientes, tab_autorizacoes = st.tabs(["📋 Pacientes", "✅ Autorizações"])


# ======================================================================================
# ABA 1: 📋 Pacientes
# ======================================================================================
with tab_pacientes:
    st.subheader("Prestadores alvo")
    prestadores_default = ["JOSE.ADORNO", "CASSIO CESAR", "FERNANDO AND", "SIMAO.MATOS"]
    prestadores_text = st.text_area(
        "Informe os prestadores (um por linha)",
        value="\n".join(prestadores_default),
        height=120,
        help="A lista é usada para filtrar os registros. A comparação é case-insensitive."
    )
    prestadores_lista = [p.strip() for p in prestadores_text.splitlines() if p.strip()]

    # Hospital do arquivo
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

    # Upload
    st.subheader("Upload de planilha (CSV ou Excel)")
    uploaded_file = st.file_uploader(
        "Escolha o arquivo",
        type=["csv", "xlsx", "xls"],
        help="Aceita CSV 'bruto' (sem cabeçalho padronizado) ou planilhas estruturadas.",
        key="uploader_pacientes"
    )

    # Estado
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

    # Reset
    col_reset1, col_reset2 = st.columns(2)
    with col_reset1:
        if st.button("🧹 Limpar tabela / reset", key="btn_reset_pacientes"):
            st.session_state.df_final = None
            st.session_state.last_upload_id = None
            st.session_state.editor_key = "editor_pacientes_reset"
            st.success("Tabela limpa. Faça novo upload para reprocessar.")

    # Processamento
    if uploaded_file is not None:
        current_upload_id = _make_upload_id(uploaded_file, selected_hospital)
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

    # Edição e persistência
    if isinstance(st.session_state.df_final, pd.DataFrame) and not st.session_state.df_final.empty:
        st.success(f"Processamento concluído! Linhas: {len(st.session_state.df_final)}")

        st.subheader("Revisar e editar nomes de Paciente (opcional)")
        st.caption("Edite apenas a coluna 'Paciente'. As demais ficam bloqueadas para evitar alterações acidentais.")

        df_to_edit = st.session_state.df_final.sort_values(
            ["Hospital", "Ano", "Mes", "Dia", "Paciente", "Prestador"]
        ).reset_index(drop=True)

        edited_df = st.data_editor(
            df_to_edit,
            use_container_width=True,
            num_rows="fixed",
            column_config={
                "Hospital":   st.column_config.TextColumn(disabled=True),
                "Ano":        st.column_config.NumberColumn(disabled=True),
                "Mes":        st.column_config.NumberColumn(disabled=True),
                "Dia":        st.column_config.NumberColumn(disabled=True),
                "Data":       st.column_config.TextColumn(disabled=True),
                "Atendimento":st.column_config.TextColumn(disabled=True),
                "Aviso":      st.column_config.TextColumn(disabled=True),
                "Convenio":   st.column_config.TextColumn(disabled=True),
                "Prestador":  st.column_config.TextColumn(disabled=True),
                "Quarto":     st.column_config.TextColumn(disabled=True),
                "Paciente":   st.column_config.TextColumn(help="Clique para editar o nome do paciente."),
            },
            hide_index=True,
            key=st.session_state.editor_key
        )

        st.session_state.df_final = edited_df

        st.subheader("Persistência")
        if st.button("Salvar no banco (exemplo.db)", key="btn_salvar_pacientes"):
            try:
                upsert_dataframe(st.session_state.df_final)
                total = count_all()
                st.success(f"Dados salvos com sucesso em exemplo.db. Total de linhas no banco: {total}")

                # Commit GitHub
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

                st.session_state.df_final = None
                st.session_state.editor_key = "editor_pacientes_after_save"

            except Exception as e:
                st.error("Falha ao salvar no banco. Veja detalhes abaixo:")
                st.exception(e)

        # Export por hospital (apenas se DF válido)
        st.subheader("Exportar Excel (multi-aba por Hospital)")
        if isinstance(st.session_state.df_final, pd.DataFrame) and not st.session_state.df_final.empty:
            excel_bytes = to_formatted_excel_by_hospital(st.session_state.df_final)
            st.download_button(
                label="Baixar Excel por Hospital",
                data=excel_bytes,
                file_name="Pacientes_por_dia_prestador_hospital.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key="dl_pacientes_excel"
            )
    else:
        st.info("Nenhum dado processado para exportar. Faça o upload e processe a planilha.")

    # Conteúdo atual
    st.divider()
    st.subheader("Conteúdo atual do banco (exemplo.db)")
    rows = read_all()
    if rows:
        cols = ["Hospital","Ano","Mes","Dia","Data","Atendimento","Paciente","Aviso","Convenio","Prestador","Quarto"]
        db_df = pd.DataFrame(rows, columns=cols)
        st.dataframe(
            db_df.sort_values(["Hospital","Ano","Mes","Dia","Paciente","Prestador"]),
            use_container_width=True
        )

        if not db_df.empty:
            st.subheader("Exportar Excel por Hospital (dados do banco)")
            excel_bytes_db = to_formatted_excel_by_hospital(db_df)
            st.download_button(
                label="Baixar Excel (Banco)",
                data=excel_bytes_db,
                file_name="Pacientes_por_dia_prestador_hospital_banco.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key="dl_pacientes_excel_banco"
            )
    else:
        st.info("Banco ainda sem dados. Faça o upload e clique em 'Salvar no banco'.")


# ======================================================================================
# ABA 2: ✅ Autorizações (sem upload; sincroniza a partir dos pacientes)
# ======================================================================================
with tab_autorizacoes:
    st.subheader("Controle de Autorizações (a partir dos pacientes do banco)")
    st.caption("Clique em 'Sincronizar com pacientes do banco' para espelhar autorizações. Depois edite Observações/Status/Guias/Fatura e salve.")

    # --- Patch 2: Auto-sync (fallback na abertura da aba, uma vez por sessão) ---
    try:
        total_aut = count_autorizacoes()
        total_pac = count_all()
        if (total_aut == 0) and (total_pac > 0) and not st.session_state.aut_sync_done_in_tab:
            novos, atualizados = sync_autorizacoes_from_pacientes(default_status="EM ANDAMENTO")
            st.session_state.aut_sync_done_in_tab = True
            if novos or atualizados:
                st.success(f"Autorizações sincronizadas automaticamente na abertura da aba: novos={novos}, atualizados={atualizados}.")
    except Exception as e:
        st.warning("Auto-sincronização na aba falhou.")
        st.exception(e)

    # Sincronização pai/equipe (manual)
    col_sync1, col_sync2, col_sync3 = st.columns(3)
    with col_sync1:
        if st.button("🔄 Sincronizar com pacientes do banco", help="Cria/atualiza autorizações (ATT ou FALLBACK: Paciente+Data+Unidade)."):
            try:
                novos, atualizados = sync_autorizacoes_from_pacientes(default_status="EM ANDAMENTO")
                st.success(f"Sincronização concluída. Novos: {novos} | Atualizados: {atualizados}")
            except Exception as e:
                st.error("Falha ao sincronizar autorizações a partir dos pacientes.")
                st.exception(e)

    with col_sync2:
        if st.button("📥 Recarregar autorizações do banco"):
            st.experimental_rerun()

    with col_sync3:
        if st.button("👥 Sincronizar equipes (a partir dos pacientes)", help="Inclui prestadores candidatos na equipe de cada autorização. Não define papel/participação."):
            try:
                novos_eq, afetadas = sync_equipes_from_pacientes()
                st.success(f"Equipes sincronizadas. Novas linhas: {novos_eq} | Autorizações afetadas: {afetadas}")
            except Exception as e:
                st.error("Falha ao sincronizar equipes.")
                st.exception(e)

    # Grid principal de autorizações (sem NK)
    rows_aut = read_autorizacoes(include_nk=False)
    if rows_aut:
        cols_aut = [
            "Unidade","Atendimento","Paciente","Profissional","Data_Cirurgia","Convenio","Tipo_Procedimento",
            "Observacoes","Guia_AMHPTISS","Guia_AMHPTISS_Complemento","Fatura","Status","UltimaAtualizacao"
        ]
        df_aut_db = pd.DataFrame(rows_aut, columns=cols_aut)

        # Filtros
        st.subheader("Filtros")
        colf1, colf2, colf3, colf4 = st.columns(4)
        with colf1:
            status_sel = st.multiselect("Status", sorted(df_aut_db["Status"].dropna().unique().tolist()))
        with colf2:
            conv_sel = st.multiselect("Convênio", sorted(df_aut_db["Convenio"].dropna().unique().tolist()))
        with colf3:
            unid_sel = st.multiselect("Unidade (Hospital)", sorted(df_aut_db["Unidade"].dropna().unique().tolist()))
        with colf4:
            prest_sel = st.multiselect("Profissional (principal)", sorted(df_aut_db["Profissional"].dropna().unique().tolist()))

        def _apply_filters(df: pd.DataFrame) -> pd.DataFrame:
            out = df.copy()
            if status_sel:
                out = out[out["Status"].isin(status_sel)]
            if conv_sel:
                out = out[out["Convenio"].isin(conv_sel)]
            if unid_sel:
                out = out[out["Unidade"].isin(unid_sel)]
            if prest_sel:
                out = out[out["Profissional"].isin(prest_sel)]
            return out

        df_filtered = _apply_filters(df_aut_db)

        # Editor principal
        st.subheader("Revisar e editar (Observações / Status / Guias / Fatura)")
        st.caption("Os campos espelhados dos pacientes (Unidade, Paciente, Profissional, Data_Cirurgia, Convênio) são atualizados pela sincronização e ficam bloqueados.")

        edited_aut = st.data_editor(
            df_filtered.sort_values(["Status","Convenio","Paciente"], kind="mergesort").reset_index(drop=True),
            use_container_width=True,
            num_rows="fixed",
            hide_index=True,
            column_config={
                "Unidade":           st.column_config.TextColumn(disabled=True),
                "Atendimento":       st.column_config.TextColumn(disabled=False),
                "Paciente":          st.column_config.TextColumn(disabled=True),
                "Profissional":      st.column_config.TextColumn(disabled=True),
                "Data_Cirurgia":     st.column_config.TextColumn(disabled=True),
                "Convenio":          st.column_config.TextColumn(disabled=True),
                "Tipo_Procedimento": st.column_config.TextColumn(disabled=True),
                "Observacoes":       st.column_config.TextColumn(help="Edite o texto da observação."),
                "Guia_AMHPTISS":     st.column_config.TextColumn(),
                "Guia_AMHPTISS_Complemento": st.column_config.TextColumn(),
                "Fatura":            st.column_config.TextColumn(),
                "Status":            st.column_config.SelectboxColumn(options=[
                    "PRONTO","NÃO COBRAR","AGUARDAR FILIAL","A DIGITAR","AGUARDAR PARAMETRIZAÇÃO","PENDENTE AUTORIZAÇÃO","EM ANDAMENTO"
                ], help="Ajuste manual, se necessário."),
                "UltimaAtualizacao": st.column_config.TextColumn(disabled=True),
            },
            key="editor_autorizacoes"
        )

        # Métricas por Status
        st.subheader("Métricas por Status")
        m_counts = edited_aut["Status"].value_counts(dropna=False).sort_index()
        ncols = min(6, max(1, len(m_counts)))
        cols_m = st.columns(ncols)
        for i, (status_name, qnt) in enumerate(m_counts.items()):
            with cols_m[i % ncols]:
                st.metric(label=status_name, value=int(qnt))

        # Persistência + GitHub
        st.subheader("Persistência (Autorizações)")
        if st.button("Salvar autorizações no banco", key="btn_salvar_aut"):
            try:
                upsert_autorizacoes(edited_aut)
                total_aut = count_autorizacoes()
                st.success(f"Autorizações salvas. Total de linhas na tabela: {total_aut}")

                if GITHUB_SYNC_AVAILABLE and GITHUB_TOKEN_OK:
                    try:
                        ok = upload_db_to_github(
                            owner=GH_OWNER,
                            repo=GH_REPO,
                            path_in_repo=GH_PATH_IN_REPO,
                            branch=GH_BRANCH,
                            local_db_path=DB_PATH,
                            commit_message="Atualiza tabela de autorizações via app (sincronizada de pacientes)"
                        )
                        if ok:
                            st.success("Sincronização automática com GitHub concluída.")
                    except Exception as e:
                        st.error("Falha ao sincronizar com GitHub (autorizações).")
                        st.exception(e)

            except Exception as e:
                st.error("Falha ao salvar autorizações.")
                st.exception(e)

        # Export por Status
        st.subheader("Exportar Excel por Status")
        excel_aut = to_formatted_excel_by_status(edited_aut)
        st.download_button(
            label="Baixar Excel (Autorizações por Status)",
            data=excel_aut,
            file_name="Autorizacoes_por_status.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            key="dl_autorizacoes_excel"
        )

        # Conciliação
        st.subheader("Conciliação com banco (por Número do Atendimento)")
        if st.toggle("Mostrar conciliação", key="toggle_join_aut"):
            try:
                joined = join_aut_por_atendimento()
                if joined:
                    cols_join = [
                        "Unidade","Atendimento","PacienteAut","ProfAut","Data_Cirurgia","Convenio","Status",
                        "Hospital","Data","PacienteDB","PrestDB"
                    ]
                    df_join = pd.DataFrame(joined, columns=cols_join)
                    df_join["Match"] = df_join["PacienteDB"].notna()
                    st.dataframe(df_join, use_container_width=True)
                    st.caption("Use filtros para localizar divergências. Ajuste 'Atendimento' nas autorizações quando necessário.")
                else:
                    st.info("Sem dados para conciliar ainda. Salve e/ou sincronize.")
            except Exception as e:
                st.error("Falha ao gerar conciliação.")
                st.exception(e)

        # ---------------- Equipe Cirúrgica (filha) ----------------
        st.subheader("Equipe Cirúrgica por Autorização")

        # Seleção de autorização por NK
        rows_aut_nk = read_autorizacoes(include_nk=True)
        if rows_aut_nk:
            cols_aut_nk = [
                "Unidade","Atendimento","Paciente","Profissional","Data_Cirurgia","Convenio","Tipo_Procedimento",
                "Observacoes","Guia_AMHPTISS","Guia_AMHPTISS_Complemento","Fatura","Status","UltimaAtualizacao","NaturalKey"
            ]
            df_aut_nk = pd.DataFrame(rows_aut_nk, columns=cols_aut_nk)

            def _label_row(r: pd.Series) -> str:
                att = str(r["Atendimento"]).strip()
                base = f"{r['Paciente']} — {r['Data_Cirurgia']} — {r['Unidade']}"
                return f"{base} (ATT:{att})" if att else base

            opts = df_aut_nk.apply(_label_row, axis=1).tolist()
            selected = st.selectbox(
                "Selecione a autorização para editar a equipe",
                options=opts,
                index=0,
                key="sel_aut_equipe"
            )
            sel_row = df_aut_nk.iloc[opts.index(selected)]
            sel_nk = sel_row["NaturalKey"]

            # Carrega equipe atual
            equipe_rows = read_equipes(sel_nk)
            df_equipe = pd.DataFrame(
                equipe_rows,
                columns=["NaturalKey","Prestador","Papel","Participacao","Observacao"]
            )
            if df_equipe.empty:
                df_equipe = pd.DataFrame(
                    columns=["NaturalKey","Prestador","Papel","Participacao","Observacao"]
                )
            df_equipe["NaturalKey"] = sel_nk  # garante NK

            st.caption("Prestadores candidatos (extraídos do módulo Pacientes para esta autorização):")
            try:
                candidatos = distinct_prestadores_for_auth(sel_nk)
                st.write(", ".join(candidatos) if candidatos else "—")
            except Exception:
                st.write("—")

            edited_team = st.data_editor(
                df_equipe,
                use_container_width=True,
                num_rows="dynamic",
                hide_index=True,
                column_config={
                    "NaturalKey": st.column_config.TextColumn(disabled=True),
                    "Prestador":  st.column_config.TextColumn(help="Nome do profissional."),
                    "Papel":      st.column_config.SelectboxColumn(
                        options=[
                            "", "Cirurgião", "Auxiliar I", "Auxiliar II", "Auxiliar III",
                            "Anestesista", "Instrumentador", "Endoscopista", "Visitante/Parecer"
                        ],
                        help="Função na equipe."
                    ),
                    "Participacao": st.column_config.TextColumn(
                        help="Percentual ou descrição (ex.: 70%, 'Responsável')."
                    ),
                    "Observacao":   st.column_config.TextColumn(help="Comentário livre."),
                },
                key=f"editor_equipe_{sel_nk}"
            )

            if st.button("Salvar equipe desta autorização", key="btn_salvar_equipe"):
                try:
                    upsert_equipes(sel_nk, edited_team)
                    st.success("Equipe salva com sucesso.")
                except Exception as e:
                    st.error("Falha ao salvar equipe.")
                    st.exception(e)
        else:
            st.info("Não há autorizações cadastradas ainda. Sincronize com os pacientes do banco.")

        # ---------------- Export completo: Autorizações + Equipes ----------------
        st.subheader("Exportar Autorizações + Equipes (completo)")
        # Autorizações com NK
        rows_aut_nk_all = read_autorizacoes(include_nk=True)
        auth_cols = [
            "Unidade","Atendimento","Paciente","Profissional","Data_Cirurgia","Convenio","Tipo_Procedimento",
            "Observacoes","Guia_AMHPTISS","Guia_AMHPTISS_Complemento","Fatura","Status","UltimaAtualizacao","NaturalKey"
        ]
        auth_df = pd.DataFrame(rows_aut_nk_all, columns=auth_cols)

        # Equipes de todas as autorizações
        team_rows_all = []
        if not auth_df.empty and "NaturalKey" in auth_df.columns:
            for nk in auth_df["NaturalKey"].dropna().astype(str).unique():
                rows_eq = read_equipes(nk)
                if rows_eq:
                    team_rows_all.extend(rows_eq)

        team_df = pd.DataFrame(
            team_rows_all,
            columns=["NaturalKey","Prestador","Papel","Participacao","Observacao"]
        )

        per_auth_tabs = st.toggle(
            "Criar aba por autorização (pode criar muitas abas)",
            value=False,
            key="toggle_tabs_per_auth"
        )

        excel_full = to_formatted_excel_authorizations_with_team(
            auth_df=auth_df,
            team_df=team_df,
            per_authorization_tabs=per_auth_tabs
        )
        st.download_button(
            label="Baixar Excel completo (Autorizações + Equipes)",
            data=excel_full,
            file_name="Autorizacoes_Equipes_Completo.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            key="dl_aut_and_team"
        )
    else:
        st.info("Sincronize com os pacientes do banco para iniciar o acompanhamento.")
