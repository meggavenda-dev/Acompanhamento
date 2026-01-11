
# app.py
import os
import streamlit as st
import pandas as pd

from db import (
    init_db, upsert_dataframe, read_all, DB_PATH, count_all,
    upsert_autorizacoes, read_autorizacoes, count_autorizacoes, join_aut_por_atendimento,
    sync_autorizacoes_from_pacientes
)
from processing import process_uploaded_file
from export import to_formatted_excel_by_hospital, to_formatted_excel_by_status

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

st.set_page_config(page_title="Pacientes e Autorizações", layout="wide")
st.title("Pacientes únicos por data, prestador e hospital")
st.caption("Importa/edita pacientes e salva no banco → sincroniza autorizações a partir dos pacientes → acompanha status → exporta e comita no GitHub.")

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

# ---------------- Navegação por Abas ----------------
tab_pacientes, tab_autorizacoes = st.tabs(["📋 Pacientes", "✅ Autorizações"])


# ======================================================================================
# ABA 1: 📋 Pacientes (conteúdo original)
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
        help="Aceita CSV 'bruto' (sem cabeçalho padronizado) ou planilhas estruturadas.",
        key="uploader_pacientes"
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
        return f"{name}-{size}-{hospital.strip()}"

    # Botão para limpar e recomeçar (opcional)
    col_reset1, col_reset2 = st.columns(2)
    with col_reset1:
        if st.button("🧹 Limpar tabela / reset", key="btn_reset_pacientes"):
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

        # ---------------- Gravar no Banco + commit automático no GitHub ----------------
        st.subheader("Persistência")
        if st.button("Salvar no banco (exemplo.db)", key="btn_salvar_pacientes"):
            try:
                upsert_dataframe(st.session_state.df_final)

                total = count_all()
                st.success(f"Dados salvos com sucesso em exemplo.db. Total de linhas no banco: {total}")

                # Commit/push automático para GitHub
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

                # Limpa DF e editor para nova importação
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
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            key="dl_pacientes_excel"
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
    st.caption("Clique em 'Sincronizar com pacientes do banco' para espelhar os pacientes na tabela de Autorizações. Depois edite Observações/Status/Guias/Fatura e salve.")

    # Sincronização/espelhamento
    col_sync1, col_sync2 = st.columns(2)
    with col_sync1:
        if st.button("🔄 Sincronizar com pacientes do banco", help="Cria/atualiza autorizações para cada paciente (chave natural baseada em Atendimento; fallback Paciente+Data+Prestador+Hospital)."):
            try:
                novos, atualizados = sync_autorizacoes_from_pacientes(default_status="EM ANDAMENTO")
                st.success(f"Sincronização concluída. Novos: {novos} | Atualizados: {atualizados}")
            except Exception as e:
                st.error("Falha ao sincronizar autorizações a partir dos pacientes.")
                st.exception(e)

    with col_sync2:
        if st.button("📥 Recarregar autorizações do banco"):
            st.experimental_rerun()

    # Carregar autorizações do banco
    rows_aut = read_autorizacoes()
    if rows_aut:
        cols_aut = [
            "Unidade","Atendimento","Paciente","Profissional","Data_Cirurgia","Convenio","Tipo_Procedimento",
            "Observacoes","Guia_AMHPTISS","Guia_AMHPTISS_Complemento","Fatura","Status","UltimaAtualizacao"
        ]
        df_aut_db = pd.DataFrame(rows_aut, columns=cols_aut)

        # Filtros básicos
        st.subheader("Filtros")
        colf1, colf2, colf3, colf4 = st.columns(4)
        with colf1:
            status_sel = st.multiselect("Status", sorted(df_aut_db["Status"].dropna().unique().tolist()))
        with colf2:
            conv_sel = st.multiselect("Convênio", sorted(df_aut_db["Convenio"].dropna().unique().tolist()))
        with colf3:
            unid_sel = st.multiselect("Unidade (Hospital)", sorted(df_aut_db["Unidade"].dropna().unique().tolist()))
        with colf4:
            prest_sel = st.multiselect("Profissional", sorted(df_aut_db["Profissional"].dropna().unique().tolist()))

        def _apply_filters(df):
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

        # Editor: Observacoes/Status/Guias/Fatura editáveis
        st.subheader("Revisar e editar (Observações / Status / Guias / Fatura)")
        st.caption("Os campos espelhados dos pacientes (Unidade, Paciente, Profissional, Data_Cirurgia, Convênio) são atualizados pela sincronização e ficam bloqueados no editor.")

        edited_aut = st.data_editor(
            df_filtered.sort_values(["Status", "Convenio", "Paciente"], kind="mergesort").reset_index(drop=True),
            use_container_width=True,
            num_rows="fixed",
            hide_index=True,
            column_config={
                "Unidade": st.column_config.TextColumn(disabled=True),
                "Atendimento": st.column_config.TextColumn(disabled=False),
                "Paciente": st.column_config.TextColumn(disabled=True),
                "Profissional": st.column_config.TextColumn(disabled=True),
                "Data_Cirurgia": st.column_config.TextColumn(disabled=True),
                "Convenio": st.column_config.TextColumn(disabled=True),
                "Tipo_Procedimento": st.column_config.TextColumn(disabled=True),
                "Observacoes": st.column_config.TextColumn(help="Edite o texto da observação."),
                "Guia_AMHPTISS": st.column_config.TextColumn(),
                "Guia_AMHPTISS_Complemento": st.column_config.TextColumn(),
                "Fatura": st.column_config.TextColumn(),
                "Status": st.column_config.SelectboxColumn(options=[
                    "PRONTO", "NÃO COBRAR", "AGUARDAR FILIAL",
                    "A DIGITAR", "AGUARDAR PARAMETRIZAÇÃO",
                    "PENDENTE AUTORIZAÇÃO", "EM ANDAMENTO"
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

                # Commit/push automático
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

        # Conciliação com banco (por Número do Atendimento)
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
                    st.caption("Use filtros para localizar divergências. Edite o campo 'Atendimento' nas autorizações quando necessário para casar com o banco de pacientes.")
                else:
                    st.info("Sem dados para conciliar ainda. Salve e/ou sincronize.")
            except Exception as e:
                st.error("Falha ao gerar conciliação.")
                st.exception(e)

    else:
        st.info("Sincronize com os pacientes do banco para iniciar o acompanhamento.")
