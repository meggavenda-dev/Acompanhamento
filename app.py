
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
st.caption("Download automático do banco no GitHub → Importação/Processamento → Revisão/Salvar → Exportar → Cirurgias → Catálogos")

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

# Inicializa DB
init_db()

# Lista única de hospitais para reuso
HOSPITAL_OPCOES = [
    "Hospital Santa Lucia Sul",
    "Hospital Santa Lucia Norte",
    "Hospital Maria Auxiliadora",
]

# ---------------- Abas principais ----------------
tabs = st.tabs(["📥 Importação & Pacientes", "🩺 Cirurgias", "📚 Cadastro (Tipos & Situações)"])

# ====================================================================================
# 📥 Aba 1: Importação & Pacientes
# ====================================================================================
with tabs[0]:
    st.subheader("Pacientes únicos por data, prestador e hospital")
    st.caption("Upload → herança/filtragem/deduplicação → Hospital → editar Paciente → salvar → exportar → commit automático no GitHub")

    # ---------------- Configuração dos Prestadores ----------------
    st.markdown("#### Prestadores alvo")
    prestadores_default = ["JOSE.ADORNO", "CASSIO CESAR", "FERNANDO AND", "SIMAO.MATOS"]
    prestadores_text = st.text_area(
        "Informe os prestadores (um por linha)",
        value="\n".join(prestadores_default),
        height=120,
        help="A lista é usada para filtrar os registros. A comparação é case-insensitive."
    )
    prestadores_lista = [p.strip() for p in prestadores_text.splitlines() if p.strip()]

    # ---------------- Hospital do arquivo (lista fixa) ----------------
    st.markdown("#### Hospital deste arquivo")
    selected_hospital = st.selectbox(
        "Selecione o Hospital referente à planilha enviada",
        options=HOSPITAL_OPCOES,
        index=0,
        help="O hospital selecionado será aplicado a todas as linhas processadas deste arquivo."
    )

    # ---------------- Upload de Arquivo ----------------
    st.markdown("#### Upload de planilha (CSV ou Excel)")
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
        return f"{name}-{size}-{hospital.strip()}"

    # Botão para limpar e recomeçar (opcional)
    col_reset1, _ = st.columns(2)
    with col_reset1:
        if st.button("🧹 Limpar tabela / reset"):
            st.session_state.df_final = None
            st.session_state.last_upload_id = None
            st.session_state.editor_key = "editor_pacientes_reset"
            st.success("Tabela limpa. Faça novo upload para reprocessar.")

    # Processamento (com reset automático do editor em nova importação)
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

    # ---------------- Revisão / Edição ----------------
    if st.session_state.df_final is not None and len(st.session_state.df_final) > 0:
        st.success(f"Processamento concluído! Linhas: {len(st.session_state.df_final)}")

        st.markdown("#### Revisar e editar nomes de Paciente (opcional)")
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
        st.markdown("#### Persistência")
        if st.button("Salvar no banco (exemplo.db)"):
            try:
                upsert_dataframe(st.session_state.df_final)
                total = count_all()
                st.success(f"Dados salvos com sucesso em exemplo.db. Total de linhas no banco: {total}")

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
                        st.error("Falha ao sincronizar com GitHub (commit automático).")
                        st.exception(e)

                st.session_state.df_final = None
                st.session_state.editor_key = "editor_pacientes_after_save"

            except Exception as e:
                st.error("Falha ao salvar no banco. Veja detalhes abaixo:")
                st.exception(e)

        # ---------------- Exportar Excel (por Hospital) ----------------
        st.markdown("#### Exportar Excel (multi-aba por Hospital)")
        excel_bytes = to_formatted_excel_by_hospital(st.session_state.df_final)
        st.download_button(
            label="Baixar Excel por Hospital (arquivo atual)",
            data=excel_bytes,
            file_name="Pacientes_por_dia_prestador_hospital.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

    # ---------------- Conteúdo atual do banco ----------------
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
        st.markdown("##### Exportar Excel por Hospital (dados do banco)")
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
        list_registros_base_all  # opcional
    )
    from export import to_formatted_excel_cirurgias

    # -------- Filtros de carregamento --------
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

    # Prestadores vazio por padrão → não filtra
    prestadores_filtro = st.text_input(
        "Prestadores (filtro base, separar por ; ) — deixe vazio para não filtrar",
        value=""
    )
    prestadores_lista_filtro = [p.strip() for p in prestadores_filtro.split(";") if p.strip()]

    # -------- Carregar catálogos (para dropdowns do grid) --------
    tipos = list_procedimento_tipos(only_active=True)
    sits = list_cirurgia_situacoes(only_active=True)
    tipo_nome2id = {t[1]: t[0] for t in tipos}
    tipo_id2nome = {t[0]: t[1] for t in tipos}
    sit_nome2id  = {s[1]: s[0] for s in sits}
    sit_id2nome  = {s[0]: s[1] for s in sits}

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

        st.info(f"Cirurgias já salvas encontradas: {len(df_cir)} | Candidatos da base (período/hospital): {len(df_base)}")

        if df_base.empty:
            st.warning("Nenhum candidato carregado da base com os filtros atuais.")
            st.markdown("- Confira se o **Hospital** na Aba 2 coincide com o hospital salvo na Aba 1.")
            st.markdown("- Ajuste **Ano/Mês** para o período dos registros ou desmarque **Filtrar por Ano/Mês**.")
            st.markdown("- Deixe **Prestadores** vazio para não filtrar, ou valide a escrita dos nomes.")

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
        st.caption("Edite diretamente no grid. Linhas com Fonte=Base são novos candidatos a cirurgia; ao salvar, viram registros na tabela de cirurgias.")

        edited_df = st.data_editor(
            df_union,
            use_container_width=True,
            num_rows="fixed",
            column_config={
                "id": st.column_config.NumberColumn(disabled=True),
                "Fonte": st.column_config.TextColumn(disabled=True),
                "Hospital": st.column_config.TextColumn(),
                "Atendimento": st.column_config.TextColumn(),
                "Paciente": st.column_config.TextColumn(),
                "Prestador": st.column_config.TextColumn(),
                "Data_Cirurgia": st.column_config.TextColumn(help="Formato livre, ex.: dd/MM/yyyy ou YYYY-MM-DD."),
                "Convenio": st.column_config.TextColumn(),
                "Tipo (nome)": st.column_config.SelectboxColumn(options=[""] + list(tipo_nome2id.keys())),
                "Situação (nome)": st.column_config.SelectboxColumn(options=[""] + list(sit_nome2id.keys())),
                "Procedimento_Tipo_ID": st.column_config.NumberColumn(disabled=True),
                "Situacao_ID": st.column_config.NumberColumn(disabled=True),
                "Guia_AMHPTISS": st.column_config.TextColumn(),
                "Guia_AMHPTISS_Complemento": st.column_config.TextColumn(),
                "Fatura": st.column_config.TextColumn(),
                "Observacoes": st.column_config.TextColumn(),
                "created_at": st.column_config.TextColumn(disabled=True),
                "updated_at": st.column_config.TextColumn(disabled=True),
            },
            key="editor_lista_cirurgias_union"
        )

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
                                commit_message="Atualiza banco SQLite via app (salvar em massa lista de cirurgias)"
                            )
                            if ok:
                                st.success("Sincronização automática com GitHub concluída.")
                        except Exception as e:
                            st.error("Falha ao sincronizar com GitHub (commit automático).")
                            st.exception(e)

                except Exception as e:
                    st.error("Falha ao salvar alterações da lista.")
                    st.exception(e)

        with colG2:
            if st.button("⬇️ Exportar Excel (Lista atual)"):
                try:
                    from export import to_formatted_excel_cirurgias
                    excel_bytes = to_formatted_excel_cirurgias(edited_df.drop(columns=["Tipo (nome)", "Situação (nome)"], errors="ignore"))
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
                            st.error("Falha ao sincronizar com GitHub (commit automático).")
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
# 📚 Aba 3: Cadastro (Tipos & Situações) — com reset counter para evitar conflitos
# ====================================================================================
with tabs[2]:
    st.subheader("Catálogos de Tipos de Procedimento e Situações da Cirurgia")

    # --------- Tipos de Procedimento -----------
    st.markdown("#### Tipos de Procedimento")
    colA, colB = st.columns([2, 1])

    # Inicializa contador de reset do formulário
    if "tipo_form_reset" not in st.session_state:
        st.session_state["tipo_form_reset"] = 0

    # Callback que salva e incrementa o reset (limpa widgets sem mexer na session_state das keys)
    def _save_tipo_and_reset():
        try:
            suffix = st.session_state["tipo_form_reset"]
            nome_key = f"tipo_nome_input_{suffix}"
            ordem_key = f"tipo_ordem_input_{suffix}"
            ativo_key = f"tipo_ativo_input_{suffix}"

            tipo_nome = st.session_state.get(nome_key, "").strip()
            tipo_ordem = int(st.session_state.get(ordem_key, 0))
            tipo_ativo = bool(st.session_state.get(ativo_key, True))

            from db import upsert_procedimento_tipo, list_procedimento_tipos
            tid = upsert_procedimento_tipo(tipo_nome, int(tipo_ativo), int(tipo_ordem))
            st.success(f"Tipo salvo (id={tid}).")

            tipos_all = list_procedimento_tipos(only_active=False)
            df_tipos = pd.DataFrame(tipos_all, columns=["id", "nome", "ativo", "ordem"])
            st.session_state["df_tipos_cached"] = df_tipos

            prox_id = (df_tipos["id"].max() + 1) if not df_tipos.empty else 1
            st.info(f"Próximo ID previsto: {prox_id}")

        except Exception as e:
            st.error("Falha ao salvar tipo.")
            st.exception(e)
        finally:
            # incrementa reset para criar novas keys de widgets (limpa formulário)
            st.session_state["tipo_form_reset"] += 1

    with colA:
        suffix = st.session_state["tipo_form_reset"]
        tipo_nome = st.text_input(
            "Novo tipo / atualizar por nome",
            placeholder="Ex.: Colecistectomia",
            key=f"tipo_nome_input_{suffix}"
        )
        tipo_ordem = st.number_input(
            "Ordem (para ordenar listagem)",
            min_value=0, value=0, step=1,
            key=f"tipo_ordem_input_{suffix}"
        )
        tipo_ativo = st.checkbox(
            "Ativo", value=True,
            key=f"tipo_ativo_input_{suffix}"
        )

        st.button("Salvar tipo de procedimento", on_click=_save_tipo_and_reset)

    with colB:
        from db import list_procedimento_tipos, set_procedimento_tipo_status
        try:
            df_tipos = st.session_state.get("df_tipos_cached")
            if df_tipos is None:
                tipos = list_procedimento_tipos(only_active=False)
                if tipos:
                    df_tipos = pd.DataFrame(tipos, columns=["id", "nome", "ativo", "ordem"])
                else:
                    df_tipos = pd.DataFrame(columns=["id", "nome", "ativo", "ordem"])

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

                        tipos = list_procedimento_tipos(only_active=False)
                        df_tipos = pd.DataFrame(tipos, columns=["id", "nome", "ativo", "ordem"])
                        st.session_state["df_tipos_cached"] = df_tipos
                        prox_id = (df_tipos["id"].max() + 1) if not df_tipos.empty else 1
                        st.info(f"Próximo ID previsto: {prox_id}")

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

    def _save_sit_and_reset():
        try:
            suffix = st.session_state["sit_form_reset"]
            nome_key = f"sit_nome_input_{suffix}"
            ordem_key = f"sit_ordem_input_{suffix}"
            ativo_key = f"sit_ativo_input_{suffix}"

            sit_nome = st.session_state.get(nome_key, "").strip()
            sit_ordem = int(st.session_state.get(ordem_key, 0))
            sit_ativo = bool(st.session_state.get(ativo_key, True))

            from db import upsert_cirurgia_situacao, list_cirurgia_situacoes
            sid = upsert_cirurgia_situacao(sit_nome, int(sit_ativo), int(sit_ordem))
            st.success(f"Situação salva (id={sid}).")

            sits_all = list_cirurgia_situacoes(only_active=False)
            df_sits = pd.DataFrame(sits_all, columns=["id", "nome", "ativo", "ordem"])
            st.session_state["df_sits_cached"] = df_sits

            prox_id_s = (df_sits["id"].max() + 1) if not df_sits.empty else 1
            st.info(f"Próximo ID previsto: {prox_id_s}")

        except Exception as e:
            st.error("Falha ao salvar situação.")
            st.exception(e)
        finally:
            st.session_state["sit_form_reset"] += 1

    with colC:
        suffix = st.session_state["sit_form_reset"]
        sit_nome = st.text_input(
            "Nova situação / atualizar por nome",
            placeholder="Ex.: Realizada, Cancelada, Adiada",
            key=f"sit_nome_input_{suffix}"
        )
        sit_ordem = st.number_input(
            "Ordem (para ordenar listagem)",
            min_value=0, value=0, step=1,
            key=f"sit_ordem_input_{suffix}"
        )
        sit_ativo = st.checkbox(
            "Ativo", value=True,
            key=f"sit_ativo_input_{suffix}"
        )

        st.button("Salvar situação", on_click=_save_sit_and_reset)

    with colD:
        from db import list_cirurgia_situacoes, set_cirurgia_situacao_status
        try:
            df_sits = st.session_state.get("df_sits_cached")
            if df_sits is None:
                sits = list_cirurgia_situacoes(only_active=False)
                if sits:
                    df_sits = pd.DataFrame(sits, columns=["id", "nome", "ativo", "ordem"])
                else:
                    df_sits = pd.DataFrame(columns=["id", "nome", "ativo", "ordem"])

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

                        sits = list_cirurgia_situacoes(only_active=False)
                        df_sits = pd.DataFrame(sits, columns=["id", "nome", "ativo", "ordem"])
                        st.session_state["df_sits_cached"] = df_sits
                        prox_id_s = (df_sits["id"].max() + 1) if not df_sits.empty else 1
                        st.info(f"Próximo ID previsto: {prox_id_s}")

                    except Exception as e:
                        st.error("Falha ao aplicar alterações nas situações.")
                        st.exception(e)
            else:
                st.info("Nenhuma situação cadastrada ainda.")
        except Exception as e:
            st.error("Erro ao listar/editar situações.")
            st.exception(e)
