
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

# Inicializa DB (cria tabela/índices se necessário) — também cria as tabelas de catálogos e cirurgias
init_db()

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
        # hospital influencia o processamento; trocando hospital também deve resetar
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

        st.markdown("#### Revisar e editar nomes de Paciente (opcional)")
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
        st.markdown("#### Persistência")
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
                            commit_message="Atualiza banco SQLite via app (salvar pacientes)"
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

        # Exportar direto do banco também (multi-aba por hospital)
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
        delete_cirurgia
    )
    from export import to_formatted_excel_cirurgias

    # Filtros base para buscar registros pré-preenchidos da tabela original
    st.markdown("#### Pré-preenchimento a partir de registros importados")
    colF1, colF2, colF3 = st.columns(3)
    with colF1:
        # Reuso das opções de hospital da aba anterior
        hosp_cad = st.selectbox("Hospital", options=[
            "Hospital Santa Lucia Sul",
            "Hospital Santa Lucia Norte",
            "Hospital Maria Auxiliadora",
        ], index=0)
    # sugere ano/mês atuais
    now = datetime.now()
    with colF2:
        ano_cad = st.number_input("Ano (filtro base)", min_value=2000, max_value=2100, value=now.year, step=1)
    with colF3:
        mes_cad = st.number_input("Mês (filtro base)", min_value=1, max_value=12, value=now.month, step=1)

    prestadores_default = ["JOSE.ADORNO", "CASSIO CESAR", "FERNANDO AND", "SIMAO.MATOS"]
    prestadores_filtro = st.text_input("Prestadores (filtro base, separar por ; )", value="; ".join(prestadores_default))
    prestadores_lista_filtro = [p.strip() for p in prestadores_filtro.split(";") if p.strip()]

    st.caption("Use o filtro acima para selecionar um registro existente e pré-preencher a cirurgia.")
    try:
        base_rows = find_registros_para_prefill(hosp_cad, ano=int(ano_cad), mes=int(mes_cad), prestadores=prestadores_lista_filtro)
        if base_rows:
            df_base = pd.DataFrame(base_rows, columns=["Hospital", "Data", "Atendimento", "Paciente", "Convenio", "Prestador"])
            st.dataframe(df_base, use_container_width=True, height=240)
            idx = st.number_input("Linha do registro base (índice)", min_value=0, max_value=len(df_base)-1, value=0, step=1)
            base = df_base.iloc[int(idx)]
        else:
            st.info("Nenhum registro encontrado para pré-preenchimento.")
            base = None
    except Exception as e:
        st.error("Erro ao carregar registros base.")
        st.exception(e)
        base = None

    # --- Formulário de Cirurgia ---
    st.divider()
    st.markdown("#### Formulário de Cirurgia")
    with st.form("form_cirurgia"):
        # Pré-preenche se houver base
        hospital = st.text_input("Hospital", value=(base["Hospital"] if base is not None else hosp_cad))
        atendimento = st.text_input("Número do atendimento", value=(base["Atendimento"] if base is not None else ""))
        paciente = st.text_input("Paciente", value=(base["Paciente"] if base is not None else ""))
        prestador = st.text_input("Prestador", value=(base["Prestador"] if base is not None else ""))
        data_cirurgia = st.text_input("Data da cirurgia (ex.: 11/01/2026)", value=(base["Data"] if base is not None else ""))  # livre (mantém dd/MM/yyyy)
        convenio = st.text_input("Convênio", value=(base["Convenio"] if base is not None else ""))

        # Catálogos
        tipos = list_procedimento_tipos(only_active=True)
        sits = list_cirurgia_situacoes(only_active=True)
        tipo_options = {t[1]: t[0] for t in tipos}  # nome -> id
        sit_options = {s[1]: s[0] for s in sits}    # nome -> id

        tipo_nome_sel = st.selectbox("Tipo de procedimento", options=["(selecione)"] + list(tipo_options.keys()))
        sit_nome_sel = st.selectbox("Situação da cirurgia", options=["(selecione)"] + list(sit_options.keys()))

        guia = st.text_input("Guia AMHPTISS (manual)", value="")
        guia_comp = st.text_area("Guia AMHPTISS - complemento (manual)", value="")
        fatura = st.text_input("Fatura (manual)", value="")
        obs = st.text_area("Observações (opcional)", value="")

        submitted = st.form_submit_button("Salvar cirurgia")
        if submitted:
            try:
                payload = {
                    "Hospital": hospital.strip(),
                    "Atendimento": atendimento.strip(),
                    "Paciente": paciente.strip(),
                    "Prestador": prestador.strip(),
                    "Data_Cirurgia": data_cirurgia.strip(),
                    "Convenio": convenio.strip(),
                    "Procedimento_Tipo_ID": tipo_options.get(tipo_nome_sel) if tipo_nome_sel != "(selecione)" else None,
                    "Situacao_ID": sit_options.get(sit_nome_sel) if sit_nome_sel != "(selecione)" else None,
                    "Guia_AMHPTISS": guia.strip(),
                    "Guia_AMHPTISS_Complemento": guia_comp.strip(),
                    "Fatura": fatura.strip(),
                    "Observacoes": obs.strip(),
                }
                cid = insert_or_update_cirurgia(payload)
                st.success(f"Cirurgia salva com sucesso (id={cid}).")

                # Commit/push automático para GitHub
                if GITHUB_SYNC_AVAILABLE and GITHUB_TOKEN_OK:
                    try:
                        ok = upload_db_to_github(
                            owner=GH_OWNER,
                            repo=GH_REPO,
                            path_in_repo=GH_PATH_IN_REPO,
                            branch=GH_BRANCH,
                            local_db_path=DB_PATH,
                            commit_message="Atualiza banco SQLite via app (salvar/editar cirurgia)"
                        )
                        if ok:
                            st.success("Sincronização automática com GitHub concluída.")
                    except Exception as e:
                        st.error("Falha ao sincronizar com GitHub (commit automático).")
                        st.exception(e)

            except Exception as e:
                st.error("Falha ao salvar cirurgia.")
                st.exception(e)

    # --- Lista / Filtros / Export ---
    st.divider()
    st.markdown("#### Lista de Cirurgias")
    colL1, colL2, colL3, colL4 = st.columns([2, 2, 2, 2])
    with colL1:
        hosp_f = st.selectbox("Filtro Hospital (lista)", options=["(todos)"] + [
            "Hospital Santa Lucia Sul",
            "Hospital Santa Lucia Norte",
            "Hospital Maria Auxiliadora",
        ], index=0)
    with colL2:
        prest_f = st.text_input("Filtro Prestador (exato)", value="")
    with colL3:
        filtro_ano_mes = st.text_input("Filtro Data Cirurgia (contém, ex.: 01/2026 ou 2026-01)", value="")
    with colL4:
        btn_recarregar = st.button("Recarregar lista")

    try:
        hosp_arg = None if hosp_f == "(todos)" else hosp_f
        rows_cir = list_cirurgias(hospital=hosp_arg, ano_mes=(filtro_ano_mes or None), prestador=(prest_f or None))
        df_cir = pd.DataFrame(rows_cir, columns=[
            "id", "Hospital", "Atendimento", "Paciente", "Prestador", "Data_Cirurgia",
            "Convenio", "Procedimento_Tipo_ID", "Situacao_ID",
            "Guia_AMHPTISS", "Guia_AMHPTISS_Complemento", "Fatura",
            "Observacoes", "created_at", "updated_at"
        ])
        st.data_editor(
            df_cir,
            use_container_width=True,
            num_rows="fixed",
            column_config={
                "id": st.column_config.NumberColumn(disabled=True),
                "Hospital": st.column_config.TextColumn(),
                "Atendimento": st.column_config.TextColumn(),
                "Paciente": st.column_config.TextColumn(),
                "Prestador": st.column_config.TextColumn(),
                "Data_Cirurgia": st.column_config.TextColumn(help="Formato livre (ex.: dd/MM/yyyy)."),
                "Convenio": st.column_config.TextColumn(),
                "Procedimento_Tipo_ID": st.column_config.NumberColumn(help="ID do tipo escolhido."),
                "Situacao_ID": st.column_config.NumberColumn(help="ID da situação escolhida."),
                "Guia_AMHPTISS": st.column_config.TextColumn(),
                "Guia_AMHPTISS_Complemento": st.column_config.TextColumn(),
                "Fatura": st.column_config.TextColumn(),
                "Observacoes": st.column_config.TextColumn(),
                "created_at": st.column_config.TextColumn(disabled=True),
                "updated_at": st.column_config.TextColumn(disabled=True),
            },
            key="editor_lista_cirurgias"
        )

        colE1, colE2, colE3 = st.columns([1, 1, 2])
        with colE1:
            from export import to_formatted_excel_cirurgias
            if st.button("Exportar Excel (Cirurgias)"):
                excel_bytes = to_formatted_excel_cirurgias(df_cir)
                st.download_button(
                    label="Baixar Cirurgias.xlsx",
                    data=excel_bytes,
                    file_name="Cirurgias.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )
        with colE2:
            del_id = st.number_input("Excluir por id", min_value=0, step=1, value=0)
            if st.button("Excluir cirurgia"):
                try:
                    delete_cirurgia(int(del_id))
                    st.success(f"Cirurgia id={int(del_id)} excluída.")
                    # Sincroniza GitHub após exclusão
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
        with colE3:
            st.caption("Obs.: para editar linhas, altere no grid e clique novamente em 'Salvar cirurgia' com os mesmos campos-chave (Hospital, Atendimento, Prestador, Data_Cirurgia) para aplicar o UPSERT. Uma atualização em massa pode ser implementada se quiser.")
    except Exception as e:
        st.error("Erro ao listar/operar cirurgias.")
        st.exception(e)

# ====================================================================================
# 📚 Aba 3: Cadastro (Tipos & Situações)
# ====================================================================================
with tabs[2]:
    st.subheader("Catálogos de Tipos de Procedimento e Situações da Cirurgia")
    # --------- Tipos de Procedimento -----------
    st.markdown("#### Tipos de Procedimento")
    colA, colB = st.columns([2, 1])

    with colA:
        from db import upsert_procedimento_tipo
        tipo_nome = st.text_input("Novo tipo / atualizar por nome", placeholder="Ex.: Colecistectomia")
        tipo_ordem = st.number_input("Ordem (para ordenar listagem)", min_value=0, value=0, step=1)
        tipo_ativo = st.checkbox("Ativo", value=True)
        if st.button("Salvar tipo de procedimento"):
            try:
                tid = upsert_procedimento_tipo(tipo_nome.strip(), int(tipo_ativo), int(tipo_ordem))
                st.success(f"Tipo salvo (id={tid}).")
            except Exception as e:
                st.error("Falha ao salvar tipo.")
                st.exception(e)

    with colB:
        from db import list_procedimento_tipos, set_procedimento_tipo_status
        try:
            tipos = list_procedimento_tipos(only_active=False)
            if tipos:
                df_tipos = pd.DataFrame(tipos, columns=["id", "nome", "ativo", "ordem"])
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
                    for _, r in df_tipos.iterrows():
                        set_procedimento_tipo_status(int(r["id"]), int(r["ativo"]))
                    st.success("Tipos atualizados.")
            else:
                st.info("Nenhum tipo cadastrado ainda.")
        except Exception as e:
            st.error("Erro ao listar/editar tipos.")
            st.exception(e)

    # --------- Situações da Cirurgia -----------
    st.markdown("#### Situações da Cirurgia")
    colC, colD = st.columns([2, 1])

    with colC:
        from db import upsert_cirurgia_situacao
        sit_nome = st.text_input("Nova situação / atualizar por nome", placeholder="Ex.: Realizada, Cancelada, Adiada")
        sit_ordem = st.number_input("Ordem (para ordenar listagem)", min_value=0, value=0, step=1, key="sit_ordem")
        sit_ativo = st.checkbox("Ativo", value=True, key="sit_ativo")
        if st.button("Salvar situação"):
            try:
                sid = upsert_cirurgia_situacao(sit_nome.strip(), int(sit_ativo), int(sit_ordem))
                st.success(f"Situação salva (id={sid}).")
            except Exception as e:
                st.error("Falha ao salvar situação.")
                st.exception(e)

    with colD:
        from db import list_cirurgia_situacoes, set_cirurgia_situacao_status
        try:
            sits = list_cirurgia_situacoes(only_active=False)
            if sits:
                df_sits = pd.DataFrame(sits, columns=["id", "nome", "ativo", "ordem"])
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
                    for _, r in df_sits.iterrows():
                        set_cirurgia_situacao_status(int(r["id"]), int(r["ativo"]))
                    st.success("Situações atualizadas.")
            else:
                st.info("Nenhuma situação cadastrada ainda.")
        except Exception as e:
            st.error("Erro ao listar/editar situações.")
            st.exception(e)
