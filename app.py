# app.py
import streamlit as st
import pandas as pd
from db import init_db, upsert_dataframe, read_all
from processing import process_uploaded_file
from export import to_formatted_excel_by_hospital

st.set_page_config(page_title="Pacientes por Dia, Prestador e Hospital", layout="wide")

st.title("Pacientes únicos por data, prestador e hospital")
st.caption("Upload → herança/filtragem/deduplicação → informar Hospital (lista) → revisar/editar Paciente → Ano/Mes/Dia → salvar em exemplo.db → exportar por Hospital")

# Inicializa DB
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

# Estado para manter o DF processado e editado
if "df_final" not in st.session_state:
    st.session_state.df_final = None

if uploaded_file is not None:
    with st.spinner("Processando arquivo com a lógica consolidada..."):
        # Processa com hospital selecionado
        df_final = process_uploaded_file(uploaded_file, prestadores_lista, selected_hospital.strip())
        st.session_state.df_final = df_final

# ---------------- Revisão / Edição ----------------
if st.session_state.df_final is not None:
    st.success(f"Processamento concluído! Linhas: {len(st.session_state.df_final)}")

    st.subheader("Revisar e editar nomes de Paciente (opcional)")
    st.caption("Edite apenas a coluna 'Paciente' se necessário. As demais estão bloqueadas para evitar alterações acidentais.")

    # Editor com restrição: somente 'Paciente' editável
    # Mantemos ordenado para uma experiência consistente
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
        key="editor_pacientes"
    )

    # Atualiza o estado com as edições realizadas
    st.session_state.df_final = edited_df

    # ---------------- Gravar no Banco ----------------
    st.subheader("Persistência")
    if st.button("Salvar no banco (exemplo.db)"):
        upsert_dataframe(st.session_state.df_final)
        st.success("Dados salvos com sucesso em exemplo.db (SQLite). Para refletir no GitHub, faça commit/push do arquivo.")

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
