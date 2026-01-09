
# app.py
import streamlit as st
import pandas as pd
from db import init_db, upsert_dataframe, read_all
from processing import process_uploaded_file
from export import to_formatted_excel_by_hospital

st.set_page_config(page_title="Pacientes por Dia, Prestador e Hospital", layout="wide")

st.title("Pacientes únicos por data, prestador e hospital")
st.caption("Upload → herança/filtragem/deduplicação → inferir Hospital → Ano/Mes/Dia → salvar em exemplo.db → exportar por Hospital")

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

# ---------------- Regras de Hospital ----------------
st.subheader("Regras de Hospital (pattern => nome)")
hospital_rules_default = """BGS SANTA LUCIA => Hospital Santa Lucia Sul
HOSPITAL SANTA LUCIA NORTE => Hospital Santa Lucia Norte
BGS OBRA MARIA AUXILI => Hospital Maria Auxiliadora"""
hospital_rules_text = st.text_area(
    "Uma regra por linha. O 'pattern' é buscado em Convênio, Centro e Quarto (case-insensitive).",
    value=hospital_rules_default,
    height=140
)

# ---------------- Upload de Arquivo ----------------
st.subheader("Upload de planilha (CSV ou Excel)")
uploaded_file = st.file_uploader(
    "Escolha o arquivo",
    type=["csv","xlsx","xls"],
    help="Aceita CSV 'bruto' (sem cabeçalho padronizado) ou planilhas estruturadas."
)

df_final = None
if uploaded_file is not None:
    with st.spinner("Processando arquivo com a lógica consolidada..."):
        # >>> usa a nova assinatura de processing.py
        df_final = process_uploaded_file(uploaded_file, prestadores_lista, hospital_rules_text)

    st.success(f"Processamento concluído! Linhas: {len(df_final)}")
    # Exibe já ordenado por Hospital/Ano/Mes/Dia
    st.dataframe(df_final.sort_values(["Hospital","Ano","Mes","Dia","Paciente","Prestador"]),
                 use_container_width=True)

    # ---------------- Gravar no Banco ----------------
    st.subheader("Persistência")
    if st.button("Salvar no banco (exemplo.db)"):
        upsert_dataframe(df_final)
        st.success("Dados salvos com sucesso em exemplo.db (SQLite). Para refletir no GitHub, faça commit/push do arquivo.")

    # ---------------- Exportar Excel (por Hospital) ----------------
    st.subheader("Exportar Excel (multi-aba por Hospital)")
    excel_bytes = to_formatted_excel_by_hospital(df_final)
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
    # Colunas esperadas do banco (db.py atualizado):
    cols = ["Hospital","Ano","Mes","Dia","Data","Atendimento","Paciente","Aviso","Convenio","Prestador","Quarto"]
    db_df = pd.DataFrame(rows, columns=cols)
    st.dataframe(db_df.sort_values(["Hospital","Ano","Mes","Dia","Paciente","Prestador"]), use_container_width=True)

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
