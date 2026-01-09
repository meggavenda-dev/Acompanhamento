# app.py
import streamlit as st
import pandas as pd
from db import init_db, upsert_dataframe, read_all
from processing import process_uploaded_file
from export import to_formatted_excel

st.set_page_config(page_title="Pacientes por Dia e Prestador", layout="wide")

st.title("Pacientes únicos por dia e prestador")
st.caption("Upload de planilha → lógica de herança/filtragem/deduplicação → grava em exemplo.db → exporta Excel formatado")

# Inicializa DB
init_db()

# Configuração dos prestadores (pode editar no app)
st.subheader("Prestadores alvo")
prestadores_default = ["JOSE.ADORNO", "CASSIO CESAR", "FERNANDO AND", "SIMAO.MATOS"]
prestadores_text = st.text_area(
    "Informe os prestadores (um por linha)",
    value="\n".join(prestadores_default),
    height=120
)
prestadores_lista = [p.strip() for p in prestadores_text.splitlines() if p.strip()]

# Upload de arquivo
st.subheader("Upload de planilha (CSV ou Excel)")
uploaded_file = st.file_uploader("Escolha o arquivo", type=["csv","xlsx","xls"], help="O app aceita o CSV 'bruto' ou tabelas já estruturadas.")

if uploaded_file is not None:
    with st.spinner("Processando arquivo com a lógica consolidada..."):
        df_final = process_uploaded_file(uploaded_file, prestadores_lista)

    st.success(f"Processamento concluído! Linhas: {len(df_final)}")
    st.dataframe(df_final, use_container_width=True)

    # Grava no banco
    if st.button("Salvar no banco (exemplo.db)"):
        upsert_dataframe(df_final)
        st.success("Dados salvos com sucesso em exemplo.db (SQLite). Faça commit/push para persistir no seu GitHub.")

    # Exportar Excel
    st.subheader("Exportar Excel")
    excel_bytes = to_formatted_excel(df_final, sheet_name="Pacientes por dia e prestador")
    st.download_button(
        label="Baixar Excel formatado",
        data=excel_bytes,
        file_name="Pacientes_unicos_por_dia_prestador.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )

st.divider()
st.subheader("Conteúdo atual do banco (exemplo.db)")
rows = read_all()
if rows:
    db_df = pd.DataFrame(rows, columns=["Data","Atendimento","Paciente","Aviso","Convenio","Prestador","Quarto"])
    st.dataframe(db_df, use_container_width=True)
else:
    st.info("Banco ainda sem dados. Faça o upload e clique em 'Salvar no banco'.")
