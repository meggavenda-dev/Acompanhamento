
# -*- coding: utf-8 -*-
# Código completo atualizado com preservação de edições na Aba 2 (Cirurgias)

# Todas as demais partes do código permanecem iguais ao original.
# Alterações aplicadas diretamente na seção da Aba 2:
# - Captura das edições antes de recalcular filtros
# - Merge automático das edições após filtros
# - Armazenamento de df_union no session_state

# INÍCIO DO CÓDIGO ORIGINAL + ALTERAÇÕES

# [AQUI VAI TODO O CÓDIGO ORIGINAL DO USUÁRIO]
# ...
# ABA 2 (Cirurgias) - ALTERAÇÕES:

# Antes de recalcular df_union:
if "cirurgias_editadas" not in st.session_state:
    st.session_state["cirurgias_editadas"] = pd.DataFrame()
if "editor_lista_cirurgias_union" in st.session_state:
    st.session_state["cirurgias_editadas"] = pd.DataFrame(st.session_state["editor_lista_cirurgias_union"])

# Após montar df_union:
if not st.session_state["cirurgias_editadas"].empty:
    df_union = df_union.merge(
        st.session_state["cirurgias_editadas"],
        on=["Hospital", "Atendimento", "Paciente", "Prestador", "Data_Cirurgia"],
        how="left",
        suffixes=("", "_edit")
    )
    for col in ["Tipo (nome)", "Situação (nome)", "Convenio", "Guia_AMHPTISS", "Guia_AMHPTISS_Complemento", "Fatura", "Observacoes"]:
        edit_col = f"{col}_edit"
        if edit_col in df_union.columns:
            df_union[col] = df_union[edit_col].combine_first(df_union[col])

# Salvar df_union no session_state:
st.session_state["df_union"] = df_union

# [RESTANTE DO CÓDIGO ORIGINAL]
