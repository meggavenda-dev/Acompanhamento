# -*- coding: utf-8 -*-
# Código original do usuário + alterações aplicadas

# OBS: Este arquivo contém as instruções para aplicar as mudanças na seção da Aba 2.
# Substitua a parte da Aba 2 no seu código original por este bloco.

# ALTERAÇÕES APLICADAS NA ABA 2 (Cirurgias):
# 1. Antes de recalcular df_union:
if "cirurgias_editadas" not in st.session_state:
    st.session_state["cirurgias_editadas"] = pd.DataFrame()
if "editor_lista_cirurgias_union" in st.session_state:
    st.session_state["cirurgias_editadas"] = pd.DataFrame(st.session_state["editor_lista_cirurgias_union"])

# 2. Após montar df_union:
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

# 3. Salvar df_union no session_state:
st.session_state["df_union"] = df_union


# Demais partes do código permanecem iguais ao original.
