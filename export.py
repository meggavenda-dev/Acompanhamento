# export.py
import io
import pandas as pd

def to_formatted_excel(df: pd.DataFrame, sheet_name="Pacientes por dia e prestador"):
    """
    Gera Excel em memória com formatação básica:
    - Cabeçalho em negrito e cor de fundo,
    - Auto filtro
    - Largura de colunas ajustada
    """
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        df.to_excel(writer, sheet_name=sheet_name, index=False)
        wb = writer.book
        ws = writer.sheets[sheet_name]

        # Formatação do cabeçalho
        header_fmt = wb.add_format({
            "bold": True,
            "bg_color": "#DCE6F1",
            "border": 1
        })
        for col_num, value in enumerate(df.columns.values):
            ws.write(0, col_num, value, header_fmt)

        # Auto filtro e ajuste de largura
        ws.autofilter(0, 0, len(df), len(df.columns)-1)
        for i, col in enumerate(df.columns):
            # largura mínima 14
            maxlen = max([len(str(x)) for x in [col] + df[col].astype(str).tolist()]) + 2
            ws.set_column(i, i, max(14, min(maxlen, 40)))
    output.seek(0)
    return output
