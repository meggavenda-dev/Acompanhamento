
# export.py
import io
import re
import pandas as pd

# ---------------- Helpers de formatação ----------------

_INVALID_SHEET_CHARS_RE = re.compile(r'[:\\/?*\[\]]')

def _sanitize_sheet_name(name: str, fallback: str = "Dados") -> str:
    """
    Limpa o nome da aba para atender restrições do Excel:
    - remove caracteres inválidos: : \ / ? * [ ]
    - limita a 31 caracteres
    - se vazio após limpeza, usa fallback
    """
    if name is None:
        name = ""
    name = str(name).strip()
    name = _INVALID_SHEET_CHARS_RE.sub("", name)
    if not name:
        name = fallback
    # Excel limita a 31
    return name[:31]

def _write_sheet(writer: pd.ExcelWriter, sheet_name: str, df: pd.DataFrame):
    """
    Escreve o DataFrame com cabeçalho formatado, autofiltro e ajuste de larguras.
    """
    df.to_excel(writer, sheet_name=sheet_name, index=False)
    wb = writer.book
    ws = writer.sheets[sheet_name]

    # Cabeçalho
    header_fmt = wb.add_format({"bold": True, "bg_color": "#DCE6F1", "border": 1})
    for col_num, value in enumerate(df.columns.values):
        ws.write(0, col_num, value, header_fmt)

    # Autofiltro e largura de colunas
    ws.autofilter(0, 0, len(df), max(0, len(df.columns) - 1))
    for i, col in enumerate(df.columns):
        valores = [str(x) for x in df[col].tolist()]
        maxlen = max([len(str(col))] + [len(v) for v in valores]) + 2
        ws.set_column(i, i, max(14, min(maxlen, 40)))


# ---------------- Exportações ----------------

def to_formatted_excel(df: pd.DataFrame, sheet_name="Pacientes por dia e prestador"):
    """
    Gera Excel em memória com formatação:
    - Cabeçalho em negrito e cor de fundo
    - Auto filtro
    - Largura de colunas ajustada
    """
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        _write_sheet(writer, _sanitize_sheet_name(sheet_name), df)
    output.seek(0)
    return output


def to_formatted_excel_by_hospital(df: pd.DataFrame):
    """
    Gera um Excel com uma aba por Hospital.
    - Se não houver coluna 'Hospital', cai para aba única 'Dados'
    - Em cada aba, ordena por: Ano, Mes, Dia, Paciente, Prestador (quando existirem)
    - Aplica formatação de cabeçalho, autofiltro e ajuste de colunas
    """
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        if "Hospital" not in df.columns:
            # fallback: única aba
            _write_sheet(writer, "Dados", df)
        else:
            # Ordenação padrão aplicada por aba (se colunas existirem)
            order_cols = [c for c in ["Ano", "Mes", "Dia", "Paciente", "Prestador"] if c in df.columns]
            # Agrupa e escreve cada hospital em uma aba
            for hosp, dfh in df.groupby("Hospital"):
                dfh_sorted = dfh.copy()
                if order_cols:
                    dfh_sorted = dfh_sorted.sort_values(order_cols, kind="mergesort")
                sheet_name = _sanitize_sheet_name(hosp or "Sem_Hospital")
                _write_sheet(writer, sheet_name, dfh_sorted)
    output.seek(0)
    return output
