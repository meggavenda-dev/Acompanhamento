
# export.py
import io
import re
import pandas as pd

# ---------------- Helpers de formatação ----------------

_INVALID_SHEET_CHARS_RE = re.compile(r'[:\\/?*\[\]]')

def _sanitize_sheet_name(name: str, fallback: str = "Dados") -> str:
    if not name:
        name = fallback
    name = str(name).strip()
    name = _INVALID_SHEET_CHARS_RE.sub("", name)
    if not name:
        name = fallback
    return name[:31]

def _write_sheet(writer: pd.ExcelWriter, sheet_name: str, df: pd.DataFrame):
    df = df.copy()
    df.to_excel(writer, sheet_name=sheet_name, index=False)
    wb = writer.book
    ws = writer.sheets[sheet_name]

    header_fmt = wb.add_format({"bold": True, "bg_color": "#DCE6F1", "border": 1})
    for col_num, value in enumerate(df.columns.values):
        ws.write(0, col_num, value, header_fmt)

    last_row = max(len(df), 1)
    ws.autofilter(0, 0, last_row, max(0, len(df.columns) - 1))

    for i, col in enumerate(df.columns):
        valores = [str(x) for x in df[col].tolist()]
        maxlen = max([len(str(col))] + [len(v) for v in valores]) + 2
        ws.set_column(i, i, max(14, min(maxlen, 40)))


# ---------------- Exportações ----------------

def to_formatted_excel(df: pd.DataFrame, sheet_name: str = "Pacientes por dia e prestador") -> io.BytesIO:
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        _write_sheet(writer, _sanitize_sheet_name(sheet_name), df)
    output.seek(0)
    return output

def to_formatted_excel_by_hospital(df: pd.DataFrame) -> io.BytesIO:
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        if "Hospital" not in df.columns:
            _write_sheet(writer, "Dados", df)
        else:
            df_aux = df.copy()
            df_aux["Hospital"] = (
                df_aux["Hospital"].fillna("Sem_Hospital").astype(str).str.strip().replace("", "Sem_Hospital")
            )
            order_cols = [c for c in ["Ano", "Mes", "Dia", "Paciente", "Prestador"] if c in df_aux.columns]
            for hosp in sorted(df_aux["Hospital"].unique()):
                dfh = df_aux[df_aux["Hospital"] == hosp].copy()
                if order_cols:
                    dfh = dfh.sort_values(order_cols, kind="mergesort")
                _write_sheet(writer, _sanitize_sheet_name(hosp, fallback="Sem_Hospital"), dfh)
    output.seek(0)
    return output

def to_formatted_excel_by_status(df: pd.DataFrame) -> io.BytesIO:
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        if "Status" not in df.columns:
            _write_sheet(writer, "Dados", df)
        else:
            df_aux = df.copy()
            df_aux["Status"] = df_aux["Status"].fillna("Sem_Status").astype(str).str.strip().replace("", "Sem_Status")
            for st_name in sorted(df_aux["Status"].unique()):
                dfs = df_aux[df_aux["Status"] == st_name].copy()
                order_cols = [c for c in ["Convenio", "Paciente"] if c in dfs.columns]
                if order_cols:
                    dfs = dfs.sort_values(order_cols, kind="mergesort")
                _write_sheet(writer, _sanitize_sheet_name(st_name, fallback="Sem_Status"), dfs)
    output.seek(0)
    return output
