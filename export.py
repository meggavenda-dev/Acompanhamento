
# export.py (acréscimo)
import io
import re
import pandas as pd

_INVALID_SHEET_CHARS_RE = re.compile(r'[:\\/?*\[\]]')

def _sanitize_sheet_name(name: str, fallback: str = "Dados") -> str:
    if not name:
        name = fallback
    name = str(name).strip()
    name = _INVALID_SHEET_CHARS_RE.sub("", name)
    return name[:31] or fallback

def _write_sheet(writer: pd.ExcelWriter, sheet_name: str, df: pd.DataFrame):
    df.to_excel(writer, sheet_name=sheet_name, index=False)
    wb = writer.book; ws = writer.sheets[sheet_name]
    header_fmt = wb.add_format({"bold": True, "bg_color": "#DCE6F1", "border": 1})
    for col_num, value in enumerate(df.columns.values):
        ws.write(0, col_num, value, header_fmt)
    last_row = max(len(df), 1)
    ws.autofilter(0, 0, last_row, max(0, len(df.columns) - 1))
    for i, col in enumerate(df.columns):
        valores = [str(x) for x in df[col].tolist()]
        maxlen = max([len(str(col))] + [len(v) for v in valores]) + 2
        ws.set_column(i, i, max(14, min(maxlen, 40)))

def to_formatted_excel_cirurgias(df: pd.DataFrame) -> io.BytesIO:
    """
    Exporta cirurgias. Se houver coluna 'Hospital', cria multi-aba por hospital.
    """
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        if "Hospital" not in df.columns:
            _write_sheet(writer, "Cirurgias", df)
        else:
            df_aux = df.copy()
            df_aux["Hospital"] = (
                df_aux["Hospital"].fillna("Sem_Hospital").astype(str).str.strip().replace("", "Sem_Hospital")
            )
            for hosp in sorted(df_aux["Hospital"].unique()):
                dfh = df_aux[df_aux["Hospital"] == hosp].copy()
                dfh = dfh.sort_values(["Data_Cirurgia", "Paciente"], kind="mergesort")
                sheet_name = _sanitize_sheet_name(hosp, "Sem_Hospital")
                _write_sheet(writer, sheet_name, dfh)
    output.seek(0)
    return output
