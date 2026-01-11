
# export.py
import io
import re
from typing import Optional, Dict
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
        _write_sheet(writer, _sanitize_sheet_name(sheet_name), df if isinstance(df, pd.DataFrame) else pd.DataFrame())
    output.seek(0)
    return output

def to_formatted_excel_by_hospital(df: pd.DataFrame) -> io.BytesIO:
    """
    Gera um Excel com uma aba por Hospital.
    - Aceita df=None ou df vazio (gera aba 'Dados' vazia)
    """
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        if df is None or not isinstance(df, pd.DataFrame):
            _write_sheet(writer, "Dados", pd.DataFrame())
            output.seek(0)
            return output

        if df.empty:
            _write_sheet(writer, "Dados", df)
            output.seek(0)
            return output

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
                sheet_name = _sanitize_sheet_name(hosp, fallback="Sem_Hospital")
                _write_sheet(writer, sheet_name, dfh)

    output.seek(0)
    return output

def to_formatted_excel_by_status(df: pd.DataFrame) -> io.BytesIO:
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        if df is None or not isinstance(df, pd.DataFrame) or df.empty or "Status" not in df.columns:
            _write_sheet(writer, "Dados", df if isinstance(df, pd.DataFrame) else pd.DataFrame())
        else:
            df_aux = df.copy()
            df_aux["Status"] = df_aux["Status"].fillna("Sem_Status").astype(str).str.strip().replace("", "Sem_Status")
            for st_name in sorted(df_aux["Status"].unique()):
                dfs = df_aux[df_aux["Status"] == st_name].copy()
                order_cols = [c for c in ["Convenio", "Paciente"] if c in dfs.columns]
                if order_cols and not dfs.empty:
                    dfs = dfs.sort_values(order_cols, kind="mergesort")
                _write_sheet(writer, _sanitize_sheet_name(st_name, fallback="Sem_Status"), dfs)
    output.seek(0)
    return output


# ---------------- Autorizações + Equipes (COMPLETO) ----------------

def _build_status_summary(auth_df: pd.DataFrame) -> pd.DataFrame:
    if auth_df is None or not isinstance(auth_df, pd.DataFrame) or auth_df.empty or "Status" not in auth_df.columns:
        return pd.DataFrame({"Status": ["(sem dados)"], "Quantidade": [0]})
    s = auth_df["Status"].fillna("Sem_Status").astype(str).str.strip().replace("", "Sem_Status")
    vc = s.value_counts(dropna=False).sort_index()
    out = vc.rename_axis("Status").reset_index(name="Quantidade")
    out.loc[len(out)] = ["TOTAL", int(vc.sum())]
    return out

def _compose_auth_sheet_name(row: Dict) -> str:
    pac = str(row.get("Paciente", "")).strip()
    hosp = str(row.get("Unidade", "")).strip() or str(row.get("Hospital", "")).strip()
    att = str(row.get("Atendimento", "")).strip()
    data = str(row.get("Data_Cirurgia", "")).strip() or str(row.get("Data", "")).strip()
    base = f"{pac} — {hosp}" if hosp else pac
    if att:
        base = f"{base} (ATT:{att})"
    elif data:
        base = f"{base} — {data}"
    return _sanitize_sheet_name(base or "Autorizacao", fallback="Autorizacao")

def to_formatted_excel_authorizations_with_team(
    auth_df: pd.DataFrame,
    team_df: pd.DataFrame,
    *,
    per_authorization_tabs: bool = False,
    sort_auth_cols: Optional[list] = None,
    sort_team_cols: Optional[list] = None
) -> io.BytesIO:
    if auth_df is None or not isinstance(auth_df, pd.DataFrame):
        auth_df = pd.DataFrame()
    if team_df is None or not isinstance(team_df, pd.DataFrame):
        team_df = pd.DataFrame(columns=["NaturalKey","Prestador","Papel","Participacao","Observacao"])

    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        resumo_df = _build_status_summary(auth_df)
        _write_sheet(writer, "Resumo por Status", resumo_df)

        auth_df_aux = auth_df.copy()
        if sort_auth_cols is None:
            sort_auth_cols = [c for c in ["Status", "Convenio", "Paciente"] if c in auth_df_aux.columns]
        if sort_auth_cols and not auth_df_aux.empty:
            auth_df_aux = auth_df_aux.sort_values(sort_auth_cols, kind="mergesort")
        _write_sheet(writer, "Autorizações", auth_df_aux)

        team_df_aux = team_df.copy()
        if sort_team_cols is None:
            sort_team_cols = [c for c in ["NaturalKey", "Papel", "Prestador"] if c in team_df_aux.columns]
        if sort_team_cols and not team_df_aux.empty:
            team_df_aux = team_df_aux.sort_values(sort_team_cols, kind="mergesort")
        _write_sheet(writer, "Equipes", team_df_aux)

        if per_authorization_tabs and not auth_df_aux.empty and "NaturalKey" in auth_df_aux.columns:
            by_nk = {}
            for _, r in auth_df_aux.iterrows():
                nk = r.get("NaturalKey", None)
                if nk is None:
                    continue
                by_nk[str(nk)] = r.to_dict()

            if not team_df_aux.empty and "NaturalKey" in team_df_aux.columns:
                for nk, grp in team_df_aux.groupby("NaturalKey", sort=False):
                    meta = by_nk.get(str(nk))
                    if meta is None:
                        sheet_name = _sanitize_sheet_name(f"Aut {str(nk)[:10]}", fallback="Aut")
                        _write_sheet(writer, sheet_name, grp[["Prestador","Papel","Participacao","Observacao"]])
                        continue
                    sheet_name = _compose_auth_sheet_name(meta)
                    meta_cols = ["Paciente","Unidade","Atendimento","Data_Cirurgia","Convenio","Status"]
                    meta_view = pd.DataFrame([{k: str(meta.get(k, "")) for k in meta_cols}])
                    _write_sheet(writer, sheet_name, meta_view)

                    sheet_team_name = _sanitize_sheet_name(f"Equipe - {sheet_name}", fallback="Equipe")
                    team_view = grp[["Prestador","Papel","Participacao","Observacao"]].copy()
                    _write_sheet(writer, sheet_team_name, team_view)
            else:
                for _, r in auth_df_aux.iterrows():
                    sheet_name = _compose_auth_sheet_name(r.to_dict())
                    meta_cols = ["Paciente","Unidade","Atendimento","Data_Cirurgia","Convenio","Status"]
                    meta_view = pd.DataFrame([{k: str(r.get(k, "")) for k in meta_cols}])
                    _write_sheet(writer, sheet_name, meta_view)

    output.seek(0)
    return output
