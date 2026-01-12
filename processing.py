
# processing.py
import io
import csv
import re
import unicodedata
import numpy as np
import pandas as pd
from dateutil import parser as dtparser  # reservado para futuras evoluções

# =========================
# Regex / Constantes
# =========================

TIME_RE = re.compile(r"^\d{1,2}:\d{2}$")
DATE_RE = re.compile(r"\b(\d{2}/\d{2}/\d{4})\b")
HAS_LETTER_RE = re.compile(r"[A-Za-zÁÉÍÓÚÃÕÇáéíóúãõç]")
SECTION_KEYWORDS = ["CENTRO CIRURGICO", "HEMODINAMICA", "CENTRO OBSTETRICO"]

EXPECTED_COLS = [
    "Centro", "Data", "Atendimento", "Paciente", "Aviso",
    "Hora_Inicio", "Hora_Fim", "Cirurgia", "Convenio", "Prestador",
    "Anestesista", "Tipo_Anestesia", "Quarto"
]

REQUIRED_COLS = [
    "Data", "Prestador", "Hora_Inicio",
    "Atendimento", "Paciente", "Aviso",
    "Convenio", "Quarto"
]

# =========================
# Heurísticas
# =========================

PROCEDURE_HINTS = {
    "HERNIA", "HERNIORRAFIA", "COLECISTECTOMIA", "APENDICECTOMIA",
    "ARTROPLASTIA", "ENDOSCOPIA", "LAPAROSCOPICA", "CRANIOTOMIA",
    "EXERESE", "RESSECCAO", "SINOVECTOMIA", "OSTEOTOMIA",
}

def _is_probably_procedure_token(tok) -> bool:
    if tok is None or pd.isna(tok):
        return False
    t = str(tok).upper()
    if any(h in t for h in PROCEDURE_HINTS):
        return True
    if any(x in t for x in [",", "/", "-", "(", ")", "%"]):
        return True
    if len(t) > 50:
        return True
    return False

def _strip_accents(s: str) -> str:
    if s is None or pd.isna(s):
        return ""
    return "".join(
        ch for ch in unicodedata.normalize("NFKD", str(s))
        if not unicodedata.combining(ch)
    )

# =========================
# Normalização
# =========================

def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df

    df.columns = [str(c).replace("\ufeff", "").strip() for c in df.columns]

    col_map = {
        "Convênio": "Convenio",
        "Tipo Anestesia": "Tipo_Anestesia",
        "Hora Início": "Hora_Inicio",
        "Centro Cirúrgico": "Centro"
    }
    return df.rename(columns=col_map)

# =========================
# Parser BLINDADO
# =========================

def _parse_raw_text_to_rows(text: str) -> pd.DataFrame:
    rows = []
    current_section = None
    current_date = None

    ctx = {
        "atendimento": None,
        "data": None,
        "data_locked": False
    }

    row_idx = 0

    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue

        # 1) Data explícita
        m = DATE_RE.search(line)
        if m:
            current_date = m.group(1)
            ctx["data_locked"] = False
            continue

        # 2) Seção
        if any(s in line.upper() for s in SECTION_KEYWORDS):
            current_section = next(s for s in SECTION_KEYWORDS if s in line.upper())
            ctx = dict.fromkeys(ctx, None)
            ctx["data_locked"] = False
            continue

        # 3) Cabeçalhos / totais
        if any(h in line.upper() for h in ["TOTAL", "PÁGINA", "HORA", "ATENDIMENTO"]):
            ctx = dict.fromkeys(ctx, None)
            ctx["data_locked"] = False
            continue

        tokens = [t.strip() for t in next(csv.reader([line])) if t.strip()]
        time_idxs = [i for i, t in enumerate(tokens) if TIME_RE.match(t)]
        if not time_idxs:
            continue

        h0 = time_idxs[0]
        h1 = h0 + 1 if h0 + 1 < len(tokens) and TIME_RE.match(tokens[h0 + 1]) else None

        hora_inicio = tokens[h0]
        hora_fim = tokens[h1] if h1 else None

        atendimento = next((t for t in tokens if re.fullmatch(r"\d{7,10}", t)), None)

        if atendimento and atendimento != ctx["atendimento"]:
            ctx["data_locked"] = False

        data_final = (
            current_date if (current_date and not ctx["data_locked"])
            else ctx["data"]
        )

        if atendimento and not current_date:
            data_final = pd.NA

        paciente = None
        for t in tokens:
            if HAS_LETTER_RE.search(t) and not _is_probably_procedure_token(t):
                paciente = t
                break

        base = h1 if h1 else h0

        rows.append({
            "Centro": current_section,
            "Data": data_final,
            "Atendimento": atendimento,
            "Paciente": paciente,
            "Aviso": None,
            "Hora_Inicio": hora_inicio,
            "Hora_Fim": hora_fim,
            "Cirurgia": tokens[base + 1] if base + 1 < len(tokens) else None,
            "Convenio": tokens[base + 2] if base + 2 < len(tokens) else None,
            "Prestador": tokens[base + 3] if base + 3 < len(tokens) else None,
            "Anestesista": tokens[base + 4] if base + 4 < len(tokens) else None,
            "Tipo_Anestesia": tokens[base + 5] if base + 5 < len(tokens) else None,
            "Quarto": tokens[base + 6] if base + 6 < len(tokens) else None,
            "_row_idx": row_idx
        })

        ctx.update({
            "atendimento": atendimento,
            "data": data_final,
            "data_locked": True if atendimento else False
        })

        row_idx += 1

    return pd.DataFrame(rows)

# =========================
# HERANÇA HOSPITALAR
# =========================

def _herdar_por_data_ordem_original(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df

    df = df.copy()
    df.replace({"": pd.NA}, inplace=True)

    if "_row_idx" not in df.columns:
        df["_row_idx"] = range(len(df))

    df["Data"] = df["Data"].ffill()  # 🔒 nunca bfill

    ctx = {
        "Data": None,
        "Atendimento": None,
        "Paciente": None,
        "Aviso": None,
    }

    for i in df.sort_values("_row_idx").index:
        row = df.loc[i]

        if pd.notna(row["Atendimento"]):
            ctx.update({
                "Data": row["Data"],
                "Atendimento": row["Atendimento"],
                "Paciente": row["Paciente"],
                "Aviso": row["Aviso"]
            })
            continue

        if (
            pd.notna(row["Prestador"]) and
            pd.notna(row["Hora_Inicio"]) and
            ctx["Atendimento"] is not None and
            row["Data"] == ctx["Data"]
        ):
            df.at[i, "Atendimento"] = ctx["Atendimento"]
            df.at[i, "Paciente"] = ctx["Paciente"]
            df.at[i, "Aviso"] = ctx["Aviso"]

    return df

# =========================
# PIPELINE PRINCIPAL
# =========================

def process_uploaded_file(upload, prestadores_lista, selected_hospital: str):
    name = upload.name.lower()

    if name.endswith((".xls", ".xlsx")):
        df_in = pd.read_excel(upload)
    else:
        try:
            df_in = pd.read_csv(upload)
        except Exception:
            upload.seek(0)
            df_in = _parse_raw_text_to_rows(upload.read().decode("utf-8", "ignore"))

    df_in = _normalize_columns(df_in)

    for c in REQUIRED_COLS:
        if c not in df_in.columns:
            df_in[c] = pd.NA

    df_in["__pac_raw"] = df_in["Paciente"]
    df_in["__att_raw"] = df_in["Atendimento"]
    df_in["__aviso_raw"] = df_in["Aviso"]

    df = _herdar_por_data_ordem_original(df_in)

    def norm(s):
        return _strip_accents("" if pd.isna(s) else s).upper().strip()

    df["Prestador_norm"] = df["Prestador"].apply(norm)
    target = [norm(p) for p in prestadores_lista]
    df = df[df["Prestador_norm"].isin(target)]

    dt = pd.to_datetime(df["Data"], errors="coerce", dayfirst=True)
    df["Ano"] = dt.dt.year
    df["Mes"] = dt.dt.month
    df["Dia"] = dt.dt.day

    df["Hospital"] = selected_hospital or "Hospital não informado"

    return df[[
        "Hospital", "Ano", "Mes", "Dia",
        "Data", "Atendimento", "Paciente",
        "Aviso", "Convenio", "Prestador", "Quarto"
    ]].reset_index(drop=True)
