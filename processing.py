
# processing.py (aplicado com blindagem no parser e ffill na herança)
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

# Conjunto de "hints" que indicam texto de procedimento (não nome de paciente)
PROCEDURE_HINTS = {
    "HERNIA", "HERNIORRAFIA", "COLECISTECTOMIA", "APENDICECTOMIA",
    "ENDOMETRIOSE", "SINOVECTOMIA", "OSTEOCONDROPLASTIA", "ARTROPLASTIA",
    "ADENOIDECTOMIA", "AMIGDALECTOMIA", "ETMOIDECTOMIA", "SEPTOPLASTIA",
    "TURBINECTOMIA", "MIOMECTOMIA", "HISTEROSCOPIA", "HISTERECTOMIA",
    "ENXERTO", "TENOLISE", "MICRONEUROLISE", "URETERO", "NEFRECTOMIA",
    "LAPAROTOMIA", "LAPAROSCOPICA", "ROBOTICA", "BIOPSIA", "CRANIOTOMIA",
    "RETIRADA", "DRENAGEM", "FISTULECTOMIA", "HEMOSTA", "ARTRODESE",
    "OSTEOTOMIA", "SEPTOPLASTA", "CIRURGIA", "EXERESE", "RESSECCAO",
    "URETEROLITOTRIPSIA", "URETEROSCOPIA", "ENDOSCOPICA", "ENDOSCOPIA",
    "CATETER", "AMIGDALECTOMIA LINGUAL", "CERVICOTOMIA", "TIREOIDECTOMIA",
    "LINFADENECTOMIA", "RECONSTRUÇÃO", "RETOSSIGMOIDECTOMIA", "PLEUROSCOPIA",
}

def _is_probably_procedure_token(tok) -> bool:
    if tok is None or pd.isna(tok):
        return False
    T = str(tok).upper().strip()
    if any(h in T for h in PROCEDURE_HINTS):
        return True
    if any(x in T for x in [",", "/", "(", ")", "%", "-", "  "]):
        return True
    if len(T) > 50:
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
# Normalização de colunas
# =========================

def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df

    df.columns = [str(c).replace("\ufeff", "").strip() for c in df.columns]

    col_map = {
        "Convênio": "Convenio",
        "Convênio*": "Convenio",
        "Tipo Anestesia": "Tipo_Anestesia",
        "Hora Inicio": "Hora_Inicio",
        "Hora Início": "Hora_Inicio",
        "Hora Fim": "Hora_Fim",
        "Centro Cirurgico": "Centro",
        "Centro Cirúrgico": "Centro",
    }
    df.rename(columns=col_map, inplace=True)
    return df


# =========================
# Parser de texto bruto (blindado)
# =========================

def _parse_raw_text_to_rows(text: str) -> pd.DataFrame:
    rows = []
    current_section = None
    current_date_str = None

    ctx = {
        "atendimento": None,
        "paciente": None,
        "aviso": None,
        "hora_inicio": None,
        "hora_fim": None,
        "quarto": None,
        "data_locked": False,
        "data": None
    }

    row_idx = 0

    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line:
            continue

        line_noacc = _strip_accents(line).upper()
        m_date_hdr = DATE_RE.search(line)

        if "DATA DE REALIZ" in line_noacc and m_date_hdr:
            yyyy = int(m_date_hdr.group(1).split("/")[-1])
            if 2010 <= yyyy <= 2035:
                current_date_str = m_date_hdr.group(1)
                ctx["data_locked"] = False
            continue

        if "CENTRO CIRUR" in line_noacc:
            current_section = next((kw for kw in SECTION_KEYWORDS if kw in line_noacc), None)
            ctx = dict.fromkeys(ctx, None)
            ctx["data_locked"] = False
            continue

        if any(h in line for h in [
            "Hora", "Atendimento", "Paciente", "Convênio",
            "Prestador", "Anestesista", "Tipo Anestesia",
            "Total", "Total Geral", "Página"
        ]):
            ctx["data_locked"] = False
            continue

        tokens = [t.strip() for t in next(csv.reader([line])) if t]

        time_idxs = [i for i, t in enumerate(tokens) if TIME_RE.match(t)]
        if not time_idxs:
            continue

        h0 = time_idxs[0]
        h1 = h0 + 1 if h0 + 1 < len(tokens) and TIME_RE.match(tokens[h0 + 1]) else None

        hora_inicio = tokens[h0]
        hora_fim = tokens[h1] if h1 else None

        aviso = tokens[h0 - 1] if h0 > 0 and re.fullmatch(r"\d{3,}", tokens[h0 - 1]) else None

        atendimento = next((t for t in tokens if re.fullmatch(r"\d{7,10}", t)), None)

        if atendimento and atendimento != ctx["atendimento"]:
            ctx["data_locked"] = False

        data_final = current_date_str if current_date_str and not ctx["data_locked"] else ctx["data"]
        if atendimento and not current_date_str:
            data_final = pd.NA

        paciente = None
        if atendimento in tokens:
            lb = tokens.index(atendimento) + 1
            ub = h0 - 2
            for t in tokens[lb:ub + 1]:
                if HAS_LETTER_RE.search(t) and not _is_probably_procedure_token(t):
                    paciente = t
                    break

        base_idx = h1 if h1 else h0

        rows.append({
            "Centro": current_section,
            "Data": data_final,
            "Atendimento": atendimento,
            "Paciente": paciente,
            "Aviso": aviso,
            "Hora_Inicio": hora_inicio,
            "Hora_Fim": hora_fim,
            "Cirurgia": tokens[base_idx + 1] if base_idx + 1 < len(tokens) else None,
            "Convenio": tokens[base_idx + 2] if base_idx + 2 < len(tokens) else None,
            "Prestador": tokens[base_idx + 3] if base_idx + 3 < len(tokens) else None,
            "Anestesista": tokens[base_idx + 4] if base_idx + 4 < len(tokens) else None,
            "Tipo_Anestesia": tokens[base_idx + 5] if base_idx + 5 < len(tokens) else None,
            "Quarto": tokens[base_idx + 6] if base_idx + 6 < len(tokens) else None,
            "_row_idx": row_idx
        })

        ctx.update({
            "atendimento": atendimento,
            "paciente": paciente,
            "aviso": aviso,
            "hora_inicio": hora_inicio,
            "hora_fim": hora_fim,
            "quarto": None,
            "data": data_final,
            "data_locked": True if atendimento else False
        })

        row_idx += 1

    return pd.DataFrame(rows)


# =========================
# Herança CONTROLADA
# =========================

def _herdar_por_data_ordem_original(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df

    df = df.copy()
    df.replace({"": pd.NA}, inplace=True)

    if "Data" not in df.columns:
        return df

    df["Data"] = df["Data"].ffill()

    for _, grp in df.groupby("Data", sort=False):
        last_att = last_pac = last_aviso = pd.NA

        for i in grp.sort_values("_row_idx").index:
            if pd.notna(df.at[i, "Atendimento"]):
                last_att = df.at[i, "Atendimento"]
            if pd.notna(df.at[i, "Paciente"]):
                last_pac = df.at[i, "Paciente"]
            if pd.notna(df.at[i, "Aviso"]):
                last_aviso = df.at[i, "Aviso"]

            if pd.isna(df.at[i, "Prestador"]):
                continue

            if pd.isna(df.at[i, "Atendimento"]):
                df.at[i, "Atendimento"] = last_att
            if pd.isna(df.at[i, "Paciente"]):
                df.at[i, "Paciente"] = last_pac
            if pd.isna(df.at[i, "Aviso"]):
                df.at[i, "Aviso"] = last_aviso

    return df


# =========================
# Pipeline principal
# =========================

def process_uploaded_file(upload, prestadores_lista, selected_hospital: str):
    name = upload.name.lower()

    if name.endswith(".xlsx"):
        df_in = pd.read_excel(upload, engine="openpyxl")
    elif name.endswith(".xls"):
        df_in = pd.read_excel(upload, engine="xlrd")
    elif name.endswith(".csv"):
        try:
            df_in = pd.read_csv(upload, sep=",", encoding="utf-8")
            if len(set(EXPECTED_COLS) & set(df_in.columns)) < 6:
                upload.seek(0)
                df_in = _parse_raw_text_to_rows(upload.read().decode("utf-8", errors="ignore"))
        except Exception:
            upload.seek(0)
            df_in = _parse_raw_text_to_rows(upload.read().decode("utf-8", errors="ignore"))
    else:
        df_in = _parse_raw_text_to_rows(upload.read().decode("utf-8", errors="ignore"))

    df_in = _normalize_columns(df_in)

    for c in REQUIRED_COLS:
        if c not in df_in.columns:
            df_in[c] = pd.NA

    df_in["__pac_raw"] = df_in["Paciente"]
    df_in["__att_raw"] = df_in["Atendimento"]
    df_in["__aviso_raw"] = df_in["Aviso"]

    df = _herdar_por_data_ordem_original(df_in)

    # ✅ ALTERAÇÃO FINAL (única)
    df["Paciente"] = np.where(
        df["__pac_raw"].isna() | (df["__pac_raw"].astype(str).str.strip() == ""),
        df["Paciente"],
        df["__pac_raw"]
    )

    def norm(s):
        return _strip_accents(s).upper().strip() if pd.notna(s) else ""

    df["Prestador_norm"] = df["Prestador"].apply(norm)
    df = df[df["Prestador_norm"].isin([norm(p) for p in prestadores_lista])]

    df["Hospital"] = selected_hospital or "Hospital não informado"

    dt = pd.to_datetime(df["Data"], format="%d/%m/%Y", errors="coerce")
    df["Ano"] = dt.dt.year
    df["Mes"] = dt.dt.month
    df["Dia"] = dt.dt.day

    out = df[[
        "Hospital", "Ano", "Mes", "Dia", "Data",
        "Atendimento", "Paciente", "Aviso",
        "Convenio", "Prestador", "Quarto"
    ]].sort_values(
        ["Hospital", "Ano", "Mes", "Dia", "Paciente", "Prestador"]
    ).reset_index(drop=True)

    return out
