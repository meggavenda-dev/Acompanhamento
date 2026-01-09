
# processing.py
import io
import csv
import re
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
# Normalização de colunas
# =========================

def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df

    df.columns = [
        str(c).replace("\ufeff", "").strip()
        for c in df.columns
    ]

    col_map = {
        "Convênio": "Convenio",
        "Convênio*": "Convenio",
        "Hora Inicio": "Hora_Inicio",
        "Hora Início": "Hora_Inicio",
        "Hora Fim": "Hora_Fim",
        "Tipo Anestesia": "Tipo_Anestesia",
        "Centro Cirurgico": "Centro",
        "Centro Cirúrgico": "Centro",
    }
    df.rename(columns=col_map, inplace=True)
    return df

# =========================
# Parser CSV bruto
# =========================

def _parse_raw_text_to_rows(text: str) -> pd.DataFrame:
    rows = []
    current_section = None
    current_date_str = None
    ctx = dict(atendimento=None, paciente=None, aviso=None,
               hora_inicio=None, hora_fim=None, quarto=None)
    row_idx = 0

    for line in text.splitlines():

        m_date = DATE_RE.search(line)
        if m_date:
            current_date_str = m_date.group(1)

        tokens = next(csv.reader([line]))
        tokens = [t.strip() for t in tokens if t is not None]

        if not tokens:
            continue

        if "Centro Cirurgico" in line or "Centro Cirúrgico" in line:
            current_section = next(
                (kw for kw in SECTION_KEYWORDS if kw in line), None
            )
            ctx = dict.fromkeys(ctx)
            continue

        header_phrases = [
            "Hora", "Atendimento", "Paciente", "Convênio", "Prestador",
            "Anestesista", "Tipo Anestesia", "Total"
        ]
        if any(h in line for h in header_phrases):
            continue

        time_idxs = [i for i, t in enumerate(tokens) if TIME_RE.match(t)]

        if time_idxs:
            h0 = time_idxs[0]
            h1 = h0 + 1 if h0 + 1 < len(tokens) and TIME_RE.match(tokens[h0 + 1]) else None

            hora_inicio = tokens[h0]
            hora_fim = tokens[h1] if h1 else None
            aviso = tokens[h0 - 1] if h0 - 1 >= 0 and re.fullmatch(r"\d{3,}", tokens[h0 - 1]) else None

            atendimento = paciente = None
            for i, t in enumerate(tokens):
                if re.fullmatch(r"\d{7,10}", t):
                    atendimento = t
                    for j in range(i + 1, len(tokens)):
                        if HAS_LETTER_RE.search(tokens[j]) and not TIME_RE.match(tokens[j]):
                            paciente = tokens[j]
                            break
                    break

            base = h1 if h1 else h0
            rows.append({
                "Centro": current_section,
                "Data": current_date_str,
                "Atendimento": atendimento,
                "Paciente": paciente,
                "Aviso": aviso,
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

            ctx.update(
                atendimento=atendimento,
                paciente=paciente,
                aviso=aviso,
                hora_inicio=hora_inicio,
                hora_fim=hora_fim,
                quarto=ctx["quarto"]
            )
            row_idx += 1
            continue

        if current_section and ctx["atendimento"]:
            rows.append({
                "Centro": current_section,
                "Data": current_date_str,
                "Atendimento": ctx["atendimento"],
                "Paciente": ctx["paciente"],
                "Aviso": ctx["aviso"],
                "Hora_Inicio": ctx["hora_inicio"],
                "Hora_Fim": ctx["hora_fim"],
                "_row_idx": row_idx
            })
            row_idx += 1

    return pd.DataFrame(rows)

# =========================
# Herança CONTROLADA
# =========================

def _herdar_por_data_ordem_original(df: pd.DataFrame) -> pd.DataFrame:
    """
    Regras:
    - Herda somente quando há Prestador.
    - Atendimento e Aviso sempre herdam.
    - Paciente só herda se houver último paciente válido.
    - Se o último paciente for vazio, mantém paciente em branco.
    """
    if df is None or df.empty:
        return df

    df = df.copy()
    df.replace({"": pd.NA}, inplace=True)

    if "_row_idx" not in df.columns:
        df["_row_idx"] = range(len(df))

    if "Data" not in df.columns:
        return df

    df["Data"] = df["Data"].ffill().bfill()

    for _, grp in df.groupby("Data", sort=False):
        last_att = last_pac = last_aviso = pd.NA

        for i in grp.sort_values("_row_idx").index:

            has_prestador = (
                "Prestador" in df.columns and
                pd.notna(df.at[i, "Prestador"]) and
                str(df.at[i, "Prestador"]).strip() != ""
            )

            att = df.at[i, "Atendimento"]
            pac = df.at[i, "Paciente"]
            av  = df.at[i, "Aviso"]

            if pd.notna(att) and str(att).strip():
                last_att = att
            if pd.notna(pac) and str(pac).strip():
                last_pac = pac
            if pd.notna(av) and str(av).strip():
                last_aviso = av

            if not has_prestador:
                continue

            if pd.isna(att) and pd.notna(last_att):
                df.at[i, "Atendimento"] = last_att

            if pd.isna(av) and pd.notna(last_aviso):
                df.at[i, "Aviso"] = last_aviso

            if pd.isna(pac):
                if pd.notna(last_pac) and str(last_pac).strip():
                    df.at[i, "Paciente"] = last_pac
                else:
                    df.at[i, "Paciente"] = pd.NA

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
            df_in = pd.read_csv(upload, encoding="utf-8")
            if len(set(EXPECTED_COLS) & set(df_in.columns)) < 6:
                upload.seek(0)
                df_in = _parse_raw_text_to_rows(
                    upload.read().decode("utf-8", errors="ignore")
                )
        except Exception:
            upload.seek(0)
            df_in = _parse_raw_text_to_rows(
                upload.read().decode("utf-8", errors="ignore")
            )
    else:
        df_in = _parse_raw_text_to_rows(
            upload.read().decode("utf-8", errors="ignore")
        )

    df_in = _normalize_columns(df_in)

    for c in REQUIRED_COLS:
        if c not in df_in.columns:
            df_in[c] = pd.NA

    if "_row_idx" not in df_in.columns:
        df_in["_row_idx"] = range(len(df_in))

    df = _herdar_por_data_ordem_original(df_in)

    def norm(s): return (s or "").strip().upper()
    target = [norm(p) for p in prestadores_lista]

    df["Prestador_norm"] = df["Prestador"].astype(str).apply(norm)
    df = df[df["Prestador_norm"].isin(target)].copy()

    hosp = selected_hospital.strip() or "Hospital não informado"
    df["Hospital"] = hosp

    dt = pd.to_datetime(df["Data"], format="%d/%m/%Y", errors="coerce")
    df["Ano"] = dt.dt.year
    df["Mes"] = dt.dt.month
    df["Dia"] = dt.dt.day

    final_cols = [
        "Hospital", "Ano", "Mes", "Dia",
        "Data", "Atendimento", "Paciente",
        "Aviso", "Convenio", "Prestador", "Quarto"
    ]

    for c in final_cols:
        if c not in df.columns:
            df[c] = pd.NA

    return (
        df[final_cols]
        .sort_values(["Hospital", "Ano", "Mes", "Dia", "Paciente", "Prestador"])
        .reset_index(drop=True)
    )
