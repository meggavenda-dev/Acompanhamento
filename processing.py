
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

# =========================
# Helpers
# =========================
def _strip_accents(s: str) -> str:
    """Remove acentos para comparações robustas (Prestador, cabeçalhos etc.)."""
    if s is None or pd.isna(s):
        return ""
    s = str(s)
    return "".join(ch for ch in unicodedata.normalize("NFKD", s) if not unicodedata.combining(ch))

def _normalize_prestador(s: str) -> str:
    """Normaliza prestador removendo acentos e pontuações para facilitar filtro."""
    s = _strip_accents(s).upper()
    for ch in (" ", ".", "-", "_", "/", "\\"):
        s = s.replace(ch, "")
    return s.strip()

def _is_probably_procedure_token(tok) -> bool:
    """Heurística para diferenciar 'Paciente' de frases técnicas/procedimentos."""
    if tok is None or pd.isna(tok):
        return False
    T = str(tok).upper().strip()
    if any(h in T for h in PROCEDURE_HINTS):
        return True
    if ("," in T) or ("/" in T) or ("(" in T) or (")" in T) or ("%" in T) or ("  " in T) or ("-" in T):
        return True
    if len(T) > 50:
        return True
    return False

def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Normaliza cabeçalhos para evitar KeyError e mapear sinônimos/acento."""
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

def _detect_centro(line: str) -> str:
    """Detecta a seção (Centro Cirúrgico, Hemodinâmica, etc.), acento-insensível."""
    L = _strip_accents(line).upper()
    if "CENTRO CIRURGICO" in L: return "CENTRO CIRURGICO"
    if "HEMODINAMICA" in L: return "HEMODINAMICA"
    if "CENTRO OBSTETRICO" in L: return "CENTRO OBSTETRICO"
    return None

def _has_data_header(line: str) -> bool:
    """Identifica o cabeçalho 'Data de Realização' de forma tolerante a variações."""
    L = _strip_accents(line).upper()
    # Não exigimos ':', nem a palavra inteira 'REALIZAÇÃO': usamos prefixo com acento-insensível
    return "DATA DE REALIZA" in L

# =========================
# Parser de texto bruto
# =========================
def _parse_raw_text_to_rows(text: str) -> pd.DataFrame:
    """
    Parser robusto para relatórios (CSV/Texto) exportados:
    - Atualiza 'Data' APENAS quando encontra o cabeçalho 'Data de Realização'.
    - Se a data não estiver na mesma linha, busca nas 3 linhas seguintes.
    - Ignora datas soltas em outras colunas (evita contaminação por 1983).
    - Captura horários, Atendimento, Paciente (com heurística), Aviso e bloco pós-horário.
    """
    rows = []
    current_section = None
    current_date_str = None
    ctx = {
        "atendimento": None, "paciente": None, "aviso": None,
        "hora_inicio": None, "hora_fim": None, "quarto": None
    }
    row_idx = 0
    lines = text.splitlines()

    for idx, line in enumerate(lines):
        # 1) Atualiza Data SOMENTE no cabeçalho 'Data de Realização'
        if _has_data_header(line):
            m_date = DATE_RE.search(line)
            if not m_date:
                # Se a data estiver quebrada, busca em até 3 linhas abaixo
                for k in range(1, 4):
                    if idx + k < len(lines):
                        m_date = DATE_RE.search(lines[idx + k])
                        if m_date:
                            break
            # Se achou, atualiza; caso contrário, mantém a última data válida
            current_date_str = m_date.group(1) if m_date else current_date_str
            # Início de novo bloco diário → zera contexto
            ctx = {k: None for k in ctx}

        # 2) Detecta Centro/Seção
        centro = _detect_centro(line)
        if centro:
            current_section = centro
            ctx = {k: None for k in ctx}
            continue

        # 3) Tokeniza respeitando aspas
        tokens = next(csv.reader([line]))
        tokens = [t.strip() for t in tokens if t is not None]
        if not tokens:
            continue

        # 4) Ignora cabeçalhos/rodapés comuns
        L = _strip_accents(line).upper()
        header_phrases = [
            "HORA", "ATENDIMENTO", "PACIENTE", "CONVENIO", "PRESTADOR",
            "ANESTESISTA", "TIPO ANESTESIA", "TOTAL", "TOTAL GERAL"
        ]
        if any(h in L for h in header_phrases):
            continue

        # 5) Procura horários
        time_idxs = [i for i, t in enumerate(tokens) if TIME_RE.match(t)]
        if time_idxs:
            h0 = time_idxs[0]
            h1 = h0 + 1 if (h0 + 1 < len(tokens) and TIME_RE.match(tokens[h0 + 1])) else None
            hora_inicio = tokens[h0]
            hora_fim = tokens[h1] if h1 is not None else None

            # Atendimento (7–10 dígitos)
            atendimento, att_pos = None, None
            for i, t in enumerate(tokens):
                if re.fullmatch(r"\d{7,10}", t):
                    atendimento, att_pos = t, i
                    break

            # Paciente — entre atendimento e horário, evitando “frase técnica”
            paciente = None
            upper_bound = (h0 - 2) if h0 is not None else len(tokens) - 1
            if atendimento is not None and upper_bound >= (att_pos or 0) + 1:
                for j in range((att_pos or 0) + 1, upper_bound + 1):
                    tj = tokens[j]
                    if tj and HAS_LETTER_RE.search(tj) and not TIME_RE.match(tj) and not _is_probably_procedure_token(tj):
                        paciente = tj
                        break

            # Aviso — 5–7 dígitos próximo ao horário; fallback ao contexto
            aviso = None
            scan_end = h0 if h0 is not None else len(tokens)
            cand = [t for t in tokens[:scan_end] if re.fullmatch(r"\d{5,7}", t)]
            if cand:
                aviso = cand[-1]
                if atendimento and aviso == atendimento and len(cand) >= 2:
                    aviso = cand[-2]
            if not aviso:
                aviso = ctx["aviso"]

            # Bloco pós-horário (Cirurgia, Convênio, Prestador, Anestesista, Tipo, Quarto)
            base_idx = h1 if h1 is not None else h0
            cirurgia     = tokens[base_idx + 1] if base_idx + 1 < len(tokens) else None
            convenio     = tokens[base_idx + 2] if base_idx + 2 < len(tokens) else None
            prestador    = tokens[base_idx + 3] if base_idx + 3 < len(tokens) else None
            anestesista  = tokens[base_idx + 4] if base_idx + 4 < len(tokens) else None
            tipo         = tokens[base_idx + 5] if base_idx + 5 < len(tokens) else None
            quarto       = tokens[base_idx + 6] if base_idx + 6 < len(tokens) else None

            rows.append({
                "Centro": current_section,
                "Data": current_date_str,
                "Atendimento": atendimento,
                "Paciente": paciente,
                "Aviso": aviso,
                "Hora_Inicio": hora_inicio,
                "Hora_Fim": hora_fim,
                "Cirurgia": cirurgia,
                "Convenio": convenio,
                "Prestador": prestador,
                "Anestesista": anestesista,
                "Tipo_Anestesia": tipo,
                "Quarto": quarto,
                "_row_idx": row_idx
            })

            # Atualiza contexto
            ctx["atendimento"] = atendimento
            ctx["paciente"] = paciente
            ctx["aviso"] = aviso
            ctx["hora_inicio"] = hora_inicio
            ctx["hora_fim"] = hora_fim
            ctx["quarto"] = quarto

            row_idx += 1
            continue

        # 6) Linhas sem horário — herda contexto e registra procedimentos adicionais
        if current_section and any(tok for tok in tokens):
            nonempty = [t for t in tokens if t]
            if len(nonempty) >= 4:
                cirurgia     = nonempty[0]
                quarto       = nonempty[-1] if nonempty else None
                tipo         = nonempty[-2] if len(nonempty) >= 2 else None
                anestesista  = nonempty[-3] if len(nonempty) >= 3 else None
                prestador    = nonempty[-4] if len(nonempty) >= 4 else None
                convenio     = nonempty[-5] if len(nonempty) >= 5 else None

                rows.append({
                    "Centro": current_section,
                    "Data": current_date_str,
                    "Atendimento": ctx["atendimento"],
                    "Paciente": ctx["paciente"],
                    "Aviso": ctx["aviso"],
                    "Hora_Inicio": ctx["hora_inicio"],
                    "Hora_Fim": ctx["hora_fim"],
                    "Cirurgia": cirurgia,
                    "Convenio": convenio,
                    "Prestador": prestador,
                    "Anestesista": anestesista,
                    "Tipo_Anestesia": tipo,
                    "Quarto": quarto,
                    "_row_idx": row_idx
                })
                row_idx += 1

    return pd.DataFrame(rows)

# =========================
# Herança CONTROLADA
# =========================
def _herdar_por_data_ordem_original(df: pd.DataFrame) -> pd.DataFrame:
    """
    Herança linha-a-linha por Data, preservando ordem original:
    - Aplica herança somente quando há Prestador na linha atual.
    - 'Atendimento' e 'Aviso' herdam se vazios e houver valor anterior no mesmo dia.
    - 'Paciente' herda apenas se o último paciente conhecido não estiver vazio; caso contrário, mantém em branco.
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
        last_att = pd.NA
        last_pac = pd.NA
        last_aviso = pd.NA

        for i in grp.sort_values("_row_idx").index:
            att = df.at[i, "Atendimento"] if "Atendimento" in df.columns else pd.NA
            pac = df.at[i, "Paciente"] if "Paciente" in df.columns else pd.NA
            av  = df.at[i, "Aviso"] if "Aviso" in df.columns else pd.NA

            if pd.notna(att) and str(att).strip():
                last_att = att
            if pd.notna(pac) and str(pac).strip():
                last_pac = pac
            if pd.notna(av) and str(av).strip():
                last_aviso = av

            has_prestador = ("Prestador" in df.columns and pd.notna(df.at[i, "Prestador"]) and str(df.at[i, "Prestador"]).strip() != "")
            if not has_prestador:
                continue

            if "Atendimento" in df.columns and (pd.isna(att) or str(att).strip() == "") and pd.notna(last_att):
                df.at[i, "Atendimento"] = last_att
            if "Aviso" in df.columns and (pd.isna(av) or str(av).strip() == "") and pd.notna(last_aviso):
                df.at[i, "Aviso"] = last_aviso
            if "Paciente" in df.columns and (pd.isna(pac) or str(pac).strip() == ""):
                df.at[i, "Paciente"] = last_pac if (pd.notna(last_pac) and str(last_pac).strip() != "") else pd.NA

    return df

# =========================
# Pipeline principal
# =========================
def process_uploaded_file(upload, prestadores_lista, selected_hospital: str):
    """
    Lê o arquivo (CSV/Excel/Texto) e retorna DataFrame organizado com colunas:
      Hospital, Ano, Mes, Dia, Data, Atendimento, Paciente, Aviso, Convenio, Prestador, Quarto
    """
    name = (getattr(upload, "name", "") or "").lower()

    # 1) Ler arquivo
    if name.endswith(".xlsx"):
        df_in = pd.read_excel(upload, engine="openpyxl")
    elif name.endswith(".xls"):
        df_in = pd.read_excel(upload, engine="xlrd")
    elif name.endswith(".csv"):
        try:
            df_in = pd.read_csv(upload, sep=",", encoding="utf-8")
            # Se não tem estrutura típica de colunas, parseia como texto bruto
            if len(set(EXPECTED_COLS) & set(df_in.columns)) < 6:
                upload.seek(0)
                text = upload.read().decode("utf-8", errors="ignore")
                df_in = _parse_raw_text_to_rows(text)
        except Exception:
            upload.seek(0)
            text = upload.read().decode("utf-8", errors="ignore")
            df_in = _parse_raw_text_to_rows(text)
    else:
        text = upload.read().decode("utf-8", errors="ignore")
        df_in = _parse_raw_text_to_rows(text)

    # 1.1) Normaliza colunas e garante mínimas
    df_in = _normalize_columns(df_in)
    if "_row_idx" not in df_in.columns:
        df_in["_row_idx"] = range(len(df_in))
    for c in REQUIRED_COLS:
        if c not in df_in.columns:
            df_in[c] = pd.NA

    # Guarda CRUS pré-herança (para dedup e evitar herança indevida do Paciente)
    df_in["__pac_raw"]   = df_in["Paciente"]
    df_in["__att_raw"]   = df_in["Atendimento"]
    df_in["__aviso_raw"] = df_in["Aviso"]

    # Sanitiza SOMENTE o __pac_raw (remove “paciente = cirurgia” / texto técnico)
    def _sanitize_one(pac_val, cir_val):
        pac = "" if pd.isna(pac_val) else str(pac_val).strip()
        cir = "" if pd.isna(cir_val) else str(cir_val).strip()
        if pac == "":
            return pd.NA
        if cir and pac.upper() == cir.upper():
            return pd.NA
        if _is_probably_procedure_token(pac):
            return pd.NA
        return pac

    df_in["__pac_raw"] = [
        _sanitize_one(p, c) for p, c in zip(
            df_in["__pac_raw"],
            df_in.get("Cirurgia", pd.Series(index=df_in.index))
        )
    ]

    # 2) Herança
    df = _herdar_por_data_ordem_original(df_in)

    # 3) Filtro de Prestadores (tolerante)
    def norm_prest(s): return _normalize_prestador(s)
    target = [norm_prest(p) for p in prestadores_lista if str(p).strip()]

    if "Prestador" not in df.columns:
        df["Prestador"] = pd.NA
    df["Prestador_norm"] = df["Prestador"].apply(norm_prest)
    if target:
        df = df[df["Prestador_norm"].isin(target)].copy()

    # 4) start_key (ordenação temporal)
    hora_inicio = df["Hora_Inicio"] if "Hora_Inicio" in df.columns else pd.Series("", index=df.index)
    data_series = df["Data"] if "Data" in df.columns else pd.Series("", index=df.index)
    df["start_key"] = pd.to_datetime(
        data_series.fillna("").astype(str) + " " + hora_inicio.fillna("").astype(str),
        format="%d/%m/%Y %H:%M",
        errors="coerce"
    )

    # 4.1) Dedup híbrida (PA > PV > P > A > V > T)
    def _norm_blank(series: pd.Series) -> pd.Series:
        return series.fillna("").astype(str).str.strip().str.upper()

    P_raw  = _norm_blank(df["__pac_raw"])
    A_raw  = _norm_blank(df["__att_raw"])
    V_raw  = _norm_blank(df["__aviso_raw"])
    D      = _norm_blank(df["Data"])
    PR     = df["Prestador_norm"].fillna("").astype(str)

    df["__dedup_tag"] = np.where((P_raw != "") & (A_raw != ""),
        "PA|" + D + "|" + P_raw + "|" + A_raw + "|" + PR,
        np.where((P_raw != "") & (V_raw != ""),
            "PV|" + D + "|" + P_raw + "|" + V_raw + "|" + PR,
            np.where(P_raw != "",
                "P|"  + D + "|" + P_raw + "|" + PR,
                np.where(A_raw != "",
                    "A|" + D + "|" + A_raw + "|" + PR,
                    np.where(V_raw != "",
                        "V|" + D + "|" + V_raw + "|" + PR,
                        "T|" + D + "|" + PR + "|" + df["start_key"].astype(str)
                    )
                )
            )
        )
    )

    df = df.sort_values(["Data", "Paciente", "Prestador_norm", "start_key"])
    df = df.drop_duplicates(subset=["__dedup_tag"], keep="first")

    # Usa Paciente CRU no resultado final
    df["Paciente"] = df["__pac_raw"]
    df = df.drop(columns=["__dedup_tag", "__pac_raw", "__att_raw", "__aviso_raw"], errors="ignore")

    # 5) Hospital + Ano/Mes/Dia
    hosp = selected_hospital.strip() if selected_hospital else "Hospital não informado"
    df["Hospital"] = hosp
    if "Data" not in df.columns:
        df["Data"] = pd.NA

    dt = pd.to_datetime(df["Data"], format="%d/%m/%Y", errors="coerce")
    df["Ano"] = dt.dt.year
    df["Mes"] = dt.dt.month
    df["Dia"] = dt.dt.day

    # 6) Seleção e ordenação final
    final_cols = [
        "Hospital", "Ano", "Mes", "Dia",
        "Data", "Atendimento", "Paciente", "Aviso",
        "Convenio", "Prestador", "Quarto"
    ]
    for c in final_cols:
        if c not in df.columns:
            df[c] = pd.NA

    out = df[final_cols].copy().sort_values(
        ["Hospital", "Ano", "Mes", "Dia", "Paciente", "Prestador"]
    ).reset_index(drop=True)
    return out
