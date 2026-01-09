
# processing.py
import io
import csv
import re
import pandas as pd
from dateutil import parser as dtparser  # (mantido caso queira evoluir parsing de datas)

# Padrões/regex utilizados
TIME_RE = re.compile(r"^\d{1,2}:\d{2}$")
DATE_RE = re.compile(r"\b(\d{2}/\d{2}/\d{4})\b")
HAS_LETTER_RE = re.compile(r"[A-Za-zÁÉÍÓÚÃÕÇáéíóúãõç]")
SECTION_KEYWORDS = ["CENTRO CIRURGICO", "HEMODINAMICA", "CENTRO OBSTETRICO"]

# Colunas esperadas quando o arquivo já vier estruturado
EXPECTED_COLS = [
    "Centro", "Data", "Atendimento", "Paciente", "Aviso",
    "Hora_Inicio", "Hora_Fim", "Cirurgia", "Convenio", "Prestador",
    "Anestesista", "Tipo_Anestesia", "Quarto"
]

# Colunas mínimas que o pipeline usa (vamos garantir mesmo que vazias)
REQUIRED_COLS = [
    "Data", "Prestador", "Hora_Inicio", "Atendimento", "Paciente",
    "Aviso", "Convenio", "Quarto"
]

# ---------------- Normalização de colunas ----------------
def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    new_cols = []
    for c in df.columns:
        s = str(c).replace("\ufeff", "").strip()
        new_cols.append(s)
    df.columns = new_cols
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

# ---------------- Parser de texto bruto ----------------
def _parse_raw_text_to_rows(text: str) -> pd.DataFrame:
    """
    Parser robusto para CSV 'bruto' (como os arquivos de centro cirúrgico),
    lendo linha a linha na ordem original e extraindo campos.
    """
    rows = []
    current_section = None
    current_date_str = None
    ctx = {'atendimento': None, 'paciente': None, 'aviso': None,
           'hora_inicio': None, 'hora_fim': None, 'quarto': None}
    row_idx = 0

    for line in text.splitlines():
        # Detecta Data em qualquer linha
        m_date = DATE_RE.search(line)
        if m_date:
            current_date_str = m_date.group(1)

        # Tokeniza respeitando aspas
        tokens = next(csv.reader([line]))
        tokens = [t.strip() for t in tokens]
        if not tokens:
            continue

        # Detecta seção
        if "Centro Cirurgico" in line or "Centro Cirúrgico" in line:
            for kw in SECTION_KEYWORDS:
                if kw in line:
                    current_section = kw
                    break
            ctx = {k: None for k in ctx}
            continue

        # Ignora cabeçalhos/rodapés
        header_phrases = [
            'Hora', 'Atendimento', 'Paciente', 'Convênio', 'Prestador',
            'Anestesista', 'Tipo Anestesia', 'Total', 'Total Geral'
        ]
        if any(h in line for h in header_phrases):
            continue

        # Horários
        time_idxs = [i for i, t in enumerate(tokens) if TIME_RE.match(t)]
        hora_inicio = hora_fim = None
        aviso = None
        atendimento = None
        paciente = None

        if time_idxs:
            h0 = time_idxs[0]
            h1 = h0 + 1 if (h0 + 1 < len(tokens) and TIME_RE.match(tokens[h0 + 1])) else None
            hora_inicio = tokens[h0]
            hora_fim = tokens[h1] if h1 is not None else None

            # Aviso imediatamente antes do horário (código numérico 3+ dígitos)
            if h0 - 1 >= 0 and re.fullmatch(r"\d{3,}", tokens[h0 - 1]):
                aviso = tokens[h0 - 1]

            # Atendimento e Paciente (Paciente = token imediatamente após Atendimento)
            for i, t in enumerate(tokens):
                # Atendimento = número de 7 a 10 dígitos
                if re.fullmatch(r"\d{7,10}", t):
                    atendimento = t
                    if i + 1 < len(tokens):
                        pj = tokens[i + 1].strip()
                        # Paciente só é válido se tiver letras e NÃO for um horário
                        if pj and HAS_LETTER_RE.search(pj) and not TIME_RE.match(pj):
                            paciente = pj
                        else:
                            paciente = None
                    else:
                        paciente = None
                    break

            # ---------- CORREÇÃO: mapear convênio/prestador/anestesista/tipo/quarto ancorando pela direita ----------
            base_idx = h1 if h1 is not None else h0
            tail = [t for t in tokens[base_idx + 1:] if t != ""]  # remove vazios explícitos
            cirurgia = convenio = prestador = anestesista = tipo = quarto = None

            if len(tail) >= 5:
                # últimos 5 tokens = Convênio, Prestador, Anestesista, Tipo_Anestesia, Quarto
                convenio, prestador, anestesista, tipo, quarto = tail[-5:]
                # o restante à esquerda forma (potencialmente multi-palavra) a cirurgia
                cirurgia = " ".join(tail[:-5]).strip() or None
            else:
                # Fallback conservador (layout incompleto): mantém lógica antiga
                cirurgia = tokens[base_idx + 1] if base_idx + 1 < len(tokens) else None
                convenio = tokens[base_idx + 2] if base_idx + 2 < len(tokens) else None
                prestador = tokens[base_idx + 3] if base_idx + 3 < len(tokens) else None
                anestesista = tokens[base_idx + 4] if base_idx + 4 < len(tokens) else None
                tipo = tokens[base_idx + 5] if base_idx + 5 < len(tokens) else None
                quarto = tokens[base_idx + 6] if base_idx + 6 < len(tokens) else None

            # Sanitizações leves
            for field_name, field_val in [("Prestador", prestador), ("Anestesista", anestesista),
                                          ("Convenio", convenio), ("Tipo_Anestesia", tipo), ("Quarto", quarto)]:
                if isinstance(field_val, str):
                    field_val = field_val.strip()
                # aplica de volta (locals não podem ser reatribuídos por nome dinâmico; segue manual)
                if field_name == "Prestador": prestador = field_val or None
                elif field_name == "Anestesista": anestesista = field_val or None
                elif field_name == "Convenio": convenio = field_val or None
                elif field_name == "Tipo_Anestesia": tipo = field_val or None
                elif field_name == "Quarto": quarto = field_val or None

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
            row_idx += 1
            continue
        # -------------------------------------------------------------------------------------

        # Linhas sem horário (procedimentos adicionais)
        if current_section and any(tok for tok in tokens):
            nonempty = [t for t in tokens if t]
            if len(nonempty) >= 4:
                cirurgia = nonempty[0]
                quarto = nonempty[-1] if nonempty else None
                tipo = nonempty[-2] if len(nonempty) >= 2 else None
                anestesista = nonempty[-3] if len(nonempty) >= 3 else None
                prestador = nonempty[-4] if len(nonempty) >= 4 else None
                convenio = nonempty[-5] if len(nonempty) >= 5 else None

                rows.append({
                    "Centro": current_section,
                    "Data": current_date_str,
                    "Atendimento": ctx['atendimento'],
                    "Paciente": ctx['paciente'],
                    "Aviso": ctx['aviso'],
                    "Hora_Inicio": ctx['hora_inicio'],
                    "Hora_Fim": ctx['hora_fim'],
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

# ---------------- Herança por data mantendo ordem original ----------------
def _herdar_por_data_ordem_original(df: pd.DataFrame) -> pd.DataFrame:
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
        last_att = last_pac = last_aviso = None
        for i in grp.sort_values("_row_idx").index:
            att = df.at[i, "Atendimento"] if "Atendimento" in df.columns else None
            pac = df.at[i, "Paciente"] if "Paciente" in df.columns else None
            av  = df.at[i, "Aviso"] if "Aviso" in df.columns else None

            att_orig_blank  = bool(df.at[i, "_orig_att_blank"])  if "_orig_att_blank"  in df.columns else pd.isna(att)
            pac_orig_blank  = bool(df.at[i, "_orig_pac_blank"])  if "_orig_pac_blank"  in df.columns else pd.isna(pac)
            av_orig_blank   = bool(df.at[i, "_orig_av_blank"])   if "_orig_av_blank"   in df.columns else pd.isna(av)
            data_orig_blank = bool(df.at[i, "_orig_data_blank"]) if "_orig_data_blank" in df.columns else False

            att_orig_filled = not att_orig_blank
            data_orig_filled = not data_orig_blank

            if pd.notna(att): last_att = att
            if pd.notna(pac): last_pac = pac
            if pd.notna(av):  last_aviso = av

            has_prestador = ("Prestador" in df.columns and pd.notna(df.at[i, "Prestador"]))
            if not has_prestador:
                continue

            # Regra 1: não herdar Paciente quando Atendimento e Data vieram preenchidos e Paciente veio vazio
            if att_orig_filled and data_orig_filled and pac_orig_blank:
                continue

            # Regra 2: herdar os três quando todos vieram vazios
            if att_orig_blank and pac_orig_blank and av_orig_blank:
                if "Atendimento" in df.columns:
                    df.at[i, "Atendimento"] = last_att
                if "Paciente" in df.columns:
                    df.at[i, "Paciente"] = last_pac
                if "Aviso" in df.columns:
                    df.at[i, "Aviso"] = last_aviso

    return df

# ---------------- Função principal ----------------
def process_uploaded_file(upload, prestadores_lista, selected_hospital: str):
    name = upload.name.lower()

    # 1) Ler arquivo (CSV/Excel ou texto bruto)
    if name.endswith(".xlsx") or name.endswith(".xls"):
        df_in = pd.read_excel(upload, engine="openpyxl")
    elif name.endswith(".csv"):
        try:
            df_in = pd.read_csv(upload, sep=",", encoding="utf-8")
            # Se não tem colunas suficientes, parseia como texto bruto
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

    # Flags do conteúdo original (antes de herdar/ffill)
    df_in["_orig_att_blank"]  = df_in["Atendimento"].isna() | (df_in["Atendimento"].astype(str).str.strip() == "")
    df_in["_orig_pac_blank"]  = df_in["Paciente"].isna()    | (df_in["Paciente"].astype(str).str.strip()    == "")
    df_in["_orig_av_blank"]   = df_in["Aviso"].isna()       | (df_in["Aviso"].astype(str).str.strip()       == "")
    df_in["_orig_data_blank"] = df_in["Data"].isna()        | (df_in["Data"].astype(str).str.strip()        == "")

    # 2) Herança por Data
    df = _herdar_por_data_ordem_original(df_in)

    # 3) Filtro de prestadores (case-insensitive com normalização)
    def normalize(s): return (s or "").strip().upper()
    target = [normalize(p) for p in prestadores_lista]
    if "Prestador" not in df.columns:
        df["Prestador"] = pd.NA
    df["Prestador_norm"] = df["Prestador"].astype(str).apply(normalize)
    df = df[df["Prestador_norm"].isin(target)].copy()

    # 4) Ordenação por tempo e deduplicação
    hora_inicio = df["Hora_Inicio"] if "Hora_Inicio" in df.columns else pd.Series("", index=df.index)
    data_series = df["Data"] if "Data" in df.columns else pd.Series("", index=df.index)

    df["start_key"] = pd.to_datetime(
        data_series.fillna("").astype(str) + " " + hora_inicio.fillna("").astype(str),
        format="%d/%m/%Y %H:%M",
        errors="coerce"
    )
    df = df.sort_values(["Data", "Paciente", "Prestador_norm", "start_key", "_row_idx"]).copy()
    df = df.drop_duplicates(subset=["Data", "Paciente", "Prestador_norm"], keep="first")

    # 5) Hospital + Ano/Mes/Dia
    hosp = (selected_hospital or "").strip() or "Hospital não informado"
    df["Hospital"] = hosp

    if "Data" not in df.columns:
        df["Data"] = pd.NA

    dt = pd.to_datetime(df["Data"], format="%d/%m/%Y", errors="coerce")
    df["Ano"] = dt.dt.year
    df["Mes"] = dt.dt.month
    df["Dia"] = dt.dt.day

    # 6) Seleção final de colunas
    final_cols = [
        "Hospital", "Ano", "Mes", "Dia",
        "Data", "Atendimento", "Paciente", "Aviso",
        "Convenio", "Prestador", "Quarto"
    ]
    for c in final_cols:
        if c not in df.columns:
            df[c] = pd.NA
    out = df[final_cols].copy()

    out = out.sort_values(["Hospital", "Ano", "Mes", "Dia", "Paciente", "Prestador"]).reset_index(drop=True)
    return out
