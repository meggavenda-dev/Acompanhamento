
# processing.py
import io
import csv
import re
import pandas as pd
from dateutil import parser as dtparser  # mantido se quiser evoluir parsing de datas

# ---------------- Padrões/regex ----------------
TIME_RE = re.compile(r"^\d{1,2}:\d{2}$")
DATE_RE = re.compile(r"\b(\d{2}/\d{2}/\d{4})\b")
HAS_LETTER_RE = re.compile(r"[A-Za-zÁÉÍÓÚÃÕÇáéíóúãõç]")

# Identificadores para validação
ID_RE = re.compile(r"^\d{7,10}$")   # Atendimento: 7 a 10 dígitos
AVISO_RE = re.compile(r"^\d{3,}$")  # Aviso: 3+ dígitos

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

# ---------------- Helpers de validação ----------------
def _has_letters(s: str) -> bool:
    s = (s or "").strip()
    return bool(HAS_LETTER_RE.search(s))

def _is_valid_case(row: pd.Series) -> bool:
    """
    Mantém apenas linhas que representam um caso real:
    - Prestador e Data presentes
    - E pelo menos um dos identificadores: Hora_Inicio OU Atendimento numérico OU Paciente com letras OU Aviso numérico
    (Evita linhas-resumo/rodapé sem paciente/atendimento/aviso/horário)
    """
    prest = str(row.get("Prestador", "") or "").strip()
    data  = str(row.get("Data", "") or "").strip()
    if prest == "" or data == "":
        return False

    has_time  = bool(TIME_RE.match(str(row.get("Hora_Inicio", "") or "")))
    has_att   = bool(ID_RE.match(str(row.get("Atendimento", "") or "").strip()))
    has_pac   = _has_letters(str(row.get("Paciente", "") or ""))
    has_aviso = bool(AVISO_RE.match(str(row.get("Aviso", "") or "").strip()))
    return has_time or has_att or has_pac or has_aviso

# ---------------- Normalização de colunas ----------------
def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    df = df.copy()
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

# ---------------- Parser de texto bruto ----------------
def _parse_raw_text_to_rows(text: str) -> pd.DataFrame:
    """
    Lê o CSV 'bruto' linha a linha e extrai campos principais.
    """
    rows = []
    current_section = None
    current_date_str = None
    ctx = {'atendimento': None, 'paciente': None, 'aviso': None,
           'hora_inicio': None, 'hora_fim': None, 'quarto': None}
    row_idx = 0

    for line in text.splitlines():
        # Detecta Data
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
                if re.fullmatch(r"\d{7,10}", t):  # Atendimento (7 a 10 dígitos)
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

            # --- Mapear tail ancorando pela direita ---
            base_idx = h1 if h1 is not None else h0
            tail = [t for t in tokens[base_idx + 1:] if t != ""]
            cirurgia = convenio = prestador = anestesista = tipo = quarto = None
            if len(tail) >= 5:
                # Últimos 5 tokens: Convênio, Prestador, Anestesista, Tipo_Anestesia, Quarto
                convenio, prestador, anestesista, tipo, quarto = tail[-5:]
                # O restante à esquerda forma a Cirurgia
                cirurgia = " ".join(tail[:-5]).strip() or None
            else:
                # Fallback conservador (layout incompleto)
                cirurgia = tokens[base_idx + 1] if base_idx + 1 < len(tokens) else None
                convenio = tokens[base_idx + 2] if base_idx + 2 < len(tokens) else None
                prestador = tokens[base_idx + 3] if base_idx + 3 < len(tokens) else None
                anestesista = tokens[base_idx + 4] if base_idx + 4 < len(tokens) else None
                tipo = tokens[base_idx + 5] if base_idx + 5 < len(tokens) else None
                quarto = tokens[base_idx + 6] if base_idx + 6 < len(tokens) else None

            # Sanitizações leves
            def _clean(v): return (v.strip() or None) if isinstance(v, str) else v
            prestador = _clean(prestador)
            anestesista = _clean(anestesista)
            convenio = _clean(convenio)
            tipo = _clean(tipo)
            quarto = _clean(quarto)

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
    """
    Herança linha-a-linha por Data com salvaguardas:
      - NÃO herdar Paciente em linhas com horário quando veio vazio.
      - Herdar SOMENTE do último atendimento COM horário do mesmo Prestador (por Data).
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
        # Mapas por prestador (atualizados APENAS quando a linha tem horário)
        last_att_by_prest = {}
        last_pac_by_prest = {}
        last_aviso_by_prest = {}

        for i in grp.sort_values("_row_idx").index:
            att   = df.at[i, "Atendimento"] if "Atendimento" in df.columns else None
            pac   = df.at[i, "Paciente"] if "Paciente" in df.columns else None
            aviso = df.at[i, "Aviso"] if "Aviso" in df.columns else None
            prest = df.at[i, "Prestador"] if "Prestador" in df.columns else None
            has_time = "Hora_Inicio" in df.columns and pd.notna(df.at[i, "Hora_Inicio"])

            # Flags do conteúdo ORIGINAL
            att_orig_blank  = bool(df.at[i, "_orig_att_blank"])  if "_orig_att_blank"  in df.columns else pd.isna(att)
            pac_orig_blank  = bool(df.at[i, "_orig_pac_blank"])  if "_orig_pac_blank"  in df.columns else pd.isna(pac)
            av_orig_blank   = bool(df.at[i, "_orig_av_blank"])   if "_orig_av_blank"   in df.columns else pd.isna(aviso)
            data_orig_blank = bool(df.at[i, "_orig_data_blank"]) if "_orig_data_blank" in df.columns else False

            att_orig_filled = not att_orig_blank
            data_orig_filled = not data_orig_blank

            # Atualizações APENAS quando há horário (definem "caso" do prestador)
            if has_time:
                if pd.notna(att):   last_att_by_prest[prest]   = att
                if pd.notna(pac):   last_pac_by_prest[prest]   = pac
                if pd.notna(aviso): last_aviso_by_prest[prest] = aviso

            # Salvaguarda: sem prestador, não tentamos herdar
            if not (isinstance(prest, str) and prest.strip()):
                continue

            # 1) Linha com horário e Paciente vazio -> NUNCA herdar Paciente
            if has_time and pac_orig_blank:
                continue

            # 2) Atendimento+Data vieram preenchidos e Paciente vazio -> NÃO herdar Paciente
            if att_orig_filled and data_orig_filled and pac_orig_blank:
                continue

            # 3) Linhas sem horário: herdar somente do último atendimento COM horário do mesmo prestador
            if (not has_time) and att_orig_blank and pac_orig_blank and av_orig_blank:
                src_att   = last_att_by_prest.get(prest, None)
                src_pac   = last_pac_by_prest.get(prest, None)
                src_aviso = last_aviso_by_prest.get(prest, None)

                if "Atendimento" in df.columns and pd.notna(src_att):
                    df.at[i, "Atendimento"] = src_att
                if "Paciente" in df.columns and pd.notna(src_pac):
                    df.at[i, "Paciente"] = src_pac
                if "Aviso" in df.columns and pd.notna(src_aviso):
                    df.at[i, "Aviso"] = src_aviso

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

    # >>> Flags do conteúdo original (antes de herdar/ffill)
    df_in["_orig_att_blank"]  = df_in["Atendimento"].isna() | (df_in["Atendimento"].astype(str).str.strip() == "")
    df_in["_orig_pac_blank"]  = df_in["Paciente"].isna()    | (df_in["Paciente"].astype(str).str.strip()    == "")
    df_in["_orig_av_blank"]   = df_in["Aviso"].isna()       | (df_in["Aviso"].astype(str).str.strip()       == "")
    df_in["_orig_data_blank"] = df_in["Data"].isna()        | (df_in["Data"].astype(str).str.strip()        == "")

    # 2) Herança por Data (com salvaguardas por prestador)
    df = _herdar_por_data_ordem_original(df_in)

    # --- SANITIZAÇÃO: Paciente nunca deve ser numérico puro ---
    df["Paciente"] = df["Paciente"].apply(lambda x: None if str(x or "").strip().isdigit() else x)

    # --- FILTRO: remover linhas-resumo/rodapé sem identificadores reais ---
    df = df[df.apply(_is_valid_case, axis=1)].copy()

    # 3) Filtro de prestadores (case-insensitive com normalização)
    def normalize(s): return (s or "").strip().upper()
    target = [normalize(p) for p in prestadores_lista]
    # Garante coluna Prestador
    if "Prestador" not in df.columns:
        df["Prestador"] = pd.NA
    df["Prestador_norm"] = df["Prestador"].astype(str).apply(normalize)
    df = df[df["Prestador_norm"].isin(target)].copy()

    # 4) Ordenação por tempo e deduplicação por (Data, Paciente, Prestador)
    hora_inicio = df["Hora_Inicio"] if "Hora_Inicio" in df.columns else pd.Series("", index=df.index)
    data_series = df["Data"] if "Data" in df.columns else pd.Series("", index=df.index)

    df["start_key"] = pd.to_datetime(
        data_series.fillna("").astype(str) + " " + hora_inicio.fillna("").astype(str),
        format="%d/%m/%Y %H:%M",
        errors="coerce"
    )
    df = df.sort_values(["Data", "Paciente", "Prestador_norm", "start_key", "_row_idx"]).copy()
    df = df.drop_duplicates(subset=["Data", "Paciente", "Prestador_norm"], keep="first")

    # 5) Hospital informado + Ano/Mes/Dia
    hosp = (selected_hospital or "").strip() or "Hospital não informado"
    df["Hospital"] = hosp

    # Garante coluna Data antes de extrair Ano/Mes/Dia
    if "Data" not in df.columns:
        df["Data"] = pd.NA

    dt = pd.to_datetime(df["Data"], format="%d/%m/%Y", errors="coerce")
    df["Ano"] = dt.dt.year
    df["Mes"] = dt.dt.month
    df["Dia"] = dt.dt.day

    # 6) Seleção das colunas finais
    final_cols = [
        "Hospital", "Ano", "Mes", "Dia",
        "Data", "Atendimento", "Paciente", "Aviso",
        "Convenio", "Prestador", "Quarto"
    ]
    for c in final_cols:
        if c not in df.columns:
            df[c] = pd.NA
    out = df[final_cols].copy()

    # Ordenação para retorno (ano/mês/dia)
    out = out.sort_values(["Hospital", "Ano", "Mes", "Dia", "Paciente", "Prestador"]).reset_index(drop=True)
    return out

