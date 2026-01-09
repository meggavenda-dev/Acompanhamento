
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
    "Centro","Data","Atendimento","Paciente","Aviso",
    "Hora_Inicio","Hora_Fim","Cirurgia","Convenio","Prestador",
    "Anestesista","Tipo_Anestesia","Quarto"
]

# Colunas mínimas que o pipeline usa (vamos garantir mesmo que vazias)
REQUIRED_COLS = [
    "Data", "Prestador", "Hora_Inicio", "Atendimento", "Paciente",
    "Aviso", "Convenio", "Quarto"
]

# ---------------- Normalização de colunas ----------------

def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normaliza cabeçalhos para evitar KeyError:
    - remove BOM, espaços no início/fim
    - mapeia sinônimos/acento para nomes esperados
    """
    if df is None or df.empty:
        return df

    # strip + remove BOM
    new_cols = []
    for c in df.columns:
        s = str(c)
        s = s.replace("\ufeff", "").strip()
        new_cols.append(s)
    df.columns = new_cols

    # mapa de sinônimos comuns -> nomes esperados
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
    Parser robusto para CSV 'bruto' (como o 12.2025.csv),
    realizando leitura linha a linha em ordem original e extraindo campos.
    """
    rows = []
    current_section = None
    current_date_str = None
    ctx = {'atendimento': None, 'paciente': None, 'aviso': None,
           'hora_inicio': None, 'hora_fim': None, 'quarto': None}
    row_idx = 0

    for line in text.splitlines():
        # Detectar Data em qualquer linha
        m_date = DATE_RE.search(line)
        if m_date:
            current_date_str = m_date.group(1)

        # Tokenizar respeitando aspas
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
        header_phrases = ['Hora','Atendimento','Paciente','Convênio','Prestador',
                          'Anestesista','Tipo Anestesia','Total','Total Geral']
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

            # Atendimento e Paciente
            for i, t in enumerate(tokens):
                if re.fullmatch(r"\d{7,10}", t):
                    atendimento = t
                    for j in range(i+1, len(tokens)):
                        tj = tokens[j]
                        if tj and HAS_LETTER_RE.search(tj) and not TIME_RE.match(tj):
                            paciente = tj
                            break
                    break

            base_idx = h1 if h1 is not None else h0
            cirurgia = tokens[base_idx + 1] if base_idx + 1 < len(tokens) else None
            convenio = tokens[base_idx + 2] if base_idx + 2 < len(tokens) else None
            prestador = tokens[base_idx + 3] if base_idx + 3 < len(tokens) else None
            anestesista = tokens[base_idx + 4] if base_idx + 4 < len(tokens) else None
            tipo = tokens[base_idx + 5] if base_idx + 5 < len(tokens) else None
            quarto = tokens[base_idx + 6] if base_idx + 6 < len(tokens) else None

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


def _herdar_por_data_ordem_original(df: pd.DataFrame) -> pd.DataFrame:
    """
    Herança linha-a-linha por Data, preservando ordem original do arquivo.
    Quando há Prestador e (Atendimento/Paciente/Aviso) vazios, herda do último acima.
    """
    if df is None or df.empty:
        return df

    df = df.copy()
    df.replace({"": pd.NA}, inplace=True)

    # Garante índice de ordem
    if "_row_idx" not in df.columns:
        df["_row_idx"] = range(len(df))

    # Se não há coluna Data, não há herança por dia — retorna DF como está
    if "Data" not in df.columns:
        return df

    # Preenche datas ausentes
    df["Data"] = df["Data"].ffill().bfill()

    # Varre por data na ordem original
    for _, grp in df.groupby("Data", sort=False):
        last_att = last_pac = last_aviso = None
        for i in grp.sort_values("_row_idx").index:
            att = df.at[i, "Atendimento"] if "Atendimento" in df.columns else None
            pac = df.at[i, "Paciente"] if "Paciente" in df.columns else None
            av  = df.at[i, "Aviso"] if "Aviso" in df.columns else None
            if pd.notna(att): last_att = att
            if pd.notna(pac): last_pac = pac
            if pd.notna(av):  last_aviso = av

            if "Prestador" in df.columns and pd.notna(df.at[i, "Prestador"]):
                if "Atendimento" in df.columns and pd.isna(att):
                    df.at[i, "Atendimento"] = last_att
                if "Paciente" in df.columns and pd.isna(pac):
                    df.at[i, "Paciente"] = last_pac
                if "Aviso" in df.columns and pd.isna(av):
                    df.at[i, "Aviso"] = last_aviso
    return df


# ------------------ Função principal (sem inferência por regras) ------------------

def process_uploaded_file(upload, prestadores_lista, selected_hospital: str):
    """
    Entrada:
      - upload: arquivo enviado pelo Streamlit (CSV/Excel/Texto)
      - prestadores_lista: lista de prestadores alvo (strings)
      - selected_hospital: nome do Hospital informado no app (aplicado a todas as linhas)

    Saída:
      DataFrame final com colunas:
        Hospital, Ano, Mes, Dia, Data, Atendimento, Paciente, Aviso, Convenio, Prestador, Quarto
    """
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
            # cria coluna vazia com alinhamento de índice
            df_in[c] = pd.NA

    # 2) Herança linha-a-linha por Data
    df = _herdar_por_data_ordem_original(df_in)

    # 3) Filtros dos prestadores (case-insensitive com normalização)
    def normalize(s): return (s or "").strip().upper()
    target = [normalize(p) for p in prestadores_lista]
    # Garante coluna Prestador
    if "Prestador" not in df.columns:
        df["Prestador"] = pd.NA
    df["Prestador_norm"] = df["Prestador"].astype(str).apply(normalize)
    df = df[df["Prestador_norm"].isin(target)].copy()

    # 4) Ordenação por tempo e deduplicação por (Data, Paciente, Prestador)
    # Garante coluna Hora_Inicio
    hora_inicio = df["Hora_Inicio"] if "Hora_Inicio" in df.columns else pd.Series("", index=df.index)
    data_series = df["Data"] if "Data" in df.columns else pd.Series("", index=df.index)

    df["start_key"] = pd.to_datetime(
        data_series.fillna("").astype(str) + " " + hora_inicio.fillna("").astype(str),
        format="%d/%m/%Y %H:%M",
        errors="coerce"
    )
    df = df.sort_values(["Data", "Paciente", "Prestador_norm", "start_key"]).copy()
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

    # 6) Seleção das colunas finais (organizado por ano/mês/dia)
    final_cols = ["Hospital", "Ano", "Mes", "Dia",
                  "Data", "Atendimento", "Paciente", "Aviso",
                  "Convenio", "Prestador", "Quarto"]
    for c in final_cols:
        if c not in df.columns:
            df[c] = pd.NA
    out = df[final_cols].copy()

    # Ordenação para retorno (ano/mês/dia)
    out = out.sort_values(["Hospital", "Ano", "Mes", "Dia", "Paciente", "Prestador"]).reset_index(drop=True)
    return out
