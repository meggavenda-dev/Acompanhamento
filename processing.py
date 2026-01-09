
# processing.py
import io
import csv
import re
import pandas as pd
from dateutil import parser as dtparser

TIME_RE = re.compile(r"^\d{1,2}:\d{2}$")
DATE_RE = re.compile(r"\b(\d{2}/\d{2}/\d{4})\b")
HAS_LETTER_RE = re.compile(r"[A-Za-zÁÉÍÓÚÃÕÇáéíóúãõç]")
SECTION_KEYWORDS = ["CENTRO CIRURGICO", "HEMODINAMICA", "CENTRO OBSTETRICO"]

EXPECTED_COLS = [
    "Centro","Data","Atendimento","Paciente","Aviso",
    "Hora_Inicio","Hora_Fim","Cirurgia","Convenio","Prestador",
    "Anestesista","Tipo_Anestesia","Quarto"
]

def _parse_raw_text_to_rows(text: str):
    """
    Parser robusto para CSV 'bruto' (como o seu 12.2025.csv),
    realizando leitura linha a linha em ordem original e extraindo campos.
    """
    rows = []
    current_section = None
    current_date_str = None
    ctx = {'atendimento': None, 'paciente': None, 'aviso': None,
           'hora_inicio': None, 'hora_fim': None, 'quarto': None}

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
        if "Centro Cirurgico" in line:
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
                "Quarto": quarto
            })
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
                    "Quarto": quarto
                })

    return pd.DataFrame(rows)


def _herdar_por_data_ordem_original(df: pd.DataFrame) -> pd.DataFrame:
    """
    Herança linha-a-linha por Data, preservando ordem original do arquivo.
    Quando há Prestador e (Atendimento/Paciente/Aviso) vazios, herda do último acima.
    """
    df = df.copy()
    df.replace({"": pd.NA}, inplace=True)
    if "Data" in df.columns:
        df["Data"] = df["Data"].ffill().bfill()

    df["_row_idx"] = range(len(df))
    for data_val, grp in df.groupby("Data", sort=False):
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
    return df.drop(columns=["_row_idx"])


def process_uploaded_file(upload, prestadores_lista):
    """
    Entrada: arquivo do Streamlit (BytesIO) e lista de prestadores.
    Saída: DataFrame final pronto (Data, Atendimento, Paciente, Aviso, Convenio, Prestador, Quarto).
    """
    name = upload.name.lower()
    # 1) Ler arquivo
    if name.endswith(".xlsx") or name.endswith(".xls"):
        df_in = pd.read_excel(upload, engine="openpyxl")
    elif name.endswith(".csv"):
        # tenta ler como CSV estruturado; se falhar, cai para parser bruto
        try:
            df_in = pd.read_csv(upload, sep=",", encoding="utf-8")
            # Se não tem as colunas esperadas, fazer parsing bruto
            if len(set(EXPECTED_COLS) & set(df_in.columns)) < 6:
                upload.seek(0)
                text = upload.read().decode("utf-8", errors="ignore")
                df_in = _parse_raw_text_to_rows(text)
        except Exception:
            upload.seek(0)
            text = upload.read().decode("utf-8", errors="ignore")
            df_in = _parse_raw_text_to_rows(text)
    else:
        # tenta parsear como texto bruto
        text = upload.read().decode("utf-8", errors="ignore")
        df_in = _parse_raw_text_to_rows(text)

    # 2) Herança linha-a-linha por Data
    df = _herdar_por_data_ordem_original(df_in)

    # 3) Filtros dos prestadores (case-insensitive com normalização)
    def normalize(s): return (s or "").strip().upper()
    target = [normalize(p) for p in prestadores_lista]
    df["Prestador_norm"] = df["Prestador"].apply(lambda s: normalize(s))
    df = df[df["Prestador_norm"].isin(target)].copy()

    # 4) Ordenação por tempo e deduplicação por (Data, Paciente, Prestador)
    df["start_key"] = pd.to_datetime(
        df["Data"].fillna("") + " " + df.get("Hora_Inicio", "").fillna(""),
        format="%d/%m/%Y %H:%M",
        errors="coerce"
    )
    df = df.sort_values(["Data","Paciente","Prestador_norm","start_key"]).copy()
    df = df.drop_duplicates(subset=["Data","Paciente","Prestador_norm"], keep="first")

    # 5) Seleção das colunas finais
    final_cols = ["Data","Atendimento","Paciente","Aviso","Convenio","Prestador","Quarto"]
    for c in final_cols:
        if c not in df.columns:
            df[c] = pd.NA
    out = df[final_cols].copy()
    return out

