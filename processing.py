import io
import csv
import re
import unicodedata
import numpy as np
import pandas as pd

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
    "CATETER", "CERVICOTOMIA", "TIREOIDECTOMIA", "LINFADENECTOMIA", 
    "RECONSTRUÇÃO", "RETOSSIGMOIDECTOMIA", "PLEUROSCOPIA",
}

# =========================
# Funções Auxiliares
# =========================

def _is_probably_procedure_token(tok) -> bool:
    if tok is None or pd.isna(tok): return False
    T = str(tok).upper().strip()
    if any(h in T for h in PROCEDURE_HINTS): return True
    if any(c in T for c in [",", "/", "(", ")", "%", "  ", "-"]): return True
    if len(T) > 50: return True
    return False

def _strip_accents(s: str) -> str:
    if s is None or pd.isna(s): return ""
    s = str(s)
    return "".join(ch for ch in unicodedata.normalize("NFKD", s) if not unicodedata.combining(ch))

def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty: return df
    df.columns = [str(c).replace("\ufeff", "").strip() for c in df.columns]
    col_map = {
        "Convênio": "Convenio", "Convênio*": "Convenio",
        "Tipo Anestesia": "Tipo_Anestesia", "Hora Inicio": "Hora_Inicio",
        "Hora Início": "Hora_Inicio", "Hora Fim": "Hora_Fim",
        "Centro Cirurgico": "Centro", "Centro Cirúrgico": "Centro",
    }
    df.rename(columns=col_map, inplace=True)
    return df

# =========================
# Parser de texto bruto
# =========================

def _parse_raw_text_to_rows(text: str) -> pd.DataFrame:
    rows = []
    current_section = None
    current_date_str = None
    ctx = {"hora_inicio": None}
    row_idx = 0

    for line in text.splitlines():
        if "Data de Realização" in line or "Data de Realiza" in line:
            m_date = DATE_RE.search(line)
            if m_date: current_date_str = m_date.group(1)

        try:
            tokens = next(csv.reader([line]))
            tokens = [t.strip() for t in tokens if t is not None]
        except: continue
        
        if not tokens: continue

        if "Centro Cirurgico" in line or "Centro Cirúrgico" in line:
            current_section = next((kw for kw in SECTION_KEYWORDS if kw in line), None)
            ctx = {"hora_inicio": None}
            continue

        header_phrases = ["Hora", "Atendimento", "Paciente", "Convênio", "Prestador"]
        if any(h in line for h in header_phrases): continue

        time_idxs = [i for i, t in enumerate(tokens) if TIME_RE.match(t)]
        if time_idxs:
            h0 = time_idxs[0]
            h1 = h0 + 1 if (h0 + 1 < len(tokens) and TIME_RE.match(tokens[h0+1])) else None
            hora_inicio, hora_fim = tokens[h0], (tokens[h1] if h1 else None)
            aviso = tokens[h0-1] if (h0-1 >= 0 and re.fullmatch(r"\d{3,}", tokens[h0-1])) else None
            atendimento, paciente = None, None

            for i, t in enumerate(tokens):
                if re.fullmatch(r"\d{7,10}", t):
                    atendimento = t
                    upper_bound = (h0 - 2) if h0 else len(tokens)-1
                    for j in range(i+1, upper_bound+1):
                        if j < len(tokens) and HAS_LETTER_RE.search(tokens[j]) and not TIME_RE.match(tokens[j]) and not _is_probably_procedure_token(tokens[j]):
                            paciente = tokens[j]
                            break
                    break

            base_idx = h1 if h1 else h0
            cirurgia, convenio = (tokens[base_idx + i] if base_idx + i < len(tokens) else None for i in [1, 2])
            
            p_cand = tokens[base_idx + 3] if base_idx + 3 < len(tokens) else None
            if p_cand and DATE_RE.search(p_cand):
                prestador = tokens[base_idx + 4] if base_idx + 4 < len(tokens) else p_cand
                anest, tipo, quarto = (tokens[base_idx+i] if base_idx+i < len(tokens) else None for i in [5,6,7])
            else:
                prestador = p_cand
                anest, tipo, quarto = (tokens[base_idx+i] if base_idx+i < len(tokens) else None for i in [4,5,6])

            rows.append({
                "Centro": current_section, "Data": current_date_str, "Atendimento": atendimento,
                "Paciente": paciente, "Aviso": aviso, "Hora_Inicio": hora_inicio, "Hora_Fim": hora_fim,
                "Cirurgia": cirurgia, "Convenio": convenio, "Prestador": prestador,
                "Anestesista": anest, "Tipo_Anestesia": tipo, "Quarto": quarto, "_row_idx": row_idx
            })
            ctx["hora_inicio"] = hora_inicio
            row_idx += 1
            continue

        if current_section and any(t for t in tokens):
            nonempty = [t for t in tokens if t]
            if len(nonempty) >= 4:
                # Linha de procedimento extra: deixa campos de paciente vazios para herança controlada
                rows.append({
                    "Centro": current_section, "Data": current_date_str, "Atendimento": None,
                    "Paciente": None, "Aviso": None, "Hora_Inicio": ctx["hora_inicio"],
                    "Cirurgia": nonempty[0], "Convenio": nonempty[-5] if len(nonempty)>=5 else None,
                    "Prestador": nonempty[-4], "Anestesista": nonempty[-3], "Tipo_Anestesia": nonempty[-2],
                    "Quarto": nonempty[-1], "_row_idx": row_idx
                })
                row_idx += 1
    return pd.DataFrame(rows)

# ===================================================
# Herança - TRAVA POR BLOCO E POR MÉDICO
# ===================================================

def _herdar_por_data_ordem_original(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty: return df
    df = df.copy()
    df.replace({"": pd.NA}, inplace=True)
    df["Data"] = df["Data"].ffill().bfill()

    if "_row_idx" not in df.columns:
        df["_row_idx"] = range(len(df))

    for _, grp in df.groupby("Data", sort=False):
        last_att, last_pac, last_av = pd.NA, pd.NA, pd.NA
        # Conjunto de médicos que já apareceram NESTE bloco cirúrgico
        medicos_no_bloco = set()
        
        for i in grp.sort_values("_row_idx").index:
            curr_att = df.at[i, "Atendimento"]
            curr_pac = df.at[i, "Paciente"]
            curr_av  = df.at[i, "Aviso"]
            curr_prest_raw = df.at[i, "Prestador"]
            curr_prest = str(curr_prest_raw).strip().upper() if pd.notna(curr_prest_raw) else ""

            # Detecta se a linha traz dados nativos (nova cirurgia ou declaração principal)
            tem_dados_nativos = pd.notna(curr_att) or pd.notna(curr_pac) or pd.notna(curr_av)

            if tem_dados_nativos:
                # Se mudou o Atendimento ou o Aviso, resetamos a lista de médicos (novo bloco)
                if (curr_att != last_att) or (curr_av != last_av):
                    medicos_no_bloco = set()
                
                # Atualiza memória e marca este médico como "já possui dados"
                last_att, last_pac, last_av = curr_att, curr_pac, curr_av
                if curr_prest != "":
                    medicos_no_bloco.add(curr_prest)
            else:
                # Linha sem dados: Herança controlada
                # REGRA: Só herda se o médico ATUAL ainda não apareceu neste bloco cirúrgico
                if curr_prest != "" and curr_prest not in medicos_no_bloco:
                    df.at[i, "Atendimento"] = last_att
                    df.at[i, "Paciente"]    = last_pac
                    df.at[i, "Aviso"]       = last_av
                    # Marca que este médico já recebeu os dados do bloco
                    medicos_no_bloco.add(curr_prest)
                
    return df

# =========================
# Pipeline principal
# =========================

def process_uploaded_file(upload, prestadores_lista, selected_hospital: str):
    name = upload.name.lower()
    if name.endswith(".csv"):
        try:
            df_in = pd.read_csv(upload, sep=",", encoding="utf-8")
            if len(set(EXPECTED_COLS) & set(df_in.columns)) < 6:
                upload.seek(0); text = upload.read().decode("utf-8", errors="ignore")
                df_in = _parse_raw_text_to_rows(text)
        except:
            upload.seek(0); text = upload.read().decode("utf-8", errors="ignore")
            df_in = _parse_raw_text_to_rows(text)
    else:
        text = upload.read().decode("utf-8", errors="ignore"); df_in = _parse_raw_text_to_rows(text)

    df_in = _normalize_columns(df_in)
    if "_row_idx" not in df_in.columns: df_in["_row_idx"] = range(len(df_in))

    # Aplica herança seletiva (bloqueia repetição para o mesmo médico)
    df = _herdar_por_data_ordem_original(df_in)

    # Filtro final pelos prestadores escolhidos
    target = [_strip_accents(p).strip().upper() for p in prestadores_lista]
    df["Prestador_norm"] = df["Prestador"].apply(lambda x: _strip_accents(x).strip().upper())
    df = df[df["Prestador_norm"].isin(target)].copy()

    dt = pd.to_datetime(df["Data"], format="%d/%m/%Y", errors="coerce")
    df["Hospital"], df["Ano"], df["Mes"], df["Dia"] = selected_hospital, dt.dt.year, dt.dt.month, dt.dt.day

    cols_to_return = ["Hospital", "Ano", "Mes", "Dia", "Data", "Atendimento", "Paciente", "Aviso", "Convenio", "Prestador", "Quarto"]
    return df.sort_values(["Ano", "Mes", "Dia", "_row_idx"])[cols_to_return].reset_index(drop=True)
