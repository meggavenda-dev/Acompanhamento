
# db.py
from __future__ import annotations

import os
import math
from typing import Optional, Tuple
import pandas as pd
from sqlalchemy import create_engine, text

# ---------------- Configuração de caminho persistente ----------------
MODULE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(MODULE_DIR, "data")
os.makedirs(DATA_DIR, exist_ok=True)

DB_PATH = os.path.join(DATA_DIR, "exemplo.db")
DB_URI = f"sqlite:///{DB_PATH}"

# Engine singleton
_ENGINE = None
def get_engine():
    global _ENGINE
    if _ENGINE is None:
        _ENGINE = create_engine(DB_URI, future=True, echo=False)
    return _ENGINE


def init_db():
    """
    Cria as tabelas e índices, caso não existam.
    """
    engine = get_engine()
    with engine.begin() as conn:
        # ---------------- Pacientes ----------------
        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS pacientes_unicos_por_dia_prestador (
            Hospital    TEXT,
            Ano         INTEGER,
            Mes         INTEGER,
            Dia         INTEGER,
            Data        TEXT,
            Atendimento TEXT,
            Paciente    TEXT,
            Aviso       TEXT,
            Convenio    TEXT,
            Prestador   TEXT,
            Quarto      TEXT
        );
        """))

        conn.execute(text("""
        CREATE UNIQUE INDEX IF NOT EXISTS idx_unicidade
        ON pacientes_unicos_por_dia_prestador (Data, Paciente, Prestador, Hospital);
        """))

        conn.execute(text("""
        CREATE INDEX IF NOT EXISTS idx_hospital_calendario
        ON pacientes_unicos_por_dia_prestador (Hospital, Ano, Mes, Dia);
        """))

        conn.execute(text("""
        CREATE INDEX IF NOT EXISTS idx_hospital_calendario_prestador
        ON pacientes_unicos_por_dia_prestador (Hospital, Ano, Mes, Dia, Prestador);
        """))

        # ---------------- Autorizações ----------------
        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS autorizacoes_pendencias (
            Unidade                     TEXT,
            Atendimento                 TEXT,
            Paciente                    TEXT,
            Profissional                TEXT,
            Data_Cirurgia               TEXT,
            Convenio                    TEXT,
            Tipo_Procedimento           TEXT,
            Observacoes                 TEXT,
            Guia_AMHPTISS               TEXT,
            Guia_AMHPTISS_Complemento   TEXT,
            Fatura                      TEXT,
            Status                      TEXT,
            NaturalKey                  TEXT UNIQUE,
            UltimaAtualizacao           TEXT
        );
        """))

        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_aut_por_status ON autorizacoes_pendencias (Status);"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_aut_por_conv   ON autorizacoes_pendencias (Convenio);"))


# ---------------- Converters/Helpers ----------------

def _safe_int(val, default: int = 0) -> int:
    if val is None:
        return default
    if isinstance(val, float):
        try:
            if math.isnan(val):
                return default
        except Exception:
            pass
    s = str(val).strip()
    if s == "":
        return default
    try:
        return int(float(s))
    except Exception:
        return default


def _safe_str(val, default: str = "") -> str:
    if val is None:
        return default
    if isinstance(val, float):
        try:
            if math.isnan(val):
                return default
        except Exception:
            pass
    return str(val).strip()


# ---------------- CRUD - Pacientes ----------------

def upsert_dataframe(df):
    if df is None or len(df) == 0:
        return

    if "Paciente" not in df.columns:
        raise ValueError("Coluna 'Paciente' não encontrada no DataFrame.")

    blank_mask = df["Paciente"].astype(str).str.strip() == ""
    num_blank = int(blank_mask.sum())
    if num_blank > 0:
        raise ValueError(f"Existem {num_blank} registro(s) com 'Paciente' vazio. Preencha todos os nomes antes de salvar.")

    engine = get_engine()
    with engine.begin() as conn:
        for _, row in df.iterrows():
            conn.execute(text("""
                INSERT OR REPLACE INTO pacientes_unicos_por_dia_prestador
                (Hospital, Ano, Mes, Dia, Data, Atendimento, Paciente, Aviso, Convenio, Prestador, Quarto)
                VALUES (:Hospital, :Ano, :Mes, :Dia, :Data, :Atendimento, :Paciente, :Aviso, :Convenio, :Prestador, :Quarto)
            """), {
                "Hospital":    _safe_str(row.get("Hospital", "")),
                "Ano":         _safe_int(row.get("Ano", 0)),
                "Mes":         _safe_int(row.get("Mes", 0)),
                "Dia":         _safe_int(row.get("Dia", 0)),
                "Data":        _safe_str(row.get("Data", "")),
                "Atendimento": _safe_str(row.get("Atendimento", "")),
                "Paciente":    _safe_str(row.get("Paciente", "")),
                "Aviso":       _safe_str(row.get("Aviso", "")),
                "Convenio":    _safe_str(row.get("Convenio", "")),
                "Prestador":   _safe_str(row.get("Prestador", "")),
                "Quarto":      _safe_str(row.get("Quarto", "")),
            })


def read_all():
    engine = get_engine()
    with engine.connect() as conn:
        rs = conn.execute(text("""
            SELECT Hospital, Ano, Mes, Dia, Data, Atendimento, Paciente, Aviso, Convenio, Prestador, Quarto
            FROM pacientes_unicos_por_dia_prestador
            ORDER BY Hospital, Ano, Mes, Dia, Paciente, Prestador
        """))
        rows = rs.fetchall()
    return rows


def read_by_hospital(hospital: str):
    engine = get_engine()
    with engine.connect() as conn:
        rs = conn.execute(text("""
            SELECT Hospital, Ano, Mes, Dia, Data, Atendimento, Paciente, Aviso, Convenio, Prestador, Quarto
            FROM pacientes_unicos_por_dia_prestador
            WHERE Hospital = :h
            ORDER BY Ano, Mes, Dia, Paciente, Prestador
        """), {"h": hospital})
        return rs.fetchall()


def read_by_hospital_period(hospital: str, ano: Optional[int] = None, mes: Optional[int] = None):
    engine = get_engine()
    where = ["Hospital = :h"]
    params = {"h": hospital}
    if ano is not None:
        where.append("Ano = :a"); params["a"] = int(ano)
    if mes is not None:
        where.append("Mes = :m"); params["m"] = int(mes)
    sql = f"""
        SELECT Hospital, Ano, Mes, Dia, Data, Atendimento, Paciente, Aviso, Convenio, Prestador, Quarto
        FROM pacientes_unicos_por_dia_prestador
        WHERE {' AND '.join(where)}
        ORDER BY Ano, Mes, Dia, Paciente, Prestador
    """
    with engine.connect() as conn:
        rs = conn.execute(text(sql), params)
        return rs.fetchall()


def delete_all():
    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM pacientes_unicos_por_dia_prestador"))


def count_all():
    engine = get_engine()
    with engine.connect() as conn:
        rs = conn.execute(text("SELECT COUNT(1) FROM pacientes_unicos_por_dia_prestador"))
        return rs.scalar_one()


# ---------------- CRUD - Autorizações ----------------

def _mk_aut_key_from_patient(patient_row: dict) -> str:
    """
    Gera a chave natural de autorizações com base nos campos de pacientes.
    Preferência: Atendimento. Senão, fallback Paciente|Data|Prestador|Hospital.
    """
    att = _safe_str(patient_row.get("Atendimento", ""))
    if att:
        return f"ATT:{att}"
    return "FALLBACK:" + "|".join([
        _safe_str(patient_row.get("Paciente", "")),
        _safe_str(patient_row.get("Data", "")),
        _safe_str(patient_row.get("Prestador", "")),
        _safe_str(patient_row.get("Hospital", "")),
    ])


def upsert_autorizacoes(df: pd.DataFrame):
    """
    UPSERT na tabela de autorizações baseado em NaturalKey.
    """
    if df is None or len(df) == 0:
        return
    now_iso = pd.Timestamp.utcnow().isoformat(timespec="seconds")

    needed = [
        "Unidade", "Atendimento", "Paciente", "Profissional", "Data_Cirurgia",
        "Convenio", "Tipo_Procedimento", "Observacoes",
        "Guia_AMHPTISS", "Guia_AMHPTISS_Complemento", "Fatura", "Status"
    ]
    for c in needed:
        if c not in df.columns:
            df[c] = pd.NA

    engine = get_engine()
    with engine.begin() as conn:
        for _, row in df.iterrows():
            payload = {
                "Unidade":                   _safe_str(row.get("Unidade", "")),
                "Atendimento":               _safe_str(row.get("Atendimento", "")),
                "Paciente":                  _safe_str(row.get("Paciente", "")),
                "Profissional":              _safe_str(row.get("Profissional", "")),
                "Data_Cirurgia":             _safe_str(row.get("Data_Cirurgia", "")),
                "Convenio":                  _safe_str(row.get("Convenio", "")),
                "Tipo_Procedimento":         _safe_str(row.get("Tipo_Procedimento", "")),
                "Observacoes":               _safe_str(row.get("Observacoes", "")),
                "Guia_AMHPTISS":             _safe_str(row.get("Guia_AMHPTISS", "")),
                "Guia_AMHPTISS_Complemento": _safe_str(row.get("Guia_AMHPTISS_Complemento", "")),
                "Fatura":                    _safe_str(row.get("Fatura", "")),
                "Status":                    _safe_str(row.get("Status", "")),
            }
            # Para upsert, geramos a mesma NaturalKey usada na sincronização
            nk = _mk_aut_key_from_patient({
                "Atendimento": payload["Atendimento"],
                "Paciente": payload["Paciente"],
                "Data": payload["Data_Cirurgia"],
                "Prestador": payload["Profissional"],
                "Hospital": payload["Unidade"],
            })

            # UPSERT (preserva a edição do usuário)
            conn.execute(text("""
                INSERT INTO autorizacoes_pendencias
                (Unidade, Atendimento, Paciente, Profissional, Data_Cirurgia, Convenio, Tipo_Procedimento,
                 Observacoes, Guia_AMHPTISS, Guia_AMHPTISS_Complemento, Fatura, Status, NaturalKey, UltimaAtualizacao)
                VALUES (:Unidade, :Atendimento, :Paciente, :Profissional, :Data_Cirurgia, :Convenio, :Tipo_Procedimento,
                        :Observacoes, :Guia_AMHPTISS, :Guia_AMHPTISS_Complemento, :Fatura, :Status, :NaturalKey, :UltimaAtualizacao)
                ON CONFLICT(NaturalKey) DO UPDATE SET
                    Unidade                     = excluded.Unidade,
                    Atendimento                 = excluded.Atendimento,
                    Paciente                    = excluded.Paciente,
                    Profissional                = excluded.Profissional,
                    Data_Cirurgia               = excluded.Data_Cirurgia,
                    Convenio                    = excluded.Convenio,
                    Tipo_Procedimento           = excluded.Tipo_Procedimento,
                    Observacoes                 = excluded.Observacoes,
                    Guia_AMHPTISS               = excluded.Guia_AMHPTISS,
                    Guia_AMHPTISS_Complemento   = excluded.Guia_AMHPTISS_Complemento,
                    Fatura                      = excluded.Fatura,
                    Status                      = excluded.Status,
                    UltimaAtualizacao           = excluded.UltimaAtualizacao
            """), {**payload, "NaturalKey": nk, "UltimaAtualizacao": now_iso})


def read_autorizacoes():
    engine = get_engine()
    with engine.connect() as conn:
        rs = conn.execute(text("""
            SELECT Unidade, Atendimento, Paciente, Profissional, Data_Cirurgia, Convenio, Tipo_Procedimento,
                   Observacoes, Guia_AMHPTISS, Guia_AMHPTISS_Complemento, Fatura, Status, UltimaAtualizacao
            FROM autorizacoes_pendencias
            ORDER BY Status, Convenio, Paciente
        """))
        return rs.fetchall()


def count_autorizacoes():
    engine = get_engine()
    with engine.connect() as conn:
        return conn.execute(text("SELECT COUNT(1) FROM autorizacoes_pendencias")).scalar_one()


def join_aut_por_atendimento():
    """
    Join Autorizações × Pacientes pelo Atendimento (LEFT JOIN).
    """
    engine = get_engine()
    with engine.connect() as conn:
        rs = conn.execute(text("""
            SELECT a.Unidade, a.Atendimento, a.Paciente AS PacienteAut, a.Profissional AS ProfAut,
                   a.Data_Cirurgia, a.Convenio, a.Status,
                   p.Hospital, p.Data, p.Paciente AS PacienteDB, p.Prestador AS PrestDB
            FROM autorizacoes_pendencias a
            LEFT JOIN pacientes_unicos_por_dia_prestador p
              ON (a.Atendimento = p.Atendimento)
            ORDER BY a.Status, a.Convenio, a.Paciente
        """))
        return rs.fetchall()


# ---------------- Sincronização Autorizações <- Pacientes ----------------

def sync_autorizacoes_from_pacientes(default_status: str = "EM ANDAMENTO") -> Tuple[int, int]:
    """
    Espelha/atualiza a tabela de autorizações com base na tabela de pacientes.
    Regra de chave natural: ATT:<Atendimento> ou FALLBACK:<Paciente|Data|Prestador|Hospital>.

    - Não sobrescreve Observacoes/Status/Guias/Fatura em registros já existentes
      (faz UPDATE dos campos espelhados e INSERT OR IGNORE para novos).
    - Retorna (novos_insertados, atualizados_campos_espelho).
    """
    engine = get_engine()
    now_iso = pd.Timestamp.utcnow().isoformat(timespec="seconds")
    novos, atualizados = 0, 0

    with engine.begin() as conn:
        rs = conn.execute(text("""
            SELECT Hospital, Data, Atendimento, Paciente, Convenio, Prestador, Quarto
            FROM pacientes_unicos_por_dia_prestador
        """))
        rows = rs.fetchall()

        for (Hospital, Data, Atendimento, Paciente, Convenio, Prestador, Quarto) in rows:
            patient = {
                "Hospital": _safe_str(Hospital, ""),
                "Data": _safe_str(Data, ""),
                "Atendimento": _safe_str(Atendimento, ""),
                "Paciente": _safe_str(Paciente, ""),
                "Convenio": _safe_str(Convenio, ""),
                "Prestador": _safe_str(Prestador, ""),
            }
            nk = _mk_aut_key_from_patient(patient)

            # Atualiza campos "espelho" se já existir (preserva edições do usuário)
            upd = conn.execute(text("""
                UPDATE autorizacoes_pendencias
                   SET Unidade = :Unidade,
                       Atendimento = :Atendimento,
                       Paciente = :Paciente,
                       Profissional = :Profissional,
                       Data_Cirurgia = :Data_Cirurgia,
                       Convenio = :Convenio,
                       UltimaAtualizacao = :UltimaAtualizacao
                 WHERE NaturalKey = :NaturalKey
            """), {
                "Unidade": patient["Hospital"],
                "Atendimento": patient["Atendimento"],
                "Paciente": patient["Paciente"],
                "Profissional": patient["Prestador"],
                "Data_Cirurgia": patient["Data"],
                "Convenio": patient["Convenio"],
                "UltimaAtualizacao": now_iso,
                "NaturalKey": nk
            })
            atualizados += upd.rowcount

            # Insere se não existir (campos editáveis iniciam em branco/default)
            ins = conn.execute(text("""
                INSERT OR IGNORE INTO autorizacoes_pendencias
                (Unidade, Atendimento, Paciente, Profissional, Data_Cirurgia, Convenio,
                 Tipo_Procedimento, Observacoes, Guia_AMHPTISS, Guia_AMHPTISS_Complemento, Fatura,
                 Status, NaturalKey, UltimaAtualizacao)
                VALUES (:Unidade, :Atendimento, :Paciente, :Profissional, :Data_Cirurgia, :Convenio,
                        '', '', '', '', '', :Status, :NaturalKey, :UltimaAtualizacao)
            """), {
                "Unidade": patient["Hospital"],
                "Atendimento": patient["Atendimento"],
                "Paciente": patient["Paciente"],
                "Profissional": patient["Prestador"],
                "Data_Cirurgia": patient["Data"],
                "Convenio": patient["Convenio"],
                "Status": default_status,
                "NaturalKey": nk,
                "UltimaAtualizacao": now_iso
            })
            novos += ins.rowcount

    return novos, atualizados
