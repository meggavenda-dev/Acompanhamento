
# db.py
from __future__ import annotations

import os
import math
from typing import Optional
from sqlalchemy import create_engine, text

# ---------------- Configuração de caminho persistente ----------------
# Coloca o arquivo do banco dentro de ./data, com caminho absoluto, e garante a pasta.
MODULE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(MODULE_DIR, "data")
os.makedirs(DATA_DIR, exist_ok=True)

DB_PATH = os.path.join(DATA_DIR, "exemplo.db")
DB_URI = f"sqlite:///{DB_PATH}"

# Engine singleton simples para evitar recriar a cada chamada (mais estável em Streamlit).
_ENGINE = None
def get_engine():
    """
    Retorna um engine SQLAlchemy para SQLite no arquivo local em ./data/exemplo.db.
    """
    global _ENGINE
    if _ENGINE is None:
        # echo=False silencioso; future=True para SQLAlchemy 2.x
        _ENGINE = create_engine(DB_URI, future=True, echo=False)
    return _ENGINE


def init_db():
    """
    Cria a tabela e índices, caso não existam.
    - Tabela: pacientes_unicos_por_dia_prestador
    - Índice único: (Data, Paciente, Prestador, Hospital)
    - Índice auxiliar: (Hospital, Ano, Mes, Dia)
    """
    engine = get_engine()
    with engine.begin() as conn:
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

        # Índice adicional útil para consultas por hospital+período+prestador (opcional, mas ajuda desempenho)
        conn.execute(text("""
        CREATE INDEX IF NOT EXISTS idx_hospital_calendario_prestador
        ON pacientes_unicos_por_dia_prestador (Hospital, Ano, Mes, Dia, Prestador);
        """))


def _safe_int(val, default: int = 0) -> int:
    """
    Converte em int com segurança (None/NaN/strings vazias viram default).
    """
    if val is None:
        return default
    if isinstance(val, float):
        try:
            # math.isnan só aceita float; se não for, ignora
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
    """
    Converte em str com segurança (None/NaN viram default) e trim.
    """
    if val is None:
        return default
    if isinstance(val, float):
        try:
            if math.isnan(val):
                return default
        except Exception:
            pass
    return str(val).strip()


def upsert_dataframe(df):
    """
    UPSERT (INSERT OR REPLACE) por (Data, Paciente, Prestador, Hospital).

    GARANTIAS:
    - ❌ Bloqueia salvamento se existir 'Paciente' vazio (None / '' / espaços)
    - Converte tipos com segurança (int/str)
    - Normaliza trim para evitar duplicatas
    """
    if df is None or len(df) == 0:
        return

    # -------- Validação dura: Paciente obrigatório --------
    if "Paciente" not in df.columns:
        raise ValueError("Coluna 'Paciente' não encontrada no DataFrame.")

    blank_mask = df["Paciente"].astype(str).str.strip() == ""
    num_blank = int(blank_mask.sum())
    if num_blank > 0:
        raise ValueError(
            f"Existem {num_blank} registro(s) com 'Paciente' vazio. "
            "Preencha todos os nomes antes de salvar."
        )
    # ----------------------------------------------------

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
    """
    Lê todos os registros, ordenando por Hospital, Ano, Mes, Dia, Paciente, Prestador.
    """
    engine = get_engine()
    with engine.connect() as conn:
        rs = conn.execute(text("""
            SELECT Hospital, Ano, Mes, Dia, Data, Atendimento, Paciente, Aviso, Convenio, Prestador, Quarto
            FROM pacientes_unicos_por_dia_prestador
            ORDER BY Hospital, Ano, Mes, Dia, Paciente, Prestador
        """))
        rows = rs.fetchall()
    return rows


# ---------- Utilitários opcionais (podem ajudar no app) ----------

def read_by_hospital(hospital: str):
    """
    Lê registros de um hospital específico, ordenados por Ano/Mes/Dia/Paciente/Prestador.
    """
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
    """
    Lê registros filtrando por hospital e (opcionalmente) ano/mes.
    """
    engine = get_engine()
    where = ["Hospital = :h"]
    params = {"h": hospital}
    if ano is not None:
        where.append("Ano = :a")
        params["a"] = int(ano)
    if mes is not None:
        where.append("Mes = :m")
        params["m"] = int(mes)
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
    """
    Limpa completamente a tabela (use com cautela).
    """
    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM pacientes_unicos_por_dia_prestador"))


def count_all():
    """
    Retorna a quantidade de linhas na tabela.
    """
    engine = get_engine()
    with engine.connect() as conn:
        rs = conn.execute(text("SELECT COUNT(1) FROM pacientes_unicos_por_dia_prestador"))
        return rs.scalar_one()
