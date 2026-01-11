
# db.py
from __future__ import annotations

import os
import math
from typing import Optional, List, Dict, Any
from sqlalchemy import create_engine, text

# ---------------- Configuração de caminho persistente ----------------
MODULE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(MODULE_DIR, "data")
os.makedirs(DATA_DIR, exist_ok=True)

DB_PATH = os.path.join(DATA_DIR, "exemplo.db")
DB_URI = f"sqlite:///{DB_PATH}"

_ENGINE = None
def get_engine():
    global _ENGINE
    if _ENGINE is None:
        _ENGINE = create_engine(DB_URI, future=True, echo=False)
    return _ENGINE


def init_db():
    """
    Cria/atualiza estrutura do banco:
    - pacientes_unicos_por_dia_prestador (já existente)
    - procedimento_tipos (NOVO)
    - cirurgia_situacoes (NOVO)
    - cirurgias (NOVO)
    """
    engine = get_engine()
    with engine.begin() as conn:
        # ---- Tabela original (mantida) ----
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

        # ---- Catálogo: Tipos de Procedimento ----
        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS procedimento_tipos (
            id     INTEGER PRIMARY KEY AUTOINCREMENT,
            nome   TEXT NOT NULL UNIQUE,
            ativo  INTEGER NOT NULL DEFAULT 1,
            ordem  INTEGER DEFAULT 0
        );
        """))
        conn.execute(text("""
        CREATE INDEX IF NOT EXISTS idx_proc_tipos_ativo
        ON procedimento_tipos (ativo, ordem, nome);
        """))

        # ---- Catálogo: Situações de Cirurgia ----
        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS cirurgia_situacoes (
            id     INTEGER PRIMARY KEY AUTOINCREMENT,
            nome   TEXT NOT NULL UNIQUE,
            ativo  INTEGER NOT NULL DEFAULT 1,
            ordem  INTEGER DEFAULT 0
        );
        """))
        conn.execute(text("""
        CREATE INDEX IF NOT EXISTS idx_cir_sit_ativo
        ON cirurgia_situacoes (ativo, ordem, nome);
        """))

        # ---- Registro de Cirurgias ----
        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS cirurgias (
            id       INTEGER PRIMARY KEY AUTOINCREMENT,
            Hospital TEXT NOT NULL,
            Atendimento TEXT,
            Paciente TEXT,
            Prestador TEXT,
            Data_Cirurgia TEXT,         -- formato livre (ex.: dd/MM/yyyy); pode ser 'YYYY-MM-DD' também
            Convenio TEXT,
            Procedimento_Tipo_ID INTEGER,
            Situacao_ID INTEGER,
            Guia_AMHPTISS TEXT,
            Guia_AMHPTISS_Complemento TEXT,
            Fatura TEXT,
            Observacoes TEXT,
            created_at TEXT DEFAULT (datetime('now')),
            updated_at TEXT,
            FOREIGN KEY (Procedimento_Tipo_ID) REFERENCES procedimento_tipos(id),
            FOREIGN KEY (Situacao_ID) REFERENCES cirurgia_situacoes(id)
        );
        """))
        # Índices úteis
        conn.execute(text("""
        CREATE INDEX IF NOT EXISTS idx_cirurgias_hosp_data
        ON cirurgias (Hospital, Data_Cirurgia);
        """))
        conn.execute(text("""
        CREATE INDEX IF NOT EXISTS idx_cirurgias_atendimento
        ON cirurgias (Atendimento);
        """))
        conn.execute(text("""
        CREATE INDEX IF NOT EXISTS idx_cirurgias_paciente
        ON cirurgias (Paciente);
        """))
        # Evita duplicar mesma cirurgia (ajuste se quiser outra regra)
        conn.execute(text("""
        CREATE UNIQUE INDEX IF NOT EXISTS idx_cirurgia_unica
        ON cirurgias (Hospital, Atendimento, Prestador, Data_Cirurgia);
        """))


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


# ---------------- UPSERT original ----------------
def upsert_dataframe(df):
    if df is None or len(df) == 0:
        return
    if "Paciente" not in df.columns:
        raise ValueError("Coluna 'Paciente' não encontrada no DataFrame.")
    blank_mask = df["Paciente"].astype(str).str.strip() == ""
    num_blank = int(blank_mask.sum())
    if num_blank > 0:
        raise ValueError(
            f"Existem {num_blank} registro(s) com 'Paciente' vazio. "
            "Preencha todos os nomes antes de salvar."
        )

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


# ---------------- Catálogos (Tipos / Situações) ----------------

def upsert_procedimento_tipo(nome: str, ativo: int = 1, ordem: int = 0) -> int:
    """
    Cria ou atualiza (por nome único). Retorna id.
    """
    engine = get_engine()
    with engine.begin() as conn:
        # tenta pegar existente
        rs = conn.execute(text("SELECT id FROM procedimento_tipos WHERE nome = :n"), {"n": nome.strip()})
        row = rs.fetchone()
        if row:
            conn.execute(text("""
                UPDATE procedimento_tipos SET ativo = :a, ordem = :o WHERE id = :id
            """), {"a": int(ativo), "o": int(ordem), "id": row[0]})
            return int(row[0])
        else:
            rs2 = conn.execute(text("""
                INSERT INTO procedimento_tipos (nome, ativo, ordem) VALUES (:n, :a, :o)
            """), {"n": nome.strip(), "a": int(ativo), "o": int(ordem)})
            return int(rs2.lastrowid)


def list_procedimento_tipos(only_active: bool = True):
    engine = get_engine()
    with engine.connect() as conn:
        if only_active:
            rs = conn.execute(text("SELECT id, nome, ativo, ordem FROM procedimento_tipos WHERE ativo = 1 ORDER BY ordem, nome"))
        else:
            rs = conn.execute(text("SELECT id, nome, ativo, ordem FROM procedimento_tipos ORDER BY ativo DESC, ordem, nome"))
        return rs.fetchall()


def set_procedimento_tipo_status(id_: int, ativo: int):
    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(text("UPDATE procedimento_tipos SET ativo = :a WHERE id = :id"), {"a": int(ativo), "id": int(id_)})


def upsert_cirurgia_situacao(nome: str, ativo: int = 1, ordem: int = 0) -> int:
    engine = get_engine()
    with engine.begin() as conn:
        rs = conn.execute(text("SELECT id FROM cirurgia_situacoes WHERE nome = :n"), {"n": nome.strip()})
        row = rs.fetchone()
        if row:
            conn.execute(text("""
                UPDATE cirurgia_situacoes SET ativo = :a, ordem = :o WHERE id = :id
            """), {"a": int(ativo), "o": int(ordem), "id": row[0]})
            return int(row[0])
        else:
            rs2 = conn.execute(text("""
                INSERT INTO cirurgia_situacoes (nome, ativo, ordem) VALUES (:n, :a, :o)
            """), {"n": nome.strip(), "a": int(ativo), "o": int(ordem)})
            return int(rs2.lastrowid)


def list_cirurgia_situacoes(only_active: bool = True):
    engine = get_engine()
    with engine.connect() as conn:
        if only_active:
            rs = conn.execute(text("SELECT id, nome, ativo, ordem FROM cirurgia_situacoes WHERE ativo = 1 ORDER BY ordem, nome"))
        else:
            rs = conn.execute(text("SELECT id, nome, ativo, ordem FROM cirurgia_situacoes ORDER BY ativo DESC, ordem, nome"))
        return rs.fetchall()


def set_cirurgia_situacao_status(id_: int, ativo: int):
    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(text("UPDATE cirurgia_situacoes SET ativo = :a WHERE id = :id"), {"a": int(ativo), "id": int(id_)})


# ---------------- Cirurgias (CRUD) ----------------

def insert_or_update_cirurgia(payload: Dict[str, Any]) -> int:
    """
    UPSERT por (Hospital, Atendimento, Prestador, Data_Cirurgia).
    Retorna id da cirurgia.
    """
    engine = get_engine()
    with engine.begin() as conn:
        # Tenta achar existente (segue a unique key)
        rs = conn.execute(text("""
            SELECT id FROM cirurgias
            WHERE Hospital = :h AND Atendimento = :att AND Prestador = :p AND Data_Cirurgia = :d
        """), {
            "h": _safe_str(payload.get("Hospital")),
            "att": _safe_str(payload.get("Atendimento")),
            "p": _safe_str(payload.get("Prestador")),
            "d": _safe_str(payload.get("Data_Cirurgia"))
        })
        row = rs.fetchone()

        params = {
            "Hospital": _safe_str(payload.get("Hospital")),
            "Atendimento": _safe_str(payload.get("Atendimento")),
            "Paciente": _safe_str(payload.get("Paciente")),
            "Prestador": _safe_str(payload.get("Prestador")),
            "Data_Cirurgia": _safe_str(payload.get("Data_Cirurgia")),
            "Convenio": _safe_str(payload.get("Convenio")),
            "Procedimento_Tipo_ID": payload.get("Procedimento_Tipo_ID"),
            "Situacao_ID": payload.get("Situacao_ID"),
            "Guia_AMHPTISS": _safe_str(payload.get("Guia_AMHPTISS")),
            "Guia_AMHPTISS_Complemento": _safe_str(payload.get("Guia_AMHPTISS_Complemento")),
            "Fatura": _safe_str(payload.get("Fatura")),
            "Observacoes": _safe_str(payload.get("Observacoes")),
        }

        if row:
            conn.execute(text("""
                UPDATE cirurgias SET
                    Paciente = :Paciente,
                    Convenio = :Convenio,
                    Procedimento_Tipo_ID = :Procedimento_Tipo_ID,
                    Situacao_ID = :Situacao_ID,
                    Guia_AMHPTISS = :Guia_AMHPTISS,
                    Guia_AMHPTISS_Complemento = :Guia_AMHPTISS_Complemento,
                    Fatura = :Fatura,
                    Observacoes = :Observacoes,
                    updated_at = datetime('now')
                WHERE id = :id
            """), {**params, "id": int(row[0])})
            return int(row[0])
        else:
            rs2 = conn.execute(text("""
                INSERT INTO cirurgias (
                    Hospital, Atendimento, Paciente, Prestador, Data_Cirurgia,
                    Convenio, Procedimento_Tipo_ID, Situacao_ID,
                    Guia_AMHPTISS, Guia_AMHPTISS_Complemento, Fatura, Observacoes
                ) VALUES (
                    :Hospital, :Atendimento, :Paciente, :Prestador, :Data_Cirurgia,
                    :Convenio, :Procedimento_Tipo_ID, :Situacao_ID,
                    :Guia_AMHPTISS, :Guia_AMHPTISS_Complemento, :Fatura, :Observacoes
                )
            """), params)
            return int(rs2.lastrowid)


def list_cirurgias(
    hospital: Optional[str] = None,
    ano_mes: Optional[str] = None,  # "YYYY-MM" ou "MM/YYYY" se Data_Cirurgia no seu formato
    prestador: Optional[str] = None
):
    """
    Lista com filtros simples. Como Data_Cirurgia é TEXT livre, o filtro de ano_mes
    faz um LIKE na string. Ajuste conforme o padrão que você adotar.
    """
    engine = get_engine()
    where = []
    params = {}
    if hospital:
        where.append("Hospital = :h"); params["h"] = hospital
    if prestador:
        where.append("Prestador = :p"); params["p"] = prestador
    if ano_mes:
        where.append("Data_Cirurgia LIKE :dm"); params["dm"] = f"%{ano_mes}%"

    sql = f"""
        SELECT id, Hospital, Atendimento, Paciente, Prestador, Data_Cirurgia,
               Convenio, Procedimento_Tipo_ID, Situacao_ID,
               Guia_AMHPTISS, Guia_AMHPTISS_Complemento, Fatura,
               Observacoes, created_at, updated_at
        FROM cirurgias
        {('WHERE ' + ' AND '.join(where)) if where else ''}
        ORDER BY Hospital, Data_Cirurgia, Paciente
    """
    with engine.connect() as conn:
        rs = conn.execute(text(sql), params)
        return rs.fetchall()


def delete_cirurgia(id_: int):
    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM cirurgias WHERE id = :id"), {"id": int(id_)})


# ------- Helper para pré-preenchimento a partir da tabela original -------

def find_registros_para_prefill(hospital: str, ano: Optional[int] = None, mes: Optional[int] = None, prestadores: Optional[List[str]] = None):
    """
    Retorna registros da tabela original para servir de base na criação de cirurgias.
    Filtros: hospital, (opcionais) ano, mes, prestadores exatos (case-sensitive aqui).
    """
    engine = get_engine()
    where = ["Hospital = :h"]
    params = {"h": hospital}
    if ano is not None:
        where.append("Ano = :a"); params["a"] = int(ano)
    if mes is not None:
        where.append("Mes = :m"); params["m"] = int(mes)
    if prestadores and len(prestadores) > 0:
        # Monta IN dinâmico
        in_list = ", ".join([f":p{i}" for i in range(len(prestadores))])
        where.append(f"Prestador IN ({in_list})")
        for i, p in enumerate(prestadores):
            params[f"p{i}"] = p

    sql = f"""
        SELECT Hospital, Data, Atendimento, Paciente, Convenio, Prestador
        FROM pacientes_unicos_por_dia_prestador
        WHERE {' AND '.join(where)}
        ORDER BY Ano, Mes, Dia, Paciente, Prestador
    """
    with engine.connect() as conn:
        rs = conn.execute(text(sql), params)
        return rs.fetchall()
