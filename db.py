
# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import math
from typing import Optional, Sequence, List, Tuple, Dict, Any
from datetime import datetime

from sqlalchemy import create_engine, text

# =============================================================================
# CAMINHOS E ENGINE
# =============================================================================

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


def dispose_engine():
    global _ENGINE
    if _ENGINE is not None:
        _ENGINE.dispose()
        _ENGINE = None


# =============================================================================
# HELPERS
# =============================================================================

def _safe_int(v, default=0):
    try:
        if v is None:
            return default
        if isinstance(v, float) and math.isnan(v):
            return default
        return int(float(str(v).strip()))
    except Exception:
        return default


def _safe_str(v, default=""):
    if v is None:
        return default
    try:
        if isinstance(v, float) and math.isnan(v):
            return default
    except Exception:
        pass
    return str(v).strip()


# =============================================================================
# INIT DB (CRIA TUDO SEMPRE)
# =============================================================================

def init_db():
    engine = get_engine()
    with engine.begin() as conn:

        # ---------------------------------------------------------------------
        # BASE (PACIENTES)
        # ---------------------------------------------------------------------
        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS pacientes_unicos_por_dia_prestador (
            Hospital    TEXT NOT NULL,
            Ano         INTEGER,
            Mes         INTEGER,
            Dia         INTEGER,
            Data        TEXT NOT NULL,
            Atendimento TEXT,
            Paciente    TEXT NOT NULL,
            Aviso       TEXT,
            Convenio    TEXT,
            Prestador   TEXT NOT NULL,
            Quarto      TEXT
        );
        """))

        conn.execute(text("""
        CREATE UNIQUE INDEX IF NOT EXISTS ux_pacientes
        ON pacientes_unicos_por_dia_prestador (Hospital, Data, Paciente, Prestador);
        """))

        # ---------------------------------------------------------------------
        # CATÁLOGOS
        # ---------------------------------------------------------------------
        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS procedimento_tipos (
            id     INTEGER PRIMARY KEY AUTOINCREMENT,
            nome   TEXT UNIQUE NOT NULL,
            ativo  INTEGER NOT NULL DEFAULT 1,
            ordem  INTEGER NOT NULL DEFAULT 1
        );
        """))

        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS cirurgia_situacoes (
            id     INTEGER PRIMARY KEY AUTOINCREMENT,
            nome   TEXT UNIQUE NOT NULL,
            ativo  INTEGER NOT NULL DEFAULT 1,
            ordem  INTEGER NOT NULL DEFAULT 1
        );
        """))

        # ---------------------------------------------------------------------
        # CIRURGIAS (CHAVE NATURAL SIMPLES E CONFIÁVEL)
        # ---------------------------------------------------------------------
        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS cirurgias (
            id                        INTEGER PRIMARY KEY AUTOINCREMENT,
            Hospital                  TEXT NOT NULL,
            Atendimento               TEXT NOT NULL DEFAULT '',
            Paciente                  TEXT NOT NULL DEFAULT '',
            Prestador                 TEXT NOT NULL,
            Data_Cirurgia             TEXT NOT NULL,

            Convenio                  TEXT,
            Procedimento_Tipo_ID      INTEGER,
            Situacao_ID               INTEGER,

            Guia_AMHPTISS             TEXT,
            Guia_AMHPTISS_Complemento TEXT,
            Fatura                    TEXT,
            Observacoes               TEXT,

            created_at                TEXT NOT NULL,
            updated_at                TEXT NOT NULL
        );
        """))

        conn.execute(text("""
        CREATE UNIQUE INDEX IF NOT EXISTS ux_cirurgias
        ON cirurgias (
            Hospital,
            Atendimento,
            Paciente,
            Prestador,
            Data_Cirurgia
        );
        """))


# =============================================================================
# RESET / MANUTENÇÃO
# =============================================================================

def vacuum():
    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(text("VACUUM;"))


def reset_db_file():
    dispose_engine()
    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)
    init_db()


def delete_all_pacientes():
    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM pacientes_unicos_por_dia_prestador;"))


def delete_all_catalogos():
    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM procedimento_tipos;"))
        conn.execute(text("DELETE FROM cirurgia_situacoes;"))


def delete_all_cirurgias():
    """
    ✅ CORRIGIDO: garante que a tabela exista e executa delete real.
    """
    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(text("""
            DELETE FROM cirurgias;
        """))


# =============================================================================
# BASE PACIENTES (UPSERT)
# =============================================================================

def upsert_dataframe(df):
    if df is None or df.empty:
        return

    if (df["Paciente"].astype(str).str.strip() == "").any():
        raise ValueError("Existem pacientes vazios — corrija antes de salvar.")

    engine = get_engine()
    with engine.begin() as conn:
        for _, r in df.iterrows():
            conn.execute(text("""
                INSERT OR REPLACE INTO pacientes_unicos_por_dia_prestador
                (Hospital, Ano, Mes, Dia, Data, Atendimento, Paciente, Aviso, Convenio, Prestador, Quarto)
                VALUES
                (:Hospital, :Ano, :Mes, :Dia, :Data, :Atendimento, :Paciente, :Aviso, :Convenio, :Prestador, :Quarto)
            """), {
                "Hospital":   _safe_str(r.get("Hospital")),
                "Ano":        _safe_int(r.get("Ano")),
                "Mes":        _safe_int(r.get("Mes")),
                "Dia":        _safe_int(r.get("Dia")),
                "Data":       _safe_str(r.get("Data")),
                "Atendimento":_safe_str(r.get("Atendimento")),
                "Paciente":   _safe_str(r.get("Paciente")),
                "Aviso":      _safe_str(r.get("Aviso")),
                "Convenio":   _safe_str(r.get("Convenio")),
                "Prestador":  _safe_str(r.get("Prestador")),
                "Quarto":     _safe_str(r.get("Quarto")),
            })


def read_all():
    engine = get_engine()
    with engine.connect() as conn:
        return conn.execute(text("""
            SELECT Hospital, Ano, Mes, Dia, Data, Atendimento,
                   Paciente, Aviso, Convenio, Prestador, Quarto
            FROM pacientes_unicos_por_dia_prestador
            ORDER BY Hospital, Ano, Mes, Dia, Paciente, Prestador
        """)).fetchall()


def count_all():
    engine = get_engine()
    with engine.connect() as conn:
        return conn.execute(text("SELECT COUNT(*) FROM pacientes_unicos_por_dia_prestador")).scalar_one()


# =============================================================================
# CATÁLOGOS
# =============================================================================

def list_procedimento_tipos(only_active=True):
    engine = get_engine()
    sql = """
        SELECT id, nome, ativo, ordem
        FROM procedimento_tipos
    """
    if only_active:
        sql += " WHERE ativo = 1"
    sql += " ORDER BY ordem, nome"
    with engine.connect() as conn:
        return conn.execute(text(sql)).fetchall()


def upsert_procedimento_tipo(nome, ativo=1, ordem=1):
    engine = get_engine()
    nome = _safe_str(nome)
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO procedimento_tipos (nome, ativo, ordem)
            VALUES (:nome, :ativo, :ordem)
            ON CONFLICT(nome) DO UPDATE
            SET ativo = excluded.ativo,
                ordem = excluded.ordem
        """), {"nome": nome, "ativo": int(ativo), "ordem": int(ordem)})

        return conn.execute(
            text("SELECT id FROM procedimento_tipos WHERE nome=:n"),
            {"n": nome}
        ).scalar_one()


def set_procedimento_tipo_status(tid, ativo):
    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(text("""
            UPDATE procedimento_tipos SET ativo=:a WHERE id=:i
        """), {"a": int(ativo), "i": int(tid)})


def list_cirurgia_situacoes(only_active=True):
    engine = get_engine()
    sql = """
        SELECT id, nome, ativo, ordem
        FROM cirurgia_situacoes
    """
    if only_active:
        sql += " WHERE ativo = 1"
    sql += " ORDER BY ordem, nome"
    with engine.connect() as conn:
        return conn.execute(text(sql)).fetchall()


def upsert_cirurgia_situacao(nome, ativo=1, ordem=1):
    engine = get_engine()
    nome = _safe_str(nome)
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO cirurgia_situacoes (nome, ativo, ordem)
            VALUES (:nome, :ativo, :ordem)
            ON CONFLICT(nome) DO UPDATE
            SET ativo = excluded.ativo,
                ordem = excluded.ordem
        """), {"nome": nome, "ativo": int(ativo), "ordem": int(ordem)})
        return conn.execute(
            text("SELECT id FROM cirurgia_situacoes WHERE nome=:n"),
            {"n": nome}
        ).scalar_one()


def set_cirurgia_situacao_status(sid, ativo):
    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(text("""
            UPDATE cirurgia_situacoes SET ativo=:a WHERE id=:i
        """), {"a": int(ativo), "i": int(sid)})


# =============================================================================
# CIRURGIAS (UPSERT ROBUSTO)
# =============================================================================

def insert_or_update_cirurgia(payload: Dict[str, Any]) -> int:
    h = _safe_str(payload.get("Hospital"))
    att = _safe_str(payload.get("Atendimento"), "")
    pac = _safe_str(payload.get("Paciente"), "")
    p = _safe_str(payload.get("Prestador"))
    d = _safe_str(payload.get("Data_Cirurgia"))

    if not h or not p or not d or (not att and not pac):
        raise ValueError("Chave mínima inválida para cirurgia.")

    now = datetime.now().isoformat(timespec="seconds")

    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO cirurgias (
                Hospital, Atendimento, Paciente, Prestador, Data_Cirurgia,
                Convenio, Procedimento_Tipo_ID, Situacao_ID,
                Guia_AMHPTISS, Guia_AMHPTISS_Complemento,
                Fatura, Observacoes,
                created_at, updated_at
            )
            VALUES (
                :Hospital, :Atendimento, :Paciente, :Prestador, :Data,
                :Convenio, :TipoID, :SitID,
                :Guia, :GuiaC, :Fatura, :Obs,
                :created, :updated
            )
            ON CONFLICT (
                Hospital, Atendimento, Paciente, Prestador, Data_Cirurgia
            ) DO UPDATE SET
                Convenio                  = excluded.Convenio,
                Procedimento_Tipo_ID      = excluded.Procedimento_Tipo_ID,
                Situacao_ID               = excluded.Situacao_ID,
                Guia_AMHPTISS             = excluded.Guia_AMHPTISS,
                Guia_AMHPTISS_Complemento = excluded.Guia_AMHPTISS_Complemento,
                Fatura                    = excluded.Fatura,
                Observacoes               = excluded.Observacoes,
                updated_at                = excluded.updated_at
        """), {
            "Hospital": h,
            "Atendimento": att,
            "Paciente": pac,
            "Prestador": p,
            "Data": d,
            "Convenio": _safe_str(payload.get("Convenio")),
            "TipoID": payload.get("Procedimento_Tipo_ID"),
            "SitID": payload.get("Situacao_ID"),
            "Guia": _safe_str(payload.get("Guia_AMHPTISS")),
            "GuiaC": _safe_str(payload.get("Guia_AMHPTISS_Complemento")),
            "Fatura": _safe_str(payload.get("Fatura")),
            "Obs": _safe_str(payload.get("Observacoes")),
            "created": now,
            "updated": now
        })

        return conn.execute(text("""
            SELECT id FROM cirurgias
            WHERE Hospital=:h AND Atendimento=:a AND Paciente=:p AND
                  Prestador=:pr AND Data_Cirurgia=:d
        """), {
            "h": h, "a": att, "p": pac, "pr": p, "d": d
        }).scalar_one()


def list_cirurgias(hospital=None, ano_mes=None, prestador=None):
    clauses, params = [], {}
    if hospital:
        clauses.append("Hospital=:h")
        params["h"] = hospital
    if prestador:
        clauses.append("Prestador=:p")
        params["p"] = prestador
    if ano_mes:
        clauses.append("Data_Cirurgia LIKE :dm")
        params["dm"] = f"{ano_mes[:7]}%"
    where = " WHERE " + " AND ".join(clauses) if clauses else ""
    sql = f"""
        SELECT id, Hospital, Atendimento, Paciente, Prestador, Data_Cirurgia,
               Convenio, Procedimento_Tipo_ID, Situacao_ID,
               Guia_AMHPTISS, Guia_AMHPTISS_Complemento, Fatura,
               Observacoes, created_at, updated_at
        FROM cirurgias
        {where}
        ORDER BY Data_Cirurgia, Prestador, Atendimento, Paciente
    """
    engine = get_engine()
    with engine.connect() as conn:
        return conn.execute(text(sql), params).fetchall()


def delete_cirurgia(cirurgia_id: int):
    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM cirurgias WHERE id=:i"), {"i": int(cirurgia_id)})


# =============================================================================
# PREFILL
# =============================================================================

def find_registros_para_prefill(hospital, ano=None, mes=None, prestadores=None):
    clauses = ["Hospital = :h"]
    params = {"h": hospital}
    if ano:
        clauses.append("Ano = :a")
        params["a"] = int(ano)
    if mes:
        clauses.append("Mes = :m")
        params["m"] = int(mes)
    if prestadores:
        clauses.append("Prestador IN (" + ",".join([f":p{i}" for i in range(len(prestadores))]) + ")")
        for i, p in enumerate(prestadores):
            params[f"p{i}"] = _safe_str(p)

    sql = f"""
        SELECT Hospital, Data, Atendimento, Paciente, Convenio, Prestador
        FROM pacientes_unicos_por_dia_prestador
        WHERE {" AND ".join(clauses)}
        ORDER BY Data, Prestador, Atendimento, Paciente
    """

    engine = get_engine()
    with engine.connect() as conn:
        return conn.execute(text(sql), params).fetchall()


def list_registros_base_all(limit=500):
    engine = get_engine()
    with engine.connect() as conn:
        return conn.execute(text(f"""
            SELECT Hospital, Data, Atendimento, Paciente, Convenio, Prestador
            FROM pacientes_unicos_por_dia_prestador
            ORDER BY Data DESC
            LIMIT {int(limit)}
        """)).fetchall()
