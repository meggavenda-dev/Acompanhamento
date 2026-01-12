
# -*- coding: utf-8 -*-
from __future__ import annotations
import os, math
from typing import Dict, Any
from datetime import datetime
from sqlalchemy import create_engine, text

# =============================================================================
# CONFIGURAÇÃO DO BANCO
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
    if _ENGINE:
        _ENGINE.dispose()
        _ENGINE = None

# =============================================================================
# HELPERS
# =============================================================================
def _safe_int(v, default=0):
    try:
        if v is None: return default
        if isinstance(v, float) and math.isnan(v): return default
        return int(float(str(v).strip()))
    except: return default

def _safe_str(v, default=""):
    if v is None: return default
    try:
        if isinstance(v, float) and math.isnan(v): return default
    except: pass
    return str(v).strip()

# =============================================================================
# GARANTIA DE ÍNDICES ÚNICOS
# =============================================================================
def ensure_unique_indexes():
    """Cria índices únicos idempotentes para garantir ON CONFLICT funcionando."""
    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE UNIQUE INDEX IF NOT EXISTS ux_pacientes_unicos
            ON pacientes_unicos_por_dia_prestador (Hospital, Atendimento, Paciente, Prestador, Data);
        """))
        conn.execute(text("""
            CREATE UNIQUE INDEX IF NOT EXISTS ux_cirurgias
            ON cirurgias (Hospital, Atendimento, Paciente, Prestador, Data_Cirurgia);
        """))

# =============================================================================
# INIT DB (com UNIQUE constraints)
# =============================================================================
def init_db():
    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS pacientes_unicos_por_dia_prestador (
            Hospital TEXT,
            Ano INTEGER,
            Mes INTEGER,
            Dia INTEGER,
            Data TEXT,
            Atendimento TEXT,
            Paciente TEXT,
            Aviso TEXT,
            Convenio TEXT,
            Prestador TEXT,
            Quarto TEXT,
            UNIQUE(Hospital, Atendimento, Paciente, Prestador, Data)
        );
        """))
        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS cirurgias (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            Hospital TEXT,
            Atendimento TEXT,
            Paciente TEXT,
            Prestador TEXT,
            Data_Cirurgia TEXT,
            Convenio TEXT,
            Procedimento_Tipo_ID INTEGER,
            Situacao_ID INTEGER,
            Guia_AMHPTISS TEXT,
            Guia_AMHPTISS_Complemento TEXT,
            Fatura TEXT,
            Observacoes TEXT,
            created_at TEXT,
            updated_at TEXT,
            UNIQUE(Hospital, Atendimento, Paciente, Prestador, Data_Cirurgia)
        );
        """))
        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS procedimento_tipos (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            nome TEXT UNIQUE,
            ativo INTEGER,
            ordem INTEGER
        );
        """))
        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS cirurgia_situacoes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            nome TEXT UNIQUE,
            ativo INTEGER,
            ordem INTEGER
        );
        """))
    ensure_unique_indexes()  # garante índices únicos

# =============================================================================
# RESET / MANUTENÇÃO
# =============================================================================
def vacuum():
    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(text("VACUUM"))

def reset_db_file():
    dispose_engine()
    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)
    init_db()

def delete_all_pacientes() -> int:
    engine = get_engine()
    with engine.begin() as conn:
        total = conn.execute(text("SELECT COUNT(*) FROM pacientes_unicos_por_dia_prestador")).scalar_one()
        conn.execute(text("DELETE FROM pacientes_unicos_por_dia_prestador"))
    return total

def delete_all_catalogos() -> int:
    engine = get_engine()
    with engine.begin() as conn:
        t1 = conn.execute(text("SELECT COUNT(*) FROM procedimento_tipos")).scalar_one()
        t2 = conn.execute(text("SELECT COUNT(*) FROM cirurgia_situacoes")).scalar_one()
        conn.execute(text("DELETE FROM procedimento_tipos"))
        conn.execute(text("DELETE FROM cirurgia_situacoes"))
    return t1 + t2

def delete_all_cirurgias() -> int:
    engine = get_engine()
    with engine.begin() as conn:
        total = conn.execute(text("SELECT COUNT(*) FROM cirurgias")).scalar_one()
        conn.execute(text("DELETE FROM cirurgias"))
    return total

# =============================================================================
# PACIENTES
# =============================================================================
def upsert_dataframe(df):
    if df is None or df.empty: return
    if (df["Paciente"].astype(str).str.strip() == "").any():
        raise ValueError("Existem pacientes vazios — corrija antes de salvar.")
    ensure_unique_indexes()  # garante índices antes do upsert
    engine = get_engine()
    with engine.begin() as conn:
        for _, r in df.iterrows():
            conn.execute(text("""
                INSERT INTO pacientes_unicos_por_dia_prestador
                (Hospital, Ano, Mes, Dia, Data, Atendimento, Paciente, Aviso, Convenio, Prestador, Quarto)
                VALUES
                (:Hospital, :Ano, :Mes, :Dia, :Data, :Atendimento, :Paciente, :Aviso, :Convenio, :Prestador, :Quarto)
                ON CONFLICT(Hospital, Atendimento, Paciente, Prestador, Data)
                DO UPDATE SET
                    Aviso=excluded.Aviso,
                    Convenio=excluded.Convenio,
                    Quarto=excluded.Quarto
            """), {
                "Hospital": _safe_str(r.get("Hospital")),
                "Ano": _safe_int(r.get("Ano")),
                "Mes": _safe_int(r.get("Mes")),
                "Dia": _safe_int(r.get("Dia")),
                "Data": _safe_str(r.get("Data")),
                "Atendimento": _safe_str(r.get("Atendimento")),
                "Paciente": _safe_str(r.get("Paciente")),
                "Aviso": _safe_str(r.get("Aviso")),
                "Convenio": _safe_str(r.get("Convenio")),
                "Prestador": _safe_str(r.get("Prestador")),
                "Quarto": _safe_str(r.get("Quarto")),
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
    sql = "SELECT id, nome, ativo, ordem FROM procedimento_tipos"
    if only_active: sql += " WHERE ativo=1"
    sql += " ORDER BY ordem, nome"
    with engine.connect() as conn:
        return conn.execute(text(sql)).fetchall()

def upsert_procedimento_tipo(nome, ativo=1, ordem=1):
    ensure_unique_indexes()
    engine = get_engine()
    nome = _safe_str(nome)
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO procedimento_tipos (nome, ativo, ordem)
            VALUES (:nome, :ativo, :ordem)
            ON CONFLICT(nome) DO UPDATE SET ativo=excluded.ativo, ordem=excluded.ordem
        """), {"nome": nome, "ativo": int(ativo), "ordem": int(ordem)})
        return conn.execute(text("SELECT id FROM procedimento_tipos WHERE nome=:n"), {"n": nome}).scalar_one()

def set_procedimento_tipo_status(tid, ativo):
    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(text("UPDATE procedimento_tipos SET ativo=:a WHERE id=:i"), {"a": int(ativo), "i": int(tid)})

def list_cirurgia_situacoes(only_active=True):
    engine = get_engine()
    sql = "SELECT id, nome, ativo, ordem FROM cirurgia_situacoes"
    if only_active: sql += " WHERE ativo=1"
    sql += " ORDER BY ordem, nome"
    with engine.connect() as conn:
        return conn.execute(text(sql)).fetchall()

def upsert_cirurgia_situacao(nome, ativo=1, ordem=1):
    ensure_unique_indexes()
    engine = get_engine()
    nome = _safe_str(nome)
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO cirurgia_situacoes (nome, ativo, ordem)
            VALUES (:nome, :ativo, :ordem)
            ON CONFLICT(nome) DO UPDATE SET ativo=excluded.ativo, ordem=excluded.ordem
        """), {"nome": nome, "ativo": int(ativo), "ordem": int(ordem)})
        return conn.execute(text("SELECT id FROM cirurgia_situacoes WHERE nome=:n"), {"n": nome}).scalar_one()

def set_cirurgia_situacao_status(sid, ativo):
    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(text("UPDATE cirurgia_situacoes SET ativo=:a WHERE id=:i"), {"a": int(ativo), "i": int(sid)})

# =============================================================================
# CIRURGIAS
# =============================================================================
def insert_or_update_cirurgia(payload: Dict[str, Any]) -> int:
    ensure_unique_indexes()
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
                Fatura, Observacoes, created_at, updated_at
            )
            VALUES (
                :Hospital, :Atendimento, :Paciente, :Prestador, :Data,
                :Convenio, :TipoID, :SitID,
                :Guia, :GuiaC, :Fatura, :Obs, :created, :updated
            )
            ON CONFLICT(Hospital, Atendimento, Paciente, Prestador, Data_Cirurgia)
            DO UPDATE SET
                Convenio=excluded.Convenio,
                Procedimento_Tipo_ID=excluded.Procedimento_Tipo_ID,
                Situacao_ID=excluded.Situacao_ID,
                Guia_AMHPTISS=excluded.Guia_AMHPTISS,
                Guia_AMHPTISS_Complemento=excluded.Guia_AMHPTISS_Complemento,
                Fatura=excluded.Fatura,
                Observacoes=excluded.Observacoes,
                updated_at=excluded.updated_at
        """), {
            "Hospital": h, "Atendimento": att, "Paciente": pac, "Prestador": p, "Data": d,
            "Convenio": _safe_str(payload.get("Convenio")),
            "TipoID": payload.get("Procedimento_Tipo_ID"),
            "SitID": payload.get("Situacao_ID"),
            "Guia": _safe_str(payload.get("Guia_AMHPTISS")),
            "GuiaC": _safe_str(payload.get("Guia_AMHPTISS_Complemento")),
            "Fatura": _safe_str(payload.get("Fatura")),
            "Obs": _safe_str(payload.get("Observacoes")),
            "created": now, "updated": now
        })
        return conn.execute(text("""
            SELECT id FROM cirurgias
            WHERE Hospital=:h AND Atendimento=:a AND Paciente=:p AND Prestador=:pr AND Data_Cirurgia=:d
        """), {"h": h, "a": att, "p": pac, "pr": p, "d": d}).scalar_one()

def list_cirurgias(hospital=None, ano_mes=None, prestador=None):
    clauses, params = [], {}
    if hospital: clauses.append("Hospital=:h"); params["h"] = hospital
    if prestador: clauses.append("Prestador=:p"); params["p"] = prestador
    if ano_mes: clauses.append("Data_Cirurgia LIKE :dm"); params["dm"] = f"{ano_mes[:7]}%"
    where = " WHERE " + " AND ".join(clauses) if clauses else ""
    sql = f"""
        SELECT id, Hospital, Atendimento, Paciente, Prestador, Data_Cirurgia,
               Convenio, Procedimento_Tipo_ID, Situacao_ID,
               Guia_AMHPTISS, Guia_AMHPTISS_Complemento, Fatura,
               Observacoes, created_at, updated_at
        FROM cirurgias {where}
        ORDER BY Data_Cirurgia, Prestador, Atendimento, Paciente
    """
    engine = get_engine()
    with engine.connect() as conn:
        return conn.execute(text(sql), params).fetchall()

def delete_cirurgia(cirurgia_id: int):
    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM cirurgias WHERE id=:i"), {"i": int(cirurgia_id)})
