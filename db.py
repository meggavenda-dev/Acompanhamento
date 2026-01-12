
# -*- coding: utf-8 -*-
from __future__ import annotations
import os, math, tempfile
from typing import Dict, Any, List, Optional, Sequence, Tuple
from datetime import datetime
from sqlalchemy import create_engine, text

# =============================================================================
# CONFIGURAÇÃO DO BANCO
# =============================================================================
# Usa diretório temporário para evitar problemas de permissão (read-only)
DATA_DIR = tempfile.gettempdir()
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
    except: 
        return default

def _safe_str(v, default=""):
    if v is None: return default
    try:
        if isinstance(v, float) and math.isnan(v): return default
    except:
        pass
    return str(v).strip()

# =============================================================================
# GARANTIA DE ÍNDICES ÚNICOS
# =============================================================================
def ensure_unique_indexes():
    """Cria índices únicos idempotentes (garante ON CONFLICT funcionando)."""
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
# PACIENTES (UPSERT / LEITURAS)
# =============================================================================
def upsert_dataframe(df):
    """
    Salva DataFrame na tabela base (pacientes_unicos_por_dia_prestador).
    Usa ON CONFLICT p/ atualizar Aviso, Convenio, Quarto.
    """
    if df is None or df.empty: 
        return
    if (df["Paciente"].astype(str).str.strip() == "").any():
        raise ValueError("Existem pacientes vazios — corrija antes de salvar.")
    ensure_unique_indexes()
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
# LEITURAS P/ ABA CIRURGIAS (BASE)
# =============================================================================
def _date_filter_clause(colname: str, ano: Optional[int], mes: Optional[int]) -> Tuple[str, dict]:
    """
    Monta filtro por ano/mês tolerante a 'dd/MM/yyyy' ou 'YYYY-MM-DD'.
    """
    params = {}
    parts = []
    if ano is not None and mes is not None:
        # 'dd/MM/yyyy' termina com '/YYYY' e contém '/MM/'
        parts.append(f"(({colname} LIKE :p1) OR ({colname} LIKE :p2))")
        params["p1"] = f"%/{mes:02d}/{ano}"
        params["p2"] = f"{ano}-{mes:02d}-%"
    elif ano is not None:
        parts.append(f"(({colname} LIKE :p3) OR ({colname} LIKE :p4))")
        params["p3"] = f"%/{ano}"
        params["p4"] = f"{ano}-%"
    clause = (" AND " + " AND ".join(parts)) if parts else ""
    return clause, params

def find_registros_para_prefill(hospital: str,
                                ano: Optional[int] = None,
                                mes: Optional[int] = None,
                                prestadores: Optional[Sequence[str]] = None) -> list:
    """
    Retorna registros da tabela base (pacientes_unicos_por_dia_prestador) para pré-preencher a Aba Cirurgias.
    Filtros opcionais: hospital (obrigatório), ano/mês, lista de prestadores.
    """
    if not hospital:
        return []

    where = ["Hospital = :h"]
    params = {"h": hospital}

    # Data (aceita 'dd/MM/yyyy' e 'YYYY-MM-DD')
    clause, p = _date_filter_clause("Data", ano, mes)
    if clause:
        where.append(clause[5:] if clause.startswith(" AND ") else clause)
        params.update(p)

    # Prestadores (case-sensitive no SQLite; normalize se quiser)
    if prestadores:
        tokens = [str(p).strip() for p in prestadores if str(p).strip()]
        if tokens:
            in_params = {}
            placeholders = []
            for i, val in enumerate(tokens):
                key = f"pp{i}"
                in_params[key] = val
                placeholders.append(f":{key}")
            where.append(f"Prestador IN ({', '.join(placeholders)})")
            params.update(in_params)

    sql = f"""
        SELECT Hospital, Data, Atendimento, Paciente, Convenio, Prestador
        FROM pacientes_unicos_por_dia_prestador
        WHERE {' AND '.join(where)}
        ORDER BY Data, Prestador, Atendimento, Paciente
    """
    engine = get_engine()
    with engine.connect() as conn:
        return conn.execute(text(sql), params).fetchall()

def list_registros_base_all(limit: int = 500) -> list:
    """
    Lista registros da base para diagnóstico rápido (limite configurável).
    """
    limit = int(limit or 500)
    engine = get_engine()
    with engine.connect() as conn:
        return conn.execute(text(f"""
            SELECT Hospital, Data, Atendimento, Paciente, Convenio, Prestador
            FROM pacientes_unicos_por_dia_prestador
            ORDER BY Data DESC, Prestador, Atendimento, Paciente
            LIMIT {limit}
        """)).fetchall()

# =============================================================================
# CATÁLOGOS (Tipos e Situações)
# =============================================================================
def list_procedimento_tipos(only_active=True):
    engine = get_engine()
    sql = "SELECT id, nome, ativo, ordem FROM procedimento_tipos"
    if only_active: 
        sql += " WHERE ativo=1"
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
    if only_active: 
        sql += " WHERE ativo=1"
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
# CIRURGIAS (UPSERT / LISTA / DELETE)
# =============================================================================
def insert_or_update_cirurgia(payload: Dict[str, Any]) -> int:
    """
    UPSERT de cirurgia. A chave é:
    (Hospital, Atendimento, Paciente, Prestador, Data_Cirurgia)
    Observação: Aceita Atendimento vazio se Paciente vier preenchido (e vice-versa).
    """
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
