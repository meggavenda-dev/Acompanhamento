from __future__ import annotations
import os
import math
import time
import tempfile
from typing import Optional, List, Dict, Any
from sqlalchemy import create_engine, text

# --- Configuração de Caminho ---
def _pick_writable_dir() -> str:
    db_dir = os.environ.get("DB_DIR", "")
    try:
        import streamlit as st
        db_dir = st.secrets.get("DB_DIR", db_dir)
    except:
        pass
    if not db_dir:
        db_dir = os.path.join(tempfile.gettempdir(), "acompanhamento_data")
    os.makedirs(db_dir, exist_ok=True)
    return db_dir

DATA_DIR = _pick_writable_dir()
DB_PATH = os.path.join(DATA_DIR, "exemplo.db")
DB_URI = f"sqlite:///{DB_PATH}"

_ENGINE = None

def get_engine():
    global _ENGINE
    if _ENGINE is None:
        _ENGINE = create_engine(
            DB_URI,
            future=True,
            connect_args={"check_same_thread": False},
            pool_pre_ping=True # Verifica se a conexão está viva
        )
    return _ENGINE

def dispose_engine():
    """Fecha todas as conexões para liberar o arquivo físico."""
    global _ENGINE
    if _ENGINE is not None:
        _ENGINE.dispose()
        _ENGINE = None

def init_db():
    """Cria a estrutura inicial do banco."""
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    engine = get_engine()
    with engine.begin() as conn:
        # Tabela de Pacientes
        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS pacientes_unicos_por_dia_prestador (
            Hospital TEXT, Ano INTEGER, Mes INTEGER, Dia INTEGER, Data TEXT,
            Atendimento TEXT, Paciente TEXT, Aviso TEXT, Convenio TEXT, Prestador TEXT, Quarto TEXT
        );"""))
        conn.execute(text("CREATE UNIQUE INDEX IF NOT EXISTS idx_unicidade ON pacientes_unicos_por_dia_prestador (Data, Paciente, Prestador, Hospital);"))
        
        # Catálogos
        conn.execute(text("CREATE TABLE IF NOT EXISTS procedimento_tipos (id INTEGER PRIMARY KEY AUTOINCREMENT, nome TEXT NOT NULL UNIQUE, ativo INTEGER NOT NULL DEFAULT 1, ordem INTEGER DEFAULT 0);"))
        conn.execute(text("CREATE TABLE IF NOT EXISTS cirurgia_situacoes (id INTEGER PRIMARY KEY AUTOINCREMENT, nome TEXT NOT NULL UNIQUE, ativo INTEGER NOT NULL DEFAULT 1, ordem INTEGER DEFAULT 0);"))
        
        # Cirurgias
        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS cirurgias (
            id INTEGER PRIMARY KEY AUTOINCREMENT, Hospital TEXT NOT NULL, Atendimento TEXT,
            Paciente TEXT, Prestador TEXT, Data_Cirurgia TEXT, Convenio TEXT,
            Procedimento_Tipo_ID INTEGER, Situacao_ID INTEGER, Guia_AMHPTISS TEXT,
            Guia_AMHPTISS_Complemento TEXT, Fatura TEXT, Observacoes TEXT,
            created_at TEXT DEFAULT (datetime('now')), updated_at TEXT
        );"""))

def reset_db_file():
    """Apaga o arquivo físico do banco de dados."""
    dispose_engine()
    time.sleep(0.5) # Pausa para o SO liberar o lock
    if os.path.exists(DB_PATH):
        try:
            os.remove(DB_PATH)
        except Exception as e:
            # Fallback: se não puder deletar, sobrescreve com arquivo vazio
            with open(DB_PATH, 'wb') as f:
                pass
    init_db()

def vacuum():
    """Compacta o banco de dados (libera espaço em disco)."""
    # O VACUUM não pode rodar dentro de transações do SQLAlchemy
    engine = get_engine()
    with engine.connect() as conn:
        conn.execution_options(isolation_level="AUTOCOMMIT").exec_driver_sql("VACUUM")

# --- Operações de Delete ---
def delete_all_pacientes():
    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM pacientes_unicos_por_dia_prestador"))

def delete_all_cirurgias():
    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM cirurgias"))

def delete_all_catalogos():
    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM procedimento_tipos"))
        conn.execute(text("DELETE FROM cirurgia_situacoes"))

# --- Funções de Leitura/Escrita (Resumo) ---
def upsert_dataframe(df):
    if df is None or df.empty: return
    engine = get_engine()
    with engine.begin() as conn:
        for _, row in df.iterrows():
            conn.execute(text("""
                INSERT OR REPLACE INTO pacientes_unicos_por_dia_prestador 
                (Hospital, Ano, Mes, Dia, Data, Atendimento, Paciente, Aviso, Convenio, Prestador, Quarto)
                VALUES (:Hospital, :Ano, :Mes, :Dia, :Data, :Atendimento, :Paciente, :Aviso, :Convenio, :Prestador, :Quarto)
            """), row.to_dict())

def read_all():
    engine = get_engine()
    with engine.connect() as conn:
        return conn.execute(text("SELECT * FROM pacientes_unicos_por_dia_prestador")).fetchall()

def count_all():
    engine = get_engine()
    with engine.connect() as conn:
        return conn.execute(text("SELECT COUNT(1) FROM pacientes_unicos_por_dia_prestador")).scalar()

# --- Funções de Catálogo e Cirurgia (Stubs mantidos para compatibilidade) ---
def list_procedimento_tipos(only_active=True):
    engine = get_engine()
    cmd = "SELECT * FROM procedimento_tipos WHERE ativo=1" if only_active else "SELECT * FROM procedimento_tipos"
    with engine.connect() as conn: return conn.execute(text(cmd)).fetchall()

def list_cirurgia_situacoes(only_active=True):
    engine = get_engine()
    cmd = "SELECT * FROM cirurgia_situacoes WHERE ativo=1" if only_active else "SELECT * FROM cirurgia_situacoes"
    with engine.connect() as conn: return conn.execute(text(cmd)).fetchall()

def list_cirurgias(hospital=None, ano_mes=None, prestador=None):
    engine = get_engine()
    with engine.connect() as conn: return conn.execute(text("SELECT * FROM cirurgias")).fetchall()

def insert_or_update_cirurgia(payload):
    engine = get_engine()
    with engine.begin() as conn:
        # Implementação simplificada de UPSERT para demonstração
        conn.execute(text("""
            INSERT INTO cirurgias (Hospital, Atendimento, Paciente, Prestador, Data_Cirurgia)
            VALUES (:Hospital, :Atendimento, :Paciente, :Prestador, :Data_Cirurgia)
        """), payload)

def delete_cirurgia(id_):
    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM cirurgias WHERE id = :id"), {"id": id_})

def find_registros_para_prefill(hospital, ano=None, mes=None, prestadores=None):
    engine = get_engine()
    with engine.connect() as conn:
        return conn.execute(text("SELECT Hospital, Data, Atendimento, Paciente, Convenio, Prestador FROM pacientes_unicos_por_dia_prestador")).fetchall()

def list_registros_base_all(limit=500):
    engine = get_engine()
    with engine.connect() as conn:
        return conn.execute(text(f"SELECT * FROM pacientes_unicos_por_dia_prestador LIMIT {limit}")).fetchall()

def upsert_procedimento_tipo(nome, ativo, ordem):
    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(text("INSERT OR REPLACE INTO procedimento_tipos (nome, ativo, ordem) VALUES (:n, :a, :o)"), {"n": nome, "a": ativo, "o": ordem})

def set_procedimento_tipo_status(id_, ativo):
    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(text("UPDATE procedimento_tipos SET ativo = :a WHERE id = :id"), {"a": ativo, "id": id_})

def upsert_cirurgia_situacao(nome, ativo, ordem):
    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(text("INSERT OR REPLACE INTO cirurgia_situacoes (nome, ativo, ordem) VALUES (:n, :a, :o)"), {"n": nome, "a": ativo, "o": ordem})

def set_cirurgia_situacao_status(id_, ativo):
    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(text("UPDATE cirurgia_situacoes SET ativo = :a WHERE id = :id"), {"a": ativo, "id": id_})
