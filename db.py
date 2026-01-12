
# db.py
from __future__ import annotations

import os
import math
import tempfile
from typing import Optional, List, Dict, Any
from sqlalchemy import create_engine, text

# ---------------- Configuração de caminho persistente (writable) ----------------
def _pick_writable_dir() -> str:
    """
    Escolhe um diretório garantidamente gravável:
    1) st.secrets["DB_DIR"] se existir;
    2) env var DB_DIR se existir;
    3) /tmp (tempfile.gettempdir()) como fallback.
    """
    db_dir = os.environ.get("DB_DIR", "")
    # Tenta pegar do Streamlit secrets sem impor dependência forte
    try:
        import streamlit as st
        db_dir = st.secrets.get("DB_DIR", db_dir)
    except Exception:
        pass

    if not db_dir:
        db_dir = os.path.join(tempfile.gettempdir(), "acompanhamento_data")

    os.makedirs(db_dir, exist_ok=True)
    # Garante permissões de escrita no diretório
    try:
        os.chmod(db_dir, 0o777)
    except Exception:
        pass
    return db_dir

DATA_DIR = _pick_writable_dir()
DB_PATH = os.path.join(DATA_DIR, "exemplo.db")
DB_URI = f"sqlite:///{DB_PATH}"

# Engine singleton simples para evitar recriar a cada chamada (mais estável em Streamlit).
_ENGINE = None
def get_engine():
    """
    Retorna um engine SQLAlchemy para SQLite no arquivo local (writable).
    Usa check_same_thread=False para compat Streamlit.
    """
    global _ENGINE
    if _ENGINE is None:
        _ENGINE = create_engine(
            DB_URI,
            future=True,
            echo=False,
            connect_args={"check_same_thread": False}
        )
    return _ENGINE


def _ensure_db_file_writable():
    """
    Garante que o arquivo do DB exista e tenha permissão de escrita.
    """
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    if not os.path.exists(DB_PATH):
        # cria arquivo vazio para assegurar permissão
        open(DB_PATH, "a").close()
    try:
        os.chmod(DB_PATH, 0o666)
    except Exception:
        pass


def init_db():
    """
    Cria/atualiza a estrutura do banco:
    - Tabela original: pacientes_unicos_por_dia_prestador
    - Catálogo: procedimento_tipos
    - Catálogo: cirurgia_situacoes
    - Tabela: cirurgias
    """
    _ensure_db_file_writable()
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

        # ---- Catálogo: Situações da Cirurgia ----
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
            Data_Cirurgia TEXT,         -- formato livre (ex.: dd/MM/yyyy ou YYYY-MM-DD)
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

        # Índices úteis para consultas
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

        # Evita duplicar mesma cirurgia com chave composta (ajuste conforme regra desejada)
        conn.execute(text("""
        CREATE UNIQUE INDEX IF NOT EXISTS idx_cirurgia_unica
        ON cirurgias (Hospital, Atendimento, Prestador, Data_Cirurgia);
        """))


def _safe_int(val, default: int = 0) -> int:
    """
    Converte em int com segurança (None/NaN/strings vazias viram default).
    """
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


# ---------------- UPSERT original (pacientes_unicos_por_dia_prestador) ----------------
def upsert_dataframe(df):
    """
    UPSERT (INSERT OR REPLACE) por (Data, Paciente, Prestador, Hospital).
    """
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


# ---------- Utilitários opcionais ----------
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
    _ensure_db_file_writable()
    engine = get_engine()
    with engine.begin() as conn:
        rs = conn.execute(text("SELECT id FROM procedimento_tipos WHERE nome = :n"), {"n": nome.strip()})
        row = rs.fetchone()
        if row:
            conn.execute(text("""
                UPDATE procedimento_tipos SET ativo = :a, ordem = :o WHERE id = :id
            """), {"a": int(ativo), "o": int(ordem), "id": int(row[0])})
            return int(row[0])
        else:
            result = conn.execute(text("""
                INSERT INTO procedimento_tipos (nome, ativo, ordem) VALUES (:n, :a, :o)
            """), {"n": nome.strip(), "a": int(ativo), "o": int(ordem)})
            return int(result.lastrowid)


def list_procedimento_tipos(only_active: bool = True):
    engine = get_engine()
    with engine.connect() as conn:
        if only_active:
            rs = conn.execute(text("SELECT id, nome, ativo, ordem FROM procedimento_tipos WHERE ativo = 1 ORDER BY ordem, nome"))
        else:
            rs = conn.execute(text("SELECT id, nome, ativo, ordem FROM procedimento_tipos ORDER BY ativo DESC, ordem, nome"))
        return rs.fetchall()


def set_procedimento_tipo_status(id_: int, ativo: int):
    _ensure_db_file_writable()
    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(text("UPDATE procedimento_tipos SET ativo = :a WHERE id = :id"), {"a": int(ativo), "id": int(id_)})


def upsert_cirurgia_situacao(nome: str, ativo: int = 1, ordem: int = 0) -> int:
    _ensure_db_file_writable()
    engine = get_engine()
    with engine.begin() as conn:
        rs = conn.execute(text("SELECT id FROM cirurgia_situacoes WHERE nome = :n"), {"n": nome.strip()})
        row = rs.fetchone()
        if row:
            conn.execute(text("""
                UPDATE cirurgia_situacoes SET ativo = :a, ordem = :o WHERE id = :id
            """), {"a": int(ativo), "o": int(ordem), "id": int(row[0])})
            return int(row[0])
        else:
            result = conn.execute(text("""
                INSERT INTO cirurgia_situacoes (nome, ativo, ordem) VALUES (:n, :a, :o)
            """), {"n": nome.strip(), "a": int(ativo), "o": int(ordem)})
            return int(result.lastrowid)


def list_cirurgia_situacoes(only_active: bool = True):
    engine = get_engine()
    with engine.connect() as conn:
        if only_active:
            rs = conn.execute(text("SELECT id, nome, ativo, ordem FROM cirurgia_situacoes WHERE ativo = 1 ORDER BY ordem, nome"))
        else:
            rs = conn.execute(text("SELECT id, nome, ativo, ordem FROM cirurgia_situacoes ORDER BY ativo DESC, ordem, nome"))
        return rs.fetchall()


def set_cirurgia_situacao_status(id_: int, ativo: int):
    _ensure_db_file_writable()
    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(text("UPDATE cirurgia_situacoes SET ativo = :a WHERE id = :id"), {"a": int(ativo), "id": int(id_)})


# ---------------- Cirurgias (CRUD) ----------------
def insert_or_update_cirurgia(payload: Dict[str, Any]) -> int:
    """
    UPSERT por (Hospital, Atendimento, Prestador, Data_Cirurgia).
    Retorna id da cirurgia.
    """
    _ensure_db_file_writable()
    engine = get_engine()
    with engine.begin() as conn:
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
            result = conn.execute(text("""
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
            return int(result.lastrowid)


def list_cirurgias(
    hospital: Optional[str] = None,
    ano_mes: Optional[str] = None,  # "YYYY-MM" ou "MM/YYYY"
    prestador: Optional[str] = None
):
    """
    Lista cirurgias com filtros simples.
    Obs.: Como Data_Cirurgia é TEXT, o filtro 'ano_mes' faz um LIKE na string.
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
    _ensure_db_file_writable()
    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM cirurgias WHERE id = :id"), {"id": int(id_)})


# ------- Helper para pré-preenchimento a partir da tabela original -------
def find_registros_para_prefill(
    hospital: str,
    ano: Optional[int] = None,
    mes: Optional[int] = None,
    prestadores: Optional[List[str]] = None
):
    """
    Retorna registros da tabela original para servir de base na criação de cirurgias.

    Filtros:
      - Hospital (case-insensitive, com TRIM)
      - Ano/Mês (opcionais) — com fallback via Data LIKE se Ano/Mês na tabela estiverem 0/NULL
      - Prestadores (opcional) — filtrado em PYTHON com normalização agressiva
        (sem acentos, sem espaços, sem pontuação, UPPER).
    """
    engine = get_engine()

    # ---- Normalizadores para filtro em Python ----
    import unicodedata
    def _strip_accents(s: str) -> str:
        return "".join(ch for ch in unicodedata.normalize("NFKD", s) if not unicodedata.combining(ch))

    def _normalize_name(s: Optional[str]) -> str:
        """
        Remove acentos, espaços e pontuações comuns; aplica UPPER.
        Ex.: 'Cássio-Cesar ' -> 'CASSIOCESAR'
        """
        if s is None:
            return ""
        t = _strip_accents(str(s)).upper()
        for ch in (" ", ".", "-", "_", "/", "\\"):
            t = t.replace(ch, "")
        return t.strip()

    # ---- Monta WHERE só com Hospital/Ano/Mês (prestador será filtrado em Python) ----
    where = []
    params = {}

    # Hospital com TRIM + UPPER para evitar problemas de espaços e case
    where.append("UPPER(TRIM(Hospital)) = UPPER(:h)")
    params["h"] = hospital.strip()

    # Ano/Mês com fallback por LIKE em Data (dd/MM/yyyy)
    if ano is not None and mes is not None:
        params["a"] = int(ano)
        params["m"] = int(mes)
        params["dm_like"] = f"%/{int(mes):02d}/{int(ano)}%"
        where.append("(Ano = :a OR Ano IS NULL OR Ano = 0 OR Data LIKE :dm_like)")
        where.append("(Mes = :m OR Mes IS NULL OR Mes = 0 OR Data LIKE :dm_like)")
    elif ano is not None:
        params["a"] = int(ano)
        params["a_like"] = f"%{int(ano)}%"
        where.append("(Ano = :a OR Ano IS NULL OR Ano = 0 OR Data LIKE :a_like)")
    elif mes is not None:
        params["m"] = int(mes)
        params["m_like"] = f"%/{int(mes):02d}/%"
        where.append("(Mes = :m OR Mes IS NULL OR Mes = 0 OR Data LIKE :m_like)")

    sql = f"""
        SELECT Hospital, Data, Atendimento, Paciente, Convenio, Prestador
        FROM pacientes_unicos_por_dia_prestador
        WHERE {' AND '.join(where)}
        ORDER BY Ano, Mes, Dia, Paciente, Prestador
    """

    with engine.connect() as conn:
        rows = conn.execute(text(sql), params).fetchall()

    # ---- Filtro opcional por prestadores em Python (accent/punct/space insensitive) ----
    prestadores = [p for p in (prestadores or []) if p and str(p).strip()]
    if not prestadores:
        return rows  # sem filtro de prestadores

    target_norm = {_normalize_name(p) for p in prestadores}
    filtered = []
    for (h, data, att, pac, conv, prest) in rows:
        if _normalize_name(prest) in target_norm:
            filtered.append((h, data, att, pac, conv, prest))

    return filtered


# ---------- (Opcional) Diagnóstico rápido ----------
def list_registros_base_all(limit: int = 500):
    """
    Lista até 'limit' registros da tabela base (pacientes_unicos_por_dia_prestador)
    para diagnóstico rápido na aba de Cirurgias.
    """
    engine = get_engine()
    with engine.connect() as conn:
        rs = conn.execute(text(f"""
            SELECT Hospital, Data, Atendimento, Paciente, Convenio, Prestador
            FROM pacientes_unicos_por_dia_prestador
            ORDER BY Hospital, Ano, Mes, Dia, Paciente, Prestador
            LIMIT {int(limit)}
        """))
        return rs.fetchall()
