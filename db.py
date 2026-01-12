
# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import math
from typing import Optional, Sequence, List, Tuple, Dict, Any
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

# ---------------- Utilitários de saneamento ----------------

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

# ---------------- Inicialização do schema ----------------

def init_db():
    """
    Cria as tabelas e índices, caso não existam.
    - Tabela: pacientes_unicos_por_dia_prestador
    - Catálogos: procedimento_tipos, cirurgia_situacoes
    - Tabela: cirurgias
    """
    engine = get_engine()
    with engine.begin() as conn:
        # Base (pacientes por dia/prestador/hospital)
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

        # Catálogo de Tipos (para dropdown no app)
        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS procedimento_tipos (
            id     INTEGER PRIMARY KEY AUTOINCREMENT,
            nome   TEXT UNIQUE,
            ativo  INTEGER DEFAULT 1,
            ordem  INTEGER DEFAULT 1
        );
        """))

        # Catálogo de Situações
        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS cirurgia_situacoes (
            id     INTEGER PRIMARY KEY AUTOINCREMENT,
            nome   TEXT UNIQUE,
            ativo  INTEGER DEFAULT 1,
            ordem  INTEGER DEFAULT 1
        );
        """))

        # Tabela de Cirurgias (UPSERT por chave natural)
        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS cirurgias (
            id                        INTEGER PRIMARY KEY AUTOINCREMENT,
            Hospital                  TEXT,
            Atendimento               TEXT,
            Paciente                  TEXT,
            Prestador                 TEXT,
            Data_Cirurgia             TEXT,
            Convenio                  TEXT,
            Procedimento_Tipo_ID      INTEGER,
            Situacao_ID               INTEGER,
            Guia_AMHPTISS             TEXT,
            Guia_AMHPTISS_Complemento TEXT,
            Fatura                    TEXT,
            Observacoes               TEXT,
            created_at                TEXT,
            updated_at                TEXT
        );
        """))

        # Índice único natural (evita duplicação em UPSERT)
        conn.execute(text("""
        CREATE UNIQUE INDEX IF NOT EXISTS idx_cirurgias_natural
        ON cirurgias (Hospital, coalesce(Atendimento,''), coalesce(Paciente,''), Prestador, Data_Cirurgia);
        """))

        # Auxiliares de consulta
        conn.execute(text("""
        CREATE INDEX IF NOT EXISTS idx_cirurgias_hospital_data
        ON cirurgias (Hospital, Data_Cirurgia);
        """))

# ---------------- Operações base (pacientes_unicos_por_dia_prestador) ----------------

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

def read_all() -> List[Tuple]:
    """
    Lê todos os registros base, ordenando por Hospital, Ano, Mes, Dia, Paciente, Prestador.
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
    """Limpa completamente a tabela base (use com cautela)."""
    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM pacientes_unicos_por_dia_prestador"))

def count_all() -> int:
    """Retorna a quantidade de linhas na tabela base."""
    engine = get_engine()
    with engine.connect() as conn:
        rs = conn.execute(text("SELECT COUNT(1) FROM pacientes_unicos_por_dia_prestador"))
        return rs.scalar_one()

# ---------------- Funções esperadas pelo app (RESET / VACUUM / FILE) ----------------

def delete_all_pacientes():
    """Apaga a tabela base de pacientes (conteúdo)."""
    delete_all()

def delete_all_cirurgias():
    """Apaga todas as linhas da tabela 'cirurgias'."""
    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM cirurgias"))

def delete_all_catalogos():
    """Apaga catálogos de tipos e situações."""
    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM procedimento_tipos"))
        conn.execute(text("DELETE FROM cirurgia_situacoes"))

def vacuum():
    """VACUUM (compactação) no SQLite."""
    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(text("VACUUM"))

def dispose_engine():
    """Fecha o engine singleton, permitindo recriar o arquivo sem lock."""
    global _ENGINE
    try:
        if _ENGINE is not None:
            _ENGINE.dispose()
    finally:
        _ENGINE = None

def reset_db_file():
    """
    Remove o arquivo .db e recria vazio com schema via init_db().
    """
    # remover arquivo
    try:
        if os.path.exists(DB_PATH):
            os.remove(DB_PATH)
    except Exception:
        pass
    # recriar
    init_db()

# ---------------- Catálogo: Tipos de Procedimento ----------------

def list_procedimento_tipos(only_active: bool = True) -> List[Tuple]:
    """
    Lista tipos (ativos por padrão), ordenando por ordem e nome (como no app).
    """
    engine = get_engine()
    with engine.connect() as conn:
        if only_active:
            rs = conn.execute(text("""
                SELECT id, nome, ativo, ordem
                FROM procedimento_tipos
                WHERE ativo = 1
                ORDER BY ordem, nome
            """))
        else:
            rs = conn.execute(text("""
                SELECT id, nome, ativo, ordem
                FROM procedimento_tipos
                ORDER BY ordem, nome
            """))
        return rs.fetchall()

def upsert_procedimento_tipo(nome: str, ativo: int = 1, ordem: int = 1) -> int:
    """
    UPSERT por nome (único). Retorna id.
    """
    nome = _safe_str(nome)
    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO procedimento_tipos (nome, ativo, ordem)
            VALUES (:nome, :ativo, :ordem)
            ON CONFLICT(nome) DO UPDATE SET
                ativo = excluded.ativo,
                ordem = excluded.ordem
        """), {"nome": nome, "ativo": int(ativo), "ordem": int(ordem)})

        rs = conn.execute(text("SELECT id FROM procedimento_tipos WHERE nome = :nome"), {"nome": nome})
        rid = rs.scalar_one()
    return int(rid)

def set_procedimento_tipo_status(tipo_id: int, ativo: int):
    """
    Atualiza o status ativo (0/1) de um tipo por id.
    """
    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(text("""
            UPDATE procedimento_tipos
            SET ativo = :ativo
            WHERE id = :id
        """), {"ativo": int(ativo), "id": int(tipo_id)})

# ---------------- Catálogo: Situações da Cirurgia ----------------

def list_cirurgia_situacoes(only_active: bool = True) -> List[Tuple]:
    """
    Lista situações (ativas por padrão), ordenando por ordem e nome (como no app).
    """
    engine = get_engine()
    with engine.connect() as conn:
        if only_active:
            rs = conn.execute(text("""
                SELECT id, nome, ativo, ordem
                FROM cirurgia_situacoes
                WHERE ativo = 1
                ORDER BY ordem, nome
            """))
        else:
            rs = conn.execute(text("""
                SELECT id, nome, ativo, ordem
                FROM cirurgia_situacoes
                ORDER BY ordem, nome
            """))
        return rs.fetchall()

def upsert_cirurgia_situacao(nome: str, ativo: int = 1, ordem: int = 1) -> int:
    """
    UPSERT por nome (único). Retorna id.
    """
    nome = _safe_str(nome)
    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO cirurgia_situacoes (nome, ativo, ordem)
            VALUES (:nome, :ativo, :ordem)
            ON CONFLICT(nome) DO UPDATE SET
                ativo = excluded.ativo,
                ordem = excluded.ordem
        """), {"nome": nome, "ativo": int(ativo), "ordem": int(ordem)})

        rs = conn.execute(text("SELECT id FROM cirurgia_situacoes WHERE nome = :nome"), {"nome": nome})
        rid = rs.scalar_one()
    return int(rid)

def set_cirurgia_situacao_status(situacao_id: int, ativo: int):
    """
    Atualiza o status ativo (0/1) de uma situação por id.
    """
    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(text("""
            UPDATE cirurgia_situacoes
            SET ativo = :ativo
            WHERE id = :id
        """), {"ativo": int(ativo), "id": int(situacao_id)})

# ---------------- Cirurgias (CRUD, UPSERT, listas) ----------------

def insert_or_update_cirurgia(payload: Dict[str, Any]) -> int:
    """
    UPSERT na tabela 'cirurgias' por chave natural:
      (Hospital, coalesce(Atendimento,''), coalesce(Paciente,''), Prestador, Data_Cirurgia)

    Retorna o id da cirurgia após UPSERT.
    """
    # Sanitiza valores obrigatórios
    h  = _safe_str(payload.get("Hospital", ""))
    att = _safe_str(payload.get("Atendimento", ""))  # pode ser vazio
    pac = _safe_str(payload.get("Paciente", ""))     # pode ser vazio
    p   = _safe_str(payload.get("Prestador", ""))
    d   = _safe_str(payload.get("Data_Cirurgia", ""))

    if not h or not p or not d or (not att and not pac):
        # Chave mínima exigida pelo app
        raise ValueError("Chave de cirurgia incompleta (Hospital/Prestador/Data_Cirurgia e (Atendimento ou Paciente) são obrigatórios).")

    # Campos complementares
    convenio   = _safe_str(payload.get("Convenio", ""))
    tipo_id    = payload.get("Procedimento_Tipo_ID")
    sit_id     = payload.get("Situacao_ID")
    guia       = _safe_str(payload.get("Guia_AMHPTISS", ""))
    guia_comp  = _safe_str(payload.get("Guia_AMHPTISS_Complemento", ""))
    fatura     = _safe_str(payload.get("Fatura", ""))
    obs        = _safe_str(payload.get("Observacoes", ""))

    from datetime import datetime
    now_iso = datetime.now().isoformat(timespec="seconds")

    engine = get_engine()
    with engine.begin() as conn:
        # Tenta atualizar; se não existir, insere
        conn.execute(text("""
            INSERT INTO cirurgias
            (Hospital, Atendimento, Paciente, Prestador, Data_Cirurgia, Convenio,
             Procedimento_Tipo_ID, Situacao_ID, Guia_AMHPTISS, Guia_AMHPTISS_Complemento,
             Fatura, Observacoes, created_at, updated_at)
            VALUES
            (:Hospital, :Atendimento, :Paciente, :Prestador, :Data_Cirurgia, :Convenio,
             :Procedimento_Tipo_ID, :Situacao_ID, :Guia_AMHPTISS, :Guia_AMHPTISS_Complemento,
             :Fatura, :Observacoes, :created_at, :updated_at)
            ON CONFLICT(Hospital, Atendimento, Paciente, Prestador, Data_Cirurgia)
            DO UPDATE SET
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
            "Data_Cirurgia": d,
            "Convenio": convenio,
            "Procedimento_Tipo_ID": tipo_id if (tipo_id is None or isinstance(tipo_id, int)) else int(tipo_id),
            "Situacao_ID": sit_id if (sit_id is None or isinstance(sit_id, int)) else int(sit_id),
            "Guia_AMHPTISS": guia,
            "Guia_AMHPTISS_Complemento": guia_comp,
            "Fatura": fatura,
            "Observacoes": obs,
            "created_at": now_iso,
            "updated_at": now_iso
        })

        rs = conn.execute(text("""
            SELECT id FROM cirurgias
            WHERE Hospital = :Hospital
              AND coalesce(Atendimento,'') = :Atendimento
              AND coalesce(Paciente,'')    = :Paciente
              AND Prestador                = :Prestador
              AND Data_Cirurgia            = :Data_Cirurgia
        """), {"Hospital": h, "Atendimento": att, "Paciente": pac, "Prestador": p, "Data_Cirurgia": d})
        rid = rs.scalar_one()
    return int(rid)

def list_cirurgias(hospital: Optional[str] = None, ano_mes: Optional[str] = None, prestador: Optional[str] = None) -> List[Tuple]:
    """
    Lista cirurgias, opcionalmente filtrando por hospital, ano_mes (YYYY-MM) e prestador.
    """
    engine = get_engine()
    where = []
    params = {}
    if hospital:
        where.append("Hospital = :h")
        params["h"] = hospital
    if ano_mes:
        # aceita 'YYYY-MM' ou 'MM/YYYY' — normaliza para LIKE
        s = ano_mes.strip()
        if "/" in s:  # e.g., 09/2025
            mm, yyyy = s.split("/")
            s_like = f"{int(yyyy):04d}-{int(mm):02d}%"
        else:         # e.g., 2025-09
            s_like = f"{s}%"
        where.append("Data_Cirurgia LIKE :dm")
        params["dm"] = s_like
    if prestador:
        where.append("Prestador = :p")
        params["p"] = prestador

    sql = """
        SELECT id, Hospital, Atendimento, Paciente, Prestador, Data_Cirurgia,
               Convenio, Procedimento_Tipo_ID, Situacao_ID,
               Guia_AMHPTISS, Guia_AMHPTISS_Complemento, Fatura,
               Observacoes, created_at, updated_at
        FROM cirurgias
    """
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY Hospital, Data_Cirurgia, Prestador, Atendimento, Paciente"

    with engine.connect() as conn:
        rs = conn.execute(text(sql), params)
        return rs.fetchall()

def delete_cirurgia(cirurgia_id: int):
    """Exclui uma cirurgia pelo id."""
    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM cirurgias WHERE id = :id"), {"id": int(cirurgia_id)})

# ---------------- Funções de Prefill (Base -> Cirurgias) ----------------

def find_registros_para_prefill(hospital: str,
                                ano: Optional[int] = None,
                                mes: Optional[int] = None,
                                prestadores: Optional[Sequence[str]] = None) -> List[Tuple]:
    """
    Retorna registros da base (pacientes_unicos_por_dia_prestador) para pré-preencher cirurgias,
    filtrando por hospital, opcionalmente por ano/mês e lista de prestadores.
    """
    engine = get_engine()
    where = ["Hospital = :h"]
    params: Dict[str, Any] = {"h": hospital}
    if ano is not None:
        where.append("Ano = :a")
        params["a"] = int(ano)
    if mes is not None:
        where.append("Mes = :m")
        params["m"] = int(mes)
    if prestadores:
        # lista exata (sem normalização aqui; o app já prepara)
        where.append(f"Prestador IN ({', '.join([f':p{i}' for i in range(len(prestadores))])})")
        for i, p in enumerate(prestadores):
            params[f"p{i}"] = _safe_str(p)

    sql = f"""
        SELECT Hospital, Data, Atendimento, Paciente, Convenio, Prestador
        FROM pacientes_unicos_por_dia_prestador
        WHERE {' AND '.join(where)}
        ORDER BY Data, Prestador, Atendimento, Paciente
    """
    with engine.connect() as conn:
        rs = conn.execute(text(sql), params)
        return rs.fetchall()

def list_registros_base_all(limit: int = 500) -> List[Tuple]:
    """Lista os primeiros N registros da base para diagnóstico no app."""
    engine = get_engine()
    with engine.connect() as conn:
        rs = conn.execute(text(f"""
            SELECT Hospital, Data, Atendimento, Paciente, Convenio, Prestador
            FROM pacientes_unicos_por_dia_prestador
            ORDER BY Hospital, Data, Prestador, Atendimento, Paciente
            LIMIT {int(limit)}
        """))
        return rs.fetchall()
