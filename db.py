
from sqlalchemy import create_engine, text

DB_PATH = "exemplo.db"

def get_engine():
    # sqlite no arquivo local do repo
    return create_engine(f"sqlite:///{DB_PATH}", future=True)

def init_db():
    engine = get_engine()
    with engine.begin() as conn:
        # Tabela principal agora inclui Hospital, Ano, Mes, Dia
        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS pacientes_unicos_por_dia_prestador (
            Hospital   TEXT,
            Ano        INTEGER,
            Mes        INTEGER,
            Dia        INTEGER,
            Data       TEXT,
            Atendimento TEXT,
            Paciente   TEXT,
            Aviso      TEXT,
            Convenio   TEXT,
            Prestador  TEXT,
            Quarto     TEXT
        );
        """))

        # Índice único: (Data, Paciente, Prestador, Hospital)
        # mantém a unicidade por dia/paciente/prestador dentro do hospital
        conn.execute(text("""
        CREATE UNIQUE INDEX IF NOT EXISTS idx_unicidade
        ON pacientes_unicos_por_dia_prestador (Data, Paciente, Prestador, Hospital);
        """))

        # Índice auxiliar para ordenação/consulta por hospital e calendário
        conn.execute(text("""
        CREATE INDEX IF NOT EXISTS idx_hospital_calendario
        ON pacientes_unicos_por_dia_prestador (Hospital, Ano, Mes, Dia);
        """))

def upsert_dataframe(df):
    """
    UPSERT (INSERT OR REPLACE) por (Data, Paciente, Prestador, Hospital).
    """
    engine = get_engine()
    with engine.begin() as conn:
        for _, row in df.iterrows():
            conn.execute(text("""
                INSERT OR REPLACE INTO pacientes_unicos_por_dia_prestador
                (Hospital, Ano, Mes, Dia, Data, Atendimento, Paciente, Aviso, Convenio, Prestador, Quarto)
                VALUES (:Hospital, :Ano, :Mes, :Dia, :Data, :Atendimento, :Paciente, :Aviso, :Convenio, :Prestador, :Quarto)
            """), {
                "Hospital":  str(row.get("Hospital", "") or ""),
                "Ano":       int(row.get("Ano", 0) or 0),
                "Mes":       int(row.get("Mes", 0) or 0),
                "Dia":       int(row.get("Dia", 0) or 0),
                "Data":      str(row.get("Data", "") or ""),
                "Atendimento": str(row.get("Atendimento", "") or ""),
                "Paciente":  str(row.get("Paciente", "") or ""),
                "Aviso":     str(row.get("Aviso", "") or ""),
                "Convenio":  str(row.get("Convenio", "") or ""),
                "Prestador": str(row.get("Prestador", "") or ""),
                "Quarto":    str(row.get("Quarto", "") or ""),
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
