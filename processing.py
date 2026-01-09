# db.py
from sqlalchemy import create_engine, text

DB_PATH = "exemplo.db"

def get_engine():
    # sqlite no arquivo local do repo
    return create_engine(f"sqlite:///{DB_PATH}", future=True)

def init_db():
    engine = get_engine()
    with engine.begin() as conn:
        # Tabela principal com índice de unicidade por (Data, Paciente, Prestador)
        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS pacientes_unicos_por_dia_prestador (
            Data TEXT,
            Atendimento TEXT,
            Paciente TEXT,
            Aviso TEXT,
            Convenio TEXT,
            Prestador TEXT,
            Quarto TEXT
        );
        """))
        conn.execute(text("""
        CREATE UNIQUE INDEX IF NOT EXISTS idx_unicidade
        ON pacientes_unicos_por_dia_prestador (Data, Paciente, Prestador);
        """))

def upsert_dataframe(df):
    """
    UPSERT (INSERT OR REPLACE) por (Data, Paciente, Prestador).
    """
    engine = get_engine()
    with engine.begin() as conn:
        for _, row in df.iterrows():
            conn.execute(text("""
                INSERT OR REPLACE INTO pacientes_unicos_por_dia_prestador
                (Data, Atendimento, Paciente, Aviso, Convenio, Prestador, Quarto)
                VALUES (:Data, :Atendimento, :Paciente, :Aviso, :Convenio, :Prestador, :Quarto)
            """), {
                "Data": str(row.get("Data", "")) if row.get("Data", "") is not None else "",
                "Atendimento": str(row.get("Atendimento", "")) if row.get("Atendimento", "") is not None else "",
                "Paciente": str(row.get("Paciente", "")) if row.get("Paciente", "") is not None else "",
                "Aviso": str(row.get("Aviso", "")) if row.get("Aviso", "") is not None else "",
                "Convenio": str(row.get("Convenio", "")) if row.get("Convenio", "") is not None else "",
                "Prestador": str(row.get("Prestador", "")) if row.get("Prestador", "") is not None else "",
                "Quarto": str(row.get("Quarto", "")) if row.get("Quarto", "") is not None else "",
            })

def read_all():
    engine = get_engine()
    with engine.connect() as conn:
        rs = conn.execute(text("SELECT * FROM pacientes_unicos_por_dia_prestador ORDER BY Data, Paciente, Prestador"))
        rows = rs.fetchall()
    return rows


