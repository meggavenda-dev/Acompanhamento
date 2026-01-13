
# db_merge.py
# -*- coding: utf-8 -*-
from sqlalchemy import create_engine, text
import shutil

def merge_sqlite_dbs(local_path: str, remote_path: str, output_path: str) -> None:
    """
    Cria um banco de saída (output_path) mesclando o remoto com o local.
    Regras:
      - pacientes_unicos_por_dia_prestador: última escrita vence p/ Aviso, Convenio, Quarto
      - procedimento_tipos / cirurgia_situacoes: por nome (UNIQUE), atualiza ativo/ordem
      - cirurgias: last-write-wins por updated_at respeitando UNIQUE da chave
    """
    # Copia o remoto para output
    shutil.copyfile(remote_path, output_path)

    eng = create_engine(f"sqlite:///{output_path}", future=True)
    with eng.begin() as conn:
        conn.execute(text(f"ATTACH DATABASE '{local_path}' AS localdb;"))

        conn.execute(text("""
            INSERT INTO pacientes_unicos_por_dia_prestador
            (Hospital, Ano, Mes, Dia, Data, Atendimento, Paciente, Aviso, Convenio, Prestador, Quarto)
            SELECT Hospital, Ano, Mes, Dia, Data, Atendimento, Paciente, Aviso, Convenio, Prestador, Quarto
            FROM localdb.pacientes_unicos_por_dia_prestador
            ON CONFLICT(Hospital, Atendimento, Paciente, Prestador, Data)
            DO UPDATE SET
                Aviso    = excluded.Aviso,
                Convenio = excluded.Convenio,
                Quarto   = excluded.Quarto;
        """))

        conn.execute(text("""
            INSERT INTO procedimento_tipos (nome, ativo, ordem)
            SELECT nome, ativo, ordem FROM localdb.procedimento_tipos
            ON CONFLICT(nome) DO UPDATE SET ativo=excluded.ativo, ordem=excluded.ordem;
        """))

        conn.execute(text("""
            INSERT INTO cirurgia_situacoes (nome, ativo, ordem)
            SELECT nome, ativo, ordem FROM localdb.cirurgia_situacoes
            ON CONFLICT(nome) DO UPDATE SET ativo=excluded.ativo, ordem=excluded.ordem;
        """))

        conn.execute(text("""
            INSERT INTO cirurgias (
                Hospital, Atendimento, Paciente, Prestador, Data_Cirurgia,
                Convenio, Procedimento_Tipo_ID, Situacao_ID,
                Guia_AMHPTISS, Guia_AMHPTISS_Complemento,
                Fatura, Observacoes, created_at, updated_at
            )
            SELECT
                l.Hospital, l.Atendimento, l.Paciente, l.Prestador, l.Data_Cirurgia,
                l.Convenio, l.Procedimento_Tipo_ID, l.Situacao_ID,
                l.Guia_AMHPTISS, l.Guia_AMHPTISS_Complemento,
                l.Fatura, l.Observacoes,
                COALESCE(l.created_at, r.created_at),
                CASE 
                    WHEN r.updated_at IS NULL THEN l.updated_at
                    WHEN l.updated_at IS NULL THEN r.updated_at
                    WHEN l.updated_at > r.updated_at THEN l.updated_at
                    ELSE r.updated_at END
            FROM localdb.cirurgias l
            LEFT JOIN cirurgias r
              ON r.Hospital=l.Hospital AND r.Atendimento=l.Atendimento AND r.Paciente=l.Paciente
             AND r.Prestador=l.Prestador AND r.Data_Cirurgia=l.Data_Cirurgia
            ON CONFLICT(Hospital, Atendimento, Paciente, Prestador, Data_Cirurgia)
            DO UPDATE SET
                Convenio=excluded.Convenio,
                Procedimento_Tipo_ID=excluded.Procedimento_Tipo_ID,
                Situacao_ID=excluded.Situacao_ID,
                Guia_AMHPTISS=excluded.Guia_AMHPTISS,
                Guia_AMHPTISS_Complemento=excluded.Guia_AMHPTISS_Complemento,
                Fatura=excluded.Fatura,
                Observacoes=excluded.Observacoes,
                updated_at=excluded.updated_at;
        """))

        conn.execute(text("DETACH DATABASE localdb;"))

