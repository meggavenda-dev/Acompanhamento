
# -*- coding: utf-8 -*-
"""
github_sync.py — Sincronização do arquivo SQLite (.db) com o GitHub (Contents API),
com controle de versão via 'sha' e merge automático em caso de conflito.

Funcionalidades:
- download_db_from_github(..., return_sha=False) -> bool ou (bool, sha)
- upload_db_to_github(..., prev_sha=None, _return_details=False) -> bool ou (ok, new_sha, status)
- safe_upload_with_merge(...) -> bool    # tenta upload; se conflito, baixa remoto, faz merge e reenvia
- merge_sqlite_dbs(local_path, remote_path, output_path) -> None  # utilitário

Dependências:
- requests (opcional; cai para urllib se ausente)
- sqlalchemy (para merge SQLite)
"""

import base64
import json
import os
import shutil
import tempfile
from typing import Optional, Tuple, Union

# HTTP: usa 'requests' se disponível; senão, 'urllib'
try:
    import requests
    _HAS_REQUESTS = True
except Exception:
    _HAS_REQUESTS = False
    import urllib.request
    import urllib.error


# =========================
# Helpers de Token/Headers
# =========================

def _resolve_token(token: Optional[str]) -> Optional[str]:
    """
    Resolve token a partir de:
    - parâmetro explícito
    - st.secrets["GITHUB_TOKEN"] (se streamlit presente e segredo definido)
    - os.environ["GITHUB_TOKEN"]
    """
    if token:
        return token
    try:
        import streamlit as st
        tok = st.secrets.get("GITHUB_TOKEN")
        if tok:
            return tok
    except Exception:
        pass
    return os.environ.get("GITHUB_TOKEN")


def _gh_headers(token: Optional[str]) -> dict:
    headers = {
        "Accept": "application/vnd.github+json",
        "User-Agent": "github-sync-sqlite/1.0",
    }
    if token:
        headers["Authorization"] = f"Bearer {token}"
    return headers


def _http_get(url: str, headers: dict) -> Tuple[int, bytes]:
    if _HAS_REQUESTS:
        resp = requests.get(url, headers=headers)
        return resp.status_code, resp.content
    # urllib fallback
    req = urllib.request.Request(url, headers=headers, method="GET")
    try:
        with urllib.request.urlopen(req) as resp:
            return resp.getcode(), resp.read()
    except urllib.error.HTTPError as e:
        return e.code, e.read()
    except urllib.error.URLError:
        return 0, b""


def _http_put_json(url: str, headers: dict, payload: dict) -> Tuple[int, bytes]:
    body = json.dumps(payload).encode("utf-8")
    hdrs = dict(headers)
    hdrs["Content-Type"] = "application/json"
    if _HAS_REQUESTS:
        resp = requests.put(url, headers=hdrs, data=json.dumps(payload))
        return resp.status_code, resp.content
    # urllib fallback
    req = urllib.request.Request(url, headers=hdrs, data=body, method="PUT")
    try:
        with urllib.request.urlopen(req) as resp:
            return resp.getcode(), resp.read()
    except urllib.error.HTTPError as e:
        return e.code, e.read()
    except urllib.error.URLError:
        return 0, b""


# =========================
# Download do .db (Contents API)
# =========================

def download_db_from_github(
    owner: str,
    repo: str,
    path_in_repo: str,
    branch: str,
    local_db_path: str,
    token: Optional[str] = None,
    return_sha: bool = False
) -> Union[bool, Tuple[bool, Optional[str]]]:
    """
    Baixa um arquivo binário do repositório GitHub (Contents API) e salva em 'local_db_path'.
    Se o arquivo não existir no repo/branch, retorna False (e None se return_sha=True).
    Parâmetros:
        - owner, repo, path_in_repo, branch: localização do arquivo no GitHub
        - local_db_path: caminho local para salvar
        - token: opcional; se não informado, tenta st.secrets["GITHUB_TOKEN"] ou os.environ
        - return_sha: se True, retorna (downloaded, remote_sha); senão, retorna apenas 'bool'
    """
    token = _resolve_token(token)
    url = f"https://api.github.com/repos/{owner}/{repo}/contents/{path_in_repo}?ref={branch}"
    status, content = _http_get(url, _gh_headers(token))
    if status == 404:
        return (False, None) if return_sha else False
    if status != 200:
        # Falha (rede/permissão/etc.)
        return (False, None) if return_sha else False

    try:
        data = json.loads(content.decode("utf-8"))
    except Exception:
        return (False, None) if return_sha else False

    # 'content' vem base64; 'sha' contém a versão atual do blob
    content_b64 = data.get("content")
    if not content_b64:
        return (False, None) if return_sha else False

    blob = base64.b64decode(content_b64)
    os.makedirs(os.path.dirname(local_db_path), exist_ok=True)
    with open(local_db_path, "wb") as f:
        f.write(blob)

    sha = data.get("sha")
    return (True, sha) if return_sha else True


# =========================
# Upload do .db (Contents API)
# =========================

def upload_db_to_github(
    owner: str,
    repo: str,
    path_in_repo: str,
    branch: str,
    local_db_path: str,
    commit_message: str,
    token: Optional[str] = None,
    prev_sha: Optional[str] = None,
    _return_details: bool = False
) -> Union[bool, Tuple[bool, Optional[str], int]]:
    """
    Faz upload (PUT) do arquivo local para o GitHub (Contents API).
    - Se 'prev_sha' for informado, evita overwrite cego: GitHub valida se a base é a versão esperada.
    - Retornos:
        * Por padrão (_return_details=False): bool (True se OK, False caso contrário)
        * Se _return_details=True: (ok: bool, new_sha: Optional[str], status_code: int)

    Observação: Contents API aceita:
      {
        "message": "...",
        "content": "<base64>",
        "branch": "...",
        "sha": "<sha atual>"    # obrigatório para update; omita para criar novo arquivo
      }
    """
    token = _resolve_token(token)
    url = f"https://api.github.com/repos/{owner}/{repo}/contents/{path_in_repo}"

    if not os.path.exists(local_db_path):
        return (False, None, 0) if _return_details else False

    with open(local_db_path, "rb") as f:
        b64 = base64.b64encode(f.read()).decode("utf-8")

    payload = {
        "message": commit_message,
        "content": b64,
        "branch": branch,
    }
    if prev_sha:
        payload["sha"] = prev_sha

    status, content = _http_put_json(url, _gh_headers(token), payload)
    if status in (200, 201):
        try:
            data = json.loads(content.decode("utf-8"))
            new_sha = (data.get("content") or {}).get("sha")
        except Exception:
            new_sha = None
        return (True, new_sha, status) if _return_details else True

    # Falha
    return (False, None, status) if _return_details else False


# =========================
# Merge de bancos SQLite (local ↔ remoto)
# =========================

def merge_sqlite_dbs(local_path: str, remote_path: str, output_path: str) -> None:
    """
    Cria um banco de saída (output_path) mesclando o remoto com o local.
    Estratégia:
    - Copia o remoto para 'output_path'
    - ATTACH DATABASE 'local' e executa UPSERT tabela-a-tabela com as regras:
        * pacientes_unicos_por_dia_prestador: ultima escrita vence para (Aviso, Convenio, Quarto)
        * procedimento_tipos / cirurgia_situacoes: por 'nome' (UNIQUE) — atualiza (ativo, ordem)
        * cirurgias: last-write-wins por 'updated_at' (se existir), respeitando UNIQUE da chave
    """
    from sqlalchemy import create_engine, text

    # Copia remoto -> output
    shutil.copyfile(remote_path, output_path)

    eng = create_engine(f"sqlite:///{output_path}", future=True)
    with eng.begin() as conn:
        # Anexar banco local
        conn.execute(text(f"ATTACH DATABASE '{local_path}' AS localdb;"))

        # 1) Base de pacientes
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

        # 2) Catálogo de tipos
        conn.execute(text("""
            INSERT INTO procedimento_tipos (nome, ativo, ordem)
            SELECT nome, ativo, ordem FROM localdb.procedimento_tipos
            ON CONFLICT(nome) DO UPDATE SET ativo=excluded.ativo, ordem=excluded.ordem;
        """))

        # 3) Catálogo de situações
        conn.execute(text("""
            INSERT INTO cirurgia_situacoes (nome, ativo, ordem)
            SELECT nome, ativo, ordem FROM localdb.cirurgia_situacoes
            ON CONFLICT(nome) DO UPDATE SET ativo=excluded.ativo, ordem=excluded.ordem;
        """))

        # 4) Cirurgias — preferir updated_at mais recente
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


# =========================
# Upload seguro com merge automático
# =========================

def safe_upload_with_merge(
    owner: str,
    repo: str,
    path_in_repo: str,
    branch: str,
    local_db_path: str,
    commit_message: str,
    token: Optional[str] = None,
    prev_sha: Optional[str] = None
) -> bool:
    """
    Tenta upload com 'prev_sha'. Se houver conflito (ex.: sha mudou no remoto),
    faz o seguinte:
      1) Baixa remoto atual (pega 'remote_sha2')
      2) Mescla local+remoto em um arquivo temporário (merge_sqlite_dbs)
      3) Substitui o local pelo mesclado
      4) Reenvia com prev_sha atualizado

    Retorna True se concluir a sincronização; False caso contrário.
    """
    # 1) Tentativa inicial de upload
    ok, new_sha, status = upload_db_to_github(
        owner, repo, path_in_repo, branch, local_db_path, commit_message,
        token=token, prev_sha=prev_sha, _return_details=True
    )
    if ok and new_sha:
        return True

    # 2) Conflito? (sha mudou ou outra condição de 409)
    if status == 409:
        # Baixa remoto atualizado (para merge)
        tmp_remote = tempfile.NamedTemporaryFile(prefix="remote_", suffix=".db", delete=False)
        tmp_remote_path = tmp_remote.name
        tmp_remote.close()

        downloaded, remote_sha2 = download_db_from_github(
            owner, repo, path_in_repo, branch, tmp_remote_path, token=token, return_sha=True
        )
        if not downloaded:
            # Não foi possível baixar o remoto; aborta
            try:
                os.unlink(tmp_remote_path)
            except Exception:
                pass
            return False

        # Gera banco mesclado
        tmp_merged = tempfile.NamedTemporaryFile(prefix="merged_", suffix=".db", delete=False)
        tmp_merged_path = tmp_merged.name
        tmp_merged.close()

        try:
            merge_sqlite_dbs(local_db_path, tmp_remote_path, tmp_merged_path)
            # Substitui local
            shutil.move(tmp_merged_path, local_db_path)
        except Exception:
            # Falha no merge
            try:
                os.unlink(tmp_merged_path)
            except Exception:
                pass
            try:
                os.unlink(tmp_remote_path)
            except Exception:
                pass
            return False

        # Reenvia com sha do remoto atualizado
        ok2, new_sha2, status2 = upload_db_to_github(
            owner, repo, path_in_repo, branch, local_db_path,
            f"{commit_message} (merge automático)", token=token, prev_sha=remote_sha2,
            _return_details=True
        )

        # Limpeza de temporários
        try:
            os.unlink(tmp_remote_path)
        except Exception:
            pass

        return bool(ok2 and new_sha2)

    # 3) Outros erros
    return False
