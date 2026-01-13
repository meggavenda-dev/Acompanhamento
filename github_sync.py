
# -*- coding: utf-8 -*-
"""
github_sync.py — Sincronização do arquivo SQLite (.db) com o GitHub (Contents API),
com controle de versão via 'sha' e merge automático em caso de conflito.

Funcionalidades:
- download_db_from_github(..., return_sha=False) -> bool ou (bool, sha)
- upload_db_to_github(..., prev_sha=None, _return_details=False) -> bool ou (ok, new_sha, status)
- safe_upload_with_merge(..., _return_details=False) -> bool ou (ok, status, message)

Dependências:
- requests (opcional; cai para urllib se ausente)
- sqlalchemy (usada pelo módulo db_merge.py) — apenas no merge
"""

import base64
import json
import os
import shutil
import tempfile
from typing import Optional, Tuple, Union

# Importa a função de merge do módulo externo
from db_merge import merge_sqlite_dbs

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
        "User-Agent": "github-sync-sqlite/1.1",
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
    """
    token = _resolve_token(token)
    url = f"https://api.github.com/repos/{owner}/{repo}/contents/{path_in_repo}?ref={branch}"
    status, content = _http_get(url, _gh_headers(token))
    if status == 404:
        return (False, None) if return_sha else False
    if status != 200:
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
    - Se 'prev_sha' for informado, evita overwrite cego.
    - Retornos:
        * _return_details=False: bool (True se OK, False caso contrário)
        * _return_details=True: (ok: bool, new_sha: Optional[str], status_code: int)
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
    prev_sha: Optional[str] = None,
    _return_details: bool = False
) -> Union[bool, Tuple[bool, int, str]]:
    """
    Tenta upload com 'prev_sha'. Em conflito (409):
      1) Baixa remoto (pega 'remote_sha2')
      2) Mescla local+remoto (merge_sqlite_dbs)
      3) Substitui local pelo mesclado
      4) Reenvia com prev_sha atualizado

    Retorno:
      - se _return_details=False: bool
      - se _return_details=True: (ok: bool, status: int, message: str)
    """
    # Tentativa inicial
    ok, new_sha, status = upload_db_to_github(
        owner, repo, path_in_repo, branch, local_db_path, commit_message,
        token=token, prev_sha=prev_sha, _return_details=True
    )
    if ok and new_sha:
        return (True, status, "Upload OK") if _return_details else True

    if status == 409:
        # Conflito → baixar remoto e mesclar
        tmp_remote = tempfile.NamedTemporaryFile(prefix="remote_", suffix=".db", delete=False)
        tmp_remote_path = tmp_remote.name
        tmp_remote.close()

        downloaded, remote_sha2 = download_db_from_github(
            owner, repo, path_in_repo, branch, tmp_remote_path, token=token, return_sha=True
        )
        if not downloaded:
            # não conseguiu baixar remoto
            try: os.unlink(tmp_remote_path)
            except Exception: pass
            msg = "Conflito 409, mas falha ao baixar remoto"
            return (False, 409, msg) if _return_details else False

        tmp_merged = tempfile.NamedTemporaryFile(prefix="merged_", suffix=".db", delete=False)
        tmp_merged_path = tmp_merged.name
        tmp_merged.close()

        try:
            merge_sqlite_dbs(local_db_path, tmp_remote_path, tmp_merged_path)
            # substitui local
            shutil.move(tmp_merged_path, local_db_path)
        except Exception as e:
            # Falha no merge
            try: os.unlink(tmp_merged_path)
            except Exception: pass
            try: os.unlink(tmp_remote_path)
            except Exception: pass
            msg = f"Falha no merge: {e}"
            return (False, 409, msg) if _return_details else False

        # Reenvia com sha atualizado do remoto
        ok2, new_sha2, status2 = upload_db_to_github(
            owner, repo, path_in_repo, branch, local_db_path,
            f"{commit_message} (merge automático)", token=token, prev_sha=remote_sha2,
            _return_details=True
        )

        try: os.unlink(tmp_remote_path)
        except Exception: pass

        if ok2 and new_sha2:
            return (True, status2, "Upload após merge OK") if _return_details else True

        return (False, status2, "Falha ao subir após merge") if _return_details else False

    # Outros erros
    return (False, status, "Falha inicial de upload") if _return_details else False
