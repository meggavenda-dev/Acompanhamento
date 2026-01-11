
# github_sync.py
import base64
import os
import json
import requests
import streamlit as st

def _gh_headers():
    token = st.secrets.get("GITHUB_TOKEN", "")
    if not token:
        raise RuntimeError("GITHUB_TOKEN não encontrado em st.secrets.")
    return {
        "Authorization": f"token {token}",
        "Accept": "application/vnd.github+json"
    }

def _normalize_repo_path(path_in_repo: str) -> str:
    """
    Normaliza o 'path' exigido pela API /repos/{owner}/{repo}/contents/{path}:
    - não pode começar com '/', './' ou '.\\'
    - remove espaços nas pontas
    """
    if not path_in_repo:
        raise ValueError("path_in_repo vazio.")
    p = str(path_in_repo).strip()
    # remove prefixos indesejados
    while p.startswith("/") or p.startswith("./") or p.startswith(".\\") or p.startswith("\\"):
        if p.startswith("./"):
            p = p[2:]
        elif p.startswith(".\\"):
            p = p[3:]
        else:
            p = p[1:]
    if p == "":
        raise ValueError("path_in_repo inválido após normalização.")
    return p

def download_db_from_github(owner: str, repo: str, path_in_repo: str, branch: str, local_db_path: str) -> bool:
    """
    Baixa (GET contents) o arquivo SQLite do GitHub e grava em local_db_path.
    Retorna True se baixou, False se não existe no repo.
    """
    path_norm = _normalize_repo_path(path_in_repo)
    url = f"https://api.github.com/repos/{owner}/{repo}/contents/{path_norm}?ref={branch}"
    headers = _gh_headers()
    r = requests.get(url, headers=headers)

    if r.status_code == 200:
        data = r.json()
        if "content" not in data:
            raise RuntimeError(f"Resposta inesperada da API do GitHub (sem 'content'): {json.dumps(data)[:300]}")
        content_b64 = data["content"].replace("\n", "")
        content_bytes = base64.b64decode(content_b64)

        os.makedirs(os.path.dirname(local_db_path), exist_ok=True)
        with open(local_db_path, "wb") as f:
            f.write(content_bytes)
        return True

    elif r.status_code == 404:
        # Arquivo ainda não existe no repo
        return False
    else:
        raise RuntimeError(f"Falha ao baixar do GitHub: {r.status_code} - {r.text}")

def upload_db_to_github(owner: str, repo: str, path_in_repo: str, branch: str, local_db_path: str, commit_message: str = "Atualiza banco SQLite via app") -> bool:
    """
    Sobe (PUT contents) o arquivo SQLite para o GitHub (commit/push).
    Se o arquivo já existir, passa o 'sha' para atualizar.
    Retorna True em sucesso.
    """
    if not os.path.exists(local_db_path):
        raise FileNotFoundError(f"Arquivo local não encontrado: {local_db_path}")

    path_norm = _normalize_repo_path(path_in_repo)
    url = f"https://api.github.com/repos/{owner}/{repo}/contents/{path_norm}"
    headers = _gh_headers()

    # Obtém o SHA atual (se existir)
    r_get = requests.get(url, headers=headers, params={"ref": branch})
    sha = r_get.json().get("sha") if r_get.status_code == 200 else None

    with open(local_db_path, "rb") as f:
        content_bytes = f.read()
    content_b64 = base64.b64encode(content_bytes).decode("utf-8")

    payload = {
        "message": commit_message,
        "content": content_b64,
        "branch": branch,
        "committer": {"name": "Streamlit App", "email": "streamlit@app.local"},
    }
    if sha:
        payload["sha"] = sha

    r_put = requests.put(url, headers=headers, json=payload)
    if r_put.status_code in (200, 201):
        return True
    else:
        raise RuntimeError(f"Falha ao subir para GitHub: {r_put.status_code} - {r_put.text}")
