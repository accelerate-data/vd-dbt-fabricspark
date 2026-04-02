"""Git clone helper for dbt projects inside Fabric notebooks.

Authenticates via GitHub App (app ID + installation ID + PEM key from
Azure Key Vault) to obtain a short-lived installation token.
"""

from __future__ import annotations

import os
import shutil
import subprocess
import time
from dataclasses import dataclass

import jwt  # PyJWT
import requests


CLONE_DIR = "/tmp/dbt_project"
CLONE_TIMEOUT_SECONDS = 120
JWT_EXPIRY_SECONDS = 600  # GitHub allows max 10 minutes


@dataclass
class RepoConfig:
    """Git repository coordinates and GitHub App credentials.

    Authentication uses a GitHub App: the PEM private key is fetched from
    Azure Key Vault at runtime, used to mint a JWT, which is exchanged for
    a short-lived installation access token.

    Attributes:
        url: Repository HTTPS URL (e.g. "https://github.com/org/repo").
        branch: Branch to clone. Defaults to "main".
        github_app_id: GitHub App ID.
        github_installation_id: GitHub App installation ID.
        github_pem_secret: Key Vault secret name that holds the PEM private key.
        vault_url: Azure Key Vault URL for fetching the PEM key.
        token: Pre-resolved token (optional — skips GitHub App flow if set).
    """

    url: str
    branch: str = "main"
    github_app_id: str = ""
    github_installation_id: str = ""
    github_pem_secret: str = ""
    vault_url: str = ""
    token: str = ""


def clone_repo(
    repo: RepoConfig,
    clone_dir: str = CLONE_DIR,
) -> str:
    """Clone a dbt project from Git into a local directory.

    Token resolution order:
      1. repo.token (pre-resolved — for testing or direct injection)
      2. GitHub App flow (app_id + installation_id + PEM from Key Vault)

    Args:
        repo: Git repository coordinates and auth config.
        clone_dir: Local path to clone into. Defaults to /tmp/dbt_project.

    Returns:
        The absolute path of the cloned project directory.

    Raises:
        ValueError: If repo.url is empty or auth config is incomplete.
        subprocess.CalledProcessError: If git clone fails.
    """
    if not repo.url:
        raise ValueError("repo.url is required")

    # Clean previous clone
    shutil.rmtree(clone_dir, ignore_errors=True)
    os.makedirs(os.path.dirname(clone_dir), exist_ok=True)

    # Resolve token
    token = _resolve_token(repo)

    auth_url = repo.url.replace(
        "https://", f"https://x-access-token:{token}@"
    )

    clone_args = ["git", "clone", "--depth", "1"]
    if repo.branch:
        clone_args.extend(["-b", repo.branch])
    clone_args.extend([auth_url, clone_dir])

    subprocess.run(
        clone_args,
        capture_output=True,
        text=True,
        timeout=CLONE_TIMEOUT_SECONDS,
        check=True,
    )
    print(f"Cloned {repo.url} (branch: {repo.branch}) into {clone_dir}")
    return clone_dir


def _resolve_token(repo: RepoConfig) -> str:
    """Resolve a GitHub installation access token."""
    if repo.token:
        print("Using pre-resolved GitHub token")
        return repo.token

    if not (
        repo.github_app_id
        and repo.github_installation_id
        and repo.github_pem_secret
        and repo.vault_url
    ):
        raise ValueError(
            "GitHub App auth requires: github_app_id, github_installation_id, "
            "github_pem_secret, and vault_url"
        )

    print("Fetching GitHub App PEM key from Key Vault")
    pem_key = _get_secret_from_key_vault(repo.vault_url, repo.github_pem_secret)

    print("Generating GitHub App JWT")
    jwt_token = _create_github_app_jwt(repo.github_app_id, pem_key)

    print("Exchanging JWT for installation access token")
    return _get_installation_token(repo.github_installation_id, jwt_token)


def _get_secret_from_key_vault(vault_url: str, secret_name: str) -> str:
    """Fetch a secret from Azure Key Vault via notebookutils."""
    # notebookutils is available in Fabric notebook runtime
    import notebookutils  # type: ignore[import-untyped]

    return notebookutils.credentials.getSecret(vault_url, secret_name)


def _create_github_app_jwt(app_id: str, pem_key: str) -> str:
    """Create a JWT signed with the GitHub App's private key."""
    now = int(time.time())
    payload = {
        "iat": now - 60,  # issued at (60s clock skew allowance)
        "exp": now + JWT_EXPIRY_SECONDS,
        "iss": app_id,
    }
    return jwt.encode(payload, pem_key, algorithm="RS256")


def _get_installation_token(installation_id: str, jwt_token: str) -> str:
    """Exchange a GitHub App JWT for an installation access token."""
    token_url = (
        f"https://api.github.com/app/installations/"
        f"{installation_id}/access_tokens"
    )
    headers = {
        "Authorization": f"Bearer {jwt_token}",
        "Accept": "application/vnd.github+json",
    }

    response = requests.post(token_url, headers=headers, timeout=30)
    response.raise_for_status()

    return response.json()["token"]
