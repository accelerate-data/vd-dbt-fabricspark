"""Git clone helper for dbt projects inside Fabric notebooks.

Authenticates via GitHub App. All three credentials (app ID, installation
ID, PEM key) are stored as Azure Key Vault secrets and fetched at runtime.
A short-lived installation access token is obtained and used for cloning.
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

    All three GitHub App credentials are Key Vault **secret names** — the
    actual values are fetched from Azure Key Vault at runtime via
    ``notebookutils.credentials.getSecret()``.

    Attributes:
        url: Repository HTTPS URL (e.g. "https://github.com/org/repo").
        branch: Branch to clone. Defaults to "main".
        github_app_id: Key Vault secret name for the GitHub App ID.
        github_installation_id: Key Vault secret name for the installation ID.
        github_pem_secret: Key Vault secret name for the PEM private key.
        vault_url: Azure Key Vault URL.
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
      3. No auth (public repo — tried automatically if credentials not provided)

    Args:
        repo: Git repository coordinates and auth config.
        clone_dir: Local path to clone into. Defaults to /tmp/dbt_project.

    Returns:
        The absolute path of the cloned project directory.

    Raises:
        ValueError: If repo.url is empty.
        RuntimeError: If clone fails (with guidance on providing credentials).
    """
    if not repo.url:
        raise ValueError("repo.url is required")

    # Clean previous clone
    shutil.rmtree(clone_dir, ignore_errors=True)
    os.makedirs(os.path.dirname(clone_dir), exist_ok=True)

    # Resolve token (returns empty string if no credentials provided)
    token = _resolve_token(repo)

    if token:
        clone_url = repo.url.replace(
            "https://", f"https://x-access-token:{token}@"
        )
    else:
        # No credentials — try as public repo
        print("No GitHub credentials provided, attempting public clone")
        clone_url = repo.url

    clone_args = ["git", "clone", "--depth", "1"]
    if repo.branch:
        clone_args.extend(["-b", repo.branch])
    clone_args.extend([clone_url, clone_dir])

    result = subprocess.run(
        clone_args,
        capture_output=True,
        text=True,
        timeout=CLONE_TIMEOUT_SECONDS,
    )

    if result.returncode != 0:
        if token:
            # Auth was provided but clone still failed
            raise RuntimeError(
                f"git clone failed with authenticated URL.\n"
                f"stderr: {result.stderr.strip()}"
            )
        else:
            # No auth was provided and clone failed — likely a private repo
            raise RuntimeError(
                f"git clone failed for {repo.url}. If this is a private repository, "
                f"provide GitHub App credentials (github_app_id, "
                f"github_installation_id, github_pem_secret, vault_url) or "
                f"a pre-resolved token.\n"
                f"stderr: {result.stderr.strip()}"
            )

    print(f"Cloned {repo.url} (branch: {repo.branch}) into {clone_dir}")
    return clone_dir


def _resolve_token(repo: RepoConfig) -> str:
    """Resolve a GitHub installation access token.

    Returns an empty string if no credentials are configured,
    allowing the caller to attempt an unauthenticated clone.
    """
    if repo.token:
        print("Using pre-resolved GitHub token")
        return repo.token

    if not (
        repo.github_app_id
        and repo.github_installation_id
        and repo.github_pem_secret
        and repo.vault_url
    ):
        return ""

    # All three credentials are Key Vault secret names — fetch actual values
    print("Fetching GitHub App credentials from Key Vault")
    app_id = _get_secret_from_key_vault(repo.vault_url, repo.github_app_id)
    installation_id = _get_secret_from_key_vault(
        repo.vault_url, repo.github_installation_id
    )
    pem_key = _get_secret_from_key_vault(
        repo.vault_url, repo.github_pem_secret
    )

    print(f"GitHub App ID resolved (secret: {repo.github_app_id})")

    print("Generating GitHub App JWT")
    jwt_token = _create_github_app_jwt(app_id, pem_key)

    print("Exchanging JWT for installation access token")
    return _get_installation_token(installation_id, jwt_token)


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
