"""Fabric Notebook runtime helpers for dbt execution.

Provides a single high-level entry point ``run_dbt_job()`` that handles
environment setup, repo cloning, and dbt command execution with full
observability. Designed to be called from thin Fabric notebook wrappers.

Usage inside a Fabric notebook::

    from dbt.adapters.fabricspark.notebook import (
        run_dbt_job,
        DbtJobConfig,
        RepoConfig,
        ConnectionConfig,
    )

    config = DbtJobConfig(
        command="dbt run --select tag:orders",
        repo=RepoConfig(
            url=repo_url,
            branch=repo_branch,
            github_app_id=github_app_id,
            github_installation_id=github_installation_id,
            github_pem_secret=github_pem_secret,
            vault_url=vault_url,
        ),
        connection=ConnectionConfig(
            lakehouse_name=lakehouse_name,
            lakehouse_id=lakehouse_id,
            workspace_id=workspace_id,
            workspace_name=workspace_name,
            schema_name=schema_name,
        ),
    )

    result = run_dbt_job(config)
"""

from __future__ import annotations

import shlex
from dataclasses import dataclass

from .environment import ConnectionConfig, setup_environment
from .repo import RepoConfig, clone_repo
from .runner import DbtResult, run_dbt


@dataclass
class DbtJobConfig:
    """Complete configuration for a dbt notebook job.

    Attributes:
        command: Full dbt command string, e.g. "dbt run --select tag:orders".
        repo: Git repository coordinates and GitHub App auth config.
        connection: Fabric Lakehouse connection coordinates.
    """

    command: str
    repo: RepoConfig
    connection: ConnectionConfig


def run_dbt_job(config: DbtJobConfig) -> DbtResult:
    """Execute a complete dbt job lifecycle.

    Steps:
      1. Set environment variables for dbt profiles.yml
      2. Clone the dbt project from Git (auth via GitHub App)
      3. Execute the dbt command with logging and artifact persistence

    Args:
        config: Complete job configuration.

    Returns:
        DbtResult with execution details.
    """
    # Derive job name from the command for logging
    job_name = _derive_job_name(config.command)

    # 1. Environment setup
    setup_environment(config.connection, job_name)

    # 2. Clone repo
    project_dir = clone_repo(config.repo)

    # 3. Execute dbt
    return run_dbt(config.command, project_dir)


def _derive_job_name(command: str) -> str:
    """Derive a job name from the dbt command string.

    Examples:
        "dbt run --select tag:orders" -> "dbt_run_tag_orders"
        "dbt test" -> "dbt_test"
    """
    parts = shlex.split(command)
    # Take meaningful parts: subcommand + any selector values
    meaningful = [parts[0]] if parts else ["dbt"]
    if len(parts) > 1:
        meaningful.append(parts[1])  # subcommand

    # Extract selector value if present
    for i, part in enumerate(parts):
        if part in ("--select", "-s") and i + 1 < len(parts):
            meaningful.append(parts[i + 1])
            break

    return "_".join(meaningful).replace(":", "_").replace(" ", "_")


__all__ = [
    "ConnectionConfig",
    "DbtJobConfig",
    "DbtResult",
    "RepoConfig",
    "run_dbt_job",
]
