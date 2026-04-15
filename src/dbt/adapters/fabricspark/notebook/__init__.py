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
from dataclasses import dataclass, field
from typing import List, Union

from .environment import ConnectionConfig, setup_environment
from .repo import RepoConfig, clone_repo
from .runner import DbtResult, run_dbt


@dataclass
class DbtJobConfig:
    """Complete configuration for a dbt notebook job.

    Attributes:
        command: A single dbt command string or a list of commands to run
            sequentially. E.g. "dbt run --select tag:orders" or
            ["dbt deps", "dbt build --select alpha_model --target ephemeral"].
        repo: Git repository coordinates and GitHub App auth config.
        connection: Fabric Lakehouse connection coordinates.
    """

    command: Union[str, List[str]]
    repo: RepoConfig
    connection: ConnectionConfig

    @property
    def commands(self) -> List[str]:
        """Normalize command to a list."""
        if isinstance(self.command, str):
            return [self.command]
        return self.command


def run_dbt_job(config: DbtJobConfig) -> DbtResult:
    """Execute a complete dbt job lifecycle.

    Steps:
      1. Set environment variables for dbt profiles.yml
      2. Clone the dbt project from Git (auth via GitHub App)
      3. Execute dbt command(s) sequentially with logging

    When multiple commands are provided, they run in order. If any
    command fails, execution stops and the failed result is returned.

    Args:
        config: Complete job configuration.

    Returns:
        DbtResult from the last executed command.
    """
    commands = config.commands

    # Derive job name from the first command for logging
    job_name = _derive_job_name(commands[0])

    # 1. Environment setup
    setup_environment(config.connection, job_name)

    # 2. Clone repo
    project_dir = clone_repo(config.repo)

    # 3. Execute dbt command(s)
    result = None
    for i, cmd in enumerate(commands):
        if len(commands) > 1:
            print(f"\n--- Running command {i + 1}/{len(commands)}: {cmd} ---")
        result = run_dbt(cmd, project_dir)

    return result


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
