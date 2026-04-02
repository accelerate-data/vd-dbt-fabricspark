"""Environment setup for dbt execution inside Fabric notebooks.

Sets the environment variables that profiles.yml reads via env_var().
"""

from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass
class ConnectionConfig:
    """Fabric Lakehouse connection coordinates."""

    lakehouse_name: str
    lakehouse_id: str
    workspace_id: str
    workspace_name: str = ""
    schema_name: str = "dbo"


def setup_environment(connection: ConnectionConfig, job_name: str) -> None:
    """Set environment variables required by dbt profiles.yml.

    Args:
        connection: Lakehouse connection coordinates.
        job_name: Human-readable job identifier for logging.
    """
    os.environ["LAKEHOUSE"] = connection.lakehouse_name
    os.environ["LAKEHOUSE_ID"] = connection.lakehouse_id
    os.environ["SCHEMA"] = connection.schema_name
    os.environ["WORKSPACE_ID"] = connection.workspace_id
    os.environ["WORKSPACE_NAME"] = connection.workspace_name
    os.environ["DBT_JOB_NAME"] = job_name

    print(
        f"Environment: LAKEHOUSE={connection.lakehouse_name}, "
        f"SCHEMA={connection.schema_name}, "
        f"WORKSPACE_ID={connection.workspace_id}, "
        f"WORKSPACE_NAME={connection.workspace_name}"
    )
