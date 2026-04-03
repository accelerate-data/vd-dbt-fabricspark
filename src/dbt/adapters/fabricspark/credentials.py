from dataclasses import dataclass, field
from typing import Any, Dict, Optional, Tuple

from dbt_common.exceptions import DbtRuntimeError

from dbt.adapters.contracts.connection import Credentials
from dbt.adapters.events.logging import AdapterLogger

logger = AdapterLogger("fabricspark")


@dataclass
class FabricSparkCredentials(Credentials):
    """Credentials for connecting to Microsoft Fabric Spark (OneLake).

    Fabric Spark SQL supports four-part naming for cross-workspace queries:
        workspace.lakehouse.schema.table

    Configuration modes:
    1. Two-part naming (standard lakehouse without schemas):
       - SQL: lakehouse.table
       - Config: schema=lakehouse_name

    2. Three-part naming (schema-enabled lakehouse):
       - SQL: lakehouse.schema.table
       - Config: lakehouse_schemas_enabled=True, lakehouse=name, schema=schema_name

    3. Four-part naming (cross-workspace queries):
       - SQL: workspace.lakehouse.schema.table
       - Use {{ source() }} or {{ ref() }} with workspace parameter
       - Native Spark SQL support, no shortcuts required
    """

    schema: Optional[str] = None  # type: ignore
    method: str = "livy"
    workspaceid: Optional[str] = None
    workspace_name: Optional[str] = (
        None  # Friendly name for current workspace (for four-part refs)
    )
    database: Optional[str] = None  # type: ignore
    lakehouse: Optional[str] = None
    lakehouseid: Optional[str] = None
    endpoint: str = "https://msitapi.fabric.microsoft.com/v1"
    client_id: Optional[str] = None
    client_secret: Optional[str] = None
    tenant_id: Optional[str] = None
    authentication: str = "az_cli"
    connect_retries: int = 1
    connect_timeout: int = 10
    create_shortcuts: Optional[bool] = False
    retry_all: bool = False
    shortcuts_json_str: Optional[str] = None
    lakehouse_schemas_enabled: bool = True
    accessToken: Optional[str] = None
    spark_config: Dict[str, Any] = field(default_factory=dict)
    reuse_livy_session: bool = False
    livy_session_path: Optional[str] = None
    session_id_file: Optional[str] = None  # Alias for livy_session_path (for compatibility)
    vdstudio_oauth_endpoint_url: Optional[str] = None

    @classmethod
    def __pre_deserialize__(cls, data: Any) -> Any:
        data = super().__pre_deserialize__(data)
        # database and lakehouse are synonyms — sync whichever is provided
        # priority: database > lakehouse
        db = data.get("database")
        lh = data.get("lakehouse")
        resolved = db or lh
        data["database"] = resolved
        data["lakehouse"] = resolved
        if resolved and "path" in data:
            data["path"]["database"] = resolved
            data["path"]["lakehouse"] = resolved
        return data

    @property
    def lakehouse_endpoint(self) -> str:
        return f"{self.endpoint}/workspaces/{self.workspaceid}/lakehouses/{self.lakehouseid}/livyapi/versions/2023-12-01"

    def __post_init__(self) -> None:
        if self.method is None:
            raise DbtRuntimeError("Must specify `method` in profile")
        if self.workspaceid is None:
            raise DbtRuntimeError("Must specify `workspaceid` (workspace GUID) in profile")
        if self.lakehouseid is None:
            raise DbtRuntimeError("Must specify `lakehouseid` (lakehouse GUID) in profile")
        if self.schema is None:
            raise DbtRuntimeError("Must specify `schema` in profile")

        # database and lakehouse are synonyms — keep them in sync
        resolved = self.database or self.lakehouse
        self.database = resolved
        self.lakehouse = resolved

        # If workspace_name is set, compose database as "workspace.lakehouse"
        # so relations render as four-part names: workspace.lakehouse.schema.table
        if self.workspace_name and self.database and "." not in self.database:
            self.database = f"{self.workspace_name}.{self.database}"
            self.lakehouse = self.database
            logger.debug(
                f"Workspace-qualified database: {self.database}"
            )

        # Schema-enabled lakehouse validation (three-part naming)
        if self.lakehouse_schemas_enabled:
            if self.database is None:
                raise DbtRuntimeError(
                    "Must specify `database` when lakehouse_schemas_enabled=True. "
                    "This enables three-part naming: lakehouse.schema.table"
                )
            logger.debug(
                f"Schema-enabled lakehouse: using three-part naming "
                f"{self.database}.{self.schema}.table"
            )
        else:
            # Standard lakehouse (two-part naming)
            if self.database is not None:
                raise DbtRuntimeError(
                    "database property is not supported for standard lakehouses. "
                    "Set database to None and use lakehouse/schema instead. "
                    "For three-part naming, set lakehouse_schemas_enabled=True."
                )

        # Validate spark_config fields
        required_keys = ["name"]
        for key in required_keys:
            if key not in self.spark_config:
                raise ValueError(f"Missing required key in spark_config: {key}")

    @property
    def type(self) -> str:
        return "fabricspark"

    @property
    def unique_field(self) -> str:
        return self.lakehouseid

    def _connection_keys(self) -> Tuple[str, ...]:
        return (
            "workspaceid",
            "workspace_name",
            "lakehouseid",
            "lakehouse",
            "endpoint",
            "schema",
        )

    def is_current_workspace(self, workspace_name: str) -> bool:
        """Check if a workspace name refers to the current workspace.

        Args:
            workspace_name: The friendly name or ID to check

        Returns:
            True if it matches the current workspace
        """
        if self.workspace_name and workspace_name == self.workspace_name:
            return True
        if workspace_name == self.workspaceid:
            return True
        return False
