from dataclasses import dataclass, field
from typing import Any, ClassVar, Dict, Optional, TypeVar

from dbt_common.utils import deep_merge

from dbt.adapters.base.relation import BaseRelation, Policy
from dbt.adapters.contracts.relation import HasQuoting, RelationConfig, RelationType
from dbt.adapters.events.logging import AdapterLogger

logger = AdapterLogger("fabricspark")

Self = TypeVar("Self", bound="BaseRelation")

# Valid RelationType values
_VALID_RELATION_TYPES = {t.value for t in RelationType}


@dataclass
class FabricSparkQuotePolicy(Policy):
    """Quote policy for Fabric Spark relations.

    OneLake uses backtick quoting for identifiers with special characters.
    By default, quoting is disabled for cleaner SQL generation.
    """

    database: bool = False
    schema: bool = False
    identifier: bool = False


@dataclass
class FabricSparkIncludePolicy(Policy):
    """Include policy for Fabric Spark relations (two-part naming).

    Fabric Spark SQL supports up to four-part naming:
        workspace.lakehouse.schema.table

    This policy is for standard two-part naming: lakehouse.table
    """

    database: bool = True
    schema: bool = True
    identifier: bool = True


@dataclass
class FabricSparkThreePartIncludePolicy(Policy):
    """Include policy for three-part naming: lakehouse.schema.table"""

    database: bool = True
    schema: bool = True
    identifier: bool = True


@dataclass
class FabricSparkFourPartIncludePolicy(Policy):
    """Include policy for four-part naming: workspace.lakehouse.schema.table

    Note: This requires custom rendering as dbt's BaseRelation only supports
    database.schema.identifier (three parts). The workspace is stored separately
    and prepended during render().
    """

    database: bool = True
    schema: bool = True
    identifier: bool = True


@dataclass(frozen=True, eq=False, repr=False)
class FabricSparkRelation(BaseRelation):
    """Relation class for Microsoft Fabric Spark (OneLake).

    Fabric Spark SQL supports four-part naming for cross-workspace queries:
        workspace.lakehouse.schema.table

    Supported naming patterns:
    1. Two-part (standard lakehouse without schemas):
       - SQL: lakehouse.table
       - dbt: schema=lakehouse, identifier=table

    2. Three-part (schema-enabled lakehouse):
       - SQL: lakehouse.schema.table
       - dbt: database=lakehouse, schema=schema, identifier=table

    3. Four-part (cross-workspace):
       - SQL: workspace.lakehouse.schema.table
       - dbt: workspace=workspace, database=lakehouse, schema=schema, identifier=table

    OneLake's OnelakeExternalCatalog requirements:
    - Only supports MANAGED, EXTERNAL, and MATERIALIZED_LAKE_VIEW table types
    - VIEWs are NOT supported
    """

    # Class-level flag set once by the connection manager after detecting schema support.
    # Controls the default include_policy for all relations created after it is set:
    #   True  → three-part naming: database.schema.identifier (lakehouse.schema.table)
    #   False → two-part naming:   schema.identifier (lakehouse.table)
    #
    # Macros can still override per-relation via .include(database=false, schema=false)
    # for temporary views which require unqualified identifiers.
    _schemas_enabled: ClassVar[bool] = False

    quote_policy: Policy = field(default_factory=lambda: FabricSparkQuotePolicy())
    include_policy: Policy = field(
        default_factory=lambda: FabricSparkIncludePolicy(
            database=FabricSparkRelation._schemas_enabled,
            schema=True,
            identifier=True,
        )
    )
    quote_character: str = "`"
    is_delta: Optional[bool] = None
    information: Optional[str] = None
    # Four-part naming: workspace for cross-workspace queries
    workspace: Optional[str] = None

    def incorporate(self, **kwargs):
        t = kwargs.get("type")

        # dbt/jinja may pass an Undefined sentinel (or "Undefined" as a string)
        if t == "Undefined" or getattr(t, "__class__", None).__name__ == "Undefined":
            kwargs = dict(kwargs)
            kwargs.pop("type", None)  # or: kwargs["type"] = None

        return super().incorporate(**kwargs)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "FabricSparkRelation":
        # Sanitize 'type' field: Jinja Undefined or invalid strings become None
        if "type" in data and data["type"] is not None:
            type_val = data["type"]
            if not isinstance(type_val, RelationType):
                type_str = str(type_val)
                if type_str not in _VALID_RELATION_TYPES:
                    logger.debug(
                        f"Replacing invalid relation type '{type_str}' with None"
                    )
                    data = dict(data)
                    data["type"] = None
        return super().from_dict(data)

    def __post_init__(self) -> None:
        # Validation is relaxed to allow flexible naming patterns
        pass

    def _render_part(self, part: Optional[str]) -> str:
        """Render a single part of the relation name with optional quoting."""
        if part is None:
            return ""
        if self.quote_policy.identifier:
            return f"{self.quote_character}{part}{self.quote_character}"
        return part

    def render(self) -> str:
        """Render the relation as a SQL identifier string.

        Supports:
        - Two-part: lakehouse.table (schema.identifier)
        - Three-part: lakehouse.schema.table (database.schema.identifier)
        - Four-part: workspace.lakehouse.schema.table (workspace.database.schema.identifier)
        """
        parts = []

        # Add workspace if present (four-part naming)
        if self.workspace:
            parts.append(self._render_part(self.workspace))
        # Support database="workspace.lakehouse" when workspace is not set.
        elif self.include_policy.database and self.database and "." in self.database:
            db_parts = [part for part in self.database.split(".") if part]
            if len(db_parts) == 2:
                for part in db_parts:
                    parts.append(self._render_part(part))
            else:
                parts.append(self._render_part(self.database))
        # Add database if included (three/four-part naming)
        elif self.include_policy.database and self.database:
            parts.append(self._render_part(self.database))

        # Add schema if included
        if self.include_policy.schema and self.schema:
            parts.append(self._render_part(self.schema))

        # Add identifier if included
        if self.include_policy.identifier and self.identifier:
            parts.append(self._render_part(self.identifier))

        return ".".join(parts)

    @classmethod
    def create_from(
        cls,
        quoting: HasQuoting,
        relation_config: RelationConfig,
        **kwargs,
    ) -> "FabricSparkRelation":
        quote_policy = kwargs.pop("quote_policy", {})

        config_quoting = relation_config.quoting_dict
        config_quoting.pop("column", None)

        catalog_name = (
            relation_config.catalog_name
            if hasattr(relation_config, "catalog_name")
            else relation_config.config.get("catalog", None)  # type: ignore
        )

        # precedence: kwargs quoting > relation config quoting > base quoting > default quoting
        quote_policy = deep_merge(
            cls.get_default_quote_policy().to_dict(omit_none=True),
            quoting.quoting,
            config_quoting,
            quote_policy,
        )

        return cls.create(
            database=relation_config.database,
            schema=relation_config.schema,
            identifier=relation_config.identifier,
            quote_policy=quote_policy,
            catalog_name=catalog_name,
            **kwargs,
        )

    @classmethod
    def create(
        cls,
        database: Optional[str] = None,
        schema: Optional[str] = None,
        identifier: Optional[str] = None,
        type: Optional[str] = None,
        workspace: Optional[str] = None,
        **kwargs,
    ) -> "FabricSparkRelation":
        """Create a new FabricSparkRelation with support for four-part naming.

        Args:
            database: Lakehouse name (for three/four-part naming)
            schema: Schema name (or lakehouse name for two-part naming)
            identifier: Table name
            type: Relation type (table, view, etc.)
            workspace: Workspace name (for four-part naming)
            **kwargs: Additional arguments passed to parent create()

        Returns:
            A new FabricSparkRelation instance
        """
        # Determine the appropriate include policy based on naming pattern
        if workspace:
            # Four-part naming: workspace.lakehouse.schema.table
            include_policy = FabricSparkFourPartIncludePolicy()
        elif database and schema and database != schema:
            # Three-part naming: lakehouse.schema.table
            include_policy = FabricSparkThreePartIncludePolicy()
        else:
            # Two-part naming: lakehouse.table
            include_policy = FabricSparkIncludePolicy()

        kwargs["include_policy"] = include_policy
        kwargs["workspace"] = workspace

        return super().create(
            database=database,
            schema=schema,
            identifier=identifier,
            type=type,
            **kwargs,
        )

    @classmethod
    def get_default_include_policy(cls) -> Policy:
        """Return the default include policy (two-part naming)."""
        return FabricSparkIncludePolicy()

    @classmethod
    def get_three_part_include_policy(cls) -> Policy:
        """Return include policy for three-part naming."""
        return FabricSparkThreePartIncludePolicy()

    @classmethod
    def get_four_part_include_policy(cls) -> Policy:
        """Return include policy for four-part naming."""
        return FabricSparkFourPartIncludePolicy()

    @property
    def is_table(self) -> bool:
        """Check if this relation is a table type."""
        return self.type == RelationType.Table

    @property
    def is_view(self) -> bool:
        """Check if this relation is a view type.

        Note: OneLake does NOT support standard Spark VIEWs.
        """
        return self.type == RelationType.View

    @property
    def is_cross_workspace(self) -> bool:
        """Check if this relation references a different workspace (four-part naming)."""
        return self.workspace is not None

    @property
    def is_four_part(self) -> bool:
        """Check if this relation uses four-part naming."""
        return self.workspace is not None

    @property
    def is_three_part(self) -> bool:
        """Check if this relation uses three-part naming."""
        return (
            self.workspace is None
            and self.database is not None
            and self.schema is not None
            and self.database != self.schema
        )

    @property
    def is_two_part(self) -> bool:
        """Check if this relation uses two-part naming."""
        return not self.is_four_part and not self.is_three_part

    def without_workspace(self) -> "FabricSparkRelation":
        """Return a copy of this relation without the workspace component.

        Useful for operations that need to work within the current workspace context.
        """
        if not self.workspace:
            return self

        return FabricSparkRelation.create(
            database=self.database,
            schema=self.schema,
            identifier=self.identifier,
            type=self.type,
            is_delta=self.is_delta,
        )

    def with_workspace(self, workspace: str) -> "FabricSparkRelation":
        """Return a copy of this relation with a workspace component.

        Args:
            workspace: The workspace name to add

        Returns:
            A new FabricSparkRelation with four-part naming
        """
        return FabricSparkRelation.create(
            database=self.database,
            schema=self.schema,
            identifier=self.identifier,
            type=self.type,
            workspace=workspace,
            is_delta=self.is_delta,
        )


# Backwards compatibility aliases
FabricSparkSchemaEnabledIncludePolicy = FabricSparkThreePartIncludePolicy
