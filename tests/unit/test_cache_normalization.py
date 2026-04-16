"""Unit tests for cross-schema cache-key normalization methods.

Tests _normalize_workspace_database and _normalize_schema_parts from
FabricSparkAdapter, plus the cache normalization logic in list_relations.
"""

from unittest.mock import MagicMock, PropertyMock, patch

import pytest

from dbt.adapters.fabricspark.impl import FabricSparkAdapter


def _make_adapter_mock():
    """Create a minimal mock that binds the real normalization methods."""
    adapter = MagicMock(spec=FabricSparkAdapter)
    adapter._normalize_workspace_database = (
        FabricSparkAdapter._normalize_workspace_database.__get__(adapter)
    )
    adapter._normalize_schema_parts = (
        FabricSparkAdapter._normalize_schema_parts.__get__(adapter)
    )
    return adapter


# ---------------------------------------------------------------------------
# _normalize_workspace_database tests
# ---------------------------------------------------------------------------


class TestNormalizeWorkspaceDatabase:
    def setup_method(self):
        self.adapter = _make_adapter_mock()

    def test_normalize_workspace_database_two_part(self):
        """'sampledata.salesforce' strips the workspace prefix."""
        result = self.adapter._normalize_workspace_database("sampledata.salesforce")
        assert result == "salesforce"

    def test_normalize_workspace_database_single(self):
        """Single-part database name is returned unchanged."""
        result = self.adapter._normalize_workspace_database("salesforce")
        assert result == "salesforce"

    def test_normalize_workspace_database_none(self):
        """None is returned as-is."""
        result = self.adapter._normalize_workspace_database(None)
        assert result is None

    def test_normalize_workspace_database_empty(self):
        """Empty string is a falsy passthrough."""
        result = self.adapter._normalize_workspace_database("")
        assert result == ""

    def test_normalize_workspace_database_with_backticks(self):
        """Backtick-quoted two-part name strips workspace prefix and backticks."""
        result = self.adapter._normalize_workspace_database("`sampledata`.`salesforce`")
        assert result == "salesforce"


# ---------------------------------------------------------------------------
# _normalize_schema_parts tests
# ---------------------------------------------------------------------------


class TestNormalizeSchemaparts:
    def setup_method(self):
        self.adapter = _make_adapter_mock()

    def test_normalize_schema_parts_three_part(self):
        """'workspace.lakehouse.schema' returns (lakehouse, schema)."""
        db, schema = self.adapter._normalize_schema_parts("workspace.lakehouse.schema")
        assert db == "lakehouse"
        assert schema == "schema"

    def test_normalize_schema_parts_two_part(self):
        """'lakehouse.schema' returns (lakehouse, schema)."""
        db, schema = self.adapter._normalize_schema_parts("lakehouse.schema")
        assert db == "lakehouse"
        assert schema == "schema"

    def test_normalize_schema_parts_one_part(self):
        """Single-part 'schema' returns (None, schema)."""
        db, schema = self.adapter._normalize_schema_parts("schema")
        assert db is None
        assert schema == "schema"

    def test_normalize_schema_parts_none(self):
        """None input returns (None, None)."""
        db, schema = self.adapter._normalize_schema_parts(None)
        assert db is None
        assert schema is None

    def test_normalize_schema_parts_backtick_stripped(self):
        """Backtick-quoted parts are stripped."""
        db, schema = self.adapter._normalize_schema_parts("`lakehouse`.`dbo`")
        assert db == "lakehouse"
        assert schema == "dbo"


# ---------------------------------------------------------------------------
# list_relations cache normalization test
# ---------------------------------------------------------------------------


class TestListRelationsCacheNormalization:
    """Verify that list_relations uses the full database string as the cache
    key while querying Spark with the normalized (short) database name."""

    def test_list_relations_normalizes_cache_database(self):
        """When database='ws.lakehouse', cache should key on 'ws.lakehouse'
        but the schema_relation used for the query should use 'lakehouse'."""
        adapter = _make_adapter_mock()

        # Bind list_relations to the mock so it runs the real method.
        adapter.list_relations = FabricSparkAdapter.list_relations.__get__(adapter)

        # _schema_enabled returns False so we skip the lakehouse-fallback branch.
        adapter._schema_enabled.return_value = False

        # _schema_is_cached returns False so we go through the query path.
        adapter._schema_is_cached.return_value = False

        # Prepare a fake relation returned by Relation.create().without_identifier().
        fake_schema_relation = MagicMock()
        fake_relation_created = MagicMock()
        fake_relation_created.without_identifier.return_value = fake_schema_relation
        adapter.Relation.create.return_value = fake_relation_created

        # list_relations_without_caching returns a single fake relation
        # whose database will be the short form (as Spark returns it).
        fake_relation = MagicMock()
        fake_relation.type = "table"
        fake_incorporated = MagicMock()
        fake_relation.incorporate.return_value = fake_incorporated
        adapter.list_relations_without_caching.return_value = [fake_relation]

        # Provide a truthy cache object.
        adapter.cache = MagicMock()
        adapter.config = MagicMock()

        # Call with a two-part database (workspace.lakehouse).
        result = adapter.list_relations(database="ws.lakehouse", schema="dbo")

        # The Relation.create call should use the *normalized* database for
        # querying Spark.
        create_kwargs = adapter.Relation.create.call_args
        assert create_kwargs.kwargs.get("database") == "lakehouse" or (
            create_kwargs[1].get("database") == "lakehouse"
        )

        # Because cache_database ('ws.lakehouse') != query_database ('lakehouse'),
        # the relation should be incorporated with the full cache_database.
        fake_relation.incorporate.assert_called_once()
        inc_kwargs = fake_relation.incorporate.call_args
        assert inc_kwargs.kwargs.get("path") == {"database": "ws.lakehouse"} or (
            inc_kwargs[1].get("path") == {"database": "ws.lakehouse"}
        )

        # The incorporated relation (with full database) should be added to the cache.
        adapter.cache.add.assert_called_once_with(fake_incorporated)

        # _schema_is_cached should have been called with the original full database.
        adapter._schema_is_cached.assert_called_once_with("ws.lakehouse", "dbo")
