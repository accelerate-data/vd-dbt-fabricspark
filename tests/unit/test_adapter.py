import unittest
from unittest import mock

from agate import Row

import dbt.flags as flags
from dbt.adapters.contracts.relation import RelationType
from dbt.adapters.fabricspark import FabricSparkAdapter, FabricSparkRelation
from dbt.exceptions import DbtRuntimeError

from .utils import config_from_parts_or_dicts


class TestSparkAdapter(unittest.TestCase):
    def setUp(self):
        flags.STRICT_MODE = False

        self.project_cfg = {
            "name": "X",
            "version": "0.1",
            "profile": "test",
            "project-root": "/tmp/dbt/does-not-exist",
            "quoting": {
                "identifier": False,
                "schema": False,
            },
            "config-version": 2,
        }

    def _get_target_livy(self, project):
        return config_from_parts_or_dicts(
            project,
            {
                "outputs": {
                    "test": {
                        "type": "fabricspark",
                        "method": "livy",
                        "authentication": "CLI",
                        "schema": "dbtsparktest",
                        "lakehouse": "dbtsparktest",
                        "workspaceid": "1de8390c-9aca-4790-bee8-72049109c0f4",
                        "lakehouseid": "8c5bc260-bc3a-4898-9ada-01e433d461ba",
                        "connect_retries": 0,
                        "connect_timeout": 10,
                        "threads": 1,
                        "endpoint": "https://dailyapi.fabric.microsoft.com/v1",
                    }
                },
                "target": "test",
            },
        )

    def test_livy_connection(self):
        config = self._get_target_livy(self.project_cfg)
        adapter = FabricSparkAdapter(config)

        def fabric_spark_livy_connect(configuration):
            self.assertEqual(configuration.method, "livy")
            self.assertEqual(configuration.type, "fabricspark")

        # with mock.patch.object(hive, 'connect', new=hive_http_connect):
        with mock.patch(
            "dbt.adapters.fabricspark.livysession.LivySessionConnectionWrapper",
            new=fabric_spark_livy_connect,
        ):
            connection = adapter.acquire_connection("dummy")
            connection.handle  # trigger lazy-load

            self.assertEqual(connection.state, "open")
            self.assertIsNotNone(connection.handle)
            self.assertEqual(connection.credentials.authentication, "CLI")
            self.assertIsNone(connection.credentials.database)

    def test_parse_relation(self):
        self.maxDiff = None
        rel_type = FabricSparkRelation.get_relation_type.Table

        relation = FabricSparkRelation.create(
            schema="default_schema", identifier="mytable", type=rel_type
        )
        assert relation.database is None

        # Mimics the output of Spark with a DESCRIBE TABLE EXTENDED
        plain_rows = [
            ("col1", "decimal(22,0)"),
            (
                "col2",
                "string",
            ),
            ("dt", "date"),
            ("struct_col", "struct<struct_inner_col:string>"),
            ("# Partition Information", "data_type"),
            ("# col_name", "data_type"),
            ("dt", "date"),
            (None, None),
            ("# Detailed Table Information", None),
            ("Database", None),
            ("Owner", "root"),
            ("Created Time", "Wed Feb 04 18:15:00 UTC 1815"),
            ("Last Access", "Wed May 20 19:25:00 UTC 1925"),
            ("Type", "MANAGED"),
            ("Provider", "delta"),
            ("Location", "/mnt/vo"),
            ("Serde Library", "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"),
            ("InputFormat", "org.apache.hadoop.mapred.SequenceFileInputFormat"),
            ("OutputFormat", "org.apache.hadoop.hive.ql.io.HiveSequenceFileOutputFormat"),
            ("Partition Provider", "Catalog"),
        ]

        input_cols = [Row(keys=["col_name", "data_type"], values=r) for r in plain_rows]

        config = self._get_target_livy(self.project_cfg)
        rows = FabricSparkAdapter(config).parse_describe_extended(relation, input_cols)
        self.assertEqual(len(rows), 4)
        self.assertEqual(
            rows[0].to_column_dict(omit_none=False),
            {
                "table_database": None,
                "table_schema": relation.schema,
                "table_name": relation.name,
                "table_type": rel_type,
                "table_owner": "root",
                "column": "col1",
                "column_index": 0,
                "dtype": "decimal(22,0)",
                "numeric_scale": None,
                "numeric_precision": None,
                "char_size": None,
            },
        )

        self.assertEqual(
            rows[1].to_column_dict(omit_none=False),
            {
                "table_database": None,
                "table_schema": relation.schema,
                "table_name": relation.name,
                "table_type": rel_type,
                "table_owner": "root",
                "column": "col2",
                "column_index": 1,
                "dtype": "string",
                "numeric_scale": None,
                "numeric_precision": None,
                "char_size": None,
            },
        )

        self.assertEqual(
            rows[2].to_column_dict(omit_none=False),
            {
                "table_database": None,
                "table_schema": relation.schema,
                "table_name": relation.name,
                "table_type": rel_type,
                "table_owner": "root",
                "column": "dt",
                "column_index": 2,
                "dtype": "date",
                "numeric_scale": None,
                "numeric_precision": None,
                "char_size": None,
            },
        )

        self.assertEqual(
            rows[3].to_column_dict(omit_none=False),
            {
                "table_database": None,
                "table_schema": relation.schema,
                "table_name": relation.name,
                "table_type": rel_type,
                "table_owner": "root",
                "column": "struct_col",
                "column_index": 3,
                "dtype": "struct<struct_inner_col:string>",
                "numeric_scale": None,
                "numeric_precision": None,
                "char_size": None,
            },
        )

    def test_parse_relation_with_integer_owner(self):
        self.maxDiff = None
        rel_type = FabricSparkRelation.get_relation_type.Table

        relation = FabricSparkRelation.create(
            schema="default_schema", identifier="mytable", type=rel_type
        )
        assert relation.database is None

        # Mimics the output of Spark with a DESCRIBE TABLE EXTENDED
        plain_rows = [
            ("col1", "decimal(22,0)"),
            ("# Detailed Table Information", None),
            ("Owner", 1234),
        ]

        input_cols = [Row(keys=["col_name", "data_type"], values=r) for r in plain_rows]

        config = self._get_target_livy(self.project_cfg)
        rows = FabricSparkAdapter(config).parse_describe_extended(relation, input_cols)

        self.assertEqual(rows[0].to_column_dict().get("table_owner"), "1234")

    def test_parse_relation_with_statistics(self):
        self.maxDiff = None
        rel_type = FabricSparkRelation.get_relation_type.Table

        relation = FabricSparkRelation.create(
            schema="default_schema", identifier="mytable", type=rel_type
        )
        assert relation.database is None

        # Mimics the output of Spark with a DESCRIBE TABLE EXTENDED
        plain_rows = [
            ("col1", "decimal(22,0)"),
            ("# Partition Information", "data_type"),
            (None, None),
            ("# Detailed Table Information", None),
            ("Database", None),
            ("Owner", "root"),
            ("Created Time", "Wed Feb 04 18:15:00 UTC 1815"),
            ("Last Access", "Wed May 20 19:25:00 UTC 1925"),
            ("Statistics", "1109049927 bytes, 14093476 rows"),
            ("Type", "MANAGED"),
            ("Provider", "delta"),
            ("Location", "/mnt/vo"),
            ("Serde Library", "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"),
            ("InputFormat", "org.apache.hadoop.mapred.SequenceFileInputFormat"),
            ("OutputFormat", "org.apache.hadoop.hive.ql.io.HiveSequenceFileOutputFormat"),
            ("Partition Provider", "Catalog"),
        ]

        input_cols = [Row(keys=["col_name", "data_type"], values=r) for r in plain_rows]

        config = self._get_target_livy(self.project_cfg)
        rows = FabricSparkAdapter(config).parse_describe_extended(relation, input_cols)
        self.assertEqual(len(rows), 1)
        self.assertEqual(
            rows[0].to_column_dict(omit_none=False),
            {
                "table_database": None,
                "table_schema": relation.schema,
                "table_name": relation.name,
                "table_type": rel_type,
                "table_owner": "root",
                "column": "col1",
                "column_index": 0,
                "dtype": "decimal(22,0)",
                "numeric_scale": None,
                "numeric_precision": None,
                "char_size": None,
                "stats:bytes:description": "",
                "stats:bytes:include": True,
                "stats:bytes:label": "bytes",
                "stats:bytes:value": 1109049927,
                "stats:rows:description": "",
                "stats:rows:include": True,
                "stats:rows:label": "rows",
                "stats:rows:value": 14093476,
            },
        )

    def test_relation_with_database(self):
        config = self._get_target_livy(self.project_cfg)
        adapter = FabricSparkAdapter(config)
        # fine
        adapter.Relation.create(schema="different", identifier="table")
        with self.assertRaises(DbtRuntimeError):
            # not fine - database set
            adapter.Relation.create(database="something", schema="different", identifier="table")

    def test_profile_with_database(self):
        profile = {
            "outputs": {
                "test": {
                    "type": "fabricspark",
                    "method": "livy",
                    "authentication": "CLI",
                    "schema": "dbtsparktest",
                    # not allowed
                    "database": "dbtsparktest",
                    "lakehouse": "dbtsparktest",
                    "workspaceid": "1de8390c-9aca-4790-bee8-72049109c0f4",
                    "lakehouseid": "8c5bc260-bc3a-4898-9ada-01e433d461ba",
                    "connect_retries": 0,
                    "connect_timeout": 10,
                    "threads": 1,
                    "endpoint": "https://dailyapi.fabric.microsoft.com/v1",
                }
            },
            "target": "test",
        }
        with self.assertRaises(DbtRuntimeError):
            config_from_parts_or_dicts(self.project_cfg, profile)

    def test_parse_columns_from_information_with_table_type_and_delta_provider(self):
        self.maxDiff = None
        rel_type = FabricSparkRelation.get_relation_type.Table

        # Mimics the output of Spark in the information column
        information = (
            "Database: default_schema\n"
            "Table: mytable\n"
            "Owner: root\n"
            "Created Time: Wed Feb 04 18:15:00 UTC 1815\n"
            "Last Access: Wed May 20 19:25:00 UTC 1925\n"
            "Created By: Spark 3.0.1\n"
            "Type: MANAGED\n"
            "Provider: delta\n"
            "Statistics: 123456789 bytes\n"
            "Location: /mnt/vo\n"
            "Serde Library: org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe\n"
            "InputFormat: org.apache.hadoop.mapred.SequenceFileInputFormat\n"
            "OutputFormat: org.apache.hadoop.hive.ql.io.HiveSequenceFileOutputFormat\n"
            "Partition Provider: Catalog\n"
            "Partition Columns: [`dt`]\n"
            "Schema: root\n"
            " |-- col1: decimal(22,0) (nullable = true)\n"
            " |-- col2: string (nullable = true)\n"
            " |-- dt: date (nullable = true)\n"
            " |-- struct_col: struct (nullable = true)\n"
            " |    |-- struct_inner_col: string (nullable = true)\n"
        )
        relation = FabricSparkRelation.create(
            schema="default_schema", identifier="mytable", type=rel_type, information=information
        )

        config = self._get_target_livy(self.project_cfg)
        columns = FabricSparkAdapter(config).parse_columns_from_information(relation)
        self.assertEqual(len(columns), 4)
        self.assertEqual(
            columns[0].to_column_dict(omit_none=False),
            {
                "table_database": None,
                "table_schema": relation.schema,
                "table_name": relation.name,
                "table_type": rel_type,
                "table_owner": "root",
                "column": "col1",
                "column_index": 0,
                "dtype": "decimal(22,0)",
                "numeric_scale": None,
                "numeric_precision": None,
                "char_size": None,
                "stats:bytes:description": "",
                "stats:bytes:include": True,
                "stats:bytes:label": "bytes",
                "stats:bytes:value": 123456789,
            },
        )

        self.assertEqual(
            columns[3].to_column_dict(omit_none=False),
            {
                "table_database": None,
                "table_schema": relation.schema,
                "table_name": relation.name,
                "table_type": rel_type,
                "table_owner": "root",
                "column": "struct_col",
                "column_index": 3,
                "dtype": "struct",
                "numeric_scale": None,
                "numeric_precision": None,
                "char_size": None,
                "stats:bytes:description": "",
                "stats:bytes:include": True,
                "stats:bytes:label": "bytes",
                "stats:bytes:value": 123456789,
            },
        )

    def test_parse_columns_from_information_with_view_type(self):
        self.maxDiff = None
        rel_type = FabricSparkRelation.get_relation_type.View
        information = (
            "Database: default_schema\n"
            "Table: myview\n"
            "Owner: root\n"
            "Created Time: Wed Feb 04 18:15:00 UTC 1815\n"
            "Last Access: UNKNOWN\n"
            "Created By: Spark 3.0.1\n"
            "Type: VIEW\n"
            "View Text: WITH base (\n"
            "    SELECT * FROM source_table\n"
            ")\n"
            "SELECT col1, col2, dt FROM base\n"
            "View Original Text: WITH base (\n"
            "    SELECT * FROM source_table\n"
            ")\n"
            "SELECT col1, col2, dt FROM base\n"
            "View Catalog and Namespace: spark_catalog.default\n"
            "View Query Output Columns: [col1, col2, dt]\n"
            "Table Properties: [view.query.out.col.1=col1, view.query.out.col.2=col2, "
            "transient_lastDdlTime=1618324324, view.query.out.col.3=dt, "
            "view.catalogAndNamespace.part.0=spark_catalog, "
            "view.catalogAndNamespace.part.1=default]\n"
            "Serde Library: org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe\n"
            "InputFormat: org.apache.hadoop.mapred.SequenceFileInputFormat\n"
            "OutputFormat: org.apache.hadoop.hive.ql.io.HiveSequenceFileOutputFormat\n"
            "Storage Properties: [serialization.format=1]\n"
            "Schema: root\n"
            " |-- col1: decimal(22,0) (nullable = true)\n"
            " |-- col2: string (nullable = true)\n"
            " |-- dt: date (nullable = true)\n"
            " |-- struct_col: struct (nullable = true)\n"
            " |    |-- struct_inner_col: string (nullable = true)\n"
        )
        relation = FabricSparkRelation.create(
            schema="default_schema", identifier="myview", type=rel_type, information=information
        )

        config = self._get_target_livy(self.project_cfg)
        columns = FabricSparkAdapter(config).parse_columns_from_information(relation)
        self.assertEqual(len(columns), 4)
        self.assertEqual(
            columns[1].to_column_dict(omit_none=False),
            {
                "table_database": None,
                "table_schema": relation.schema,
                "table_name": relation.name,
                "table_type": rel_type,
                "table_owner": "root",
                "column": "col2",
                "column_index": 1,
                "dtype": "string",
                "numeric_scale": None,
                "numeric_precision": None,
                "char_size": None,
            },
        )

        self.assertEqual(
            columns[3].to_column_dict(omit_none=False),
            {
                "table_database": None,
                "table_schema": relation.schema,
                "table_name": relation.name,
                "table_type": rel_type,
                "table_owner": "root",
                "column": "struct_col",
                "column_index": 3,
                "dtype": "struct",
                "numeric_scale": None,
                "numeric_precision": None,
                "char_size": None,
            },
        )

    def test_parse_columns_from_information_with_table_type_and_parquet_provider(self):
        self.maxDiff = None
        rel_type = FabricSparkRelation.get_relation_type.Table

        information = (
            "Database: default_schema\n"
            "Table: mytable\n"
            "Owner: root\n"
            "Created Time: Wed Feb 04 18:15:00 UTC 1815\n"
            "Last Access: Wed May 20 19:25:00 UTC 1925\n"
            "Created By: Spark 3.0.1\n"
            "Type: MANAGED\n"
            "Provider: parquet\n"
            "Statistics: 1234567890 bytes, 12345678 rows\n"
            "Location: /mnt/vo\n"
            "Serde Library: org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe\n"
            "InputFormat: org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat\n"
            "OutputFormat: org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat\n"
            "Schema: root\n"
            " |-- col1: decimal(22,0) (nullable = true)\n"
            " |-- col2: string (nullable = true)\n"
            " |-- dt: date (nullable = true)\n"
            " |-- struct_col: struct (nullable = true)\n"
            " |    |-- struct_inner_col: string (nullable = true)\n"
        )
        relation = FabricSparkRelation.create(
            schema="default_schema", identifier="mytable", type=rel_type, information=information
        )

        config = self._get_target_livy(self.project_cfg)
        columns = FabricSparkAdapter(config).parse_columns_from_information(relation)
        self.assertEqual(len(columns), 4)

        self.assertEqual(
            columns[2].to_column_dict(omit_none=False),
            {
                "table_database": None,
                "table_schema": relation.schema,
                "table_name": relation.name,
                "table_type": rel_type,
                "table_owner": "root",
                "column": "dt",
                "column_index": 2,
                "dtype": "date",
                "numeric_scale": None,
                "numeric_precision": None,
                "char_size": None,
                "stats:bytes:description": "",
                "stats:bytes:include": True,
                "stats:bytes:label": "bytes",
                "stats:bytes:value": 1234567890,
                "stats:rows:description": "",
                "stats:rows:include": True,
                "stats:rows:label": "rows",
                "stats:rows:value": 12345678,
            },
        )

        self.assertEqual(
            columns[3].to_column_dict(omit_none=False),
            {
                "table_database": None,
                "table_schema": relation.schema,
                "table_name": relation.name,
                "table_type": rel_type,
                "table_owner": "root",
                "column": "struct_col",
                "column_index": 3,
                "dtype": "struct",
                "numeric_scale": None,
                "numeric_precision": None,
                "char_size": None,
                "stats:bytes:description": "",
                "stats:bytes:include": True,
                "stats:bytes:label": "bytes",
                "stats:bytes:value": 1234567890,
                "stats:rows:description": "",
                "stats:rows:include": True,
                "stats:rows:label": "rows",
                "stats:rows:value": 12345678,
            },
        )


class TestOneLakeRelationTypeParsing(unittest.TestCase):
    """Tests for OneLake-specific relation type parsing.

    OneLake's OnelakeExternalCatalog returns specific table types that need
    to be properly mapped to dbt's RelationType enum.
    """

    def setUp(self):
        flags.STRICT_MODE = False
        self.project_cfg = {
            "name": "X",
            "version": "0.1",
            "profile": "test",
            "project-root": "/tmp/dbt/does-not-exist",
            "quoting": {"identifier": False, "schema": False},
            "config-version": 2,
        }

    def _get_config(self):
        return config_from_parts_or_dicts(
            self.project_cfg,
            {
                "outputs": {
                    "test": {
                        "type": "fabricspark",
                        "method": "livy",
                        "authentication": "CLI",
                        "schema": "dbtsparktest",
                        "lakehouse": "dbtsparktest",
                        "workspaceid": "1de8390c-9aca-4790-bee8-72049109c0f4",
                        "lakehouseid": "8c5bc260-bc3a-4898-9ada-01e433d461ba",
                        "connect_retries": 0,
                        "connect_timeout": 10,
                        "threads": 1,
                        "endpoint": "https://dailyapi.fabric.microsoft.com/v1",
                    }
                },
                "target": "test",
            },
        )

    def test_parse_relation_type_managed(self):
        """MANAGED tables should map to RelationType.Table"""
        config = self._get_config()
        adapter = FabricSparkAdapter(config)
        information = "Type: MANAGED\nProvider: delta\n"
        result = adapter._parse_relation_type(information)
        self.assertEqual(result, RelationType.Table)

    def test_parse_relation_type_external(self):
        """EXTERNAL tables should map to RelationType.Table"""
        config = self._get_config()
        adapter = FabricSparkAdapter(config)
        information = "Type: EXTERNAL\nProvider: delta\n"
        result = adapter._parse_relation_type(information)
        self.assertEqual(result, RelationType.Table)

    def test_parse_relation_type_materialized_lake_view(self):
        """MATERIALIZED_LAKE_VIEW should map to RelationType.Table"""
        config = self._get_config()
        adapter = FabricSparkAdapter(config)
        information = "Type: MATERIALIZED_LAKE_VIEW\nProvider: delta\n"
        result = adapter._parse_relation_type(information)
        self.assertEqual(result, RelationType.Table)

    def test_parse_relation_type_view(self):
        """VIEW should map to RelationType.View"""
        config = self._get_config()
        adapter = FabricSparkAdapter(config)
        information = "Type: VIEW\n"
        result = adapter._parse_relation_type(information)
        self.assertEqual(result, RelationType.View)

    def test_parse_relation_type_table(self):
        """TABLE should map to RelationType.Table"""
        config = self._get_config()
        adapter = FabricSparkAdapter(config)
        information = "Type: TABLE\nProvider: delta\n"
        result = adapter._parse_relation_type(information)
        self.assertEqual(result, RelationType.Table)

    def test_parse_relation_type_unknown_defaults_to_table(self):
        """Unknown types should default to RelationType.Table"""
        config = self._get_config()
        adapter = FabricSparkAdapter(config)
        information = "Type: UNKNOWN_TYPE\nProvider: delta\n"
        result = adapter._parse_relation_type(information)
        self.assertEqual(result, RelationType.Table)

    def test_parse_relation_type_case_insensitive(self):
        """Type parsing should be case-insensitive"""
        config = self._get_config()
        adapter = FabricSparkAdapter(config)

        # lowercase
        information = "Type: managed\nProvider: delta\n"
        result = adapter._parse_relation_type(information)
        self.assertEqual(result, RelationType.Table)

        # mixed case
        information = "Type: Managed\nProvider: delta\n"
        result = adapter._parse_relation_type(information)
        self.assertEqual(result, RelationType.Table)

    def test_parse_relation_type_legacy_view_detection(self):
        """Should fall back to legacy 'Type: VIEW' string detection"""
        config = self._get_config()
        adapter = FabricSparkAdapter(config)
        # Legacy format without regex match
        information = "Some info\nType: VIEW\nMore info"
        result = adapter._parse_relation_type(information)
        self.assertEqual(result, RelationType.View)

    def test_parse_relation_type_no_type_defaults_to_table(self):
        """Missing type information should default to RelationType.Table"""
        config = self._get_config()
        adapter = FabricSparkAdapter(config)
        information = "Provider: delta\nLocation: /mnt/data"
        result = adapter._parse_relation_type(information)
        self.assertEqual(result, RelationType.Table)


class TestFourPartNaming(unittest.TestCase):
    """Tests for Fabric Spark SQL four-part naming support.

    Fabric Spark SQL natively supports four-part naming:
        workspace.lakehouse.schema.table
    """

    def test_two_part_relation_render(self):
        """Two-part naming: lakehouse.table"""
        relation = FabricSparkRelation.create(
            schema="my_lakehouse",
            identifier="my_table",
        )
        self.assertEqual(relation.render(), "my_lakehouse.my_table")
        self.assertTrue(relation.is_two_part)
        self.assertFalse(relation.is_three_part)
        self.assertFalse(relation.is_four_part)

    def test_three_part_relation_render(self):
        """Three-part naming: lakehouse.schema.table"""
        relation = FabricSparkRelation.create(
            database="my_lakehouse",
            schema="dbo",
            identifier="my_table",
        )
        self.assertEqual(relation.render(), "my_lakehouse.dbo.my_table")
        self.assertFalse(relation.is_two_part)
        self.assertTrue(relation.is_three_part)
        self.assertFalse(relation.is_four_part)

    def test_four_part_relation_render(self):
        """Four-part naming: workspace.lakehouse.schema.table"""
        relation = FabricSparkRelation.create(
            workspace="analytics_workspace",
            database="gold_lakehouse",
            schema="dbo",
            identifier="dim_customers",
        )
        self.assertEqual(
            relation.render(),
            "analytics_workspace.gold_lakehouse.dbo.dim_customers"
        )
        self.assertFalse(relation.is_two_part)
        self.assertFalse(relation.is_three_part)
        self.assertTrue(relation.is_four_part)
        self.assertTrue(relation.is_cross_workspace)

    def test_four_part_with_workspace_method(self):
        """Test with_workspace() method creates four-part relation"""
        # Start with three-part
        relation = FabricSparkRelation.create(
            database="my_lakehouse",
            schema="dbo",
            identifier="my_table",
        )
        self.assertFalse(relation.is_four_part)

        # Add workspace
        four_part = relation.with_workspace("remote_workspace")
        self.assertTrue(four_part.is_four_part)
        self.assertEqual(
            four_part.render(),
            "remote_workspace.my_lakehouse.dbo.my_table"
        )

    def test_four_part_without_workspace_method(self):
        """Test without_workspace() method removes workspace"""
        relation = FabricSparkRelation.create(
            workspace="analytics_workspace",
            database="gold_lakehouse",
            schema="dbo",
            identifier="dim_customers",
        )
        self.assertTrue(relation.is_four_part)

        # Remove workspace
        three_part = relation.without_workspace()
        self.assertFalse(three_part.is_four_part)
        self.assertTrue(three_part.is_three_part)
        self.assertEqual(three_part.render(), "gold_lakehouse.dbo.dim_customers")

    def test_cross_workspace_property(self):
        """Test is_cross_workspace property"""
        # Local relation
        local = FabricSparkRelation.create(
            schema="my_lakehouse",
            identifier="my_table",
        )
        self.assertFalse(local.is_cross_workspace)

        # Cross-workspace relation
        remote = FabricSparkRelation.create(
            workspace="other_workspace",
            database="other_lakehouse",
            schema="dbo",
            identifier="other_table",
        )
        self.assertTrue(remote.is_cross_workspace)

    def test_relation_with_same_database_schema(self):
        """When database equals schema, should be two-part naming"""
        relation = FabricSparkRelation.create(
            database="my_lakehouse",
            schema="my_lakehouse",
            identifier="my_table",
        )
        # When database == schema, it's effectively two-part
        self.assertTrue(relation.is_two_part)

    def test_four_part_cross_workspace_join_scenario(self):
        """Test scenario for cross-workspace joins"""
        # Table in workspace A
        table_a = FabricSparkRelation.create(
            workspace="sales_workspace",
            database="transactions_lh",
            schema="dbo",
            identifier="orders",
        )

        # Table in workspace B
        table_b = FabricSparkRelation.create(
            workspace="analytics_workspace",
            database="gold_lh",
            schema="dbo",
            identifier="dim_customers",
        )

        # Both should render as four-part names for cross-workspace join
        self.assertEqual(
            table_a.render(),
            "sales_workspace.transactions_lh.dbo.orders"
        )
        self.assertEqual(
            table_b.render(),
            "analytics_workspace.gold_lh.dbo.dim_customers"
        )
