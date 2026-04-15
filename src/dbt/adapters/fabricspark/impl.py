import re
from concurrent.futures import Future
from dataclasses import dataclass
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    FrozenSet,
    Iterable,
    List,
    Optional,
    Set,
    Tuple,
    Union,
)

from typing_extensions import TypeAlias

if TYPE_CHECKING:
    # Indirectly imported via agate_helper, which is lazy loaded further downfile.
    # Used by mypy for earlier type hints.
    import agate

from dbt_common.clients.agate_helper import DEFAULT_TYPE_TESTER
from dbt_common.contracts.constraints import ConstraintType
from dbt_common.exceptions import CompilationError, DbtRuntimeError
from dbt_common.utils import AttrDict, executor

from dbt.adapters.base import AdapterConfig, BaseRelation
from dbt.adapters.base.impl import ConstraintSupport, catch_as_completed
from dbt.adapters.base.relation import InformationSchema
from dbt.adapters.contracts.relation import RelationConfig, RelationType
from dbt.adapters.events.logging import AdapterLogger
from dbt.adapters.exceptions import RelationReturnedMultipleResultsError
from dbt.adapters.fabricspark import FabricSparkColumn, FabricSparkConnectionManager
from dbt.adapters.fabricspark.relation import FabricSparkRelation
from dbt.adapters.sql import SQLAdapter

logger = AdapterLogger("fabricspark")

GET_COLUMNS_IN_RELATION_RAW_MACRO_NAME = "get_columns_in_relation_raw"
LIST_SCHEMAS_MACRO_NAME = "list_schemas"
LIST_RELATIONS_MACRO_NAME = "list_relations_without_caching"
LIST_RELATIONS_SHOW_TABLES_MACRO_NAME = "list_relations_show_tables_without_caching"
DESCRIBE_TABLE_EXTENDED_MACRO_NAME = "describe_table_extended_without_caching"

KEY_TABLE_OWNER = "Owner"
KEY_TABLE_STATISTICS = "Statistics"

# OneLake supported table types and their mapping to RelationType
# OneLake's OnelakeExternalCatalog only supports: MANAGED, EXTERNAL, MATERIALIZED_LAKE_VIEW
# Standard Spark VIEWs are NOT supported in OneLake
ONELAKE_TYPE_REGEX = re.compile(r"Type:\s*(\w+)", re.IGNORECASE)

TABLE_OR_VIEW_NOT_FOUND_MESSAGES = (
    "[TABLE_OR_VIEW_NOT_FOUND]",
    "Table or view not found",
    "NoSuchTableException",
)


@dataclass
class FabricSparkConfig(AdapterConfig):
    file_format: str = "parquet"
    location_root: Optional[str] = None
    partition_by: Optional[Union[List[str], str]] = None
    clustered_by: Optional[Union[List[str], str]] = None
    buckets: Optional[int] = None
    options: Optional[Dict[str, str]] = None
    merge_update_columns: Optional[str] = None


class FabricSparkAdapter(SQLAdapter):
    COLUMN_NAMES = (
        "table_database",
        "table_schema",
        "table_name",
        "table_type",
        "table_comment",
        "table_owner",
        "column_name",
        "column_index",
        "column_type",
        "column_comment",
        "stats:bytes:label",
        "stats:bytes:value",
        "stats:bytes:description",
        "stats:bytes:include",
        "stats:rows:label",
        "stats:rows:value",
        "stats:rows:description",
        "stats:rows:include",
    )
    INFORMATION_COLUMNS_REGEX = re.compile(r"^ \|-- (.*): (.*) \(nullable = (.*)\b", re.MULTILINE)
    INFORMATION_OWNER_REGEX = re.compile(r"^Owner: (.*)$", re.MULTILINE)
    INFORMATION_STATISTICS_REGEX = re.compile(r"^Statistics: (.*)$", re.MULTILINE)

    CONSTRAINT_SUPPORT = {
        ConstraintType.check: ConstraintSupport.NOT_ENFORCED,
        ConstraintType.not_null: ConstraintSupport.NOT_ENFORCED,
        ConstraintType.unique: ConstraintSupport.NOT_ENFORCED,
        ConstraintType.primary_key: ConstraintSupport.NOT_ENFORCED,
        ConstraintType.foreign_key: ConstraintSupport.NOT_ENFORCED,
    }

    Relation: TypeAlias = FabricSparkRelation
    RelationInfo = Tuple[str, str, str]
    Column: TypeAlias = FabricSparkColumn
    ConnectionManager: TypeAlias = FabricSparkConnectionManager
    AdapterSpecificConfigs: TypeAlias = FabricSparkConfig

    @classmethod
    def date_function(cls) -> str:
        return "current_timestamp()"

    @classmethod
    def convert_text_type(cls, agate_table: "agate.Table", col_idx: int) -> str:
        return "string"

    @classmethod
    def convert_number_type(cls, agate_table: "agate.Table", col_idx: int) -> str:
        import agate

        decimals = agate_table.aggregate(agate.MaxPrecision(col_idx))
        return "double" if decimals else "bigint"

    @classmethod
    def convert_integer_type(cls, agate_table: "agate.Table", col_idx: int) -> str:
        return "bigint"

    @classmethod
    def convert_date_type(cls, agate_table: "agate.Table", col_idx: int) -> str:
        return "date"

    @classmethod
    def convert_time_type(cls, agate_table: "agate.Table", col_idx: int) -> str:
        return "time"

    @classmethod
    def convert_datetime_type(cls, agate_table: "agate.Table", col_idx: int) -> str:
        return "timestamp"

    def quote(self, identifier: str) -> str:
        return "`{}`".format(identifier)

    def _schema_enabled(self) -> bool:
        creds = getattr(self.config, "credentials", None)
        return bool(getattr(creds, "lakehouse_schemas_enabled", False))

    def _normalize_workspace_database(self, database: Optional[str]) -> Optional[str]:
        if not database:
            return database
        parts = [part for part in database.replace("`", "").split(".") if part]
        if len(parts) == 2:
            return parts[1]
        return database

    def _normalize_schema_parts(
        self, raw_schema: Optional[str]
    ) -> Tuple[Optional[str], Optional[str]]:
        """Normalize Fabric schema strings into (database, schema)."""
        if raw_schema is None:
            return None, None

        cleaned = raw_schema.replace("`", "")
        parts = [part for part in cleaned.split(".") if part]

        if len(parts) >= 3:
            # workspace.lakehouse.schema -> database=lakehouse, schema=schema
            return parts[-2], parts[-1]
        if len(parts) == 2:
            # lakehouse.schema -> database=lakehouse, schema=schema
            return parts[0], parts[1]
        if len(parts) == 1:
            # schema only
            return None, parts[0]
        return None, raw_schema

    def _get_relation_information(self, row: "agate.Row") -> RelationInfo:
        """relation info was fetched with SHOW TABLES EXTENDED"""
        try:
            _schema, name, _, information = row
        except ValueError:
            raise DbtRuntimeError(
                f'Invalid value from "show tables extended ...", got {len(row)} values, expected 4'
            )

        return _schema, name, information

    def _get_relation_information_using_describe(self, row: "agate.Row") -> RelationInfo:
        """Relation info fetched using SHOW TABLES and an auxiliary DESCRIBE statement"""
        try:
            _schema, name, _ = row
        except ValueError:
            raise DbtRuntimeError(
                f'Invalid value from "show tables ...", got {len(row)} values, expected 3'
            )

        table_name = f"{_schema}.{name}"
        try:
            table_results = self.execute_macro(
                DESCRIBE_TABLE_EXTENDED_MACRO_NAME, kwargs={"table_name": table_name}
            )
        except DbtRuntimeError as e:
            logger.debug(f"Error while retrieving information about {table_name}: {e.msg}")
            table_results = AttrDict()

        information = ""
        for info_row in table_results:
            info_type, info_value, _ = info_row
            if not info_type.startswith("#"):
                information += f"{info_type}: {info_value}\n"

        return _schema, name, information

    def _parse_relation_type(self, information: str) -> RelationType:
        """Parse the relation type from DESCRIBE EXTENDED information string.

        OneLake's OnelakeExternalCatalog supports:
        - MANAGED: Delta tables in /Tables folder
        - EXTERNAL: Tables with explicit LOCATION
        - MATERIALIZED_LAKE_VIEW: Fabric's materialized views (mapped to Table)

        Standard Spark VIEWs are NOT supported in OneLake, but we still handle
        them for backwards compatibility with other Spark environments.
        """
        type_match = ONELAKE_TYPE_REGEX.search(information)
        if type_match:
            raw_type = type_match.group(1).upper()
            # Map OneLake types to RelationType
            if raw_type == "VIEW":
                return RelationType.View
            elif raw_type in ("MANAGED", "EXTERNAL", "MATERIALIZED_LAKE_VIEW", "TABLE"):
                return RelationType.Table
            else:
                # Log unknown types but default to Table to avoid errors
                logger.debug(f"Unknown OneLake table type '{raw_type}', defaulting to Table")
                return RelationType.Table

        # Fallback: check for VIEW in the information string (legacy behavior)
        if "Type: VIEW" in information:
            return RelationType.View

        return RelationType.Table

    def _build_spark_relation_list(
        self,
        row_list: "agate.Table",
        relation_info_func: Callable[["agate.Row"], RelationInfo],
    ) -> List[BaseRelation]:
        """Aggregate relations with format metadata included."""
        relations = []
        for row in row_list:
            _schema, name, information = relation_info_func(row)
            norm_database, norm_schema = self._normalize_schema_parts(_schema)

            rel_type: RelationType = self._parse_relation_type(information)
            is_delta: bool = "Provider: delta" in information
            relation: BaseRelation = self.Relation.create(
                database=norm_database,
                schema=norm_schema or _schema,
                identifier=name,
                type=rel_type,
                information=information,
                is_delta=is_delta,
            )
            relations.append(relation)

        return relations

    def list_relations_without_caching(self, schema_relation: BaseRelation) -> List[BaseRelation]:
        """Distinct Spark compute engines may not support the same SQL featureset. Thus, we must
        try different methods to fetch relation information."""

        kwargs = {"schema_relation": schema_relation}

        try:
            # Default compute engine behavior: show tables extended
            show_table_extended_rows = self.execute_macro(LIST_RELATIONS_MACRO_NAME, kwargs=kwargs)
            x = self._build_spark_relation_list(
                row_list=show_table_extended_rows,
                relation_info_func=self._get_relation_information,
            )
            return x
        except DbtRuntimeError as e:
            errmsg = getattr(e, "msg", "")
            if f"Database '{schema_relation}' not found" in errmsg:
                return []
            # Iceberg compute engine behavior: show table
            elif "SHOW TABLE EXTENDED is not supported for v2 tables" in errmsg:
                # this happens with spark-iceberg with v2 iceberg tables
                # https://issues.apache.org/jira/browse/SPARK-33393
                try:
                    # Iceberg behavior: 3-row result of relations obtained
                    show_table_rows = self.execute_macro(
                        LIST_RELATIONS_SHOW_TABLES_MACRO_NAME, kwargs=kwargs
                    )
                    return self._build_spark_relation_list(
                        row_list=show_table_rows,
                        relation_info_func=self._get_relation_information_using_describe,
                    )
                except DbtRuntimeError as e:
                    description = "Error while retrieving information about"
                    logger.debug(f"{description} {schema_relation}: {e.msg}")
                    return []
            else:
                logger.debug(
                    f"Error while retrieving information about {schema_relation}: {errmsg}"
                )
                return []

    def list_relations(self, database: Optional[str], schema: str) -> List[BaseRelation]:
        if self._schema_enabled() and database is None:
            database = getattr(self.config.credentials, "lakehouse", None)

        cache_database = database
        query_database = self._normalize_workspace_database(database)
        if self._schema_is_cached(cache_database, schema):
            return self.cache.get_relations(cache_database, schema)

        schema_relation = self.Relation.create(
            database=query_database,
            schema=schema,
            identifier="",
            quote_policy=self.config.quoting,
        ).without_identifier()

        relations = self.list_relations_without_caching(schema_relation)

        if self.cache:
            if cache_database and cache_database != query_database:
                adjusted = []
                for relation in relations:
                    adjusted.append(
                        relation.incorporate(
                            path={"database": cache_database},
                            type=relation.type,
                        )
                    )
                relations = adjusted
            for relation in relations:
                self.cache.add(relation)
            if not relations:
                self.cache.update_schemas([(cache_database, schema)])

        return relations

    def _relations_cache_for_schemas(
        self,
        relation_configs: Iterable[RelationConfig],
        cache_schemas: Optional[Set[BaseRelation]] = None,
    ) -> None:
        """Populate the relations cache for the given schemas with logging."""
        if not cache_schemas:
            cache_schemas = self._get_cache_schemas(relation_configs)
        with executor(self.config) as tpe:
            # Track each future with its originating cache_schema so we can
            # normalize the relation's database to match the key that will
            # later be used for cache lookups (see list_relations which does
            # the same adjustment). Without this, relations get cached under
            # the short database name returned by `SHOW TABLE EXTENDED`
            # (e.g. 'salesforce') while the schemas-cache is keyed by the
            # full manifest database (e.g. 'sampledata.salesforce') - causing
            # cross-schema get_relation() calls (like Elementary's
            # on-run-end hooks) to silently miss.
            futures: List[Tuple[BaseRelation, Future[List[BaseRelation]]]] = []
            for cache_schema in cache_schemas:
                fut = tpe.submit_connected(
                    self,
                    f"list_{cache_schema.database}_{cache_schema.schema}",
                    self.list_relations_without_caching,
                    cache_schema,
                )
                futures.append((cache_schema, fut))

            for cache_schema, future in futures:
                relations = future.result()
                cache_database = cache_schema.database
                query_database = self._normalize_workspace_database(cache_database)
                if cache_database and cache_database != query_database:
                    relations = [
                        r.incorporate(path={"database": cache_database}, type=r.type)
                        for r in relations
                    ]
                for relation in relations:
                    self.cache.add(relation)

        cache_update: Set[Tuple[Optional[str], str]] = set()
        for relation in cache_schemas:
            if relation.schema:
                db = relation.database
                if self._schema_enabled() and db is None:
                    db = getattr(self.config.credentials, "lakehouse", None)
                cache_update.add((db, relation.schema))
        self.cache.update_schemas(cache_update)

    def get_relation(self, database: str, schema: str, identifier: str) -> Optional[BaseRelation]:
        if not self.Relation.get_default_include_policy().database:
            database = None  # type: ignore
        if self._schema_enabled() and database is None:
            database = getattr(self.config.credentials, "lakehouse", None)

        relations_list = self.list_relations(database, schema)

        matches = self._make_match(relations_list, database, schema, identifier)

        if len(matches) > 1:
            kwargs = {
                "identifier": identifier,
                "schema": schema,
                "database": database,
            }
            raise RelationReturnedMultipleResultsError(kwargs, matches)

        if matches:
            return matches[0]

        return None

    def parse_describe_extended(
        self, relation: BaseRelation, raw_rows: AttrDict
    ) -> List[FabricSparkColumn]:
        # Convert the Row to a dict
        dict_rows = [dict(zip(row._keys, row._values)) for row in raw_rows]
        # Find the separator between the rows and the metadata provided
        # by the DESCRIBE TABLE EXTENDED statement
        pos = self.find_table_information_separator(dict_rows)

        # Remove rows that start with a hash, they are comments
        rows = [row for row in raw_rows[0:pos] if not row["col_name"].startswith("#")]
        metadata = {col["col_name"]: col["data_type"] for col in raw_rows[pos + 1 :]}

        raw_table_stats = metadata.get(KEY_TABLE_STATISTICS)
        table_stats = FabricSparkColumn.convert_table_stats(raw_table_stats)
        return [
            FabricSparkColumn(
                table_database=relation.database,
                table_schema=relation.schema,
                table_name=relation.name,
                table_type=relation.type,
                table_owner=str(metadata.get(KEY_TABLE_OWNER)),
                table_stats=table_stats,
                column=column["col_name"],
                column_index=idx,
                dtype=column["data_type"],
            )
            for idx, column in enumerate(rows)
        ]

    @staticmethod
    def find_table_information_separator(rows: List[dict]) -> int:
        pos = 0
        for row in rows:
            if not row["col_name"] or row["col_name"].startswith("#"):
                break
            pos += 1
        return pos

    def get_columns_in_relation(self, relation: BaseRelation) -> List[FabricSparkColumn]:
        columns = []
        try:
            rows: AttrDict = self.execute_macro(
                GET_COLUMNS_IN_RELATION_RAW_MACRO_NAME, kwargs={"relation": relation}
            )
            columns = self.parse_describe_extended(relation, rows)
        except DbtRuntimeError as e:
            # spark would throw error when table doesn't exist, where other
            # CDW would just return and empty list, normalizing the behavior here
            errmsg = getattr(e, "msg", "")
            found_msgs = (msg in errmsg for msg in TABLE_OR_VIEW_NOT_FOUND_MESSAGES)
            if any(found_msgs):
                pass
            else:
                raise e
        return columns

    def parse_columns_from_information(self, relation: BaseRelation) -> List[FabricSparkColumn]:
        if hasattr(relation, "information"):
            information = relation.information or ""
        else:
            information = ""
        owner_match = re.findall(self.INFORMATION_OWNER_REGEX, information)
        owner = owner_match[0] if owner_match else None
        matches = re.finditer(self.INFORMATION_COLUMNS_REGEX, information)
        columns = []
        stats_match = re.findall(self.INFORMATION_STATISTICS_REGEX, information)
        raw_table_stats = stats_match[0] if stats_match else None
        table_stats = FabricSparkColumn.convert_table_stats(raw_table_stats)
        for match_num, match in enumerate(matches):
            column_name, column_type, nullable = match.groups()
            column = FabricSparkColumn(
                table_database=relation.database,
                table_schema=relation.schema,
                table_name=relation.table,
                table_type=relation.type,
                column_index=match_num,
                table_owner=owner,
                column=column_name,
                dtype=column_type,
                table_stats=table_stats,
            )
            columns.append(column)
        return columns

    def _get_columns_for_catalog(self, relation: BaseRelation) -> Iterable[Dict[str, Any]]:
        table_name = f"{relation.schema}.{relation.identifier}"
        raw_rows = None
        try:
            raw_rows = self.execute_macro(
                DESCRIBE_TABLE_EXTENDED_MACRO_NAME, kwargs={"table_name": table_name}
            )
        except DbtRuntimeError as e:
            logger.debug(f"Error while retrieving information about {table_name}: {e.msg}")
            raise e

        # Not using parsing to extract schema and other properties using describe table extended command
        columns = self.parse_describe_extended(relation, raw_rows)

        for column in columns:
            # convert SparkColumns into catalog dicts
            as_dict = column.to_column_dict()
            as_dict["column_name"] = as_dict.pop("column", None)
            as_dict["column_type"] = as_dict.pop("dtype")
            as_dict["table_database"] = relation.database
            yield as_dict

    def get_catalog(
        self,
        relation_configs: Iterable[RelationConfig],
        used_schemas: FrozenSet[Tuple[str, str]],
    ) -> Tuple["agate.Table", List[Exception]]:
        schema_map = self._get_catalog_schemas(relation_configs)

        with executor(self.config) as tpe:
            futures: List[Future["agate.Table"]] = []
            for info, schemas in schema_map.items():
                for schema in schemas:
                    futures.append(
                        tpe.submit_connected(
                            self,
                            schema,
                            self._get_one_catalog,
                            info,
                            [schema],
                            relation_configs,
                        )
                    )
            catalogs, exceptions = catch_as_completed(futures)
        return catalogs, exceptions

    def _get_one_catalog(
        self,
        information_schema: InformationSchema,
        schemas: Set[str],
        used_schemas: FrozenSet[Tuple[str, str]],
    ) -> "agate.Table":
        if len(schemas) != 1:
            raise CompilationError(
                f"Expected only one schema in spark _get_one_catalog, found {schemas}"
            )

        database = information_schema.database
        logger.debug("database name is ", database)
        schema = list(schemas)[0]

        columns: List[Dict[str, Any]] = []
        for relation in self.list_relations(database, schema):
            logger.debug("Getting table schema for relation {}", str(relation))
            columns_to_add = self._get_columns_for_catalog(relation)
            columns.extend(columns_to_add)
        import agate

        return agate.Table.from_object(columns, column_types=DEFAULT_TYPE_TESTER)

    def check_schema_exists(self, database: str, schema: str) -> bool:
        results = self.execute_macro(LIST_SCHEMAS_MACRO_NAME, kwargs={"database": database})

        exists = True if schema in [row[0] for row in results] else False
        return exists

    def get_rows_different_sql(
        self,
        relation_a: BaseRelation,
        relation_b: BaseRelation,
        column_names: Optional[List[str]] = None,
        except_operator: str = "EXCEPT",
    ) -> str:
        """Generate SQL for a query that returns a single row with two
        columns: the number of rows that are different between the two
        relations and the number of mismatched rows.
        """
        # This method only really exists for test reasons.
        names: List[str]
        if column_names is None:
            columns = self.get_columns_in_relation(relation_a)
            names = sorted((self.quote(c.name) for c in columns))
        else:
            names = sorted((self.quote(n) for n in column_names))
        columns_csv = ", ".join(names)

        sql = COLUMNS_EQUAL_SQL.format(
            columns=columns_csv,
            relation_a=str(relation_a),
            relation_b=str(relation_b),
        )

        return sql

    def run_sql_for_tests(self, sql, fetch, conn):
        cursor = conn.handle.cursor()
        try:
            cursor.execute(sql)
            if fetch == "one":
                if hasattr(cursor, "fetchone"):
                    return cursor.fetchone()
                else:
                    return cursor.fetchall()[0]
            elif fetch == "all":
                return cursor.fetchall()
            else:
                return
        except BaseException:
            raise
        finally:
            conn.transaction_open = False

    def standardize_grants_dict(self, grants_table: "agate.Table") -> dict:
        grants_dict: Dict[str, List[str]] = {}
        for row in grants_table:
            grantee = row["Principal"]
            privilege = row["ActionType"]
            object_type = row["ObjectType"]

            # we only want to consider grants on this object
            # (view or table both appear as 'TABLE')
            # and we don't want to consider the OWN privilege
            if object_type == "TABLE" and privilege != "OWN":
                if privilege in grants_dict.keys():
                    grants_dict[privilege].append(grantee)
                else:
                    grants_dict.update({privilege: [grantee]})
        return grants_dict

    def debug_query(self) -> None:
        """Override for DebugTask method"""
        self.execute("select 1 as id")

    @classmethod
    def _get_adapter_specific_run_info(cls, config: RelationConfig) -> Dict[str, Any]:
        table_format: Optional[str] = None
        # Full table_format support within this adapter is coming. Until then, for telemetry,
        # we're relying on table_formats_within_file_formats - a subset of file_format values
        table_formats_within_file_formats = ["delta"]

        if (
            config
            and hasattr(config, "_extra")
            and (file_format := config._extra.get("file_format"))
        ):
            if file_format in table_formats_within_file_formats:
                table_format = file_format

        return {
            "adapter_type": "fabricspark",
            "table_format": table_format,
        }


# spark does something interesting with joins when both tables have the same
# static values for the join condition and complains that the join condition is
# "trivial". Which is true, though it seems like an unreasonable cause for
# failure! It also doesn't like the `from foo, bar` syntax as opposed to
# `from foo cross join bar`.
COLUMNS_EQUAL_SQL = """
with diff_count as (
    SELECT
        1 as id,
        COUNT(*) as num_missing FROM (
            (SELECT {columns} FROM {relation_a} EXCEPT
             SELECT {columns} FROM {relation_b})
             UNION ALL
            (SELECT {columns} FROM {relation_b} EXCEPT
             SELECT {columns} FROM {relation_a})
        ) as a
), table_a as (
    SELECT COUNT(*) as num_rows FROM {relation_a}
), table_b as (
    SELECT COUNT(*) as num_rows FROM {relation_b}
), row_count_diff as (
    select
        1 as id,
        table_a.num_rows - table_b.num_rows as difference
    from table_a
    cross join table_b
)
select
    INT(row_count_diff.difference) as row_count_difference,
    INT(diff_count.num_missing) as num_mismatched
from row_count_diff
cross join diff_count
""".strip()
