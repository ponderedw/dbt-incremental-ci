import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Any
from sqlalchemy import create_engine, text, MetaData, Table
from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)


class TableCopier:
    """Handles copying tables from production schema to CI schema."""

    def __init__(self, database_uri: str, ci_schema: str, threads: int = 1, dry_run: bool = False, base_schema: str = None):
        self.database_uri = database_uri
        self.ci_schema = ci_schema
        self.base_schema = base_schema  # Production base schema (e.g., 'edu_dbt')
        self.threads = threads
        self.dry_run = dry_run
        # Only create engine if not in dry-run mode
        self.engine = None if dry_run else create_engine(database_uri)

    def _get_dialect_name(self) -> str:
        """Get the SQL dialect name."""
        if self.dry_run:
            # In dry-run mode, parse from URI
            if 'postgresql' in self.database_uri:
                return 'postgresql'
            elif 'redshift' in self.database_uri:
                return 'redshift'
            elif 'bigquery' in self.database_uri:
                return 'bigquery'
            elif 'trino' in self.database_uri:
                return 'trino'
            else:
                return 'unknown'
        return self.engine.dialect.name

    def _compute_target_schema(self, source_schema: str) -> str:
        """
        Compute the target CI schema based on source schema.
        If source schema has a custom suffix (e.g., 'edu_dbt_incremental_models'),
        preserve the suffix in the CI schema (e.g., 'ci_test_incremental_models').

        Args:
            source_schema: The source schema name (may include custom suffix)

        Returns:
            The target CI schema name with preserved custom suffix
        """
        # If no base schema is provided, just use the CI schema
        if not self.base_schema:
            return self.ci_schema

        # Check if source schema has a custom suffix
        if source_schema.startswith(self.base_schema):
            # Extract the custom suffix (everything after base schema)
            suffix = source_schema[len(self.base_schema):]

            # If there's a suffix, append it to CI schema
            if suffix:
                return f"{self.ci_schema}{suffix}"

        # No custom suffix found, use CI schema as-is
        return self.ci_schema

    def _create_schema_if_not_exists(self, schema_name: str = None):
        """Create CI schema if it doesn't exist."""
        target_schema = schema_name or self.ci_schema

        if self.dry_run:
            logger.info(f"[DRY RUN] Would create schema: {target_schema}")
            return

        dialect = self._get_dialect_name()

        # BigQuery uses datasets, not schemas
        if dialect == 'bigquery':
            # For BigQuery, schema creation is handled differently
            # Usually the schema (dataset) should already exist
            logger.info(f"BigQuery detected - ensure dataset {target_schema} exists")
            return

        with self.engine.connect() as conn:
            try:
                if dialect in ['postgresql', 'redshift']:
                    conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {target_schema}"))
                elif dialect == 'trino':
                    # Trino might not support IF NOT EXISTS depending on connector
                    try:
                        conn.execute(text(f"CREATE SCHEMA {target_schema}"))
                    except Exception:
                        # Schema might already exist
                        pass
                else:
                    # Generic approach
                    conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {target_schema}"))

                logger.info(f"Ensured schema {target_schema} exists")
            except Exception as e:
                logger.warning(f"Could not create schema: {e}")

    def _build_copy_query(
        self,
        source_database: str,
        source_schema: str,
        source_table: str,
        target_table: str
    ) -> str:
        """
        Build a SQL query to copy a table from source to target.
        Handles different SQL dialects.
        Preserves custom schema suffixes from source schema.
        """
        dialect = self._get_dialect_name()

        # Determine full source table name
        if source_database and dialect != 'bigquery':
            source_full = f"{source_database}.{source_schema}.{source_table}"
        else:
            # For BigQuery and others without explicit database
            source_full = f"{source_schema}.{source_table}"

        # Compute target schema with preserved custom suffix
        target_schema = self._compute_target_schema(source_schema)
        target_full = f"{target_schema}.{target_table}"

        if dialect == 'bigquery':
            # BigQuery uses CREATE OR REPLACE TABLE ... AS SELECT
            query = f"""
                CREATE OR REPLACE TABLE `{target_full}`
                AS SELECT * FROM `{source_full}`
            """
        elif dialect in ['postgresql', 'redshift']:
            # Drop and create
            query = f"""
                DROP TABLE IF EXISTS {target_full};
                CREATE TABLE {target_full} AS SELECT * FROM {source_full};
            """
        elif dialect == 'trino':
            # Trino uses CREATE TABLE AS
            query = f"""
                DROP TABLE IF EXISTS {target_full};
                CREATE TABLE {target_full} AS SELECT * FROM {source_full};
            """
        else:
            # Generic SQL - try standard approach
            query = f"""
                DROP TABLE IF EXISTS {target_full};
                CREATE TABLE {target_full} AS SELECT * FROM {source_full};
            """

        return query

    def _copy_single_table(self, node: Dict[str, Any]) -> Dict[str, Any]:
        """
        Copy a single table from production to CI schema.
        Returns a result dict with status information.
        """
        source_database = node.get('database')
        source_schema = node.get('schema')
        source_table = node.get('alias', node.get('name'))
        unique_id = node.get('unique_id')
        materialization = node.get('materialization', 'unknown')

        # Compute target schema with preserved custom suffix
        target_schema = self._compute_target_schema(source_schema)

        if self.dry_run:
            # Dry-run mode: just log what would be copied
            logger.info(f"[DRY RUN] Would copy: {source_schema}.{source_table} -> {target_schema}.{source_table} (type: {materialization})")

            query = self._build_copy_query(
                source_database=source_database,
                source_schema=source_schema,
                source_table=source_table,
                target_table=source_table
            )

            return {
                'unique_id': unique_id,
                'source': f"{source_schema}.{source_table}",
                'target': f"{target_schema}.{source_table}",
                'materialization': materialization,
                'status': 'dry_run',
                'error': None,
                'query': query.strip()
            }

        # Create the target schema if it doesn't exist
        self._create_schema_if_not_exists(target_schema)

        logger.info(f"Copying table: {source_schema}.{source_table} -> {target_schema}.{source_table}")

        try:
            query = self._build_copy_query(
                source_database=source_database,
                source_schema=source_schema,
                source_table=source_table,
                target_table=source_table
            )

            with self.engine.connect() as conn:
                # For dialects that need statement splitting
                dialect = self._get_dialect_name()

                if dialect in ['postgresql', 'redshift', 'trino']:
                    # Execute statements separately
                    statements = [s.strip() for s in query.split(';') if s.strip()]
                    for statement in statements:
                        conn.execute(text(statement))
                else:
                    # Execute as single query
                    conn.execute(text(query))

            logger.info(f"Successfully copied {source_schema}.{source_table}")
            return {
                'unique_id': unique_id,
                'source': f"{source_schema}.{source_table}",
                'target': f"{target_schema}.{source_table}",
                'status': 'success',
                'error': None
            }

        except Exception as e:
            logger.error(f"Failed to copy {source_schema}.{source_table}: {e}")
            return {
                'unique_id': unique_id,
                'source': f"{source_schema}.{source_table}",
                'target': f"{target_schema}.{source_table}",
                'status': 'failed',
                'error': str(e)
            }

    def copy_tables(self, nodes: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Copy multiple tables using parallel threads.
        Returns a list of result dicts.
        """
        if not nodes:
            logger.info("No tables to copy")
            return []

        results = []

        if self.threads == 1:
            # Sequential execution
            for node in nodes:
                result = self._copy_single_table(node)
                results.append(result)
        else:
            # Parallel execution
            logger.info(f"Copying {len(nodes)} tables using {self.threads} threads")

            with ThreadPoolExecutor(max_workers=self.threads) as executor:
                # Submit all tasks
                future_to_node = {
                    executor.submit(self._copy_single_table, node): node
                    for node in nodes
                }

                # Collect results as they complete
                for future in as_completed(future_to_node):
                    result = future.result()
                    results.append(result)

        # Log summary
        if self.dry_run:
            dry_run_count = sum(1 for r in results if r['status'] == 'dry_run')
            logger.info(f"[DRY RUN] Would copy {dry_run_count} tables")
        else:
            successful = sum(1 for r in results if r['status'] == 'success')
            failed = sum(1 for r in results if r['status'] == 'failed')
            logger.info(f"Copy complete: {successful} successful, {failed} failed")

        return results

    def close(self):
        """Close database connection."""
        if self.engine:
            self.engine.dispose()
