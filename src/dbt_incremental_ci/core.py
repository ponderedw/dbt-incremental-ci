import logging
import tempfile
from typing import List, Dict, Any, Optional

from .dbt_helper import DbtHelper
from .copier import TableCopier
from .dbt_cloud import DbtCloudClient

logger = logging.getLogger(__name__)


class DbtIncrementalCI:
    """
    Main class for dbt incremental CI workflow.

    This class orchestrates the process of:
    1. Detecting modified dbt nodes
    2. Filtering for incremental models and snapshots
    3. Copying those tables from production to CI schema
    """

    def __init__(
        self,
        dbt_project_dir: str,
        database_uri: str,
        ci_schema: str,
        prod_manifest_path: Optional[str] = None,
        dbt_cloud_token: Optional[str] = None,
        dbt_cloud_account_id: Optional[str] = None,
        dbt_cloud_job_id: Optional[str] = None,
        dbt_cloud_run_id: Optional[str] = None,
        base_schema: Optional[str] = None,
        threads: int = 1,
        dry_run: bool = False
    ):
        """
        Initialize DbtIncrementalCI.

        Args:
            dbt_project_dir: Path to dbt project directory
            database_uri: SQLAlchemy database connection URI
            ci_schema: Target CI schema name (base schema for CI)
            prod_manifest_path: Path to production dbt manifest.json (if using local file)
            dbt_cloud_token: dbt Cloud API token (if using dbt Cloud)
            dbt_cloud_account_id: dbt Cloud account ID (if using dbt Cloud)
            dbt_cloud_job_id: dbt Cloud job ID (if using dbt Cloud)
            dbt_cloud_run_id: Optional dbt Cloud run ID (uses latest successful if not provided)
            base_schema: Production base schema name (will be auto-detected from manifest if not provided)
            threads: Number of parallel threads for copying tables
            dry_run: If True, only show what would be copied without actually copying
        """
        self.dbt_project_dir = dbt_project_dir
        self.database_uri = database_uri
        self.ci_schema = ci_schema
        self.base_schema = base_schema
        self.threads = threads
        self.dry_run = dry_run
        self._temp_manifest_path = None

        # Determine manifest source
        if prod_manifest_path:
            # Use local manifest file
            self.prod_manifest_path = prod_manifest_path
            logger.info(f"Using local manifest: {prod_manifest_path}")
        elif dbt_cloud_token and dbt_cloud_account_id and dbt_cloud_job_id:
            # Fetch manifest from dbt Cloud
            logger.info("Fetching manifest from dbt Cloud")
            self.prod_manifest_path = self._fetch_manifest_from_dbt_cloud(
                api_token=dbt_cloud_token,
                account_id=dbt_cloud_account_id,
                job_id=dbt_cloud_job_id,
                run_id=dbt_cloud_run_id
            )
        else:
            raise ValueError(
                "Must provide either prod_manifest_path OR "
                "(dbt_cloud_token, dbt_cloud_account_id, dbt_cloud_job_id)"
            )

        self.dbt_helper = DbtHelper(
            dbt_project_dir=dbt_project_dir,
            prod_manifest_path=self.prod_manifest_path
        )

        # Auto-detect base schema if not provided
        if not self.base_schema:
            self.base_schema = self._detect_base_schema()
            logger.info(f"Auto-detected base schema: {self.base_schema}")

        self.copier = TableCopier(
            database_uri=database_uri,
            ci_schema=ci_schema,
            base_schema=self.base_schema,
            threads=threads,
            dry_run=dry_run
        )

    def _detect_base_schema(self) -> Optional[str]:
        """
        Auto-detect the base schema from the production manifest.
        Finds a model without custom schema configuration.

        Returns:
            The detected base schema name, or None if not found
        """
        manifest = self.dbt_helper.prod_manifest
        nodes = manifest.get('nodes', {})

        # Find any model without a custom schema config
        for node_key, node in nodes.items():
            if node.get('resource_type') == 'model':
                custom_schema = node.get('config', {}).get('schema')
                if not custom_schema:
                    # This model has no custom schema, so its schema is the base schema
                    base_schema = node.get('schema')
                    if base_schema:
                        return base_schema

        # Fallback: if all models have custom schemas, try to infer from the first model
        # by removing the custom suffix
        for node_key, node in nodes.items():
            if node.get('resource_type') == 'model':
                schema = node.get('schema')
                custom_schema = node.get('config', {}).get('schema')
                if schema and custom_schema:
                    # Remove custom suffix (assuming it's appended with underscore)
                    if schema.endswith(f"_{custom_schema}"):
                        base_schema = schema[:-len(f"_{custom_schema}")]
                        return base_schema

        logger.warning("Could not auto-detect base schema from manifest")
        return None

    def _fetch_manifest_from_dbt_cloud(
        self,
        api_token: str,
        account_id: str,
        job_id: str,
        run_id: Optional[str] = None
    ) -> str:
        """
        Fetch manifest from dbt Cloud and save to temporary file.

        Args:
            api_token: dbt Cloud API token
            account_id: dbt Cloud account ID
            job_id: dbt Cloud job ID
            run_id: Optional specific run ID

        Returns:
            Path to temporary manifest file
        """
        client = DbtCloudClient(api_token, account_id)

        # Create temporary file for manifest

        temp_path = 'manifest.json'

        # Save manifest to temp file
        client.save_manifest_to_file(job_id, temp_path, run_id)
        self._temp_manifest_path = temp_path

        return temp_path

    def run(self) -> Dict[str, Any]:
        """
        Execute the full CI workflow.

        Returns:
            Dict with execution results including:
            - modified_nodes: Set of modified node names
            - filtered_nodes: List of incremental/snapshot nodes
            - copy_results: List of copy operation results
        """
        logger.info("Starting dbt incremental CI workflow")

        # Step 1: Get modified nodes
        logger.info("Step 1: Detecting modified nodes")
        modified_nodes = self.dbt_helper.get_modified_nodes()

        if not modified_nodes:
            logger.info("No modified nodes found, nothing to copy")
            return {
                'modified_nodes': set(),
                'filtered_nodes': [],
                'copy_results': []
            }

        # Step 2: Filter for incremental models and snapshots
        logger.info("Step 2: Filtering for incremental models and snapshots")
        filtered_nodes = self.dbt_helper.filter_incremental_and_snapshots(modified_nodes)

        if not filtered_nodes:
            logger.info("No incremental models or snapshots found in modified nodes")
            return {
                'modified_nodes': modified_nodes,
                'filtered_nodes': [],
                'copy_results': []
            }

        # Step 3: Copy tables to CI schema (or dry-run)
        if self.dry_run:
            logger.info("Step 3: DRY RUN - Showing tables that would be copied")
        else:
            logger.info("Step 3: Copying tables to CI schema")

        copy_results = self.copier.copy_tables(filtered_nodes)

        if self.dry_run:
            logger.info("DRY RUN completed - no tables were actually copied")
        else:
            logger.info("dbt incremental CI workflow completed")

        return {
            'modified_nodes': modified_nodes,
            'filtered_nodes': filtered_nodes,
            'copy_results': copy_results
        }

    def cleanup(self):
        """Clean up resources."""
        self.copier.close()

        # Clean up temporary manifest file if it was created
        if self._temp_manifest_path:
            import os
            try:
                os.unlink(self._temp_manifest_path)
                logger.debug(f"Removed temporary manifest: {self._temp_manifest_path}")
            except Exception as e:
                logger.warning(f"Could not remove temporary manifest: {e}")
