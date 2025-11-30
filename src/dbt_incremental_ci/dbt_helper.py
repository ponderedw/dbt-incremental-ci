import json
import subprocess
from pathlib import Path
from typing import List, Dict, Set, Any
import logging

logger = logging.getLogger(__name__)


class DbtHelper:
    """Helper class for interacting with dbt and parsing manifests."""

    def __init__(self, dbt_project_dir: str, prod_manifest_path: str):
        self.dbt_project_dir = Path(dbt_project_dir)
        self.prod_manifest_path = Path(prod_manifest_path)
        self.prod_manifest = self._load_manifest(self.prod_manifest_path)

    def _load_manifest(self, manifest_path: Path) -> Dict[str, Any]:
        """Load and parse a dbt manifest.json file."""
        try:
            with open(manifest_path, 'r') as f:
                return json.load(f)
        except FileNotFoundError:
            raise FileNotFoundError(f"Manifest file not found: {manifest_path}")
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON in manifest file {manifest_path}: {e}")

    def get_modified_nodes(self) -> Set[str]:
        """
        Run 'dbt ls --select modified+ --defer' to get modified nodes.
        Returns a set of unique node IDs.
        """
        logger.info("Running dbt ls to detect modified nodes...")

        cmd = [
            "dbt", "ls",
            "--select", "state:modified+",
            "--defer",
            "--state", str(self.prod_manifest_path.parent),
            "--project-dir", str(self.dbt_project_dir)
        ]

        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                check=True,
                cwd=self.dbt_project_dir
            )

            # Parse output - each line is a model unique_id
            # Filter out logging lines that start with ANSI escape codes or timestamps
            modified_nodes = set()
            for line in result.stdout.strip().split('\n'):
                line = line.strip()
                # Skip empty lines
                if not line:
                    continue
                # Skip lines that start with ANSI codes (colored logging output)
                if line.startswith('[0m') or line.startswith('['):
                    continue
                # Skip common log patterns
                if any(pattern in line for pattern in [
                    'Running with dbt',
                    'Registered adapter',
                    'Found',
                    'models,',
                    'data tests,'
                ]):
                    continue
                # This should be a model name
                modified_nodes.add(line)

            logger.info(f"Found {len(modified_nodes)} modified nodes")
            return modified_nodes

        except subprocess.CalledProcessError as e:
            logger.error(f"dbt ls command failed: {e.stderr}")
            raise RuntimeError(f"Failed to run dbt ls: {e.stderr}")

    def filter_incremental_and_snapshots(
        self,
        modified_nodes: Set[str]
    ) -> List[Dict[str, Any]]:
        """
        Filter modified nodes to only include incremental models and snapshots
        that exist in the production manifest.

        Returns a list of dicts with node information:
        - unique_id: str
        - resource_type: str (model or snapshot)
        - materialization: str
        - database: str
        - schema: str
        - alias/name: str
        """
        filtered_nodes = []

        # Get all nodes from prod manifest
        prod_nodes = self.prod_manifest.get('nodes', {})
        prod_sources = self.prod_manifest.get('sources', {})
        all_prod = {**prod_nodes, **prod_sources}

        for node_name in modified_nodes:
            # Try to find the node in production manifest
            # node_name from dbt ls is like: DbtEducationalDataProject.marts.core.model_name
            # manifest keys are like: model.DbtEducationalDataProject.model_name
            # We need to extract the model name and match it

            # Get the last part (model name)
            model_name = node_name.split('.')[-1]

            # Find matching keys
            matching_keys = [k for k in all_prod.keys() if model_name in k and k.endswith(model_name)]

            if not matching_keys:
                logger.debug(f"Node {node_name} not found in production manifest, skipping")
                continue

            for node_key in matching_keys:
                node = all_prod[node_key]
                resource_type = node.get('resource_type')

                # Check if it's a model with incremental materialization or a snapshot
                if resource_type == 'model':
                    config = node.get('config', {})
                    materialization = config.get('materialized')

                    if materialization == 'incremental':
                        filtered_nodes.append({
                            'unique_id': node_key,
                            'resource_type': resource_type,
                            'materialization': materialization,
                            'database': node.get('database'),
                            'schema': node.get('schema'),
                            'alias': node.get('alias', node.get('name')),
                            'name': node.get('name')
                        })
                        logger.debug(f"Added incremental model: {node_key}")

                elif resource_type == 'snapshot':
                    filtered_nodes.append({
                        'unique_id': node_key,
                        'resource_type': resource_type,
                        'materialization': 'snapshot',
                        'database': node.get('database'),
                        'schema': node.get('schema'),
                        'alias': node.get('alias', node.get('name')),
                        'name': node.get('name')
                    })
                    logger.debug(f"Added snapshot: {node_key}")

        logger.info(
            f"Filtered to {len(filtered_nodes)} incremental models/snapshots "
            f"from {len(modified_nodes)} modified nodes"
        )
        return filtered_nodes
