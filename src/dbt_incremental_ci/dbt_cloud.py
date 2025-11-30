import json
import logging
import requests
from typing import Dict, Any, Optional
from pathlib import Path

logger = logging.getLogger(__name__)


class DbtCloudClient:
    """Client for interacting with dbt Cloud API."""

    def __init__(self, api_token: str, account_id: str):
        """
        Initialize dbt Cloud API client.

        Args:
            api_token: dbt Cloud API token
            account_id: dbt Cloud account ID
        """
        self.api_token = api_token
        self.account_id = account_id
        self.base_url = "https://cloud.getdbt.com/api/v2"
        self.headers = {
            "Authorization": f"Token {api_token}",
            "Content-Type": "application/json"
        }

    def get_job_runs(self, job_id: str, limit: int = 10) -> Dict[str, Any]:
        """
        Get recent runs for a job.

        Args:
            job_id: dbt Cloud job ID
            limit: Maximum number of runs to fetch

        Returns:
            API response with job runs
        """
        url = f"{self.base_url}/accounts/{self.account_id}/runs/"
        params = {
            "job_definition_id": job_id,
            "limit": limit,
            "order_by": "-finished_at"  # Most recent first
        }

        logger.info(f"Fetching runs for job {job_id}")
        response = requests.get(url, headers=self.headers, params=params)
        response.raise_for_status()

        return response.json()

    def get_latest_successful_run(self, job_id: str) -> Optional[Dict[str, Any]]:
        """
        Get the latest successful run for a job.

        Args:
            job_id: dbt Cloud job ID

        Returns:
            Run metadata dict or None if no successful run found
        """
        runs_response = self.get_job_runs(job_id, limit=50)
        runs = runs_response.get("data", [])

        for run in runs:
            # Status 10 = success in dbt Cloud
            if run.get("status") == 10:
                logger.info(f"Found successful run: {run.get('id')} (finished at {run.get('finished_at')})")
                return run

        logger.warning(f"No successful runs found for job {job_id}")
        return None

    def get_run_artifact(self, run_id: str, artifact_path: str) -> Dict[str, Any]:
        """
        Get an artifact from a specific run.

        Args:
            run_id: dbt Cloud run ID
            artifact_path: Path to artifact (e.g., 'manifest.json')

        Returns:
            Artifact contents as dict
        """
        url = f"{self.base_url}/accounts/{self.account_id}/runs/{run_id}/artifacts/{artifact_path}"

        logger.info(f"Fetching artifact {artifact_path} from run {run_id}")
        response = requests.get(url, headers=self.headers)
        response.raise_for_status()

        return response.json()

    def get_manifest_from_job(self, job_id: str, run_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Get manifest.json from a job's latest successful run or a specific run.

        Args:
            job_id: dbt Cloud job ID
            run_id: Optional specific run ID. If not provided, uses latest successful run.

        Returns:
            manifest.json contents as dict
        """
        if run_id:
            logger.info(f"Fetching manifest from specific run {run_id}")
        else:
            logger.info(f"Fetching manifest from latest successful run of job {job_id}")
            run = self.get_latest_successful_run(job_id)
            if not run:
                raise ValueError(f"No successful run found for job {job_id}")
            run_id = str(run.get("id"))

        manifest = self.get_run_artifact(run_id, "manifest.json")
        logger.info("Successfully fetched manifest from dbt Cloud")
        return manifest

    def save_manifest_to_file(
        self,
        job_id: str,
        output_path: str,
        run_id: Optional[str] = None
    ) -> str:
        """
        Fetch manifest from dbt Cloud and save to file.

        Args:
            job_id: dbt Cloud job ID
            output_path: Path where to save manifest.json
            run_id: Optional specific run ID

        Returns:
            Path to saved manifest file
        """
        manifest = self.get_manifest_from_job(job_id, run_id)

        output_file = Path(output_path)
        output_file.parent.mkdir(parents=True, exist_ok=True)

        with open(output_file, 'w') as f:
            json.dump(manifest, f, indent=2)

        logger.info(f"Manifest saved to {output_file}")
        return str(output_file)


def fetch_manifest_from_dbt_cloud(
    api_token: str,
    account_id: str,
    job_id: str,
    run_id: Optional[str] = None,
    output_path: Optional[str] = None
) -> Dict[str, Any]:
    """
    Convenience function to fetch manifest from dbt Cloud.

    Args:
        api_token: dbt Cloud API token
        account_id: dbt Cloud account ID
        job_id: dbt Cloud job ID
        run_id: Optional specific run ID
        output_path: Optional path to save manifest

    Returns:
        manifest.json as dict
    """
    client = DbtCloudClient(api_token, account_id)

    if output_path:
        client.save_manifest_to_file(job_id, output_path, run_id)

    return client.get_manifest_from_job(job_id, run_id)
