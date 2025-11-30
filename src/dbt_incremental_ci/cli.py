import click
import logging
import sys
from typing import Optional

from .core import DbtIncrementalCI


def setup_logging(verbose: bool = False):
    """Configure logging for the CLI."""
    level = logging.DEBUG if verbose else logging.INFO

    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout)
        ]
    )


@click.command()
@click.option(
    '--prod-manifest-path',
    type=click.Path(exists=True),
    help='Path to production dbt manifest.json file (use this OR dbt Cloud options)'
)
@click.option(
    '--dbt-cloud-token',
    type=str,
    envvar='DBT_CLOUD_API_TOKEN',
    help='dbt Cloud API token (or set DBT_CLOUD_API_TOKEN env var)'
)
@click.option(
    '--dbt-cloud-account-id',
    type=str,
    envvar='DBT_CLOUD_ACCOUNT_ID',
    help='dbt Cloud account ID (or set DBT_CLOUD_ACCOUNT_ID env var)'
)
@click.option(
    '--dbt-cloud-job-id',
    type=str,
    help='dbt Cloud job ID to fetch manifest from'
)
@click.option(
    '--dbt-cloud-run-id',
    type=str,
    help='Optional: specific dbt Cloud run ID (uses latest successful if not provided)'
)
@click.option(
    '--dbt-project-dir',
    required=True,
    type=click.Path(exists=True, file_okay=False, dir_okay=True),
    help='Path to dbt project directory'
)
@click.option(
    '--database-uri',
    required=True,
    type=str,
    help='SQLAlchemy database connection URI (e.g., postgresql://user:pass@host:5432/db)'
)
@click.option(
    '--ci-schema',
    required=True,
    type=str,
    help='Target CI schema name (base schema) where tables will be copied'
)
@click.option(
    '--base-schema',
    type=str,
    help='Production base schema name (will be auto-detected from manifest if not provided)'
)
@click.option(
    '--threads',
    default=1,
    type=int,
    help='Number of parallel threads for copying tables (default: 1)'
)
@click.option(
    '--verbose',
    '-v',
    is_flag=True,
    help='Enable verbose logging'
)
@click.option(
    '--dry-run',
    is_flag=True,
    help='Show what would be copied without actually copying tables'
)
def main(
    prod_manifest_path: str,
    dbt_cloud_token: str,
    dbt_cloud_account_id: str,
    dbt_cloud_job_id: str,
    dbt_cloud_run_id: str,
    dbt_project_dir: str,
    database_uri: str,
    ci_schema: str,
    base_schema: str,
    threads: int,
    verbose: bool,
    dry_run: bool
):
    """
    dbt Incremental CI - Copy production incremental models and snapshots to CI schema.

    This tool helps with dbt CI workflows by:
    1. Detecting modified dbt models using 'dbt ls --select modified+ --defer'
    2. Filtering for incremental models and snapshots that exist in production
    3. Copying those tables from production to CI schema with all data
    4. Supporting parallel execution for faster copying

    Manifest Source Options:
    - Local file: --prod-manifest-path ./prod/manifest.json
    - dbt Cloud: --dbt-cloud-token <token> --dbt-cloud-account-id <id> --dbt-cloud-job-id <id>

    Examples:

        # Using local manifest
        dbt-incremental-ci \\
          --prod-manifest-path ./prod/manifest.json \\
          --dbt-project-dir ./dbt_project \\
          --database-uri "postgresql://user:pass@localhost:5432/mydb" \\
          --ci-schema ci_schema \\
          --threads 4

        # Using dbt Cloud
        dbt-incremental-ci \\
          --dbt-cloud-token <token> \\
          --dbt-cloud-account-id <account_id> \\
          --dbt-cloud-job-id <job_id> \\
          --dbt-project-dir ./dbt_project \\
          --database-uri "postgresql://user:pass@localhost:5432/mydb" \\
          --ci-schema ci_schema \\
          --threads 4
    """
    setup_logging(verbose)

    logger = logging.getLogger(__name__)

    try:
        # Validate manifest source
        has_local_manifest = prod_manifest_path is not None
        has_dbt_cloud_config = all([dbt_cloud_token, dbt_cloud_account_id, dbt_cloud_job_id])

        if not has_local_manifest and not has_dbt_cloud_config:
            click.echo(
                "Error: Must provide either --prod-manifest-path OR "
                "(--dbt-cloud-token, --dbt-cloud-account-id, --dbt-cloud-job-id)",
                err=True
            )
            sys.exit(1)

        if has_local_manifest and has_dbt_cloud_config:
            click.echo(
                "Warning: Both local manifest and dbt Cloud options provided. Using local manifest.",
                err=True
            )

        if dry_run:
            logger.info("Initializing dbt Incremental CI in DRY RUN mode")
        else:
            logger.info("Initializing dbt Incremental CI")

        if has_dbt_cloud_config and not has_local_manifest:
            logger.info(f"Fetching manifest from dbt Cloud (job: {dbt_cloud_job_id})")

        ci = DbtIncrementalCI(
            dbt_project_dir=dbt_project_dir,
            database_uri=database_uri,
            ci_schema=ci_schema,
            prod_manifest_path=prod_manifest_path,
            dbt_cloud_token=dbt_cloud_token,
            dbt_cloud_account_id=dbt_cloud_account_id,
            dbt_cloud_job_id=dbt_cloud_job_id,
            dbt_cloud_run_id=dbt_cloud_run_id,
            base_schema=base_schema,
            threads=threads,
            dry_run=dry_run
        )

        # Run the workflow
        results = ci.run()

        # Print summary
        click.echo("\n" + "="*60)
        if dry_run:
            click.echo("DRY RUN SUMMARY")
        else:
            click.echo("SUMMARY")
        click.echo("="*60)
        click.echo(f"Modified nodes: {len(results['modified_nodes'])}")
        click.echo(f"Incremental/Snapshot nodes: {len(results['filtered_nodes'])}")

        if results['copy_results']:
            if dry_run:
                # Dry run summary
                dry_run_count = sum(1 for r in results['copy_results'] if r['status'] == 'dry_run')
                click.echo(f"\nTables that would be copied: {dry_run_count}")

                if dry_run_count > 0:
                    click.echo("\nTables to copy:")
                    for result in results['copy_results']:
                        if result['status'] == 'dry_run':
                            click.echo(f"  - {result['source']} -> {result['target']} ({result.get('materialization', 'unknown')})")

                    if verbose:
                        click.echo("\nSQL queries that would be executed:")
                        for idx, result in enumerate(results['copy_results'], 1):
                            if result['status'] == 'dry_run':
                                click.echo(f"\n{idx}. {result['source']}:")
                                click.echo(f"   {result.get('query', 'N/A')}")
            else:
                # Actual run summary
                successful = sum(1 for r in results['copy_results'] if r['status'] == 'success')
                failed = sum(1 for r in results['copy_results'] if r['status'] == 'failed')

                click.echo(f"Tables copied successfully: {successful}")
                click.echo(f"Tables failed: {failed}")

                if failed > 0:
                    click.echo("\nFailed tables:")
                    for result in results['copy_results']:
                        if result['status'] == 'failed':
                            click.echo(f"  - {result['source']}: {result['error']}")

        click.echo("="*60 + "\n")

        # Cleanup
        ci.cleanup()

        # Exit with error code if any copies failed
        if results['copy_results'] and any(r['status'] == 'failed' for r in results['copy_results']):
            sys.exit(1)

    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=verbose)
        click.echo(f"\nError: {e}", err=True)
        sys.exit(1)


if __name__ == '__main__':
    main()
