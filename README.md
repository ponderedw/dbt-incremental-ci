# dbt-incremental-ci

A Python library to copy production incremental models and snapshots to CI schema for dbt testing.

## Overview

This library helps with dbt CI workflows by:
1. Identifying modified dbt models using `dbt ls --select modified+ --defer`
2. Filtering for incremental models and snapshots that exist in production
3. Copying those tables from production to CI schema with all data
4. Supporting parallel execution for faster copying

## Installation

```bash
pip install dbt-incremental-ci
```

## Usage

### CLI - Local Manifest

```bash
dbt-incremental-ci \
  --prod-manifest-path path/to/prod/manifest.json \
  --dbt-project-dir path/to/dbt/project \
  --database-uri "postgresql://user:pass@host:5432/db" \
  --ci-schema "ci_schema" \
  --threads 4
```

### CLI - dbt Cloud

```bash
# Set environment variables (recommended)
export DBT_CLOUD_API_TOKEN="your_token"
export DBT_CLOUD_ACCOUNT_ID="12345"

dbt-incremental-ci \
  --dbt-cloud-job-id "67890" \
  --dbt-project-dir path/to/dbt/project \
  --database-uri "postgresql://user:pass@host:5432/db" \
  --ci-schema "ci_schema" \
  --threads 4

# Or pass credentials directly
dbt-incremental-ci \
  --dbt-cloud-token "your_token" \
  --dbt-cloud-account-id "12345" \
  --dbt-cloud-job-id "67890" \
  --dbt-project-dir path/to/dbt/project \
  --database-uri "postgresql://user:pass@host:5432/db" \
  --ci-schema "ci_schema" \
  --threads 4
```

### Python API - Local Manifest

```python
from dbt_incremental_ci import DbtIncrementalCI

ci = DbtIncrementalCI(
    prod_manifest_path="path/to/prod/manifest.json",
    dbt_project_dir="path/to/dbt/project",
    database_uri="postgresql://user:pass@host:5432/db",
    ci_schema="ci_schema",
    threads=4
)

ci.run()
ci.cleanup()
```

### Python API - dbt Cloud

```python
from dbt_incremental_ci import DbtIncrementalCI

ci = DbtIncrementalCI(
    dbt_cloud_token="your_token",
    dbt_cloud_account_id="12345",
    dbt_cloud_job_id="67890",
    dbt_project_dir="path/to/dbt/project",
    database_uri="postgresql://user:pass@host:5432/db",
    ci_schema="ci_schema",
    threads=4
)

ci.run()
ci.cleanup()
```

## Parameters

### Manifest Source (choose one)
- `--prod-manifest-path`: Path to production dbt manifest.json (local file)
- `--dbt-cloud-token`: dbt Cloud API token (or set `DBT_CLOUD_API_TOKEN` env var)
- `--dbt-cloud-account-id`: dbt Cloud account ID (or set `DBT_CLOUD_ACCOUNT_ID` env var)
- `--dbt-cloud-job-id`: dbt Cloud job ID to fetch manifest from
- `--dbt-cloud-run-id`: Optional specific run ID (uses latest successful if not provided)

### Required
- `--dbt-project-dir`: Path to dbt project directory
- `--database-uri`: SQLAlchemy database URI
- `--ci-schema`: Target CI schema name

### Optional
- `--threads`: Number of parallel threads for copying (default: 1)
- `--base-schema`: Production base schema name (auto-detected from manifest if not provided)
- `--dry-run`: Show what would be copied without actually copying tables
- `--verbose` / `-v`: Enable verbose logging

## Supported Databases

- PostgreSQL
- Amazon Redshift
- Google BigQuery
- Trino
- Any database supported by SQLAlchemy

## Why Use This Tool?

Traditional dbt CI runs all models from scratch, which can be slow and expensive for large projects. This tool optimizes CI by:

- Only copying data for incremental models and snapshots that have changed
- Leveraging existing production data instead of rebuilding everything
- Running copies in parallel for maximum speed
- Supporting any SQL database through SQLAlchemy

## How It Works

```
┌─────────────────────────────────────────────────────────┐
│ 1. Detect Modified Models                               │
│    Run: dbt ls --select modified+ --defer               │
│    Output: List of changed models                       │
└─────────────────────────────────────────────────────────┘
                          ↓
┌─────────────────────────────────────────────────────────┐
│ 2. Filter for Incremental/Snapshots                     │
│    Check production manifest for:                       │
│    - Incremental materialization                        │
│    - Snapshot resource type                             │
└─────────────────────────────────────────────────────────┘
                          ↓
┌─────────────────────────────────────────────────────────┐
│ 3. Copy Tables to CI Schema                             │
│    CREATE TABLE ci_schema.table                         │
│    AS SELECT * FROM prod_schema.table                   │
│    (in parallel with configurable threads)              │
└─────────────────────────────────────────────────────────┘
```

## Dry Run Mode

Preview what tables would be copied without actually copying them:

```bash
dbt-incremental-ci \
  --prod-manifest-path prod/manifest.json \
  --dbt-project-dir . \
  --database-uri "postgresql://user:pass@host:5432/db" \
  --ci-schema "ci_schema" \
  --dry-run \
  --verbose
```

This will show:
- Which tables would be copied
- Source and target table names
- Materialization type (incremental or snapshot)
- SQL queries that would be executed (with `--verbose`)

## dbt Cloud Integration

Fetch the production manifest directly from dbt Cloud API:

```bash
# Using environment variables (recommended for CI/CD)
export DBT_CLOUD_API_TOKEN="your_api_token"
export DBT_CLOUD_ACCOUNT_ID="12345"

dbt-incremental-ci \
  --dbt-cloud-job-id "67890" \
  --dbt-project-dir . \
  --database-uri "$DATABASE_URI" \
  --ci-schema "ci_pr_123" \
  --threads 4
```

The tool will:
1. Connect to dbt Cloud API
2. Find the latest successful run for the specified job
3. Download the manifest.json artifact
4. Use it to identify incremental models and snapshots

### Getting dbt Cloud Credentials

- **API Token**: Generate from Account Settings → API Access
- **Account ID**: Found in your dbt Cloud URL: `https://cloud.getdbt.com/deploy/{account_id}/...`
- **Job ID**: Found in the job URL or job settings

## Example CI Workflows

### Using Local Manifest

```bash
# Step 1: Download production manifest
aws s3 cp s3://my-bucket/prod/manifest.json ./prod_manifest.json

# Step 2: Copy incremental tables to CI
dbt-incremental-ci \
  --prod-manifest-path ./prod_manifest.json \
  --dbt-project-dir . \
  --database-uri "$DATABASE_URI" \
  --ci-schema "ci_pr_123" \
  --threads 4

# Step 3: Run dbt on CI schema
dbt build --select modified+ --schema ci_pr_123

# Step 4: Run tests
dbt test --schema ci_pr_123
```

### Using dbt Cloud

```bash
# Step 1: Copy incremental tables from dbt Cloud production job
export DBT_CLOUD_API_TOKEN="your_token"
export DBT_CLOUD_ACCOUNT_ID="12345"

dbt-incremental-ci \
  --dbt-cloud-job-id "67890" \
  --dbt-project-dir . \
  --database-uri "$DATABASE_URI" \
  --ci-schema "ci_pr_${PR_NUMBER}" \
  --threads 4

# Step 2: Run dbt on CI schema
dbt build --select modified+ --schema "ci_pr_${PR_NUMBER}"

# Step 3: Run tests
dbt test --schema "ci_pr_${PR_NUMBER}"
```

## Features

- **dbt Cloud integration** - Fetch manifest directly from dbt Cloud API
- **Custom schema support** - Automatically preserves dbt custom schema suffixes when copying to CI
- Multi-database support (PostgreSQL, Redshift, BigQuery, Trino, etc.)
- Parallel table copying with configurable thread count
- **Dry-run mode** to preview changes before executing
- **Auto-detection** of production base schema from manifest
- Automatic schema creation (including custom schema suffixes)
- Comprehensive error handling and logging
- CLI and Python API interfaces
- CI/CD ready with exit codes and detailed output

## Custom Schema Handling

The tool automatically handles dbt custom schemas. If your production model uses:

```yaml
models:
  my_project:
    marts:
      core:
        my_incremental_model:
          +schema: custom_suffix
          +materialized: incremental
```

And the production table is in `prod_schema_custom_suffix`, the tool will:
1. Auto-detect the base schema (`prod_schema`)
2. Detect the custom suffix (`_custom_suffix`)
3. Copy to CI with the same suffix: `ci_schema_custom_suffix`

Example:
- Production: `edu_dbt_incremental_models.student_history`
- CI target: `ci_test_incremental_models.student_history`

The base schema is auto-detected from the manifest, or you can specify it with `--base-schema`.

## Requirements

- Python 3.8+
- dbt-core
- SQLAlchemy 1.4+
- Database-specific drivers (psycopg2, redshift-connector, etc.)

## Development

### Setting Up Development Environment

```bash
# Clone the repository
git clone https://github.com/yourusername/dbt-incremental-ci.git
cd dbt-incremental-ci

# Install Poetry
curl -sSL https://install.python-poetry.org | python3 -

# Install dependencies
poetry install

# Activate virtual environment
poetry shell
```

### Running Tests Locally

```bash
# Test the CLI
poetry run dbt-incremental-ci --help

# Run linting
poetry run flake8 src/dbt_incremental_ci

# Build the package
poetry build
```

## CI/CD

This project uses Poetry for dependency management and includes GitHub Actions workflows:

### CI Workflow (`.github/workflows/ci.yml`)
- Runs on push and pull requests to main branch
- Tests package installation on Python 3.9, 3.10, 3.11, and 3.12
- Lints code with flake8
- Builds package artifacts with Poetry

### Publish Workflow (`.github/workflows/publish.yml`)
- Triggers on push to main branch
- Uses `dorny/paths-filter` to detect changes in:
  - `src/dbt_incremental_ci/**/*`
  - `poetry.lock`
  - `pyproject.toml`
- Only publishes if package files have changed
- Automatically publishes to PyPI using Poetry when changes are detected
- Requires `PYPI_TOKEN` secret to be set in repository settings

### Setting Up PyPI Token

1. Generate a PyPI API token at https://pypi.org/manage/account/token/
2. Add it to GitHub repository secrets as `PYPI_TOKEN`:
   - Go to repository Settings → Secrets and variables → Actions
   - Click "New repository secret"
   - Name: `PYPI_TOKEN`
   - Value: Your PyPI token

## License

MIT License