# Databricks Asset Bundles - Enterprise Deployment Guide

This repository contains a complete Databricks Asset Bundle (DAB) setup for deploying data pipelines across multiple environments (DEV → QA → UAT → PROD).

## 📁 Project Structure(Example)

```
.
├── databricks.yml                    # Root DAB configuration file
├── resources/
│   └── jobs/
│       └── data_pipeline_job.yml    # Job definition with task workflow
├── config/
│   ├── dev.json                     # DEV environment config
│   ├── qa.json                      # QA environment config
│   ├── uat.json                     # UAT environment config
│   └── prod.json                    # PROD environment config
├── notebooks/                        # Databricks notebooks
│   ├── 01_data_validation.py
│   ├── 02_data_processing.py
│   ├── 03_data_quality.py
│   └── 04_reporting.py
├── parquet_2_csv.py                 # Conversion script
├── csv_data_source/                 # CSV data files
├── parquet_data_source/             # Parquet data files
├── .github/
│   └── workflows/
│       └── deploy-dabs.yml          # GitHub Actions CI/CD pipeline
└── README.md                        # This file
```

## 🎯 What are Databricks Asset Bundles?

Databricks Asset Bundles (DABs) are a standardized way to:
- **Package** notebooks, jobs, pipelines, and configurations
- **Deploy** consistently across multiple environments
- **Version control** all infrastructure as code
- **Automate** CI/CD workflows for data engineering projects

### Key Concepts

1. **Bundle**: The complete package of your project
2. **Resources**: Jobs, pipelines, models that make up your project
3. **Targets**: Environment-specific configurations (dev, qa, uat, prod)
4. **Variables**: Parameterized values that change per environment

## 🚀 Getting Started

### Prerequisites

1. **Install Databricks CLI**:
   ```bash
   pip install databricks-cli
   ```

2. **Configure Databricks Authentication**:
   ```bash
   # Set up authentication profile
   databricks configure --token
   
   # Or use environment variables
   export DATABRICKS_HOST=https://adb-<workspace-id>.azuredatabricks.net
   export DATABRICKS_TOKEN=<your-token>
   ```

3. **Update Configuration**:
   - Replace `<workspace-id>` in `databricks.yml` with your actual workspace IDs
   - Update email addresses for notifications
   - Configure data paths to match your storage setup

### Local Development & Testing

#### 1. Validate Bundle Configuration
```bash
# Validate the bundle for DEV environment
databricks bundle validate -t dev

# Validate for other environments
databricks bundle validate -t qa
databricks bundle validate -t uat
databricks bundle validate -t prod
```

#### 2. Deploy to DEV Environment
```bash
# Deploy bundle to DEV
databricks bundle deploy -t dev

# This will:
# - Upload notebooks to Databricks workspace
# - Create/update job definitions
# - Configure environment-specific settings
```

#### 3. Run the Job
```bash
# Run the job and wait for completion
databricks bundle run data_pipeline_job -t dev

# Run without waiting (async)
databricks bundle run data_pipeline_job -t dev --no-wait

# Run with custom parameters
databricks bundle run data_pipeline_job -t dev --params source_date=2024-01-15
```

#### 4. Monitor Job Execution
```bash
# Get job status
databricks jobs list --output JSON | jq '.jobs[] | select(.settings.name | contains("data-pipeline-job-dev"))'

# Get latest run status
databricks jobs runs list --limit 1 --job-id <job-id>
```

## 🌍 Environment Configuration

### Environment Variables in `databricks.yml`

Each environment (target) defines specific variables:

| Variable | DEV | QA | UAT | PROD |
|----------|-----|-----|-----|------|
| `data_source_path` | `/mnt/dev/data` | `/mnt/qa/data` | `/mnt/uat/data` | `/mnt/prod/data` |
| `parquet_source_path` | `/mnt/dev/parquet_data` | `/mnt/qa/parquet_data` | `/mnt/uat/parquet_data` | `/mnt/prod/parquet_data` |
| `csv_output_path` | `/mnt/dev/csv_output` | `/mnt/qa/csv_output` | `/mnt/uat/csv_output` | `/mnt/prod/csv_output` |
| Cluster Size | 2 workers | 2 workers | 3 workers | 5-10 workers (autoscale) |
| Mode | `development` | `development` | `production` | `production` |

### Accessing Variables in Notebooks

Within your Databricks notebooks, access variables using widgets:

```python
# In your notebook
dbutils.widgets.text("environment", "dev")
dbutils.widgets.text("source_path", "/mnt/dev/data")

# Get parameter values
environment = dbutils.widgets.get("environment")
source_path = dbutils.widgets.get("source_path")

print(f"Running in {environment} environment")
print(f"Reading data from {source_path}")
```

### Accessing Variables in Python Scripts

For standalone Python files like `parquet_2_csv.py`:

```python
import sys
import argparse

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-path", required=True)
    parser.add_argument("--target-path", required=True)
    parser.add_argument("--environment", required=True)
    
    args = parser.parse_args()
    
    print(f"Environment: {args.environment}")
    print(f"Source: {args.source_path}")
    print(f"Target: {args.target_path}")
    
    # Your conversion logic here

if __name__ == "__main__":
    main()
```

## 🔄 CI/CD Pipeline & Environment Promotion

### Branching Strategy

```
develop branch   →  Auto-deploy to DEV
    ↓
main branch      →  Auto-deploy to QA
    ↓
release/* branch →  Deploy to UAT (with approval)
    ↓
Manual trigger   →  Deploy to PROD (with approval)
```

### Deployment Workflow

#### 1. **DEV Environment** (Continuous Deployment)
- **Trigger**: Push to `develop` branch
- **Process**: Automatic deployment after validation
- **Purpose**: Rapid development and testing

```bash
# Local development
git checkout develop
git add .
git commit -m "feat: new data validation logic"
git push origin develop
# → Automatically deploys to DEV
```

#### 2. **QA Environment** (Automated Testing)
- **Trigger**: Push to `main` branch (from merged PR)
- **Process**: Automatic deployment with integration tests
- **Purpose**: Quality assurance and automated testing

```bash
# Promote to QA
git checkout main
git merge develop
git push origin main
# → Automatically deploys to QA and runs tests
```

#### 3. **UAT Environment** (User Acceptance)
- **Trigger**: Push to `release/*` branch
- **Process**: Deployment with manual approval required
- **Purpose**: User acceptance testing before production

```bash
# Create release branch for UAT
git checkout -b release/v1.2.0 main
git push origin release/v1.2.0
# → Deploys to UAT after approval in GitHub
```

#### 4. **PROD Environment** (Production)
- **Trigger**: Manual workflow dispatch
- **Process**: Requires explicit approval, creates backup
- **Purpose**: Production deployment

```bash
# Via GitHub UI:
# Actions → Deploy Databricks Asset Bundle → Run workflow
# Select environment: prod
```

### GitHub Actions Secrets

Configure these secrets in GitHub repository settings:

```
DATABRICKS_DEV_HOST=https://adb-xxx.azuredatabricks.net
DATABRICKS_DEV_TOKEN=dapi...

DATABRICKS_QA_HOST=https://adb-yyy.azuredatabricks.net
DATABRICKS_QA_TOKEN=dapi...

DATABRICKS_UAT_HOST=https://adb-zzz.azuredatabricks.net
DATABRICKS_UAT_TOKEN=dapi...

DATABRICKS_PROD_HOST=https://adb-ppp.azuredatabricks.net
DATABRICKS_PROD_TOKEN=dapi...

SLACK_WEBHOOK_URL=https://hooks.slack.com/services/...
```

### Using Azure DevOps Instead

If you prefer Azure DevOps, create `azure-pipelines.yml`:

```yaml
trigger:
  branches:
    include:
      - main
      - develop
      - release/*

pool:
  vmImage: 'ubuntu-latest'

stages:
  - stage: Validate
    jobs:
      - job: ValidateBundle
        steps:
          - task: UsePythonVersion@0
            inputs:
              versionSpec: '3.10'
          - script: |
              pip install databricks-cli
              databricks bundle validate -t dev

  - stage: DeployDev
    condition: eq(variables['Build.SourceBranch'], 'refs/heads/develop')
    jobs:
      - deployment: DeployToDev
        environment: 'dev'
        strategy:
          runOnce:
            deploy:
              steps:
                - script: |
                    databricks bundle deploy -t dev
                  env:
                    DATABRICKS_HOST: $(DATABRICKS_DEV_HOST)
                    DATABRICKS_TOKEN: $(DATABRICKS_DEV_TOKEN)

  # Add similar stages for QA, UAT, PROD
```

## 🔐 Secrets Management

### Using Databricks Secret Scopes

```python
# In your notebooks
storage_key = dbutils.secrets.get(scope="prod-secrets", key="storage-account-key")
sp_id = dbutils.secrets.get(scope="prod-secrets", key="service-principal-id")

# Configure storage access
spark.conf.set(
    f"fs.azure.account.key.<storage-account>.dfs.core.windows.net",
    storage_key
)
```

### Creating Secret Scopes

```bash
# Using Databricks CLI
databricks secrets create-scope --scope prod-secrets

# Add secrets
databricks secrets put --scope prod-secrets --key storage-account-key
databricks secrets put --scope prod-secrets --key service-principal-id
```

### Azure Key Vault Integration (Recommended for Prod)

Link Databricks secret scope to Azure Key Vault:

```bash
databricks secrets create-scope \
  --scope prod-secrets \
  --scope-backend-type AZURE_KEYVAULT \
  --resource-id /subscriptions/<sub-id>/resourceGroups/<rg>/providers/Microsoft.KeyVault/vaults/<vault-name> \
  --dns-name https://<vault-name>.vault.azure.net/
```

## 📊 Monitoring & Observability

### Job Run Monitoring

```python
# Add to notebooks for tracking
import json
from datetime import datetime

# Log job metadata
job_metadata = {
    "job_run_id": dbutils.widgets.get("job_run_id"),
    "environment": dbutils.widgets.get("environment"),
    "start_time": datetime.now().isoformat(),
    "records_processed": record_count
}

# Write to Delta table for monitoring
spark.createDataFrame([job_metadata]).write \
    .mode("append") \
    .saveAsTable("monitoring.job_execution_log")
```

### Set Up Alerts

Configure alerts in `databricks.yml`:

```yaml
health:
  rules:
    - metric: RUN_DURATION_SECONDS
      op: GREATER_THAN
      value: 7200  # 2 hours
    - metric: RUN_JOB_OUTCOME
      op: NOT_EQUALS
      value: SUCCESS
```

## 🛠️ Best Practices

### 1. **Separate Configuration from Code**
- Keep environment-specific values in `databricks.yml` targets
- Use `config/*.json` for complex configurations
- Never hardcode paths or credentials

### 2. **Use Parameters for Flexibility**
```python
# ✅ Good - Parameterized
source_path = dbutils.widgets.get("source_path")
df = spark.read.parquet(source_path)

# ❌ Bad - Hardcoded
df = spark.read.parquet("/mnt/prod/data")
```

### 3. **Implement Proper Error Handling**
```python
try:
    df = spark.read.parquet(source_path)
    df.write.mode("overwrite").saveAsTable("target_table")
    dbutils.notebook.exit(json.dumps({"status": "success"}))
except Exception as e:
    dbutils.notebook.exit(json.dumps({"status": "failed", "error": str(e)}))
```

### 4. **Version Control Everything**
- All notebooks, scripts, and configurations in Git
- Use meaningful commit messages
- Tag production releases

### 5. **Test Before Production**
```bash
# Always validate before deploying
databricks bundle validate -t prod

# Deploy to lower environment first
databricks bundle deploy -t uat

# Run tests
databricks bundle run data_pipeline_job -t uat --wait

# Only then deploy to prod
```

## 🔧 Troubleshooting

### Common Issues

#### Bundle Validation Fails
```bash
# Check syntax
databricks bundle validate -t dev

# Common issues:
# - Invalid YAML indentation
# - Missing required fields
# - Invalid variable references
```

#### Deployment Fails
```bash
# Check authentication
databricks workspace ls /

# Verify permissions
databricks workspace get-status /Shared/.bundle/

# Check logs
databricks jobs runs get-output --run-id <run-id>
```

#### Job Fails in Specific Environment
```python
# Add debug logging
print(f"Environment: {dbutils.widgets.get('environment')}")
print(f"Source path: {dbutils.widgets.get('source_path')}")
print(f"Cluster: {spark.conf.get('spark.databricks.clusterUsageTags.clusterName')}")
```

## 📚 Additional Resources

- [Databricks Asset Bundles Documentation](https://docs.databricks.com/dev-tools/bundles/index.html)
- [Databricks CLI Reference](https://docs.databricks.com/dev-tools/cli/index.html)
- [GitHub Actions for Databricks](https://github.com/databricks/setup-cli)
- [Azure DevOps with Databricks](https://learn.microsoft.com/en-us/azure/databricks/dev-tools/ci-cd/)

## 🤝 Contributing

1. Create a feature branch from `develop`
2. Make your changes
3. Test locally: `databricks bundle deploy -t dev`
4. Create a pull request to `develop`
5. After approval, changes deploy automatically to DEV

## 📝 License

This project is licensed under the MIT License.
