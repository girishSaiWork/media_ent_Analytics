# 🚀 Quick Start Guide - Databricks Asset Bundles

This guide will help you deploy your first Databricks Asset Bundle in under 10 minutes.

## Prerequisites Checklist

- [ ] Databricks workspace access
- [ ] Databricks personal access token
- [ ] Python 3.8+ installed
- [ ] Git installed (optional, for version control)

## Step-by-Step Setup

### 1. Install Databricks CLI

```bash
# Using pip
pip install databricks-cli

# Verify installation
databricks --version
```

### 2. Configure Authentication

**Option A: Interactive Configuration**
```bash
databricks configure --token
# When prompted, enter:
# - Databricks Host: https://adb-<workspace-id>.azuredatabricks.net
# - Token: dapi... (your personal access token)
```

**Option B: Environment Variables (Recommended for CI/CD)**
```bash
# Windows PowerShell
$env:DATABRICKS_HOST="https://adb-<workspace-id>.azuredatabricks.net"
$env:DATABRICKS_TOKEN="dapi..."

# Linux/Mac
export DATABRICKS_HOST="https://adb-<workspace-id>.azuredatabricks.net"
export DATABRICKS_TOKEN="dapi..."
```

### 3. Get Your Workspace ID

1. Go to your Databricks workspace in a browser
2. Look at the URL: `https://adb-1234567890123456.azuredatabricks.net`
3. Copy the number: `1234567890123456`

### 4. Update Configuration Files

#### Update `databricks.yml`

Replace `<workspace-id>` with your actual workspace ID in the following sections:

```yaml
targets:
  dev:
    workspace:
      host: https://adb-1234567890123456.azuredatabricks.net  # ← Update this
```

Do this for all environments (dev, qa, uat, prod).

#### Update Email Notifications (Optional)

Find and replace email addresses in `databricks.yml`:
- `dev-team@company.com` → your team email
- `data-ops@company.com` → your ops team email

### 5. Validate Your Bundle

```bash
# Navigate to project root
cd "E:\Study Space\Analytics Enginerring\Data Engineering\Azure Databricks\Asset Bundles"

# Validate the bundle
databricks bundle validate -t dev
```

**Expected Output:**
```
✓ Configuration is valid
```

### 6. Deploy to DEV Environment

**Windows PowerShell:**
```powershell
.\scripts\deploy.ps1 -Environment dev
```

**Linux/Mac:**
```bash
chmod +x scripts/deploy.sh
./scripts/deploy.sh dev
```

**Or manually:**
```bash
databricks bundle deploy -t dev
```

### 7. Verify Deployment

```bash
# List deployed resources
databricks workspace ls "/Workspace/Users/<your-email>/.bundle/data-pipeline-bundle/dev"

# List jobs
databricks jobs list | Select-String "data-pipeline-job-dev"
```

### 8. Run Your First Job

```bash
# Run and wait for completion
databricks bundle run data_pipeline_job -t dev

# Or run asynchronously
databricks bundle run data_pipeline_job -t dev --no-wait
```

## 🎯 What Just Happened?

When you deployed the bundle, Databricks:

1. ✅ Created a workspace folder structure
2. ✅ Uploaded your notebooks to the workspace
3. ✅ Created a job definition with your tasks
4. ✅ Configured environment-specific settings
5. ✅ Set up cluster configurations

## 📊 View Your Deployment

### In Databricks UI:

1. **Go to Workflows** (left sidebar)
2. **Find your job**: `data-pipeline-job-dev`
3. **Click on it** to see:
   - Task dependencies
   - Cluster configuration
   - Schedule settings
   - Run history

### Check Deployed Files:

```bash
# List workspace files
databricks workspace ls /Workspace/Users/<your-email>/.bundle/data-pipeline-bundle/dev/notebooks

# Export a notebook to view locally
databricks workspace export /Workspace/Users/<your-email>/.bundle/data-pipeline-bundle/dev/notebooks/01_data_validation.py ./local_copy.py
```

## 🔄 Making Changes

### Workflow for Updates:

1. **Modify code** (notebooks, scripts, or config)
2. **Validate changes**:
   ```bash
   databricks bundle validate -t dev
   ```
3. **Deploy updated bundle**:
   ```bash
   databricks bundle deploy -t dev
   ```
4. **Test the changes**:
   ```bash
   databricks bundle run data_pipeline_job -t dev
   ```

### Example: Update a Notebook

1. Edit `notebooks/02_data_processing.py` locally
2. Deploy the change:
   ```bash
   databricks bundle deploy -t dev
   ```
3. The notebook in Databricks workspace is automatically updated!

## 🌍 Deploying to Other Environments

### DEV → QA Promotion:

```bash
# Validate for QA
databricks bundle validate -t qa

# Deploy to QA
databricks bundle deploy -t qa

# Run in QA
databricks bundle run data_pipeline_job -t qa
```

### QA → UAT Promotion:

```bash
databricks bundle deploy -t uat
```

### UAT → PROD Promotion:

```bash
# Extra caution for production!
.\scripts\deploy.ps1 -Environment prod  # Windows
# OR
./scripts/deploy.sh prod  # Linux/Mac
```

## 🔧 Common Commands Cheat Sheet

```bash
# Validate bundle
databricks bundle validate -t <env>

# Deploy bundle
databricks bundle deploy -t <env>

# Run job
databricks bundle run <job-name> -t <env>

# List jobs
databricks jobs list

# List workspace files
databricks workspace ls <path>

# Check current user
databricks current-user me

# View job runs
databricks jobs runs list --job-id <job-id>

# Get job run output
databricks jobs runs get-output --run-id <run-id>

# Destroy deployment (remove resources)
databricks bundle destroy -t <env>
```

## 🐛 Troubleshooting

### Issue: "Authentication failed"

**Solution:**
```bash
# Reconfigure authentication
databricks configure --token

# Or check environment variables
echo $DATABRICKS_HOST
echo $DATABRICKS_TOKEN
```

### Issue: "Bundle validation failed"

**Solution:**
```bash
# Check YAML syntax
databricks bundle validate -t dev

# Common issues:
# - Incorrect indentation
# - Missing workspace-id
# - Invalid variable references
```

### Issue: "Cannot find workspace path"

**Solution:**
```bash
# List root workspace
databricks workspace ls /

# Check if bundle was deployed
databricks workspace ls /Workspace/Users
```

### Issue: "Job fails when running"

**Solution:**
1. Check job logs in Databricks UI
2. Verify data paths exist:
   ```bash
   databricks fs ls /mnt/dev/data
   ```
3. Check cluster configuration in `databricks.yml`

## 📚 Next Steps

Now that you have a working DAB deployment:

1. **Add your actual notebooks** to the `notebooks/` folder
2. **Update job tasks** in `resources/jobs/data_pipeline_job.yml`
3. **Configure data paths** to match your storage
4. **Set up CI/CD** using the GitHub Actions workflow
5. **Read the full README.md** for advanced features

## 💡 Pro Tips

1. **Always validate before deploying**:
   ```bash
   databricks bundle validate -t <env> && databricks bundle deploy -t <env>
   ```

2. **Use version control**:
   ```bash
   git add .
   git commit -m "feat: updated data processing logic"
   git push
   ```

3. **Check what changed**:
   ```bash
   databricks bundle deploy -t dev --dry-run
   ```

4. **Monitor job runs**:
   - Set up email notifications in `databricks.yml`
   - Use Databricks UI for real-time monitoring
   - Check logs regularly

5. **Test in DEV first**:
   - Never deploy directly to production
   - Follow the DEV → QA → UAT → PROD promotion path

## 🎓 Learning Resources

- [Full README](./README.md) - Comprehensive documentation
- [Databricks Asset Bundles Docs](https://docs.databricks.com/dev-tools/bundles/index.html)
- [Databricks CLI Reference](https://docs.databricks.com/dev-tools/cli/index.html)

## 🆘 Need Help?

- Check the [Troubleshooting section](#-troubleshooting) above
- Review error messages carefully
- Validate configuration: `databricks bundle validate -t dev`
- Check Databricks UI for job logs

---

**🎉 Congratulations!** You've successfully deployed your first Databricks Asset Bundle!
