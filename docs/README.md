# Documentation Assets

This folder contains documentation images and diagrams for the Media Analytics Pipeline.

## Pipeline Workflow Diagram

**To add the pipeline workflow diagram:**

1. Save your Databricks job workflow screenshot as `pipeline_workflow.png` in this directory
2. The image is already referenced in `DATA_ENGINEERING_README.md` at line 7

### How to capture the workflow diagram:

1. Go to your Databricks workspace
2. Navigate to **Workflows** → **Jobs**
3. Click on your job: `media_analytics_pipeline_run`
4. Click on the **Graph** tab to see the task dependency visualization
5. Take a screenshot of the workflow graph
6. Save it as `docs/pipeline_workflow.png`

### Current Workflow Structure:

```
bronze_ingestion
    │
    ├── silver_assest_transformation
    ├── silver_delivery_transformation
    ├── silver_device_task
    ├── silver_geo_task
    └── silver_metrics_task
            │
            ├── gold_executive_summary
            ├── gold_metrics_assets
            ├── gold_metrics_delivery
            ├── gold_device_metrics
            └── gold_geo_metrics
```

The workflow image you provided shows all these tasks with their:
- Execution status (✓ Succeeded)
- Duration times
- Notebook paths
- Cluster assignments

## Other Documentation

You can add additional diagrams or screenshots here:
- Architecture diagrams
- Data flow diagrams
- ER diagrams for data models
- Performance benchmarks
- Dashboard screenshots
