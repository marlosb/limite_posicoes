# Fabric Data Platform Demo

This project is a simple demonstration of how a data platform like **Microsoft Fabric** can speed up development for systems with analytical patterns.

The demo shows how a lightweight backend + UI can orchestrate analytical workflows (notebooks and pipelines), update pipeline steps, and trigger runs through the Fabric REST API.

## What this demo includes

- A FastAPI backend (`backend/main.py`) that:
  - Lists Fabric pipelines and notebooks
  - Reads pipeline definitions and notebook steps
  - Updates notebook activities in a pipeline
  - Triggers pipeline execution
- A simple frontend (`static/`) to select pipelines, edit steps, and run them
- Sample data and notebooks for analytical scenarios (`data/`, `notebooks/`)

## Why this matters

Analytical systems often require rapid iteration across:

- Data ingestion
- Transformation logic
- Business rules
- Operational orchestration

Fabric helps centralize these pieces, while this app shows how to automate and operationalize them quickly using APIs.

## Prerequisites

- Python 3.10+
- Access to a Microsoft Fabric workspace
- Azure CLI (`az`) logged in with permission to call Fabric APIs

## Quick start

1. Install backend dependencies:

```powershell
pip install -r backend\requirements.txt
```

2. Set workspace and token environment variables:

```powershell
$env:FABRIC_WORKSPACE_ID="<your-workspace-id>"
$env:FABRIC_TOKEN = az account get-access-token --resource https://api.fabric.microsoft.com --query accessToken -o tsv
```

3. Run the API server:

```powershell
uvicorn backend.main:app --host 127.0.0.1 --port 8000
```

4. Open:

`http://127.0.0.1:8000/`

## Notes

- `FABRIC_TOKEN` is required by the backend and is read from environment variables.
- Tokens expire; generate a new one when needed.
- For quick demos, session-level environment variables are usually enough.
