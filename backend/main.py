import base64
import json
import os
from pathlib import Path
from typing import Any

import requests
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from typing import Optional


FABRIC_API_BASE = "https://api.fabric.microsoft.com/v1"
DEFAULT_WORKSPACE_ID = "4b17364d-6208-4dd4-b353-6b270501c47a"
BASE_DIR = Path(__file__).resolve().parent.parent
STATIC_DIR = BASE_DIR / "static"

app = FastAPI(title="Fabric Workspace Backend")
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


def _get_required_env(name: str, default: str | None = None) -> str:
    value = os.getenv(name, default)
    if not value:
        raise HTTPException(status_code=500, detail=f"Missing environment variable: {name}")
    return value


def _fabric_get(path: str, token: str) -> dict[str, Any]:
    url = f"{FABRIC_API_BASE}{path}"
    headers = {"Authorization": f"Bearer {token}"}
    response = requests.get(url, headers=headers, timeout=60)
    if response.status_code >= 400:
        raise HTTPException(
            status_code=response.status_code,
            detail=f"Fabric API error: {response.text}",
        )
    return response.json()


def _fabric_post(path: str, token: str, payload: dict[str, Any] | None = None) -> dict[str, Any]:
    response = _fabric_post_response(path, token, payload)
    if not response.text.strip():
        return {}
    return response.json()


def _fabric_post_response(
    path: str, token: str, payload: dict[str, Any] | None = None
) -> requests.Response:
    url = f"{FABRIC_API_BASE}{path}"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    response = requests.post(url, headers=headers, json=payload, timeout=60)
    if response.status_code >= 400:
        raise HTTPException(
            status_code=response.status_code,
            detail=f"Fabric API error: {response.text}",
        )
    return response


def _flatten_activities(activities: list[dict[str, Any]]) -> list[dict[str, Any]]:
    flattened: list[dict[str, Any]] = []
    for activity in activities:
        flattened.append(activity)
        for key in ("activities", "ifTrueActivities", "ifFalseActivities"):
            nested = activity.get(key)
            if isinstance(nested, list):
                flattened.extend(_flatten_activities(nested))
        for key in ("cases",):
            nested_cases = activity.get(key)
            if isinstance(nested_cases, list):
                for case in nested_cases:
                    case_activities = case.get("activities")
                    if isinstance(case_activities, list):
                        flattened.extend(_flatten_activities(case_activities))
    return flattened


def _update_step_activity(
    activities: list[dict[str, Any]],
    step_name: str,
    notebook_id: str,
    notebook_name: str,
    base_parameter_name: Optional[str],
    base_parameter_value: Any,
) -> int:
    updated = 0
    for activity in activities:
        if activity.get("name") == step_name:
            type_properties = activity.setdefault("typeProperties", {})
            # TridentNotebook activities use notebookId/parameters.
            # Some pipelines can also carry notebook/baseParameters shape.
            type_properties["notebookId"] = notebook_id
            if isinstance(type_properties.get("notebook"), dict):
                type_properties["notebook"] = {"id": notebook_id, "name": notebook_name}

            if base_parameter_name is not None and base_parameter_value is not None:
                param_obj: dict[str, Any]
                if isinstance(base_parameter_value, bool):
                    param_obj = {"value": base_parameter_value, "type": "bool"}
                elif isinstance(base_parameter_value, int):
                    param_obj = {"value": str(base_parameter_value), "type": "int"}
                elif isinstance(base_parameter_value, float):
                    param_obj = {"value": str(base_parameter_value), "type": "float"}
                else:
                    param_obj = {"value": str(base_parameter_value), "type": "string"}

                params = type_properties.get("parameters")
                if not isinstance(params, dict):
                    params = {}
                params[base_parameter_name] = param_obj
                type_properties["parameters"] = params

                base_params = type_properties.get("baseParameters")
                if isinstance(base_params, dict):
                    base_params[base_parameter_name] = base_parameter_value
                    type_properties["baseParameters"] = base_params
            updated += 1

        for key in ("activities", "ifTrueActivities", "ifFalseActivities"):
            nested = activity.get(key)
            if isinstance(nested, list):
                updated += _update_step_activity(
                    nested,
                    step_name,
                    notebook_id,
                    notebook_name,
                    base_parameter_name,
                    base_parameter_value,
                )

        cases = activity.get("cases")
        if isinstance(cases, list):
            for case in cases:
                case_activities = case.get("activities")
                if isinstance(case_activities, list):
                    updated += _update_step_activity(
                        case_activities,
                        step_name,
                        notebook_id,
                        notebook_name,
                        base_parameter_name,
                        base_parameter_value,
                    )
    return updated


class PipelineUpdateRequest(BaseModel):
    step: str
    notebook_name: str
    base_parameter_value: Any | None = None
    base_parameter_name: str | None = None


@app.get("/")
def index() -> FileResponse:
    index_file = STATIC_DIR / "index.html"
    if not index_file.exists():
        raise HTTPException(status_code=404, detail="static/index.html not found")
    return FileResponse(str(index_file))


@app.get("/pipelines")
def list_pipelines() -> list[dict[str, Any]]:
    token = _get_required_env("FABRIC_TOKEN")
    workspace_id = _get_required_env("FABRIC_WORKSPACE_ID", DEFAULT_WORKSPACE_ID)

    items_response = _fabric_get(f"/workspaces/{workspace_id}/items", token)
    items = items_response.get("value", [])

    pipelines = [
        {
            "id": item.get("id"),
            "displayName": item.get("displayName"),
            "description": item.get("description"),
            "type": item.get("type"),
            "workspaceId": item.get("workspaceId"),
        }
        for item in items
        if item.get("type") == "DataPipeline"
    ]
    return pipelines


@app.get("/notebooks")
def list_notebooks() -> list[dict[str, Any]]:
    token = _get_required_env("FABRIC_TOKEN")
    workspace_id = _get_required_env("FABRIC_WORKSPACE_ID", DEFAULT_WORKSPACE_ID)

    items_response = _fabric_get(f"/workspaces/{workspace_id}/items", token)
    items = items_response.get("value", [])

    notebooks = [
        {
            "id": item.get("id"),
            "displayName": item.get("displayName"),
            "description": item.get("description"),
            "type": item.get("type"),
            "workspaceId": item.get("workspaceId"),
        }
        for item in items
        if item.get("type") == "Notebook"
    ]
    return notebooks


@app.put("/pipelines/{pipeline_id}")
def update_pipeline_step(pipeline_id: str, request: PipelineUpdateRequest) -> dict[str, Any]:
    token = _get_required_env("FABRIC_TOKEN")
    workspace_id = _get_required_env("FABRIC_WORKSPACE_ID", DEFAULT_WORKSPACE_ID)

    items_response = _fabric_get(f"/workspaces/{workspace_id}/items", token)
    items = items_response.get("value", [])

    notebook_item = next(
        (
            item
            for item in items
            if item.get("type") == "Notebook" and item.get("displayName") == request.notebook_name
        ),
        None,
    )
    if notebook_item is None:
        raise HTTPException(status_code=404, detail=f"Notebook '{request.notebook_name}' not found")

    pipeline_item = next(
        (
            item
            for item in items
            if item.get("type") == "DataPipeline" and item.get("id") == pipeline_id
        ),
        None,
    )
    if pipeline_item is None:
        raise HTTPException(status_code=404, detail=f"Pipeline '{pipeline_id}' not found")

    definition_response = _fabric_post(
        f"/workspaces/{workspace_id}/items/{pipeline_id}/getDefinition",
        token,
    )
    parts = definition_response.get("definition", {}).get("parts", [])
    pipeline_part = next((p for p in parts if p.get("path") == "pipeline-content.json"), None)
    if pipeline_part is None:
        raise HTTPException(status_code=404, detail="pipeline-content.json not found in pipeline definition")

    pipeline_json = json.loads(base64.b64decode(pipeline_part["payload"]).decode("utf-8"))
    activities = pipeline_json.get("properties", {}).get("activities", [])

    updated_count = _update_step_activity(
        activities=activities,
        step_name=request.step,
        notebook_id=notebook_item["id"],
        notebook_name=request.notebook_name,
        base_parameter_name=request.base_parameter_name,
        base_parameter_value=request.base_parameter_value,
    )
    if updated_count == 0:
        raise HTTPException(status_code=404, detail=f"Step '{request.step}' not found in pipeline")

    encoded_pipeline = base64.b64encode(
        json.dumps(pipeline_json, ensure_ascii=False).encode("utf-8")
    ).decode("utf-8")
    updated_parts: list[dict[str, Any]] = []
    for part in parts:
        if part.get("path") == "pipeline-content.json":
            updated_parts.append(
                {
                    "path": "pipeline-content.json",
                    "payload": encoded_pipeline,
                    "payloadType": "InlineBase64",
                }
            )
        else:
            updated_parts.append(part)

    payload = {
        "displayName": pipeline_item.get("displayName"),
        "type": "DataPipeline",
        "definition": {"parts": updated_parts},
    }
    _fabric_post(
        f"/workspaces/{workspace_id}/items/{pipeline_id}/updateDefinition",
        token,
        payload=payload,
    )

    return {
        "pipelineId": pipeline_id,
        "updatedStep": request.step,
        "newNotebookName": request.notebook_name,
        "baseParameterName": request.base_parameter_name,
        "baseParameterValue": request.base_parameter_value,
        "updatedActivities": updated_count,
    }


@app.post("/pipelines/{pipeline_id}/run")
def run_pipeline(pipeline_id: str) -> dict[str, Any]:
    token = _get_required_env("FABRIC_TOKEN")
    workspace_id = _get_required_env("FABRIC_WORKSPACE_ID", DEFAULT_WORKSPACE_ID)

    items_response = _fabric_get(f"/workspaces/{workspace_id}/items", token)
    pipeline_item = next(
        (
            item
            for item in items_response.get("value", [])
            if item.get("type") == "DataPipeline" and item.get("id") == pipeline_id
        ),
        None,
    )
    if pipeline_item is None:
        raise HTTPException(status_code=404, detail=f"Pipeline '{pipeline_id}' not found")

    payload = {
        "executionData": {
            "pipelineName": pipeline_item.get("displayName"),
        }
    }
    response = _fabric_post_response(
        f"/workspaces/{workspace_id}/items/{pipeline_id}/jobs/instances?jobType=Pipeline",
        token,
        payload=payload,
    )

    response_body: dict[str, Any] = {}
    if response.text.strip():
        try:
            response_body = response.json()
        except ValueError:
            response_body = {}

    return {
        "pipelineId": pipeline_id,
        "statusCode": response.status_code,
        "location": response.headers.get("Location"),
        "run": response_body,
    }


@app.get("/pipelines/{pipeline_id}/steps")
def list_pipeline_steps(pipeline_id: str) -> list[dict[str, Any]]:
    token = _get_required_env("FABRIC_TOKEN")
    workspace_id = _get_required_env("FABRIC_WORKSPACE_ID", DEFAULT_WORKSPACE_ID)

    definition_response = _fabric_post(
        f"/workspaces/{workspace_id}/items/{pipeline_id}/getDefinition",
        token,
    )
    parts = definition_response.get("definition", {}).get("parts", [])
    pipeline_part = next((p for p in parts if p.get("path") == "pipeline-content.json"), None)
    if pipeline_part is None:
        raise HTTPException(status_code=404, detail="pipeline-content.json not found in pipeline definition")

    pipeline_json = json.loads(base64.b64decode(pipeline_part["payload"]).decode("utf-8"))
    activities = pipeline_json.get("properties", {}).get("activities", [])
    all_activities = _flatten_activities(activities)

    items_response = _fabric_get(f"/workspaces/{workspace_id}/items", token)
    notebook_name_by_id = {
        item.get("id"): item.get("displayName")
        for item in items_response.get("value", [])
        if item.get("type") == "Notebook"
    }

    notebook_steps: list[dict[str, Any]] = []
    for activity in all_activities:
        activity_type = activity.get("type")
        type_properties = activity.get("typeProperties", {})
        notebook_ref = type_properties.get("notebook")
        notebook_id = type_properties.get("notebookId")
        notebook_name = notebook_name_by_id.get(notebook_id)
        if isinstance(notebook_ref, dict):
            notebook_id = notebook_ref.get("id") or notebook_id
            notebook_name = notebook_ref.get("name") or notebook_name

        is_notebook_activity = notebook_id is not None or notebook_ref is not None or "Notebook" in str(activity_type)
        if not is_notebook_activity:
            continue

        params = type_properties.get("baseParameters")
        if params is None:
            params = type_properties.get("parameters")
        normalized_params: dict[str, Any] = {}
        if isinstance(params, dict):
            for k, v in params.items():
                if isinstance(v, dict) and "value" in v:
                    normalized_params[k] = v.get("value")
                else:
                    normalized_params[k] = v

        notebook_steps.append(
            {
                "name": activity.get("name"),
                "description": activity.get("description"),
                "type": activity_type,
                "notebookId": notebook_id,
                "notebookName": notebook_name,
                "parameters": normalized_params,
            }
        )

    return notebook_steps
