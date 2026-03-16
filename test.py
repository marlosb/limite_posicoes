import base64
import json
import os
import sys

import requests


WORKSPACE_ID = "4b17364d-6208-4dd4-b353-6b270501c47a"
PIPELINE_NAME = "pipeline1"
FABRIC_API_BASE = "https://api.fabric.microsoft.com/v1"


def require_token() -> str:
    token = os.getenv("FABRIC_TOKEN")
    if not token:
        raise RuntimeError(
            "Missing FABRIC_TOKEN environment variable. "
            "Set it before running this script."
        )
    return token


def fabric_get(path: str, token: str) -> dict:
    url = f"{FABRIC_API_BASE}{path}"
    headers = {"Authorization": f"Bearer {token}"}
    response = requests.get(url, headers=headers, timeout=60)
    response.raise_for_status()
    return response.json()


def fabric_post(path: str, token: str) -> dict:
    url = f"{FABRIC_API_BASE}{path}"
    headers = {"Authorization": f"Bearer {token}"}
    response = requests.post(url, headers=headers, timeout=60)
    response.raise_for_status()
    return response.json()


def main() -> int:
    try:
        token = require_token()
        items_resp = fabric_get(f"/workspaces/{WORKSPACE_ID}/items", token)
        items = items_resp.get("value", [])

        notebooks = [i for i in items if i.get("type") == "Notebook"]
        pipeline = next(
            (i for i in items if i.get("type") == "DataPipeline" and i.get("displayName") == PIPELINE_NAME),
            None,
        )
        if pipeline is None:
            raise RuntimeError(f"Pipeline '{PIPELINE_NAME}' not found in workspace {WORKSPACE_ID}.")

        definition_resp = fabric_post(
            f"/workspaces/{WORKSPACE_ID}/items/{pipeline['id']}/getDefinition",
            token,
        )
        parts = definition_resp["definition"]["parts"]
        pipeline_part = next((p for p in parts if p.get("path") == "pipeline-content.json"), None)
        if pipeline_part is None:
            raise RuntimeError("pipeline-content.json not found in pipeline definition parts.")

        pipeline_json = json.loads(base64.b64decode(pipeline_part["payload"]).decode("utf-8"))
        activities = pipeline_json.get("properties", {}).get("activities", [])

        print("Notebooks in workspace:")
        for nb in notebooks:
            print(f"- {nb.get('displayName')} ({nb.get('id')})")

        print("\nPipeline steps:")
        for act in activities:
            print(f"- {act.get('name')}")

        return 0
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
