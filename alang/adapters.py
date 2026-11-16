"""Trusted host adapters. IR never supplies credentials, URLs or shell commands."""
import hashlib
import os
import re
import subprocess
import sys
from pathlib import Path

import httpx

from .contracts import Fault, canonical, object_schema, parse, validate
from .runtime import Host, Model, Operation


def read_json(path):
    return parse(Path(path).read_text(encoding="utf-8"))


def fixture_host(scope, case):
    for name, node in scope["resources"].items():
        if name.startswith("context::"):
            node.setdefault("capability", "read:review_material")
    responses = case["fixture"]["responses"]
    models = set(case["fixture"]["available_models"])

    def invoke(request, call_id, timeout):
        try:
            response = responses[request["task"]][request["ordinal"] - 1]
        except (KeyError, IndexError) as exc:
            raise Fault("FIXTURE_EXHAUSTED", "No fixture response for request") from exc
        return {"text": response} if isinstance(response, str) else response

    def preflight(request, call_id, timeout):
        return {"passed": case["fixture"]["preflight_result"] == "passed"}

    return Host(
        scope, set(case["grant"]), {f"fixture:{model}" for model in models},
        models={"fixture": Model(invoke, "invoke:review_models", models.__contains__)},
        preflights={"fixture_preflight": Operation("invoke:review_models", preflight)},
    )


def openai_compatible(base_url, key_environment, capability):
    """Use an explicitly configured endpoint and a named credential environment variable."""
    endpoint = httpx.URL(base_url)
    if (endpoint.scheme != "https" or not endpoint.host or endpoint.userinfo
            or endpoint.query or endpoint.fragment):
        raise Fault("INVALID_MODEL_ENDPOINT", "Expected an HTTPS API base URL")
    endpoint = str(endpoint).rstrip("/") + "/chat/completions"

    def invoke(request, call_id, timeout):
        key = os.environ.get(key_environment)
        if not key:
            raise Fault("MODEL_CREDENTIAL_UNAVAILABLE", "Configured model credential is unavailable")
        messages = [
            {"role": "system", "content": "Follow the requested task. Return only the requested value."},
            {"role": "user", "content": canonical({"task": request["task"], "arguments": request["arguments"]})},
        ]
        params = request["params"]
        if not params.keys() <= {"temperature", "max_tokens", "top_p", "seed", "response_format"}:
            raise Fault("MODEL_CONFIG_CONFLICT", "Unsupported chat request parameter")
        payload = {"model": request["model"], "messages": messages, **params}
        if request["submit"] is not None:
            payload["tools"] = [{"type": "function", "function": {
                "name": "submit", "description": "Submit the validated ALang result.",
                "parameters": object_schema({"value": request["submit"]}),
            }}]
            payload["tool_choice"] = {"type": "function", "function": {"name": "submit"}}
        try:
            response = httpx.post(endpoint, json=payload, timeout=max(0.01, timeout * 0.9),
                                  headers={"Authorization": f"Bearer {key}"}, follow_redirects=False)
            response.raise_for_status()
        except httpx.HTTPStatusError as exc:
            raise Fault("MODEL_HTTP_ERROR", "Model endpoint returned an HTTP error",
                        http_status=exc.response.status_code) from exc
        except httpx.HTTPError as exc:
            raise Fault("EFFECT_UNCERTAIN", "Model request completion is unknown", reason=type(exc).__name__) from exc
        data = response.json()
        message = data["choices"][0]["message"]
        result = {"text": message.get("content") or "", "usage": data.get("usage")}
        if request["submit"] is not None:
            result["submissions"] = []
            for call in message.get("tool_calls", []):
                function = call["function"]
                if function["name"] == "submit":
                    arguments = validate(parse(function["arguments"], "INVALID_SUBMISSION"),
                                         object_schema({"value": request["submit"]}), code="INVALID_SUBMISSION")
                    result["submissions"].append({"id": call["id"], "value": arguments["value"]})
        return result

    return Model(invoke, capability)


def sha256(data):
    return hashlib.sha256(data).hexdigest()


def inside(root, relative):
    if not isinstance(relative, str) or not relative:
        raise Fault("INVALID_PATH", "Expected a relative path")
    path = Path(relative)
    if path.anchor or any(part == ".." or ":" in part for part in path.parts):
        raise Fault("AUTHORIZATION_DENIED", "Path escapes the workspace")
    target = (root / path).resolve()
    if target == root or not target.is_relative_to(root):
        raise Fault("AUTHORIZATION_DENIED", "Path escapes the workspace")
    return target


def atomic_write(path, data, call_id):
    staging = path.with_name(f".{path.name}.{call_id}.tmp")
    with staging.open("xb") as handle:
        handle.write(data)
        handle.flush()
        os.fsync(handle.fileno())
    try:
        staging.replace(path)
    finally:
        staging.unlink(missing_ok=True)


def investigation_tools(workspace):
    root = Path(workspace).resolve(strict=True)
    source = "calculator.py"

    def read(args, call_id, timeout):
        validate(args, object_schema({"path": {"const": source}}), code="INVALID_TOOL_INPUT")
        data = inside(root, source).read_bytes()
        return {"text": data.decode(), "sha256": sha256(data)}

    def check(args, call_id, timeout):
        validate(args, object_schema({}), code="INVALID_TOOL_INPUT")
        inside(root, "check.py")
        try:
            result = subprocess.run([sys._base_executable, "-B", str(inside(root, "check.py"))],
                                    cwd=root, stdin=subprocess.DEVNULL, capture_output=True, text=True, timeout=timeout * 0.9)
        except subprocess.TimeoutExpired as exc:
            raise Fault("EXTERNAL_TIMEOUT", "Workspace check timed out", cancelled=True) from exc
        return {"ok": result.returncode == 0, "exit_code": result.returncode,
                "stdout": result.stdout, "stderr": result.stderr}

    def fix(args, call_id, timeout):
        validate(args, object_schema({"expected_sha256": {"type": "string"}}), code="INVALID_TOOL_INPUT")
        path = inside(root, source)
        original = path.read_bytes()
        if sha256(original) != args["expected_sha256"] or b"return price + tax" not in original:
            raise Fault("WORKSPACE_CONFLICT", "Fixture source no longer matches the expected revision")
        new = original.replace(b"return price + tax", b"return price * (1 + tax)", 1)
        atomic_write(path, new, call_id)
        return {"path": source, "sha256": sha256(new), "base_sha256": sha256(original)}

    return {
        "read_calculator": Operation("read:investigation", read),
        "check_calculator": Operation("exec:investigation", check),
        "fix_calculator": Operation("write:investigation", fix),
    }


def apply_artifact(result, sandbox, workspace, grants):
    """Apply the one fixture artifact through an explicit host action."""
    if "apply:calculator" not in grants:
        raise Fault("AUTHORIZATION_DENIED", "Host application is not authorized")
    validate(result, object_schema({
        "path": {"const": "calculator.py"}, "sha256": {"type": "string"},
        "base_sha256": {"type": "string"},
    }), code="INVALID_ARTIFACT")
    src = inside(Path(sandbox).resolve(strict=True), result["path"])
    dst = inside(Path(workspace).resolve(strict=True), result["path"])
    data, original = src.read_bytes(), dst.read_bytes()
    if sha256(data) != result["sha256"] or sha256(original) != result["base_sha256"]:
        raise Fault("WORKSPACE_CONFLICT", "Host or artifact content changed")
    import uuid
    atomic_write(dst, data, uuid.uuid4().hex)
    return {"before": sha256(original), "after": sha256(dst.read_bytes())}
