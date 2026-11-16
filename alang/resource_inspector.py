"""Read-only, scope-only inspection of ALang resources.

Graph.link is the sole validation authority. Reporting walks Graph.edges and
never sends model requests, probes availability, runs preflight, or uses the network.
"""
from __future__ import annotations

from .compiler import Graph
from .contracts import Fault

_DEFAULT_MODEL = "<default>"

_UNAVAILABLE = {
    "source_identity": "EffectIdentity is declared by host adapters, not by a scope",
    "policy_binding": "Runtime binds policy identity during Procedure execution; a scope has no such association",
    "availability": "Model availability requires a host adapter and is never probed",
    "preflight": "Preflight hooks belong to the host and are never executed",
    "credentials": "Never included: inspection is scope-only",
}

_REASONS = {
    "select": "Dynamic selection is resolved at execution time",
    "host": "Dynamic host resolution is resolved at execution time",
    "compute": "Dynamic computation is resolved at execution time",
}


def _identity(key):
    return {"module": key[0], "revision": key[1],
            "name": key[2], "resource": list(key)}


def _endpoint(node):
    value = node.get("value")
    endpoint = {}
    if isinstance(value, dict):
        for field in ("adapter", "model"):
            if isinstance(value.get(field), str):
                endpoint[field] = value[field]
    return endpoint


class _Roots:
    """Program shim so Graph.link validates one declared address."""

    input_schema = {"type": "object"}

    def __init__(self, address):
        self.roots = [(address, None)]


def _entry(graph, key, declared, ancestors, addressed, candidates):
    node = graph.node(key)
    kind = node.get("type")
    definition = node.get("kind")
    resource = list(key)
    path = [*ancestors, resource]
    addressed_path = [*addressed, declared]
    entry = {"role": definition, "declared": declared, "resource": resource,
             "identity": _identity(key), "type": kind if isinstance(kind, str) else None,
             "path": path, "addressed_path": addressed_path}
    for field in ("capability", "binding"):
        if field in node:
            entry[field] = node[field]
    edges = graph.edges.get(key, [])

    def child(index, label):
        return _entry(graph, edges[index], label, path, addressed_path, candidates)

    if definition == "terminal":
        if kind == "ModelEndpoint":
            entry["endpoint"] = _endpoint(node)
            candidates.append({"resource": resource, "identity": entry["identity"],
                               "endpoint": entry["endpoint"],
                               "capability": node.get("capability"),
                               "path": path, "addressed_path": addressed_path})
        elif kind == "Procedure":
            value = node.get("value") if isinstance(node.get("value"), dict) else {}
            entry["procedure"] = {"name": value.get("name"),
                                  "ir_version": value.get("ir_version")}
        else:
            entry["value"] = {"redacted": True}
    elif definition == "alias":
        entry["target"] = child(0, node["target"])
    elif definition == "pool":
        declared_targets = list(node.get("candidates", []))
        entry["candidates"] = [child(i, label) for i, label in enumerate(declared_targets)]
        entry["policy_binding"] = {"status": "unavailable",
                                   "reason": _UNAVAILABLE["policy_binding"]}
    elif definition in ("select", "host", "compute"):
        declared_targets = node.get("candidates", []) if definition != "compute" else []
        if declared_targets:
            entry["candidates"] = [child(i, label)
                                   for i, label in enumerate(declared_targets)]
        dependencies = []
        cursor = len(declared_targets) if isinstance(declared_targets, (list, tuple)) else 0

        def dependency(role, name, declared_label):
            nonlocal cursor
            if cursor >= len(edges):
                return
            target = _entry(graph, edges[cursor], declared_label, path,
                            addressed_path, [])
            dependencies.append({"role": role, "name": name, "node": target})
            cursor += 1

        for name, address in node.get("inputs", {}).items():
            dependency("input", name, address)
        if definition == "select":
            resolver = node.get("selector")
        elif definition == "compute":
            resolver = node.get("resolver")
        else:
            resolver = None
        if isinstance(resolver, dict):
            model = resolver.get("model")
            dependency("resolver_model", None,
                       model if isinstance(model, str) else _DEFAULT_MODEL)
            for name, address in resolver.get("inputs", {}).items():
                dependency("resolver_input", name, address)
        entry["dependencies"] = dependencies
        entry["resolution"] = {"status": "unresolved", "reason": _REASONS[definition]}
    return entry


def inspect(scope, address):
    try:
        graph = Graph(scope)
        for module in graph.modules.values():
            if type(module.get("revision", "")) not in (str, int):
                raise Fault("INVALID_SCOPE", "Resource revisions must be text or integers", phase="link")
        key = graph.lookup(address, graph.root)
        graph.link(_Roots(address))
    except Fault as fault:
        error = fault.error
        if error["code"] == "INVALID_SCHEMA":
            error = {"code": "INVALID_SCHEMA", "message": "Resource schema is invalid", "phase": "link"}
        return {"status": "failed", "address": address, "error": error, "diagnostics": [error]}
    candidates = []
    tree = _entry(graph, key, address, [], [], candidates)
    return {"status": "ok", "address": address, "scope": graph.root,
            "resource": list(key), "type": tree["type"], "tree": tree, "candidates": candidates,
            "diagnostics": [], "unavailable": _UNAVAILABLE}
