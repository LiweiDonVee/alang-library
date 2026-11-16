"""JSON command-line host for controlled local environments."""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

from .adapters import fixture_host, investigation_tools, openai_compatible, read_json
from .contracts import Fault, canonical, digest, parse
from .registry import Registry
from .runtime import Host, Runtime, failed
from .resource_inspector import inspect as inspect_resource
from .tools import SCHEMAS, Tools


def runtime_from_config(path, db=None):
    path = Path(path).resolve()
    config = read_json(path)

    def relative(name):
        return path.parent / config[name]

    scope = read_json(relative("scope")) if isinstance(config["scope"], str) else config["scope"]
    if "modules" in config:
        scope["modules"] = {name: read_json(path.parent / value) for name, value in config["modules"].items()}
    if "fixture" in config:
        host = fixture_host(scope, read_json(relative("fixture")))
    else:
        host = Host(scope, set(config.get("grants", [])), set(config.get("destinations", [])))
    if "workspace" in config:
        host.tools.update(investigation_tools(relative("workspace")))
    for name, model in config.get("models", {}).items():
        host.models[name] = openai_compatible(model["base_url"], model["credential_env"], model["capability"])
    host.environment = digest({
        "workspace": str(relative("workspace").resolve()) if "workspace" in config else None,
        "models": config.get("models", {}), "fixture": config.get("fixture"),
    })
    host.limits = config.get("limits", {})
    host.wait_seconds = config.get("wait_seconds", 3600)
    host.manage_registry = config.get("manage_registry", False)
    if "procedures" in config:
        host.procedures = set(config["procedures"])
    return Runtime(host, db or path.parent / config.get("database", ".alang/state.sqlite"))


def parser():
    root = argparse.ArgumentParser(description=__doc__)
    root.add_argument("--host", help="Trusted local host configuration JSON")
    root.add_argument("--db")
    sub = root.add_subparsers(dest="action", required=True)
    tool = sub.add_parser("tool")
    tool.add_argument("name", choices=SCHEMAS)
    tool.add_argument("arguments", help="JSON argument file or '-' for stdin")
    trace = sub.add_parser("trace")
    trace.add_argument("run_id")
    recover = sub.add_parser("recover")
    recover.add_argument("run_id")
    inspect = sub.add_parser("inspect-resource")
    inspect.add_argument("--scope", required=True)
    inspect.add_argument("--address", required=True)
    candidate = sub.add_parser("candidate")
    candidate.add_argument("source")
    candidate.add_argument("--parent")
    for action in ("publish", "default", "deactivate"):
        operation = sub.add_parser(action)
        operation.add_argument("name")
        operation.add_argument("revision")
        if action == "publish":
            operation.add_argument("--validation", required=True)
        if action == "default":
            operation.add_argument("--expected")
    return root


def inspect_scope(path, address):
    try:
        return inspect_resource(read_json(path), address)
    except Fault as fault:
        error = fault.error
    except (OSError, KeyError, TypeError, ValueError, AttributeError, RecursionError):
        error = Fault("INVALID_SCOPE", "Could not read or inspect the scope document", phase="link").error
    return {"status": "failed", "address": address, "error": error, "diagnostics": [error]}


def main():
    root = parser()
    args = root.parse_args()
    if args.action != "inspect-resource" and args.host is None:
        root.error("the following arguments are required: --host")
    try:
        if args.action == "inspect-resource":
            result = inspect_scope(args.scope, args.address)
        else:
            runtime = runtime_from_config(args.host, args.db)
            registry = Registry(runtime)
            if args.action == "tool":
                values = parse(sys.stdin.read()) if args.arguments == "-" else read_json(args.arguments)
                result = Tools(runtime).call(args.name, values)
            elif args.action == "trace":
                result = {"trace": runtime.trace(args.run_id)}
            elif args.action == "recover":
                result = runtime.recover(args.run_id)
            elif args.action == "candidate":
                result = registry.save_candidate(read_json(args.source), args.parent)
            elif args.action == "publish":
                result = registry.publish(args.name, args.revision, args.validation)
            elif args.action == "default":
                result = registry.set_default(args.name, args.revision, expected=args.expected)
            else:
                registry.deactivate(args.name, args.revision)
                result = {"deactivated": args.revision}
    except Fault as fault:
        result = failed(fault)
    except (OSError, KeyError, TypeError, ValueError):
        result = failed(Fault("INVALID_HOST_CONFIG", "Could not load the host configuration or argument file",
                              phase="transport"))
    sys.stdout.buffer.write((canonical(result) + "\n").encode("utf-8"))
    return int(result.get("status") == "failed")


if __name__ == "__main__":
    raise SystemExit(main())
