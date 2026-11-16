"""JSON value contracts and stable diagnostics."""
from __future__ import annotations

import hashlib
import json

from jsonschema import Draft202012Validator
from jsonschema.exceptions import SchemaError


class Fault(Exception):
    def __init__(self, code, message, *, phase="execute", **details):
        super().__init__(message)
        self.error = {"code": code, "message": message, "phase": phase, **details}


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)


def digest(value):
    return hashlib.sha256(canonical(value).encode()).hexdigest()


def parse(text, code="INVALID_JSON"):
    def reject_constant(value):
        raise ValueError(value)

    def unique_object(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise ValueError(f"duplicate field {key}")
            result[key] = value
        return result

    try:
        return json.loads(text, parse_constant=reject_constant, object_pairs_hook=unique_object)
    except (ValueError, TypeError, RecursionError) as exc:
        raise Fault(code, "Invalid JSON value") from exc


def object_schema(properties):
    return {
        "type": "object", "properties": properties,
        "required": list(properties), "additionalProperties": False,
    }


MODEL_REF = object_schema({"$model": {"type": "string", "minLength": 1}})
RESOURCE_KEY = {"type": "array", "items": {"type": "string"}, "minItems": 3, "maxItems": 3}

BUILTINS = {
    "Text": {"type": "string"},
    "Bool": {"type": "boolean"},
    "Int": {"type": "integer"},
    "Number": {"type": "number"},
    "JSON": {},
    "ModelRef": MODEL_REF,
    "ResolvedPool": object_schema({
        "kind": {"const": "ResolvedPool"},
        "id": {"type": "string", "minLength": 1},
        "address": {"type": "string"},
        "resource": RESOURCE_KEY,
        "candidates": {"type": "array", "minItems": 1, "items": object_schema({
            "ref": MODEL_REF, "resource": RESOURCE_KEY,
            "path": {"type": "array", "items": RESOURCE_KEY},
            "model": {"type": "string"},
            "destination": {"type": "string"}, "source": {"type": ["string", "null"]},
        })},
    }),
    "ModelEndpoint": {
        "type": "object",
        "properties": {
            "adapter": {"type": "string", "minLength": 1},
            "model": {"type": "string", "minLength": 1},
            "preflight": {"type": "string"},
            "defaults": {"type": "object"},
            "route_overrides": {"type": "object"},
            "allow_overrides": {"type": "array", "items": {"type": "string"}},
        },
        "required": ["adapter", "model"],
        "additionalProperties": False,
    },
    "Procedure": {"type": "object"},
}


def schema_for(type_name, types):
    if isinstance(type_name, dict):
        return checked_schema(type_name)
    if not isinstance(type_name, str):
        raise Fault("UNKNOWN_TYPE", "Expected a type name", phase="compile")
    if type_name.startswith("List<") and type_name.endswith(">"):
        return {"type": "array", "items": schema_for(type_name[5:-1], types)}
    if type_name.startswith("Ref<") and type_name.endswith(">"):
        target = type_name[4:-1]
        schema_for(target, types)
        return object_schema({"$ref": object_schema({
            "id": {"type": "string", "minLength": 1}, "type": {"const": target},
        })})
    if type_name in BUILTINS:
        return BUILTINS[type_name]
    if type_name in types:
        return checked_schema(types[type_name])
    raise Fault("UNKNOWN_TYPE", f"Unknown type {type_name}", phase="compile")


def checked_schema(schema):
    def visit(value):
        if isinstance(value, dict):
            # Hosted programs cannot cause the validator to load an external document.
            for key, child in value.items():
                if key in {"$ref", "$dynamicRef"} and (
                    not isinstance(child, str) or not child.startswith("#/")
                ):
                    raise Fault("INVALID_SCHEMA", "Only local JSON Schema pointers are supported", phase="compile")
                if key in {"$id", "$schema"}:
                    raise Fault("INVALID_SCHEMA", "Schema dialect and resource IDs are fixed", phase="compile")
                visit(child)
        elif isinstance(value, list):
            for child in value:
                visit(child)

    visit(schema)
    try:
        Draft202012Validator.check_schema(schema)
    except SchemaError as exc:
        raise Fault("INVALID_SCHEMA", exc.message, phase="compile") from exc
    return schema


def validate(value, schema, *, code="TYPE_MISMATCH", phase="execute", location=None):
    try:
        error = next(Draft202012Validator(schema).iter_errors(value), None)
    except Exception as exc:
        raise Fault("INVALID_SCHEMA", "Schema validation could not complete", phase=phase, location=location) from exc
    if error:
        # Do not echo data from the caller, external service, or resolver.
        raise Fault(
            code, f"Value violates {error.validator}", phase=phase,
            location=location, path=list(error.absolute_path),
        )
    return value


def references(value):
    """Find explicitly tagged references, including references nested in input objects."""
    if isinstance(value, dict):
        if set(value) == {"$ref"}:
            reference = value["$ref"]
            if (not isinstance(reference, dict) or set(reference) != {"id", "type"}
                    or not all(isinstance(item, str) and item for item in reference.values())):
                raise Fault("INVALID_REFERENCE", "Expected a typed resource reference", phase="link")
            yield reference
        elif set(value) != {"$literal"}:
            for child in value.values():
                yield from references(child)
    elif isinstance(value, list):
        for child in value:
            yield from references(child)
