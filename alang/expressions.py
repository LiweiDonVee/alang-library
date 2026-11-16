"""Side-effect-free JSON expressions shared by all ALang entry points."""
import operator

from .contracts import Fault, canonical, parse, validate


def try_parse_json(text, schema):
    if not isinstance(text, str):
        raise Fault("TYPE_MISMATCH", "Expected Text for try_parse_json")
    try:
        value = validate(parse(text), schema)
    except Fault as fault:
        error = fault.error
        if error["code"] not in {"INVALID_JSON", "TYPE_MISMATCH"}:
            raise
        return {"ok": False, "error": {"code": error["code"], "path": error.get("path", []),
                                       "message": error["message"]}}
    return {"ok": True, "value": value}


PURE_OPERATIONS = {
    "equals": (2, operator.eq),
    "lt": (2, operator.lt),
    "add": (2, operator.add),
    "length": (1, len),
    "tail": (2, lambda items, limit: items[-limit:] if limit else []),
    "flatten": (1, lambda groups: [item for group in groups for item in group]),
    "json": (1, canonical),
    "try_parse_json": (2, try_parse_json),
    "join": (2, lambda separator, items: separator.join(items)),
    "get": (3, lambda value, key, fallback: value.get(key, fallback) if isinstance(value, dict) else fallback),
    "choose_note": (2, lambda fallback, items: next((item for item in reversed(items)
                                                    if isinstance(item, str) and item), fallback)),
}
