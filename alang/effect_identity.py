"""Host-declared, non-secret identity for an external effect."""
from dataclasses import dataclass

from .contracts import Fault, digest

_HOST_FIELDS = ("source_id", "account_ref", "route_id", "route_revision", "adapter_version", "provider_model_id")
_REQUIRED_FIELDS = (*_HOST_FIELDS[:-1], "adapter_id", "destination")


@dataclass(frozen=True)
class EffectIdentity:
    source_id: str
    account_ref: str
    route_id: str
    route_revision: str
    adapter_version: str
    provider_model_id: str | None = None


def effect_binding(adapter, adapter_id, destination, kind):
    identity = adapter.identity
    if identity is None:
        return None
    if not isinstance(identity, EffectIdentity):
        raise Fault("INVALID_EFFECT_IDENTITY", "Expected an EffectIdentity")
    binding = {name: getattr(identity, name) for name in _HOST_FIELDS}
    binding.update(adapter_id=adapter_id, destination=destination)
    if not _complete(binding, kind) or adapter.source not in (None, identity.source_id):
        raise Fault("INVALID_EFFECT_IDENTITY", "Expected a complete non-secret adapter identity")
    return binding


def _complete(binding, kind):
    if not isinstance(binding, dict) or set(binding) != {*_REQUIRED_FIELDS, "provider_model_id"}:
        return False
    required = (*_REQUIRED_FIELDS, "provider_model_id") if kind == "model" else _REQUIRED_FIELDS
    return all(isinstance(binding[key], str) and binding[key].strip() for key in required) and (
        binding["provider_model_id"] is None or isinstance(binding["provider_model_id"], str))


def require_original_binding(intent, run_id, call_id, phase):
    intent = intent if isinstance(intent, dict) else {}
    binding = intent.get("binding")
    if (intent.get("kind") in ("model", "tool", "binding", "preflight")
            and _complete(binding, intent["kind"])
            and intent.get("run_id") == run_id and intent.get("call_id") == call_id
            and isinstance(intent.get("capability"), str)
            and intent.get("binding_fingerprint") == digest(binding)
            and intent.get("source") == binding["source_id"] and intent.get("adapter") == binding["adapter_id"]
            and intent.get("destination") == binding["destination"]
            and "request" in intent and intent.get("request_hash") == digest(intent["request"])):
        return binding
    raise Fault("ORIGINAL_BINDING_UNAVAILABLE", "Original effect lacks a verifiable binding",
                phase=phase, call_id=call_id)


def check_effect_binding(original, current, call_id, phase):
    if current is None:
        raise Fault("ORIGINAL_BINDING_UNAVAILABLE", "Current adapter lacks the original effect identity",
                    phase=phase, call_id=call_id)
    if digest(original) == digest(current):
        return
    for keys, code in (
        (("source_id",), "SOURCE_BINDING_MISMATCH"),
        (("account_ref",), "ACCOUNT_BINDING_MISMATCH"),
        (("route_id", "route_revision", "provider_model_id", "destination"), "ROUTE_BINDING_MISMATCH"),
        (("adapter_id", "adapter_version"), "ADAPTER_BINDING_MISMATCH"),
    ):
        if any(original.get(key) != current.get(key) for key in keys):
            raise Fault(code, "Current adapter does not match the original effect binding", phase=phase, call_id=call_id)
    raise Fault("EFFECT_BINDING_MISMATCH", "Current effect identity differs from the original",
                phase=phase, call_id=call_id)
