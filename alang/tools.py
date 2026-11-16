"""Transport-neutral host tools. A trusted host owns scope and grants."""
from .contracts import Fault, object_schema, validate
from .registry import Registry
from .runtime import failed

TEXT = {"type": "string", "minLength": 1}
OBJECT = {"type": "object"}
RESULT = {
    "type": "object",
    "properties": {"status": {"enum": ["completed", "awaiting_input", "failed"]}},
    "required": ["status"],
}


def arguments(required, optional=None):
    schema = object_schema(required)
    schema["properties"].update(optional or {})
    return schema


SCHEMAS = {
    "alang_run_inline": (
        "Execute an ALang Procedure using the host's scope.",
        arguments({"source": OBJECT, "inputs": OBJECT}, {"limits": OBJECT}), RESULT),
    "alang_run_procedure": (
        "Execute a published ALang Procedure revision or the current default.",
        arguments({"name": TEXT, "inputs": OBJECT}, {"revision": TEXT, "limits": OBJECT}), RESULT),
    "alang_resume": (
        "Continue an ALang run from its input checkpoint.",
        arguments({"token": TEXT, "inputs": {}, "request_id": TEXT}), RESULT),
    "alang_list_procedures": (
        "List published defaults visible in the host scope.",
        arguments({}), object_schema({"procedures": {"type": "array"}})),
    "alang_describe_procedure": (
        "Read the signature and metadata of a published revision.",
        arguments({"name": TEXT}, {"revision": TEXT}), OBJECT),
}

PINNED_SCHEMAS = {
    "alang_list_procedures": ("List the Procedure revisions selected by this host.", arguments({}),
                              SCHEMAS["alang_list_procedures"][2]),
    "alang_describe_procedure": ("Read the signature and metadata of a selected Procedure revision.",
                                 arguments({"name": TEXT, "revision": TEXT}), OBJECT),
    "alang_run_procedure": ("Run a selected ALang Procedure. Pass the exact revision returned by discovery.",
                            arguments({"name": TEXT, "revision": TEXT, "inputs": OBJECT}), RESULT),
}


class Tools:
    def __init__(self, runtime):
        self.runtime, self.registry = runtime, Registry(runtime)
        self.handlers = {
            "alang_run_inline": runtime.run_inline,
            "alang_run_procedure": self.registry.run,
            "alang_resume": runtime.resume,
            "alang_list_procedures": lambda: {"procedures": self.registry.list()},
            "alang_describe_procedure": self.registry.describe,
        }

    def call(self, name, values):
        if name not in self.handlers:
            raise Fault("UNKNOWN_TOOL", "No such ALang tool", phase="transport", tool=name)
        validate(values, SCHEMAS[name][1], code="INVALID_TOOL_INPUT", phase="transport")
        try:
            return self.handlers[name](**values)
        except Fault as fault:
            return failed(fault)


class PinnedTools:
    """Discover and invoke only the exact revisions chosen by the trusted host."""

    def __init__(self, runtime, revisions):
        self.registry = Registry(runtime)
        self.revisions = dict(revisions)

    def call(self, name, values):
        if name not in PINNED_SCHEMAS:
            raise Fault("UNKNOWN_TOOL", "No such pinned ALang tool", phase="transport", tool=name)
        validate(values, PINNED_SCHEMAS[name][1], code="INVALID_TOOL_INPUT", phase="transport")
        try:
            if name == "alang_list_procedures":
                return {"procedures": [self.registry.describe(procedure, revision)
                                       for procedure, revision in sorted(self.revisions.items())]}
            if self.revisions.get(values["name"]) != values["revision"]:
                raise Fault("UNKNOWN_PROCEDURE", "Procedure revision is not selected by this host", phase="transport")
            if name == "alang_describe_procedure":
                return self.registry.describe(**values)
            return self.registry.run(**values)
        except Fault as fault:
            return failed(fault)
