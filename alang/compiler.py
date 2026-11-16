"""IR validation, lexical name binding and effect-free graph linking."""
from __future__ import annotations

from dataclasses import dataclass

from .contracts import Fault, checked_schema, digest, object_schema, references, schema_for, validate
from .expressions import PURE_OPERATIONS


# Required fields, optional fields, expressions evaluated in the current block.
OPERATIONS = {
    "let": ("as value", "", "value"),
    "emit": ("value", "", "value"),
    "resolve": ("as ref", "", "ref"),
    "resolve_model": ("as", "address", "address"),
    "model_call": ("as task arguments", "model params submit on_unavailable stream stream_tools", "model arguments params"),
    "stream_next": ("as stream", "", "stream"),
    "parse_json": ("as text schema", "", "text"),
    "tool_call": ("as tool arguments", "stream_call", "arguments stream_call"),
    "call": ("as procedure arguments", "", "arguments"),
    "parallel_call": ("as tasks", "coordination procedures max_tasks settlement", ""),
    "parallel_add": ("as batch tasks", "", "batch"),
    "parallel_dispatch_reserved": ("as batch task_ids", "", "batch task_ids"),
    "parallel_next": ("as batch", "", "batch"),
    "parallel_cancel": ("as batch task_ids", "", "batch task_ids"),
    "if": ("condition then", "else", "condition"),
    "for_each": ("items item collect_as body yield", "", "items"),
    "while": ("condition body", "", "condition"),
    "await_input": ("as schema reason", "", ""),
    "poll_steering": ("as", "", ""),
    "return": ("value", "", "value"),
}

def validate_parallel_tasks(tasks, max_tasks=None, *, phase="execute", location=None):
    code = "INVALID_IR" if phase == "compile" else "INVALID_PARALLEL_TASKS"
    if not isinstance(tasks, list) or not tasks or (max_tasks is not None and len(tasks) > max_tasks):
        message = "Expected a nonempty parallel task list" if max_tasks is None else f"Expected between 1 and {max_tasks} parallel tasks"
        raise Fault(code, message, phase=phase, location=location)
    task_ids = set()
    for task in tasks:
        if (not isinstance(task, dict) or set(task) != {"id", "procedure", "arguments", "limits"}
                or not all(isinstance(task[field], str) and task[field] for field in ("id", "procedure"))
                or task["id"] in task_ids):
            raise Fault(code, "Expected a unique task ID, procedure, arguments and limits", phase=phase, location=location)
        task_ids.add(task["id"])
        limits = task["limits"]
        if (not isinstance(limits, dict) or set(limits) != {"calls", "steps"}
                or type(limits["calls"]) is not int or not 0 <= limits["calls"] <= 10**9
                or type(limits["steps"]) is not int or not 0 < limits["steps"] <= 10**9):
            raise Fault(code, "Expected nonnegative calls and positive steps limits", phase=phase, location=location)
    return tasks


@dataclass
class Program:
    source: dict
    input_schema: dict
    output_schema: dict
    hash: str
    blocks: dict
    roots: list
    scope: dict | None = None


def invalid(message, location):
    raise Fault("INVALID_IR", message, phase="compile", location=location)


def expression(value, variables, location, types):
    if isinstance(value, list):
        for item in value:
            expression(item, variables, location, types)
    if isinstance(value, dict):
        if set(value) == {"$var"}:
            name = value["$var"]
            if not isinstance(name, str) or name not in variables:
                invalid("Unbound variable", location)
        elif set(value) == {"$op"}:
            spec = value["$op"]
            if (not isinstance(spec, list) or not spec or not isinstance(spec[0], str)
                    or spec[0] not in PURE_OPERATIONS or len(spec) != PURE_OPERATIONS[spec[0]][0] + 1):
                invalid("Invalid pure expression", location)
            if spec[0] == "try_parse_json":
                if not isinstance(spec[2], str):
                    invalid("Expected type name", location)
                schema_for(spec[2], types)
            for item in spec[1:]:
                expression(item, variables, location, types)
        elif set(value) == {"$field"}:
            pair = value["$field"]
            if not isinstance(pair, list) or len(pair) != 2 or not isinstance(pair[1], str):
                invalid("Expected $field [expression, name]", location)
            expression(pair[0], variables, location, types)
        elif set(value) not in ({"$ref"}, {"$literal"}):
            for item in value.values():
                expression(item, variables, location, types)


def is_constant(value):
    if isinstance(value, list):
        return all(is_constant(item) for item in value)
    if isinstance(value, dict):
        return set(value) not in ({"$var"}, {"$field"}, {"$literal"}, {"$op"}) and all(
            is_constant(item) for item in value.values())
    return True


def compile_program(source):
    required = {"ir_version", "name", "inputs", "output", "capabilities", "body"}
    if not isinstance(source, dict) or not required <= source.keys() <= required | {"types", "description"}:
        invalid("Invalid Procedure fields", "$")
    if type(source["ir_version"]) is not int or source["ir_version"] != 1:
        raise Fault("UNSUPPORTED_IR_VERSION", "Expected IR version 1", phase="compile")
    types = source.get("types", {})
    if not isinstance(types, dict) or not all(isinstance(item, dict) for item in types.values()):
        invalid("Expected a named schema object", "$.types")
    for item in types.values():
        checked_schema(item)
    if not isinstance(source["name"], str) or not source["name"]:
        invalid("Expected Procedure name", "$.name")
    if not isinstance(source.get("description", ""), str):
        invalid("Expected description text", "$.description")
    if not isinstance(source["inputs"], dict):
        invalid("Expected named inputs", "$.inputs")
    if not isinstance(source["capabilities"], list) or not all(
        isinstance(cap, str) and cap for cap in source["capabilities"]
    ):
        invalid("Expected capability names", "$.capabilities")
    input_schema = object_schema({name: schema_for(kind, types) for name, kind in source["inputs"].items()})
    output_schema = schema_for(source["output"], types)
    blocks = {}
    roots = []

    def block(body, parent, variables):
        if not isinstance(body, list):
            invalid("Expected instruction list", parent)
        blocks[parent] = body
        variables = set(variables)
        ids = set()
        for step in body:
            if not isinstance(step, dict) or not isinstance(step.get("id"), str) or not step["id"]:
                invalid("Each instruction needs an ID", parent)
            name = step["id"]
            if name in ids or any(char in name for char in "/:"):
                invalid("Duplicate or invalid instruction ID", parent)
            ids.add(name)
            location = f"{parent}/{name}"
            op = step.get("op")
            if not isinstance(op, str) or op not in OPERATIONS:
                invalid("Unknown operation", location)
            required, optional, expressions = (set(fields.split()) for fields in OPERATIONS[op])
            if not required | {"op", "id"} <= step.keys() <= required | optional | {"op", "id"}:
                invalid("Invalid instruction fields", location)
            if "on_unavailable" in step and step["on_unavailable"] != "return":
                invalid("Expected on_unavailable to be 'return'", location)
            if "on_unavailable" in step and "model" not in step:
                invalid("on_unavailable requires an explicit model expression", location)
            if op == "model_call" and "stream" in step and (type(step["stream"]) is not bool or
                                     (step["stream"] and {"submit", "on_unavailable"} & step.keys())):
                invalid("Expected a boolean stream option without submit or on_unavailable in streaming mode", location)
            if "stream_tools" in step:
                tools = step["stream_tools"]
                if not step.get("stream") or not isinstance(tools, dict) or not all(
                    isinstance(tool, str) and tool and isinstance(schema, str) for tool, schema in tools.items()
                ):
                    invalid("Expected static stream tool names mapped to schema names", location)
                for schema in tools.values():
                    schema_for(schema, types)
            for field in {"as", "task", "tool", "address", "procedure", "item", "collect_as", "reason"} & step.keys():
                if not isinstance(step[field], str) or not step[field]:
                    invalid(f"Expected text for {field}", location)
            for field in sorted(expressions & step.keys()):
                expression(step[field], variables, location, types)
                roots.extend((ref["id"], ref["type"]) for ref in references(step[field]))
            for field in {"schema", "submit"} & step.keys():
                schema_for(step[field], types)
            static_checks = {"return": ("value", output_schema), "if": ("condition", {"type": "boolean"}),
                             "while": ("condition", {"type": "boolean"}), "for_each": ("items", {"type": "array"})}
            if op in static_checks:
                field, expected = static_checks[op]
                if is_constant(step[field]):
                    validate(step[field], expected, phase="compile", location=location)
            if op == "resolve_model" or (op == "model_call" and "model" not in step):
                roots.append((step.get("address"), "ModelEndpoint"))
            if op == "call":
                roots.append((step["procedure"], "Procedure"))
            if op in {"parallel_call", "parallel_add"}:
                if "settlement" in step and step["settlement"] != "reconcile_uncertain":
                    invalid("Expected reconcile_uncertain settlement", location)
                if "coordination" in step and step["coordination"] != "event_driven":
                    invalid("Expected event_driven coordination", location)
                tasks = step["tasks"]
                bound = step.get("max_tasks")
                if bound is not None and (type(bound) is not int or bound < 1):
                    invalid("Expected positive max_tasks", location)
                procedures = step.get("procedures")
                if "procedures" in step:
                    if (not isinstance(procedures, list) or not procedures
                            or not all(isinstance(address, str) and address for address in procedures)
                            or len(set(procedures)) != len(procedures)):
                        invalid("Expected a finite unique static Procedure allowlist", location)
                    roots.extend((address, "Procedure") for address in procedures)
                if isinstance(tasks, list):
                    validate_parallel_tasks(tasks, bound, phase="compile", location=location)
                    for task in tasks:
                        if procedures is not None and task["procedure"] not in procedures:
                            invalid("Parallel task Procedure is outside the allowlist", location)
                        expression(task["arguments"], variables, location, types)
                        roots.append((task["procedure"], "Procedure"))
                        roots.extend((ref["id"], ref["type"]) for ref in references(task["arguments"]))
                else:
                    if op == "parallel_call" and (procedures is None or "max_tasks" not in step):
                        invalid("Runtime parallel tasks require procedures and max_tasks", location)
                    expression(tasks, variables, location, types)
                    roots.extend((ref["id"], ref["type"]) for ref in references(tasks))
            if op == "if":
                block(step["then"], f"{location}:then", variables)
                block(step.get("else", []), f"{location}:else", variables)
            elif op == "while":
                block(step["body"], f"{location}:body", variables)
            elif op == "for_each":
                locals_after = block(step["body"], f"{location}:body", variables | {step["item"]})
                expression(step["yield"], locals_after, location, types)
                roots.extend((ref["id"], ref["type"]) for ref in references(step["yield"]))
            for field in {"as", "collect_as"} & step.keys():
                variables.add(step[field])
        return variables

    block(source["body"], "$", source["inputs"])
    if not source["body"] or source["body"][-1].get("op") != "return":
        invalid("Top-level block must end with return", "$")
    return Program(source, input_schema, output_schema, digest(source), blocks, roots)


class Graph:
    def __init__(self, scope):
        self.scope = scope
        self.root = scope.get("scope")
        if not isinstance(self.root, str) or not self.root:
            raise Fault("INVALID_SCOPE", "Scope name is required", phase="link")
        if self.root in scope.get("modules", {}):
            raise Fault("INVALID_SCOPE", "A module shadows the root scope", phase="link")
        self.modules = {**scope.get("modules", {}), self.root: scope}
        self.programs = {}
        self.types = scope.get("types", {})
        self.ranks = {}
        self.edges = {}

    def lookup(self, address, module):
        if address is None:
            current_default = self.modules[module].get("default_model")
            if current_default is None and module != self.root:
                return self.lookup(None, self.root)
            address = current_default
        if not isinstance(address, str) or not address:
            raise Fault("UNKNOWN_SYMBOL", "Resource address is missing", phase="link")
        head, sep, tail = address.partition("::")
        if sep and head in self.modules:
            return self.local(head, tail)
        local = self.local(module, address, optional=True)
        if local:
            return local
        matches = [
            found for imported in dict.fromkeys(self.modules[module].get("imports", []))
            if (found := self.local(imported, address, optional=True))
        ]
        if len(matches) == 1:
            return matches[0]
        if matches:
            raise Fault("AMBIGUOUS_SYMBOL", f"Ambiguous symbol {address}", phase="link", sources=[key[0] for key in matches])
        raise Fault("UNKNOWN_SYMBOL", f"Unknown symbol {address}", phase="link", module=module)

    def local(self, module, name, optional=False):
        definition = self.modules.get(module)
        if definition is not None:
            resources = definition.get("resources", {})
            matches = [key for key in resources if key.rsplit("::", 1)[-1] == name] if "::" not in name else []
            if name in resources:
                return module, str(definition.get("revision", "")), name
            if len(matches) == 1:
                return module, str(definition.get("revision", "")), matches[0]
            if len(matches) > 1:
                raise Fault("AMBIGUOUS_SYMBOL", f"Ambiguous symbol {name}", phase="link", sources=matches)
        elif optional:
            raise Fault("UNKNOWN_MODULE", f"Unknown imported module {module}", phase="link")
        if optional:
            return None
        raise Fault("UNKNOWN_SYMBOL", f"Unknown symbol {name}", phase="link", module=module)

    def node(self, key):
        return self.modules[key[0]]["resources"][key[2]]

    def roots_for(self, program, module, inputs=None):
        roots = list(program.roots)
        if inputs is not None:
            validate(inputs, program.input_schema, phase="link")
            roots.extend((ref["id"], ref["type"]) for ref in references(inputs))
        return [(self.lookup(address, module), kind) for address, kind in roots]

    def link(self, program, inputs=None):
        roots = self.roots_for(program, self.root, inputs)
        active = []
        visited_programs = set()
        pure_candidates = set()

        def visit(key, expected, pool_path=False):
            node = self.node(key)
            kind = node.get("type")
            definition = node.get("kind")
            pool_path = pool_path or kind == "ResolvedPool"
            if pool_path and not (
                (kind == "ResolvedPool" and definition in {"pool", "alias"})
                or (kind == "ModelEndpoint" and definition in {"terminal", "alias"})
            ):
                raise Fault("INVALID_POOL_CANDIDATE", "Expected a static model endpoint or pool",
                            phase="link", resource=key)
            schema = schema_for(kind, self.types)
            if "capability" in node and (not isinstance(node["capability"], str) or not node["capability"]):
                raise Fault("INVALID_RESOURCE", "Expected a capability name", phase="link", resource=key)
            if expected and kind != expected:
                raise Fault("TYPE_MISMATCH", f"Expected {expected}, found {kind}", phase="link", resource=key)
            if key in active:
                cycle = active[active.index(key):] + [key]
                raise Fault("REFERENCE_CYCLE", "Resource dependency cycle", phase="link",
                            cycle=[item[2] for item in cycle], canonical_cycle=cycle)
            if key in self.ranks and (not pool_path or key in pure_candidates):
                return self.ranks[key]
            active.append(key)
            deps = []

            def edge(address, target_type=None):
                deps.append((self.lookup(address, key[0]), target_type))

            if node.get("binding", "first_read") not in {"first_read", "snapshot"}:
                raise Fault("INVALID_RESOURCE", "Unknown binding policy", phase="link", resource=key)
            if definition == "terminal":
                validate(node.get("value"), schema, phase="link", location=key)
                if kind == "Procedure" and key not in visited_programs:
                    child = compile_program(node["value"])
                    self.programs[key] = child
                    visited_programs.add(key)
                    # Calls belong to the execution graph, where recursion is budgeted.
                    roots.extend(self.roots_for(child, key[0]))
            elif definition == "alias":
                edge(node["target"], kind)
            elif definition == "pool":
                if kind != "ResolvedPool":
                    raise Fault("INVALID_RESOURCE", "Pool resource must have type ResolvedPool",
                                phase="link", resource=key)
                candidates = node.get("candidates")
                if not isinstance(candidates, list) or not candidates:
                    raise Fault("EMPTY_CANDIDATES", "Pool needs a finite candidate set", phase="link", resource=key)
                for candidate in candidates:
                    edge(candidate)
            elif definition in {"select", "compute", "host"}:
                if kind in {"ModelEndpoint", "Procedure"} and definition != "select":
                    raise Fault("INVALID_RESOURCE", "Endpoint and Procedure routes must select declared candidates",
                                phase="link", resource=key)
                if definition == "select":
                    candidates = node.get("candidates")
                    if not isinstance(candidates, list) or not candidates:
                        raise Fault("EMPTY_CANDIDATES", "Selection needs a finite candidate set", phase="link", resource=key)
                    for candidate in candidates:
                        edge(candidate, kind)
                    resolver = node.get("selector")
                    if resolver is None and (kind != "ModelEndpoint" or node.get("strategy") != "first_available"):
                        raise Fault("INVALID_RESOURCE", "Unknown selection strategy", phase="link", resource=key)
                elif definition == "host":
                    resolver = None
                    if node.get("binding") == "snapshot" and not node.get("source_revision"):
                        raise Fault("INVALID_RESOURCE", "Snapshot binding needs a source revision",
                                    phase="link", resource=key)
                    for address in node.get("candidates", []):
                        edge(address, kind)
                else:
                    resolver = node.get("resolver")
                    if resolver is None:
                        raise Fault("INVALID_RESOURCE", "Compute requires a resolver", phase="link", resource=key)
                for address in node.get("inputs", {}).values():
                    edge(address)
                if resolver is not None:
                    edge(resolver.get("model"), "ModelEndpoint")
                    for address in resolver.get("inputs", {}).values():
                        edge(address)
            else:
                raise Fault("INVALID_RESOURCE", "Unknown resource definition", phase="link", resource=key)
            self.edges[key] = [dep for dep, _ in deps]
            rank = 1 + max((visit(dep, target, pool_path) for dep, target in deps), default=-1)
            self.ranks[key] = rank
            if pool_path:
                pure_candidates.add(key)
            active.pop()
            return rank

        for key, expected in roots:
            visit(key, expected)
        return self.ranks
