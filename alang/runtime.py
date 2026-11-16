"""Single interpreter used by the Library and host transports."""
from __future__ import annotations

import copy
import inspect
import sqlite3
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field

from .compiler import Graph, compile_program, validate_parallel_tasks
from .effect_identity import EffectIdentity, check_effect_binding, effect_binding, require_original_binding
from .expressions import PURE_OPERATIONS
from .model_stream import TERMINAL as STREAM_TERMINAL, Stream, coalesce
from .contracts import Fault, canonical, digest, parse, schema_for, validate
from .store import ParallelCancelled, Store, parallel_event_checkpoint


@dataclass
class Model:
    invoke: object
    capability: str
    available: object = lambda name: True
    recover: object = None
    source: str | None = None
    reconcile: object = None
    identity: EffectIdentity | None = None
    open_stream: object = None


@dataclass
class Operation:
    capability: str
    invoke: object
    recover: object = None
    source: str | None = None
    reconcile: object = None
    identity: EffectIdentity | None = None


@dataclass
class Host:
    scope: dict
    grants: set
    destinations: set
    models: dict = field(default_factory=dict)
    preflights: dict = field(default_factory=dict)
    tools: dict = field(default_factory=dict)
    bindings: dict = field(default_factory=dict)
    limits: dict = field(default_factory=dict)
    request_schema: dict = field(default_factory=lambda: {"type": "object"})
    wait_seconds: float = 3600
    manage_registry: bool = False
    procedures: set | None = None
    environment: str | None = None
    authorize_model: object = lambda destination, request: True
    max_limits: dict = field(default_factory=dict)
    cancelled: object = lambda: False
    authorize_reconciliation: object = None
    authorize_steering: object = None
    parallel_tools: set[str] = field(default_factory=set)
    parallel_models: set[str] = field(default_factory=set)
    parallel_preflights: set[str] = field(default_factory=set)
    parallel_limits: dict = field(default_factory=lambda: {"active": 64, "cumulative": 64})
    authorize_parallel_cancel: object = None
    authorize_stream_cancel: object = None
    authorize_reconciled_continuation: object = None
    authorize_parallel_continuation: object = None
    authorize_reserved_dispatch: object = None
    authorize_child_takeover: object = None
    verify_child_exit: object = None
    authorize_parent_settlement: object = None
    verify_parent_exit: object = None
    authorize_parallel_child_input: object = None


LIMITS = {"steps": 1000, "calls": 80, "seconds": 120, "operation_seconds": 30}
STEERING_READ = "steering:read"
PARALLEL_CANCEL = "parallel:cancel"
PARALLEL_CHILD_OPS = {"let", "emit", "resolve", "resolve_model", "model_call", "stream_next", "parse_json", "tool_call",
                      "call", "if", "for_each", "while", "await_input", "parallel_next", "parallel_add",
                      "parallel_cancel", "return"}
PARALLEL_COORDINATION_OPS = {"let", "emit", "parse_json", "if", "for_each", "while", "parallel_next", "parallel_cancel",
                             "parallel_add", "parallel_dispatch_reserved"}
STREAM_OPS = {"let", "emit", "parse_json", "if", "for_each", "while", "stream_next", "tool_call"}
PARALLEL_BATCH_FAULTS = {"AUTHORIZATION_DENIED", "RUN_OWNERSHIP_LOST", "RUN_CANCELLED", "PERSISTENCE_FAILED",
                         "PARALLEL_RESOURCE_DENIED"}


class Waiting(Exception):
    def __init__(self, key, schema, reason):
        self.key, self.schema, self.reason = key, schema, reason


class ParallelBlocked(Exception):
    """An internal interpreter yield at a persisted batch checkpoint."""


class ParallelWorkers:
    def __init__(self, store, count):
        self.store = store
        self.pool = ThreadPoolExecutor(max_workers=count)
        self.futures = {}

    def submit(self, run_id, work, *args):
        with self.store.parallel_wake:
            if run_id not in self.futures:
                self.futures[run_id] = self.pool.submit(work, *args)
                self.futures[run_id].add_done_callback(lambda _: self.store.wake_parallel())

    def prune(self, read_results=True):
        for run_id, future in list(self.futures.items()):
            if future.done():
                del self.futures[run_id]
                if read_results:
                    future.result()

    def close(self, read_results, advance=None):
        # A running slice can enqueue descendants. Wait for the queue to quiesce before closing it.
        try:
            with self.store.parallel_wake:
                while True:
                    self.prune(read_results)
                    if advance is not None:
                        advance()
                    if not self.futures:
                        break
                    self.store.parallel_wake.wait(0.1)
        finally:
            with self.store.parallel_wake:
                while any(not future.done() for future in self.futures.values()):
                    self.store.parallel_wake.wait(0.1)
            self.pool.shutdown()


def evaluate(expr, values, types):
    if isinstance(expr, list):
        return [evaluate(item, values, types) for item in expr]
    if isinstance(expr, dict):
        if set(expr) == {"$op"}:
            name, *arguments = expr["$op"]
            args = [evaluate(argument, values, types) for argument in arguments]
            if name == "try_parse_json":
                args[1] = schema_for(args[1], types)
            return PURE_OPERATIONS[name][1](*args)
        if set(expr) == {"$var"}:
            return values[expr["$var"]]
        if set(expr) == {"$field"}:
            target, name = expr["$field"]
            value = evaluate(target, values, types)
            if not isinstance(value, dict) or name not in value:
                raise Fault("INVALID_FIELD", f"Field {name} is unavailable")
            return value[name]
        if set(expr) == {"$literal"}:
            return expr["$literal"]
        if set(expr) == {"$ref"}:
            return expr
        return {key: evaluate(item, values, types) for key, item in expr.items()}
    return expr


def failed(fault, run_id=None):
    return {"status": "failed", "run_id": run_id, "error": fault.error}


def check_parallel_value(value):
    if isinstance(value, dict):
        if (set(value) in ({"$model"}, {"run_id", "call_id"}, {"run_id", "batch_id"})
                or value.get("kind") == "ResolvedPool"):
            raise Fault("PARALLEL_VALUE_DENIED", "Run-local handles cannot cross a parallel task boundary")
        for item in value.values():
            check_parallel_value(item)
    elif isinstance(value, list):
        for item in value:
            check_parallel_value(item)


def parallel_outcome(task):
    cancellation = task["cancellation_id"]
    row = {"id": task["id"], "run_id": task["run_id"]}
    result = task["result"]
    if result is None:
        if task["dispatched"]:
            status = "cancel_pending" if cancellation else ("awaiting_input" if task["waiting"] else "unresolved")
        else:
            status = "not_dispatched"
        return {**row, "status": status, "effects": task["effects"]}
    if result["status"] == "not_dispatched":
        return {**row, "status": "not_dispatched", "cancellation_id": result["cancellation_id"], "effects": task["effects"]}
    if result["status"] == "completed":
        return {**row, "status": "completed", "value": result["value"], "effects": task["effects"]}
    if result["status"] == "cancelled":
        return {**row, "status": "cancelled", "error": result["error"], "effects": task["effects"]}
    if result["status"] != "failed":
        raise Fault("PARALLEL_STATE_INVALID", "Invalid durable parallel task outcome", child_run_id=task["run_id"])
    error = result["error"]
    status = "effect_uncertain" if error["code"] == "EFFECT_UNCERTAIN" else "failed"
    row.update(status=status, error=error, effects=task["effects"])
    if cancellation and status == "effect_uncertain" and error.get("reason") == "EXTERNAL_TIMEOUT":
        row["execution_status"] = "cancel_pending"
    return row


def parallel_settlement(tasks, batch_id, *, reconcile_uncertain):
    for task in tasks:
        result = task["result"]
        skipped = result is not None and result["status"] == "not_dispatched"
        if ((result is None or skipped) != (task["finish_order"] is None)
                or (skipped and (task["dispatched"] or not task["cancellation_id"]))):
            raise Fault("PARALLEL_STATE_INVALID", "Inconsistent parallel task settlement", batch_id=batch_id)
    outcomes = [parallel_outcome(task) for task in tasks]
    unresolved = [row for row in outcomes if row["status"] in {"unresolved", "cancel_pending", "awaiting_input"}
                  or row.get("execution_status") == "cancel_pending"]
    uncertain = [row for row in outcomes if row["status"] == "effect_uncertain"
                 and row.get("execution_status") != "cancel_pending"] if reconcile_uncertain else []
    return outcomes, unresolved, uncertain


def incomplete_parallel(batch_id, cancellation, outcomes, unresolved, *, supervised=False):
    message = ("Original batch was settled from storage and requires an authorized Join" if supervised
               else "Parallel batch has no complete durable join")
    return Fault("PARALLEL_INCOMPLETE", message, batch_id=batch_id, cancellation_id=cancellation,
                 completed_tasks=[row for row in outcomes if row["status"] == "completed"],
                 failed_tasks=[row for row in outcomes if row["status"] in {"failed", "effect_uncertain"}
                               and row.get("execution_status") != "cancel_pending"],
                 cancelled_tasks=[row for row in outcomes if row["status"] == "cancelled"],
                 unresolved_tasks=unresolved,
                 not_dispatched_tasks=[row for row in outcomes if row["status"] == "not_dispatched"])


def parallel_task_ids(task_ids, *, phase="cancel"):
    if (not isinstance(task_ids, list) or not task_ids
            or not all(isinstance(item, str) and item for item in task_ids)
            or len(set(task_ids)) != len(task_ids)):
        code = "INVALID_PARALLEL_CANCEL_REQUEST" if phase == "cancel" else "INVALID_PARALLEL_DISPATCH_REQUEST"
        raise Fault(code, "Expected a nonempty list of distinct task IDs", phase=phase)
    return sorted(task_ids)


def frame(key, values, grants, *, block="$", **extra):
    return {"program": key, "block": block, "pc": 0, "values": dict(values), "grants": sorted(grants),
            "binding": digest([key, values, sorted(grants)]), **extra}


class Runtime:
    def __init__(self, host, db):
        self.host = host
        self.store = db if isinstance(db, Store) else Store(db)
        self._listeners = {}
        self._listener_lock = threading.Lock()

    def subscribe(self, listener):
        """Subscribe a synchronous (value, context) callback to future IR emissions."""
        if not callable(listener):
            raise TypeError("Expected a callable event listener")
        registration = object()
        with self._listener_lock:
            self._listeners[registration] = listener

        def unsubscribe():
            with self._listener_lock:
                self._listeners.pop(registration, None)

        return unsubscribe

    def _publish(self, value, context):
        with self._listener_lock:
            listeners = tuple(self._listeners.values())
        for listener in listeners:
            try:
                completion = listener(value, context.copy())
                if inspect.isawaitable(completion):
                    if inspect.iscoroutine(completion):
                        completion.close()
                    raise TypeError("Event listeners must finish synchronously")
            except Exception as exc:
                raise Fault("SUBSCRIBER_FAILED", "An event subscriber failed", phase="event",
                            location=context["location"], subscriber_error=type(exc).__name__) from exc

    def compile(self, source, scope=None):
        program = compile_program(copy.deepcopy(source))
        program.scope = copy.deepcopy(self.host.scope if scope is None else scope)
        self._link(program, program.scope)
        return program

    def _link(self, program, scope, inputs=None):
        graph = Graph(scope)
        graph.link(program, inputs)
        for linked in (program, *graph.programs.values()):
            for block in linked.blocks.values():
                for step in block:
                    if step["op"] == "tool_call" and step["tool"] not in self.host.tools:
                        raise Fault("UNKNOWN_TOOL", "Tool is not registered", phase="link",
                                    location=step["id"], tool=step["tool"])
        return graph

    def run(self, program, inputs, limits=None, *, revision=None):
        if digest(program.source) != program.hash:
            return failed(Fault("COMPILED_PROGRAM_CHANGED", "Compile the changed Procedure before running it",
                                phase="start"))
        return self._run_inline(program.source, inputs, limits, revision, program.scope or self.host.scope)

    def run_inline(self, source, inputs, limits=None, *, revision=None):
        return self._run_inline(source, inputs, limits, revision, self.host.scope)

    def _run_inline(self, source, inputs, limits, revision, scope):
        try:
            source, scope, inputs = copy.deepcopy((source, scope, inputs))
            program = compile_program(source)
            start = time.perf_counter()
            graph = self._link(program, scope, inputs)
            link_ms = (time.perf_counter() - start) * 1000
            policy = {key: min(self.host.max_limits.get(key, value), self.host.limits.get(key, value))
                      for key, value in LIMITS.items()}
            for key, value in (limits if limits is not None else {}).items():
                if (key not in policy or type(value) not in (int, float) or not 0 < value <= 1e9
                        or (key in {"steps", "calls"} and type(value) is not int)):
                    raise Fault("INVALID_LIMIT", f"Invalid limit {key}", phase="start")
                policy[key] = min(policy[key], value)
            grants = self.host.grants.intersection(source["capabilities"])
            state = {
                "run_id": uuid.uuid4().hex, "source": source, "scope": scope, "inputs": inputs,
                "program_hash": program.hash, "revision": revision or program.hash,
                "scope_hash": digest(scope), "limits": policy, "environment": self.host.environment,
                "frames": [frame(None, inputs, grants)], "root_grants": sorted(grants),
                "destinations": sorted(self.host.destinations), "accessed": [],
                "cache": {}, "models": {}, "steps": 0, "calls": 0, "elapsed": 0,
                "original_deadline": time.time() + policy["seconds"],
                "trace": [{"event": "linked", "milliseconds": link_ms,
                           "graph": [{"key": key, "rank": rank, "edges": graph.edges[key]}
                                     for key, rank in graph.ranks.items()]}],
            }
            owner = self.store.create(state)
            return Execution(self, state, owner, program, graph).drive()
        except Fault as fault:
            return failed(fault)
        except (TypeError, KeyError, AttributeError, ValueError, RecursionError):
            return failed(Fault("INVALID_INPUT", "Malformed IR, scope, limits or inputs", phase="start"))

    def _reauthorize(self, state, phase="resume"):
        if (state["scope"]["scope"] != self.host.scope["scope"]
                or state["environment"] != self.host.environment):
            raise Fault("AUTHORIZATION_DENIED", "Checkpoint belongs to a different scope", phase=phase)
        grants = set(state["root_grants"]) & self.host.grants
        destinations = set(state["destinations"]) & self.host.destinations
        for category, value in state["accessed"]:
            allowed = grants if category == "capability" else destinations
            if value not in allowed:
                raise Fault("AUTHORIZATION_DENIED", "Previously accessed resource is no longer authorized",
                            phase=phase, target=value)
        for name in state.get("parallel_tools_used", []):
            if name not in self.host.parallel_tools:
                raise Fault("AUTHORIZATION_DENIED", "Parallel tool approval was withdrawn", phase=phase, tool=name)
        for destination in state.get("parallel_models_used", []):
            if destination not in self.host.parallel_models:
                raise Fault("AUTHORIZATION_DENIED", "Parallel model approval was withdrawn", phase=phase, target=destination)
        for name in state.get("parallel_preflights_used", []):
            if name not in self.host.parallel_preflights:
                raise Fault("AUTHORIZATION_DENIED", "Parallel preflight approval was withdrawn", phase=phase, target=name)
        state["root_grants"] = sorted(grants)
        state["destinations"] = sorted(destinations)
        for active in state["frames"]:
            active["grants"] = sorted(set(active["grants"]) & grants)

    def _authorize_continuation(self, state):
        self._reauthorize(state)
        if state.get("steering_cursor", 0):
            self._authorize_steering(state, "read")

    def resume(self, token, inputs, request_id, *, parent_settlement_id=None, expected_parent_owner=None):
        try:
            binding = self._child_input_settlement(parent_settlement_id, expected_parent_owner)
            if not all(isinstance(item, str) and 0 < len(item) <= 256 for item in (token, request_id)):
                raise Fault("INVALID_RESUME_REQUEST", "Expected token and request ID", phase="resume")
            owner, state = self.store.claim(token, inputs, request_id, self._authorize_continuation,
                                            self._authorize_parallel_child_input, parent_settlement=binding)
            return self._restart(owner, state) if owner else state
        except Fault as fault:
            return failed(fault)

    @staticmethod
    def _child_input_settlement(settlement_id, expected_owner):
        if settlement_id is None and expected_owner is None:
            return None
        if not all(isinstance(value, str) and 0 < len(value) <= 256 for value in (settlement_id, expected_owner)):
            raise Fault("INVALID_RESUME_REQUEST", "Expected a parent settlement ID and original owner", phase="resume")
        return {"id": settlement_id, "previous_owner": expected_owner}

    def parallel_child_waits(self, parent_run_id, batch_id, *, parent_settlement_id=None, expected_parent_owner=None):
        """Read current child input tokens as an explicitly authorized Host."""
        if not all(isinstance(value, str) and 0 < len(value) <= 256 for value in (parent_run_id, batch_id)):
            raise Fault("INVALID_RESUME_REQUEST", "Expected a parent Run ID and batch ID", phase="resume")
        binding = self._child_input_settlement(parent_settlement_id, expected_parent_owner)
        return self.store.parallel_child_waits(parent_run_id, batch_id, self._authorize_parallel_child_input,
                                               parent_settlement=binding)

    def _authorize_parallel_child_input(self, parent, child, task, checkpoint, action, inputs):
        authorize = self.host.authorize_parallel_child_input
        if not callable(authorize):
            raise Fault("AUTHORIZATION_DENIED", "Host did not authorize child input", phase="resume")
        binding = {"parent_run_id": parent["run_id"], "batch_id": child["parallel_parent"]["batch_id"],
                   "task_id": task, "child_run_id": child["run_id"], "revision": child["revision"],
                   "parent_owner": child["parallel_parent"]["owner"], "checkpoint": checkpoint}
        if settlement := child.get("child_input_settlement"):
            binding["parent_settlement_id"] = settlement["id"]
        if authorize(action, binding, copy.deepcopy(inputs)) is not True or self.host.authorize_parallel_child_input is not authorize:
            raise Fault("AUTHORIZATION_DENIED", "Host rejected child input", phase="resume")
        self._authorize_continuation(parent)
        self._authorize_continuation(child)
        if "child_input_settlement" in child and action == "resume":
            original_limits = dict(child["limits"])
            self._continuation_program(parent)
            self._continuation_program(child)
            if child["limits"] != original_limits:
                raise Fault("AUTHORIZATION_DENIED", "Original child limits exceed current Host policy", phase="resume")
        if (not set(child["root_grants"]) <= set(parent["root_grants"])
                or not set(child["destinations"]) <= set(parent["destinations"])):
            raise Fault("AUTHORIZATION_DENIED", "Child input exceeds the original parent authority", phase="resume")
        permitted = self.host.procedures
        if permitted is not None and (parent["source"]["name"] not in permitted or child["source"]["name"] not in permitted):
            raise Fault("AUTHORIZATION_DENIED", "Parent or child Procedure is no longer allowed", phase="resume")
        if action == "resume":
            if (digest(child["source"]) != child["program_hash"] or child["revision"] != child["program_hash"]
                    or digest(child["scope"]) != child["scope_hash"] or child["scope_hash"] != parent["scope_hash"]):
                raise Fault("CONTINUATION_REVISION_MISMATCH", "Original child Procedure or scope differs", phase="resume")
            self._execution_graph(child)

    def enqueue_steering(self, run_id, event_id, text):
        """Append a host-authorized event to a running or waiting Procedure."""
        try:
            if (not all(isinstance(item, str) and 0 < len(item) <= 256 for item in (run_id, event_id))
                    or not isinstance(text, str)):
                raise Fault("INVALID_STEERING_EVENT", "Expected a run ID, event ID and text", phase="steering")
            return self.store.enqueue_steering(run_id, event_id, text, self._authorize_steering)
        except Fault as fault:
            return failed(fault, run_id)

    def _authorize_steering(self, state, action):
        authorize = self.host.authorize_steering
        if (authorize is None or state["scope"]["scope"] != self.host.scope["scope"]
                or state["environment"] != self.host.environment
                or STEERING_READ not in self.host.grants or STEERING_READ not in state["root_grants"]
                or not authorize(action, state["run_id"])):
            raise Fault("AUTHORIZATION_DENIED", "Host rejected steering access", phase="steering")

    def request_parallel_cancel(self, parent_run_id, batch_id, cancellation_id, task_ids=None):
        """Persist a Host-authorized stop request for a reserved parallel batch."""
        try:
            if not all(isinstance(item, str) and 0 < len(item) <= 256
                       for item in (parent_run_id, batch_id, cancellation_id)):
                raise Fault("INVALID_PARALLEL_CANCEL_REQUEST", "Expected a parent run ID, batch ID and cancellation ID",
                            phase="cancel")
            if task_ids is not None:
                task_ids = parallel_task_ids(task_ids)
            return self.store.request_parallel_cancel(parent_run_id, batch_id, cancellation_id,
                                                      self._authorize_parallel_cancel, task_ids)
        except Fault as fault:
            return failed(fault, parent_run_id)
        except sqlite3.Error:
            return failed(Fault("PERSISTENCE_FAILED", "Could not persist the cancellation request", phase="cancel"),
                          parent_run_id)

    def _authorize_parallel_cancel(self, state, batch_id):
        authorize = self.host.authorize_parallel_cancel
        if (authorize is None or state["scope"]["scope"] != self.host.scope["scope"]
                or state["environment"] != self.host.environment or not authorize(state["run_id"], batch_id)):
            raise Fault("AUTHORIZATION_DENIED", "Host rejected parallel cancellation", phase="cancel")

    def request_stream_cancel(self, run_id, call_id, cancellation_id):
        """Persist a cancellation request for one model stream, without cancelling its Run."""
        try:
            if not all(isinstance(value, str) and 0 < len(value) <= 256 for value in (run_id, call_id, cancellation_id)):
                raise Fault("INVALID_CANCELLATION", "Expected run, stream effect and cancellation IDs", phase="cancel")
            return self.store.request_stream_cancel(run_id, call_id, cancellation_id, self._authorize_stream_cancel)
        except Fault as fault:
            return failed(fault, run_id)
        except sqlite3.Error:
            return failed(Fault("PERSISTENCE_FAILED", "Could not persist stream cancellation", phase="cancel"), run_id)

    def _authorize_stream_cancel(self, state, intent):
        self._reauthorize(state, phase="cancel")
        authorize = self.host.authorize_stream_cancel
        if (authorize is None or intent is None or intent.get("kind") != "model"
                or intent.get("request", {}).get("stream") is not True
                or intent["capability"] not in state["root_grants"] or intent["destination"] not in state["destinations"]
                or not authorize(state["run_id"], intent["call_id"])
                or not self.host.authorize_model(intent["destination"], copy.deepcopy(intent["request"]))):
            raise Fault("AUTHORIZATION_DENIED", "Host rejected stream cancellation", phase="cancel")
        self._reauthorize(state, phase="cancel")
        if self.host.authorize_stream_cancel is not authorize:
            raise Fault("AUTHORIZATION_DENIED", "Host cancellation policy changed", phase="cancel")
        require_original_binding(intent, state["run_id"], intent["call_id"], "cancel")
        self._bound_adapter(intent, phase="cancel")

    def recover(self, run_id):
        try:
            return self._restart(*self.store.recover(run_id, self._authorize_continuation, self._bound_adapter,
                                                    self._authorize_reconciled_continuation,
                                                    self._authorize_parallel_continuation,
                                                    self._authorize_reserved_dispatch))
        except Fault as fault:
            return failed(fault, run_id)
        except sqlite3.Error:
            return failed(Fault("PERSISTENCE_FAILED", "Could not read or persist the recovery claim"), run_id)

    def continue_reconciled_run(self, run_id, call_id, continuation_id):
        """Continue the original checkpoint after a trusted, persisted effect confirmation."""
        try:
            if not all(isinstance(value, str) and 0 < len(value) <= 256 for value in (run_id, call_id, continuation_id)):
                raise Fault("INVALID_CONTINUATION", "Expected run, effect and continuation IDs", phase="continue")
            owner, state = self.store.claim_reconciled(run_id, call_id, continuation_id,
                                                      self._authorize_reconciled_continuation, self._bound_adapter)
            return self._restart(owner, state) if owner else state
        except Fault as fault:
            return failed(fault, run_id)
        except sqlite3.Error:
            return failed(Fault("PERSISTENCE_FAILED", "Could not claim or read the continuation", phase="continue"), run_id)

    def continue_parallel_run(self, run_id, batch_id, continuation_id):
        """Resume a supervised batch checkpoint after the original children settle."""
        try:
            if not all(isinstance(value, str) and 0 < len(value) <= 256 for value in (run_id, batch_id, continuation_id)):
                raise Fault("INVALID_CONTINUATION", "Expected Run, batch and continuation IDs", phase="continue")
            owner, state = self.store.claim_parallel_continuation(
                run_id, batch_id, continuation_id, self._authorize_parallel_continuation, self._bound_adapter)
            return self._restart(owner, state) if owner else state
        except Fault as fault:
            return failed(fault, run_id)
        except sqlite3.Error:
            return failed(Fault("PERSISTENCE_FAILED", "Could not claim or read the parent continuation", phase="continue"),
                          run_id)

    def settle_abandoned_parallel_parent(self, parent_run_id, batch_id, takeover_id, *, expected_owner):
        """Record the original batch outcomes after a trusted Host verifies all local writers exited."""
        try:
            identifiers = (parent_run_id, batch_id, takeover_id, expected_owner)
            if not all(isinstance(value, str) and 0 < len(value) <= 256 for value in identifiers):
                raise Fault("INVALID_CONTINUATION", "Expected parent, batch, takeover and owner IDs", phase="continue")
            authorize = self.host.authorize_parent_settlement

            def check_authorization(state, binding):
                self._authorize_continuation(copy.deepcopy(state))
                if (self.host.authorize_parent_settlement is not authorize or not callable(authorize)
                        or authorize(copy.deepcopy(binding)) is not True or self.host.authorize_parent_settlement is not authorize):
                    raise Fault("AUTHORIZATION_DENIED", "Host rejected parent settlement", phase="continue")
                checked = copy.deepcopy(state)
                self._authorize_continuation(checked)
                self._continuation_program(checked)

            return self.store.settle_abandoned_parallel_parent(*identifiers, check_authorization,
                                                                self._verify_parent_exit, self._settle_parent_batch)
        except Fault as fault:
            return failed(fault, parent_run_id)
        except sqlite3.Error:
            return failed(Fault("PERSISTENCE_FAILED", "Could not settle the original parallel batch", phase="continue"), parent_run_id)

    def _verify_parent_exit(self, binding):
        verify = self.host.verify_parent_exit
        return callable(verify) and verify(binding) is True and self.host.verify_parent_exit is verify

    def _settle_parent_batch(self, state, batch_id, tasks, cancellation):
        # Inspect the saved instruction and durable outcomes without entering Execution.
        program, graph = self._execution_graph(state)
        current = state["frames"][-1]
        executing = program if current["program"] is None else graph.programs[tuple(current["program"])]
        block = executing.blocks.get(current["block"], [])
        instruction = block[current["pc"]] if 0 <= current["pc"] < len(block) else {}
        pending = current.get("pending", {})
        expected_handle = {"run_id": state["run_id"], "batch_id": batch_id}
        operation = pending.get("id", "")
        active = state.get("active_parallel", {})
        op = instruction.get("op")
        schedule = state.get("parallel_schedule")
        if schedule == "event":
            coordination = op == "parallel_next" and bool(operation)
        else:
            coordination = schedule is None and ((current["pc"] == len(block) and not pending)
                                                 or (op in PARALLEL_COORDINATION_OPS and op != "parallel_dispatch_reserved"))
        event_checkpoint = ("parallel_parent" in state and active.get("batch_id") == batch_id
                            and coordination)
        if event_checkpoint:
            eligible = (op not in {"parallel_next", "parallel_add", "parallel_cancel"}
                        or evaluate(instruction["batch"], current["values"], executing.source.get("types", {}))
                        == expected_handle)
        elif op == "parallel_call":
            eligible = pending.get("batch_id") == batch_id and instruction.get("coordination") != "event_driven"
        else:
            eligible = (op in {"parallel_next", "parallel_dispatch_reserved"} and active.get("batch_id") == batch_id
                        and evaluate(instruction["batch"], current["values"], executing.source.get("types", {})) == expected_handle
                        and (op != "parallel_dispatch_reserved"
                             or (pending.get("reserved_dispatch", {}).get("status") == "claimed"
                                 and pending["reserved_dispatch"].get("batch_id") == batch_id)))
        if (not eligible or (operation or not event_checkpoint)
                and operation.split(":", 1)[-1] != f"{current['block']}/{instruction.get('id')}"):
            raise Fault("CONTINUATION_PC_MISMATCH", "Original parent is outside a settled batch checkpoint", phase="continue")
        available_grants = set(current["grants"]) & self.host.grants
        available_destinations = set(state["destinations"]) & self.host.destinations
        for task in tasks:
            child = copy.deepcopy(task["state"])
            self._reauthorize(child, phase="continue")
            if any(value not in (available_grants if category == "capability" else available_destinations)
                   for category, value in child["accessed"]):
                raise Fault("AUTHORIZATION_DENIED", "Parent no longer has access to a child outcome", phase="continue")
        outcomes, unresolved, uncertain = parallel_settlement(
            tasks, batch_id, reconcile_uncertain=state.get("parallel_settlement", {}).get(batch_id) == "reconcile_uncertain")
        state["parallel_join_checkpoint"] = {"batch_id": batch_id, "operation": operation, "supervised": True,
                                             "takeover_children": [row["run_id"] for row in unresolved if row["status"] == "unresolved"],
                                             "waiting_children": [row["run_id"] for row in unresolved if row["status"] == "awaiting_input"],
                                             "children": [row["run_id"] for row in uncertain]}
        if event_checkpoint:
            state["parallel_join_checkpoint"]["event_checkpoint"] = parallel_event_checkpoint(state, batch_id)
        fault = incomplete_parallel(batch_id, cancellation, outcomes, unresolved + uncertain, supervised=True)
        return {**failed(fault, state["run_id"]),
                "revision": state["revision"], "scope_revision": state["scope"].get("revision"),
                "metrics": {key: state[key] for key in ("steps", "calls", "elapsed")}}

    def take_over_parallel_child(self, parent_run_id, batch_id, task_id, takeover_id, *, expected_owner):
        """Resume the original child after a trusted Host verifies its execution domain exited."""
        try:
            identifiers = (parent_run_id, batch_id, task_id, takeover_id, expected_owner)
            if not all(isinstance(value, str) and 0 < len(value) <= 256 for value in identifiers):
                raise Fault("INVALID_CONTINUATION", "Expected parent, batch, task, takeover and owner IDs", phase="continue")
            owner, state = self.store.claim_child_takeover(*identifiers, self._authorize_child_takeover,
                                                            self._verify_child_exit)
            return self._restart(owner, state) if owner else state
        except Fault as fault:
            return failed(fault, parent_run_id)
        except sqlite3.Error:
            return failed(Fault("PERSISTENCE_FAILED", "Could not claim or read child takeover", phase="continue"), parent_run_id)

    def _authorize_child_takeover(self, parent, child, binding):
        # Check the saved child against current policy without rewriting its frames.
        self._authorize_continuation(parent)
        authorize = self.host.authorize_child_takeover
        if (not callable(authorize) or authorize(copy.deepcopy(binding)) is not True
                or self.host.authorize_child_takeover is not authorize):
            raise Fault("AUTHORIZATION_DENIED", "Host rejected child takeover", phase="continue")
        self._authorize_continuation(parent)
        checked_child = copy.deepcopy(child)
        self._authorize_continuation(checked_child)
        self._continuation_program(parent)
        self._continuation_program(checked_child)
        if any(checked_child[key] != child[key] for key in ("root_grants", "destinations", "frames", "limits")):
            raise Fault("AUTHORIZATION_DENIED", "Original child grants or limits exceed current Host policy", phase="continue")
        self._execution_graph(checked_child)

    def _verify_child_exit(self, binding):
        verify = self.host.verify_child_exit
        # The trusted Host matches the saved owner to its supervised execution domain.
        return callable(verify) and verify(binding) is True and self.host.verify_child_exit is verify

    def _authorize_parallel_continuation(self, state, batch_id):
        self._authorize_continuation(state)
        authorize = self.host.authorize_parallel_continuation
        if authorize is None or not authorize(state["run_id"], batch_id):
            raise Fault("AUTHORIZATION_DENIED", "Host rejected parent continuation", phase="continue")
        self._authorize_continuation(state)
        if authorize is not self.host.authorize_parallel_continuation:
            raise Fault("AUTHORIZATION_DENIED", "Host parent continuation policy changed", phase="continue")
        self._continuation_program(state)
        self._execution_graph(state)

    def continue_reserved_dispatch(self, run_id, batch_id, task_ids, continuation_id):
        """Reopen the original coordinator under a scoped first-dispatch authorization."""
        try:
            if not all(isinstance(value, str) and 0 < len(value) <= 256 for value in (run_id, batch_id, continuation_id)):
                raise Fault("INVALID_CONTINUATION", "Expected Run, batch and continuation IDs", phase="continue")
            task_ids = parallel_task_ids(task_ids, phase="continue")
            owner, state = self.store.claim_reserved_dispatch(run_id, batch_id, task_ids, continuation_id,
                                                               self._authorize_reserved_dispatch, self._bound_adapter)
            return self._restart(owner, state) if owner else state
        except Fault as fault:
            return failed(fault, run_id)
        except sqlite3.Error:
            return failed(Fault("PERSISTENCE_FAILED", "Could not claim reserved-dispatch continuation", phase="continue"), run_id)

    def _authorize_reserved_dispatch(self, state, authorization, *, check_pc=False):
        self._authorize_continuation(state)
        authorize = self.host.authorize_reserved_dispatch
        if (authorize is None or authorization["parent_run_id"] != state["run_id"]
                or authorization["parent_revision"] != state["revision"]
                or not authorize(copy.deepcopy(authorization))):
            raise Fault("AUTHORIZATION_DENIED", "Host rejected reserved dispatch", phase="continue")
        self._authorize_continuation(state)
        if authorize is not self.host.authorize_reserved_dispatch:
            raise Fault("AUTHORIZATION_DENIED", "Host reserved-dispatch policy changed", phase="continue")
        self._continuation_program(state)
        program, graph = self._execution_graph(state)
        if check_pc:
            current = state["frames"][-1]
            executing = program if current["program"] is None else graph.programs[tuple(current["program"])]
            block = executing.blocks.get(current["block"], [])
            instruction = block[current["pc"]] if 0 <= current["pc"] < len(block) else {}
            handle = {"run_id": state["run_id"], "batch_id": authorization["batch_id"]}
            types = executing.source.get("types", {})
            if "settlement" in authorization:
                pending = current.get("pending", {})
                appended = pending.get("parallel_add", {})
                targets = [{"id": task["task_id"], "run_id": task["run_id"]} for task in authorization["tasks"]]
                if (instruction.get("op") != "parallel_add" or appended.get("status") != "reserved"
                        or appended.get("run_id") != state["run_id"] or appended.get("batch_id") != handle["batch_id"]
                        or any(target not in appended.get("tasks", []) for target in targets)
                        or pending.get("reserved_dispatch") is not None
                        or pending.get("id", "").split(":", 1)[-1] != f"{current['block']}/{instruction['id']}"
                        or evaluate(instruction["batch"], current["values"], types) != handle):
                    raise Fault("CONTINUATION_PC_MISMATCH", "Expected a committed append for the original slots", phase="continue")
                return
            if (instruction.get("op") != "parallel_next"
                    or current["pending"]["id"].split(":", 1)[-1] != f"{current['block']}/{instruction['id']}"
                    or evaluate(instruction["batch"], current["values"], types) != handle):
                raise Fault("CONTINUATION_PC_MISMATCH", "Expected the original parallel_next instruction", phase="continue")
            if not any(step["op"] == "parallel_dispatch_reserved"
                       for linked in (program, *graph.programs.values()) for block in linked.blocks.values() for step in block):
                raise Fault("RUN_NOT_CONTINUABLE", "Original coordinator has no reserved-dispatch instruction", phase="continue")

    def _continuation_program(self, state):
        for key, value in state["limits"].items():
            state["limits"][key] = min(value, self.host.limits.get(key, value), self.host.max_limits.get(key, value))
        program = compile_program(state["source"])
        if program.hash != state["program_hash"] or digest(state["scope"]) != state["scope_hash"]:
            raise Fault("CONTINUATION_REVISION_MISMATCH", "Original Procedure or scope snapshot differs", phase="continue")
        if self.host.procedures is not None and program.source["name"] not in self.host.procedures:
            raise Fault("AUTHORIZATION_DENIED", "Procedure is no longer allowed by this Host", phase="continue")
        return program

    def _authorize_reconciled_continuation(self, state, intent, envelope, *, check_pc=False):
        self._authorize_continuation(state)
        authorize = self.host.authorize_reconciled_continuation
        if (authorize is None or intent["capability"] not in state["root_grants"]
                or not authorize(state["run_id"], intent["call_id"])):
            raise Fault("AUTHORIZATION_DENIED", "Host rejected reconciled continuation", phase="continue")
        if intent["kind"] == "model" and (intent["destination"] not in state["destinations"]
                or not self.host.authorize_model(intent["destination"], copy.deepcopy(intent["request"]))):
            raise Fault("AUTHORIZATION_DENIED", "Host rejected the original model request", phase="continue")
        self._authorize_continuation(state)
        if self.host.authorize_reconciled_continuation is not authorize:
            raise Fault("AUTHORIZATION_DENIED", "Host continuation policy changed", phase="continue")
        self._bound_adapter(intent, phase="continue")
        self._continuation_program(state)
        if check_pc or "parallel_origin" in state:
            program, graph = self._execution_graph(state)
        if check_pc:
            current = state["frames"][-1]
            executing = program if current["program"] is None else graph.programs[tuple(current["program"])]
            block = executing.blocks.get(current["block"], [])
            if not 0 <= current["pc"] < len(block):
                raise Fault("CONTINUATION_PC_MISMATCH", "Original instruction is unavailable", phase="continue")
            instruction = block[current["pc"]]
            op = instruction["op"]
            if intent.get("request", {}).get("stream") is True:
                checkpoint = state.get("settled_stream", {})
                stream = checkpoint.get("stream", {})
                cursor = stream.get("cursor")
                journal = envelope.get("journal", [])
                ready = {event["id"]: {"name": event["name"], "arguments": event["arguments"]}
                         for sequence, event in journal if type(cursor) is int and sequence <= cursor
                         and event["kind"] == "tool_call_ready"}
                saved_ready = stream.get("ready", {})
                if (op != "stream_next" or checkpoint.get("operation") != current.get("pending", {}).get("id")
                        or checkpoint.get("frames_hash") != digest(state["frames"])
                        or stream.get("call_id") != intent["call_id"]
                        or stream.get("capability") != intent["capability"]
                        or stream.get("destination") != intent["destination"]
                        or type(cursor) is not int or not 0 <= cursor < len(journal)
                        or set(saved_ready) != set(ready)
                        or any({key: value.get(key) for key in ("name", "arguments")} != ready[name]
                               for name, value in saved_ready.items())
                        or evaluate(instruction.get("stream"), current["values"], executing.source.get("types", {}))
                        != {"run_id": state["run_id"], "call_id": intent["call_id"]}):
                    raise Fault("CONTINUATION_PC_MISMATCH", "Checkpoint is not the original sealed stream consumer",
                                phase="continue")
                return
            label = instruction.get("task") if op == "model_call" else instruction.get("tool")
            operation = current["pending"]["id"]
            expected = digest([state["run_id"], operation, intent["kind"], label])
            if (op != {"model": "model_call", "tool": "tool_call"}.get(intent["kind"])
                    or instruction.get("stream") or "stream_call" in instruction
                    or expected != intent["call_id"]
                    or operation.split(":", 1)[-1] != f"{current['block']}/{instruction['id']}"):
                raise Fault("CONTINUATION_PC_MISMATCH", "Checkpoint is not the original direct model or tool instruction",
                            phase="continue")
            if op == "model_call":
                raw = envelope["raw_response"]
                field = "submissions" if "submit" in instruction else "text"
                if not isinstance(raw, dict) or not isinstance(raw.get(field), list if field == "submissions" else str):
                    raise Fault("CONTINUATION_RESPONSE_INVALID", "Receipt lacks the model adapter result", phase="continue")

    def _bound_adapter(self, intent, phase="resume"):
        registries = {"model": self.host.models, "tool": self.host.tools,
                      "binding": self.host.bindings, "preflight": self.host.preflights}
        kind, adapter_id = intent["kind"], intent["adapter"]
        adapter = registries[kind].get(adapter_id)
        if adapter is None:
            raise Fault("ORIGINAL_ADAPTER_UNAVAILABLE", "Original adapter is unavailable",
                        phase=phase, call_id=intent["call_id"])
        current = effect_binding(adapter, adapter_id, intent["destination"], kind)
        check_effect_binding(intent["binding"], current, intent["call_id"], phase)
        if adapter.capability != intent["capability"]:
            raise Fault("ADAPTER_BINDING_MISMATCH", "Current adapter capability differs from the original",
                        phase=phase, call_id=intent["call_id"])
        return adapter

    def reconcile_effect(self, run_id, call_id, verification_id):
        """Verify a settled effect through a trusted Host lookup. Never continue the run."""
        result = {"run_id": run_id, "original_call_id": call_id, "verification_id": verification_id,
                  "receipt": None, "usage": None}
        try:
            if not all(isinstance(value, str) and 0 < len(value) <= 256
                       for value in (run_id, call_id, verification_id)):
                raise Fault("INVALID_VERIFICATION", "Expected run, effect and verification IDs", phase="reconcile")
            if self.host.authorize_reconciliation is None:
                raise Fault("AUTHORIZATION_DENIED", "Host reconciliation is disabled", phase="reconcile")
            target = self.store.reconciliation_target(run_id, call_id, verification_id)
            if target is None:
                return {**result, "status": "not_found", "reason": "RUN_NOT_FOUND"}
            self._reauthorize(target["state"], phase="reconcile")
            if target["effect_status"] is None:
                return {**result, "status": "not_found", "reason": "EFFECT_NOT_FOUND"}
            if not target["eligible"]:
                return {**result, "status": "rejected", "reason": "EFFECT_NOT_RECONCILABLE"}
            intent = target["intent"]
            try:
                require_original_binding(intent, run_id, call_id, "reconcile")
            except Fault:
                return {**result, "status": "still_unknown", "reason": "ORIGINAL_BINDING_UNAVAILABLE"}
            adapter = self._authorize_reconciliation(target["state"], intent)
            if target["audit"] is not None:
                if target["audit_call_id"] != call_id:
                    raise Fault("VERIFICATION_ID_CONFLICT", "Verification ID belongs to another effect", phase="reconcile")
                return target["audit"]

            response = None
            lookup = adapter.reconcile
            if lookup is None:
                result.update(status="still_unknown", reason="LOOKUP_UNSUPPORTED")
            else:
                try:
                    observed = lookup(copy.deepcopy(intent))
                except Exception:
                    result.update(status="still_unknown", reason="LOOKUP_FAILED")
                else:
                    if observed is None:
                        result.update(status="still_unknown", reason="NO_FINAL_RECEIPT")
                    else:
                        response = _reconciliation_response(intent, observed)
                        if response is None:
                            result.update(status="rejected", reason="RECEIPT_MISMATCH")
                        else:
                            result.update(status="confirmed", receipt=response["receipt"], usage=response["usage"])

            def authorize_commit(state, stored_intent):
                current = self._authorize_reconciliation(state, stored_intent)
                if current is not adapter or current.reconcile is not lookup:
                    raise Fault("RECONCILIATION_SOURCE_CHANGED", "The receipt adapter changed during lookup",
                                phase="reconcile")

            return self.store.commit_reconciliation(run_id, call_id, verification_id, intent,
                                                    authorize_commit, result, response)
        except Fault as fault:
            code = fault.error["code"]
            status = "conflict" if code in {"VERIFICATION_ID_CONFLICT", "RECEIPT_CONFLICT"} else "rejected"
            return {**result, "status": status, "reason": code, "receipt": None, "usage": None}

    def _authorize_reconciliation(self, state, intent):
        self._reauthorize(state, phase="reconcile")
        authorize = self.host.authorize_reconciliation
        if (authorize is None or intent["capability"] not in state["root_grants"]
                or not authorize(copy.deepcopy(intent))):
            raise Fault("AUTHORIZATION_DENIED", "Host rejected effect reconciliation", phase="reconcile")
        if intent["kind"] in {"model", "preflight"} and (intent["destination"] not in state["destinations"]
                or not self.host.authorize_model(intent["destination"], copy.deepcopy(intent["request"]))):
            raise Fault("AUTHORIZATION_DENIED", "Host rejected the original model destination or request",
                        phase="reconcile")
        self._reauthorize(state, phase="reconcile")
        if self.host.authorize_reconciliation is not authorize:
            raise Fault("AUTHORIZATION_DENIED", "Host verification policy changed", phase="reconcile")
        return self._bound_adapter(intent, phase="reconcile")

    def _restart(self, owner, state):
        if state.get("parallel_schedule") == "input":
            with self.store.parallel_wake:
                while True:
                    result = self.store.parallel_input_result(state, owner)
                    if result is not None:
                        return result
                    self.store.parallel_wake.notify_all()
                    self.store.parallel_wake.wait(0.1)
        program, graph = self._execution_graph(state)
        state["trace"].append({"event": "continued", "request_id": state.get("request_id")})
        return Execution(self, state, owner, program, graph).drive()

    def _execution_graph(self, state):
        program = compile_program(state["source"])
        origin = state.get("parallel_origin")
        if origin is None:
            graph = Graph(state["scope"])
            graph.link(program, state["inputs"])
        else:
            link = state.get("parallel_link", origin)
            parent = compile_program(link["source"])
            graph = self._link(parent, state["scope"], link["inputs"])
            linked = graph.programs.get(tuple(origin["target"]))
            if (parent.hash != link["program_hash"] or digest(origin["source"]) != origin["program_hash"]
                    or linked is None or linked.hash != program.hash):
                raise Fault("CONTINUATION_REVISION_MISMATCH", "Original parallel Procedure binding differs", phase="continue")
        return program, graph

    def trace(self, run_id):
        state, _ = self.store.inspect(run_id)
        self._reauthorize(state)
        return state["trace"]


def _reconciliation_response(intent, observed):
    try:
        # Normalize and detach adapter data before comparing or persisting it.
        envelope = parse(canonical(observed))
        if not isinstance(envelope, dict) or not {"receipt", "raw_response"} <= envelope.keys():
            return None
        receipt = envelope["receipt"]
        binding = {key: intent[key] for key in ("run_id", "call_id", "source", "destination",
                                                "request_hash", "binding_fingerprint")}
        binding["response_hash"] = digest(envelope["raw_response"])
        usage = envelope.get("usage")
        if (not isinstance(receipt, dict) or receipt.get("final") is not True
                or any(receipt.get(key) != value for key, value in binding.items())
                or (usage is not None and not isinstance(usage, dict))):
            return None
        if "provider_request_id" in receipt and (not isinstance(receipt["provider_request_id"], str)
                                                  or not receipt["provider_request_id"]):
            return None
        return {"receipt": receipt, "raw_response": envelope["raw_response"], "usage": usage}
    except (Fault, TypeError, ValueError, RecursionError):
        return None


class Execution:
    def __init__(self, runtime, state, owner, program, graph, *, deadline=None, workers=None):
        self.runtime, self.state, self.owner = runtime, state, owner
        self.host, self.store = runtime.host, runtime.store
        self.program, self.graph = program, graph
        self.started = time.monotonic()
        self.prior_time = state["elapsed"]
        self.deadline = deadline
        self.current = None
        self.capture = []
        self.parallel_workers = workers
        self.owns_parallel_workers = False
        self.live_stream = None
        self.reconciled_response_read = False
        self.emission = None

    def trace(self, event, **data):
        self.state["trace"].append({"event": event, **data})

    def update_time(self):
        now = time.monotonic()
        self.state["elapsed"] = self.prior_time + now - self.started
        remaining = self.state["limits"]["seconds"] - self.state["elapsed"]
        if "parallel_parent" in self.state and "original_deadline" in self.state:
            remaining = min(remaining, self.state["original_deadline"] - time.time())
        if "continuation_deadline" in self.state:
            remaining = min(remaining, self.state["continuation_deadline"] - time.time())
        return min(remaining, self.deadline - now) if self.deadline is not None else remaining

    def require(self, category, value):
        if value is None:
            return
        allowed = self.current["grants"] if category == "capability" else self.state["destinations"]
        if value not in allowed or ("parallel_parent" in self.state and value not in
                                    (self.host.grants if category == "capability" else self.host.destinations)):
            raise Fault("AUTHORIZATION_DENIED", "Operation exceeds the current grant", target=value)
        access = [category, value]
        if access not in self.state["accessed"]:
            self.state["accessed"].append(access)
        for capture in self.capture:
            if access not in capture:
                capture.append(access)

    def budget_used(self, kind):
        return self.state[kind] + self.state.get("parallel_reserved", {}).get(kind, 0)

    def check_parallel_data_node(self, key, node):
        if node["kind"] not in {"terminal", "alias"} and not (node["kind"] == "pool" and node["type"] == "ResolvedPool"):
            raise Fault("PARALLEL_RESOURCE_DENIED", "Parallel children require statically linked resources", resource=key)

    def check_parallel_tool(self, name, grants):
        tool = self.host.tools.get(name)
        if (name not in self.host.parallel_tools or tool is None or tool.capability not in grants
                or tool.capability not in self.host.grants):
            raise Fault("AUTHORIZATION_DENIED", "Tool is not approved for this parallel child", tool=name)
        return tool

    def parallel_policy(self):
        policy = {"active": 64, "cumulative": 64, **self.host.parallel_limits}
        policy.setdefault("workers", policy["active"])
        if set(policy) != {"active", "cumulative", "workers"} or any(type(value) is not int or value < 1 for value in policy.values()):
            raise Fault("INVALID_LIMIT", "Expected positive Host parallel active, cumulative and worker limits")
        return policy

    @staticmethod
    def check_child_batch(instruction):
        if "settlement" in instruction:
            raise Fault("PARALLEL_OPERATION_DENIED", "A child-owned batch cannot request independent reconciliation",
                        operation="parallel_call")

    def check_parallel_program(self, target, grants, inputs=None, visited=None):
        visited = set() if visited is None else visited
        marker = (target, tuple(sorted(grants)))
        if marker in visited:
            return
        visited.add(marker)
        program = self.graph.programs[target]
        for block in program.blocks.values():
            for step in block:
                op = step["op"]
                if op == "parallel_call":
                    self.check_child_batch(step)
                elif op not in PARALLEL_CHILD_OPS:
                    raise Fault("PARALLEL_OPERATION_DENIED", "Instruction is outside the parallel subset", operation=op)
                if op == "tool_call":
                    self.check_parallel_tool(step["tool"], grants)
        for resource, expected_type in self.graph.roots_for(program, target[0], inputs):
            if resource not in self.graph.ranks or (expected_type and self.graph.node(resource)["type"] != expected_type):
                raise Fault("UNLINKED_REFERENCE", "Parallel child input reference was not linked", resource=resource)
            pending = [resource]
            seen = set()
            while pending:
                key = pending.pop()
                if key in seen:
                    continue
                seen.add(key)
                node = self.graph.node(key)
                self.check_parallel_data_node(key, node)
                if node.get("capability") is not None and node["capability"] not in grants:
                    raise Fault("AUTHORIZATION_DENIED", "Parallel child cannot access a linked resource", resource=key)
                if node["kind"] == "terminal" and node["type"] == "Procedure":
                    nested = self.graph.programs[key]
                    self.check_parallel_program(key, grants & set(nested.source["capabilities"]), visited=visited)
                if node["kind"] == "terminal" and node["type"] == "ModelEndpoint":
                    endpoint = node["value"]
                    destination = f"{endpoint['adapter']}:{endpoint['model']}"
                    adapter = self.host.models.get(endpoint["adapter"])
                    if (destination not in self.host.parallel_models or destination not in self.host.destinations
                            or destination not in self.state["destinations"] or adapter is None or adapter.capability not in grants):
                        raise Fault("AUTHORIZATION_DENIED", "Model is not approved for parallel use", target=destination)
                    hook = endpoint.get("preflight")
                    if hook and (hook not in self.host.parallel_preflights or hook not in self.host.preflights
                                 or self.host.preflights[hook].capability not in grants):
                        raise Fault("AUTHORIZATION_DENIED", "Preflight is not approved for parallel use", target=hook)
                pending.extend(self.graph.edges[key])

    def prepare_parallel_child(self, task, module, types, batch_id, ordinal, seconds):
        target = self.graph.lookup(task["procedure"], module)
        while True:
            node = self.graph.node(target)
            if node["type"] != "Procedure" or node["kind"] not in {"alias", "terminal"}:
                raise Fault("PARALLEL_PROCEDURE_DENIED", "Parallel calls require a static Procedure", resource=target)
            capability = node.get("capability")
            self.require("capability", capability)
            if capability is not None and capability not in self.host.grants:
                raise Fault("AUTHORIZATION_DENIED", "Parallel Procedure grant was withdrawn", resource=target)
            if node["kind"] == "terminal":
                break
            target = self.graph.edges[target][0]
        child = self.graph.programs[target]
        arguments = copy.deepcopy(task["arguments"])
        validate(arguments, child.input_schema)
        check_parallel_value(arguments)
        grants = set(self.current["grants"]) & set(child.source["capabilities"]) & self.host.grants
        self.check_parallel_program(target, grants, arguments)
        state = {
            "run_id": digest([batch_id, ordinal]), "source": child.source, "scope": self.state["scope"],
            "inputs": arguments, "program_hash": child.hash, "revision": child.hash,
            "scope_hash": self.state["scope_hash"], "environment": self.state["environment"],
            "limits": {**self.state["limits"], **task["limits"], "seconds": seconds},
            "frames": [frame(list(target), arguments, grants)], "root_grants": sorted(grants),
            "destinations": sorted(set(self.state["destinations"]) & self.host.destinations),
            "accessed": [], "cache": {}, "models": {}, "steps": 0, "calls": 0, "elapsed": 0,
            "original_deadline": min(self.state.get("original_deadline", time.time() + seconds), time.time() + seconds),
            "trace": [], "parallel_parent": {"run_id": self.state["run_id"], "owner": self.owner, "batch_id": batch_id},
            "parallel_origin": {"source": self.state["source"], "inputs": self.state["inputs"],
                                "program_hash": self.state["program_hash"], "target": list(target)},
        }
        if "parallel_origin" in self.state:
            state["parallel_link"] = self.state.get("parallel_link", self.state["parallel_origin"])
        return state

    def static_data_path(self, key):
        while True:
            node = self.graph.node(key)
            self.check_parallel_data_node(key, node)
            yield key
            if node["kind"] != "alias":
                break
            key = self.graph.edges[key][0]

    def run_parallel_child(self, child, deadline, owner=None):
        if owner is None:
            owner = self.store.dispatch_parallel(child)
        if owner is None:
            return
        program = self.graph.programs[tuple(child["frames"][0]["program"])]
        result = Execution(self.runtime, child, owner, program, self.graph, deadline=deadline,
                           workers=self.parallel_workers).drive()
        if result is not None and result.get("error", {}).get("code") == "PERSISTENCE_FAILED":
            raise Fault("PERSISTENCE_FAILED", "Could not persist the parallel child", child_run_id=child["run_id"],
                        batch_id=child["parallel_parent"]["batch_id"])

    def parallel_call(self, key, instruction, module, types):
        pending = self.current["pending"]
        batch_id = pending.get("batch_id")
        coordinated = instruction.get("coordination") == "event_driven"
        child_owned = "parallel_parent" in self.state
        park_join = child_owned and not coordinated
        if child_owned:
            self.check_child_batch(instruction)
        if batch_id is None:
            if self.state.get("active_parallel_continuation", {}).get("event_batch"):
                raise Fault("PARALLEL_OPERATION_DENIED", "Event continuation cannot start another batch")
            if "active_reserved_dispatch" in self.state:
                raise Fault("PARALLEL_OPERATION_DENIED", "Reserved-dispatch continuation cannot start another batch")
            if child_owned and self.parallel_workers is None:
                raise Fault("PARALLEL_CHILD_CONTINUATION_DENIED", "Independent child execution cannot start a nested batch")
            batch_id = digest([self.state["run_id"], key])
            tasks = evaluate(instruction["tasks"], self.current["values"], types)
            validate_parallel_tasks(tasks, instruction.get("max_tasks"))
            policy = self.parallel_policy()
            self.state["parallel_policy"] = policy
            allowed = instruction.get("procedures")
            if allowed is None:
                allowed = [task["procedure"] for task in instruction["tasks"]]
            if any(task["procedure"] not in allowed for task in tasks):
                raise Fault("PARALLEL_PROCEDURE_DENIED", "Parallel task Procedure is outside the static allowlist")
            reservation = {kind: sum(task["limits"][kind] for task in tasks) for kind in ("calls", "steps")}
            for kind, amount in reservation.items():
                if self.budget_used(kind) + amount > self.state["limits"][kind]:
                    raise Fault("BUDGET_EXHAUSTED", "Parallel reservation exceeds remaining budget", limit=kind)
            seconds = self.update_time()
            if seconds <= 0:
                raise Fault("BUDGET_EXHAUSTED", "Parallel batch has no remaining time")
            deadline = time.monotonic() + seconds
            children = [(task["id"], self.prepare_parallel_child(task, module, types, batch_id, ordinal, seconds))
                        for ordinal, task in enumerate(tasks)]
            previous = self.state.get("parallel_reserved")
            self.state["parallel_reserved"] = {kind: (previous or {}).get(kind, 0) + amount
                                               for kind, amount in reservation.items()}
            self.state.setdefault("parallel_batches", {})[batch_id] = [
                {"id": task_id, "run_id": child["run_id"]} for task_id, child in children]
            pending["batch_id"] = batch_id
            if park_join:
                self.state["parallel_schedule"] = "join"
            if coordinated:
                self.state["active_parallel"] = {"batch_id": batch_id, "cursor": 0,
                                                 "procedures": list(allowed), "module": module}
            if "settlement" in instruction:
                self.state.setdefault("parallel_settlement", {})[batch_id] = instruction["settlement"]
            self.trace("parallel_reserved", batch_id=batch_id, reservation=reservation)
            try:
                self.store.reserve_parallel(self.state, self.owner, batch_id, children)
            except (sqlite3.Error, Fault) as exc:
                if park_join:
                    del self.state["parallel_schedule"]
                del pending["batch_id"]
                del self.state["parallel_batches"][batch_id]
                if not self.state["parallel_batches"]:
                    del self.state["parallel_batches"]
                if coordinated:
                    del self.state["active_parallel"]
                if "settlement" in instruction:
                    del self.state["parallel_settlement"][batch_id]
                    if not self.state["parallel_settlement"]:
                        del self.state["parallel_settlement"]
                if previous is None:
                    del self.state["parallel_reserved"]
                else:
                    self.state["parallel_reserved"] = previous
                self.state["trace"].pop()
                if isinstance(exc, Fault):
                    raise
                raise Fault("PERSISTENCE_FAILED", "Could not reserve the parallel batch", batch_id=batch_id) from exc
            if not child_owned:
                self.open_parallel_workers(policy)
            self.dispatch_parallel_children(children, deadline, batch_id)
            if park_join:
                raise ParallelBlocked
        if coordinated:
            return {"run_id": self.state["run_id"], "batch_id": batch_id}
        if not child_owned:
            self.wait_parallel_children(batch_id)
        return self.join_parallel(batch_id)

    def dispatch_parallel_children(self, children, deadline, batch_id, *, owners=None):
        try:
            for _, child in children:
                run_id = child["run_id"]
                self.parallel_workers.submit(run_id, self.run_parallel_child, child, deadline,
                                             None if owners is None else owners[run_id])
        except RuntimeError as exc:
            raise Fault("PARALLEL_DISPATCH_FAILED", "Could not start reserved parallel tasks", batch_id=batch_id) from exc

    def add_parallel_tasks(self, handle, tasks, types):
        active = self.active_parallel(handle)
        batch_id = active["batch_id"]
        pending = self.current["pending"]
        if "parallel_add" in pending:
            grant = self.state.get("active_reserved_dispatch", {})
            if "settlement" in grant:
                # A separate Host request can claim the original slots at the saved append PC.
                self.dispatch_reserved_tasks(handle, [task["task_id"] for task in grant["tasks"]], pending["id"])
            return pending["parallel_add"]
        event_continuation = self.state.get("active_parallel_continuation", {}).get("event_batch") == batch_id
        if (self.parallel_workers is None and not event_continuation) or "active_reserved_dispatch" in self.state:
            self.join_parallel(batch_id)
            raise Fault("PARALLEL_INCOMPLETE", "Recovery cannot append new tasks", batch_id=batch_id)
        validate_parallel_tasks(tasks)
        manifest = self.state["parallel_batches"][batch_id]
        if {row["id"] for row in manifest} & {task["id"] for task in tasks}:
            raise Fault("INVALID_PARALLEL_TASKS", "Appended task IDs must be unique across the batch")
        if any(task["procedure"] not in active.get("procedures", []) for task in tasks):
            raise Fault("PARALLEL_PROCEDURE_DENIED", "Appended Procedure is outside the batch's static allowlist")
        policy = self.parallel_policy()
        if event_continuation:
            policy = {kind: min(limit, self.state["parallel_policy"][kind]) for kind, limit in policy.items()}
        reservation = {kind: sum(task["limits"][kind] for task in tasks) for kind in ("calls", "steps")}
        for kind, amount in reservation.items():
            if self.budget_used(kind) + amount > self.state["limits"][kind]:
                raise Fault("BUDGET_EXHAUSTED", "Parallel reservation exceeds remaining budget", limit=kind)
        seconds = self.update_time()
        if seconds <= 0:
            raise Fault("BUDGET_EXHAUSTED", "Parallel append has no remaining time")
        children = [(task["id"], self.prepare_parallel_child(task, active["module"], types,
                    batch_id, ordinal, seconds)) for ordinal, task in enumerate(tasks, len(manifest))]
        previous = self.state["parallel_reserved"]
        self.state["parallel_reserved"] = {kind: previous[kind] + amount for kind, amount in reservation.items()}
        added = [{"id": task_id, "run_id": child["run_id"]} for task_id, child in children]
        self.state["parallel_batches"][batch_id] = [*manifest, *added]
        receipt = {"status": "reserved", "run_id": self.state["run_id"], "batch_id": batch_id, "tasks": added}
        pending["parallel_add"] = receipt
        self.trace("parallel_added", batch_id=batch_id, tasks=added, reservation=reservation)
        try:
            self.store.append_parallel(self.state, self.owner, batch_id, children, policy)
        except (sqlite3.Error, Fault) as exc:
            self.state["parallel_reserved"] = previous
            self.state["parallel_batches"][batch_id] = manifest
            del pending["parallel_add"]
            self.state["trace"].pop()
            if isinstance(exc, Fault):
                raise
            raise Fault("PERSISTENCE_FAILED", "Could not reserve appended parallel tasks", batch_id=batch_id) from exc
        if self.parallel_workers is None:
            self.open_parallel_workers(policy)
        self.dispatch_parallel_children(children, time.monotonic() + seconds, batch_id)
        return receipt

    def dispatch_reserved_tasks(self, handle, task_ids, operation):
        batch_id = self.active_parallel(handle)["batch_id"]
        task_ids = parallel_task_ids(task_ids, phase="continue")
        pending = self.current["pending"]
        if "reserved_dispatch" in pending:
            return pending["reserved_dispatch"]
        if self.update_time() <= 0:
            raise Fault("BUDGET_EXHAUSTED", "Reserved dispatch has no remaining time")
        policy = self.parallel_policy()
        nested_dispatch = "settlement" in self.state.get("active_reserved_dispatch", {})
        if nested_dispatch:
            policy = {kind: min(limit, self.state["parallel_policy"][kind]) for kind, limit in policy.items()}

        def prepare(child):
            self.runtime._reauthorize(child, phase="continue")
            target = tuple(child["parallel_origin"]["target"])
            linked = self.graph.programs.get(target)
            if linked is None or linked.hash != child["revision"]:
                raise Fault("CONTINUATION_REVISION_MISMATCH", "Original reserved Procedure is not linked", phase="continue")
            grants = set(child["root_grants"]) & set(self.current["grants"])
            self.check_parallel_program(target, grants, child["inputs"])
            if self.host.procedures is not None and child["source"]["name"] not in self.host.procedures:
                raise Fault("AUTHORIZATION_DENIED", "Reserved child Procedure is no longer allowed", phase="continue")
            limits = {kind: min(value, self.host.limits.get(kind, value), self.host.max_limits.get(kind, value))
                      for kind, value in child["limits"].items()}
            if nested_dispatch and (grants != set(child["root_grants"]) or limits != child["limits"]):
                raise Fault("AUTHORIZATION_DENIED", "Reserved child exceeds the current Host policy", phase="continue")
            child["root_grants"] = child["frames"][0]["grants"] = sorted(grants)
            child["limits"] = limits
            if child["original_deadline"] <= time.time():
                raise Fault("BUDGET_EXHAUSTED", "Reserved child original deadline expired", phase="continue")

        receipt, claims = self.store.dispatch_reserved(self.state, self.owner, batch_id, task_ids, operation,
                                                       self.runtime._authorize_reserved_dispatch, prepare, policy["active"])
        pending["reserved_dispatch"] = receipt
        self.trace("reserved_dispatch_claimed", batch_id=batch_id, tasks=receipt["tasks"], operation=operation)
        if self.parallel_workers is None:
            self.open_parallel_workers(policy)
        children = [(task["task_id"], child) for task, (child, _) in zip(receipt["tasks"], claims)]
        owners = {child["run_id"]: owner for child, owner in claims}
        self.dispatch_parallel_children(children, time.monotonic() + self.update_time(), batch_id, owners=owners)
        return receipt

    def open_parallel_workers(self, policy):
        self.parallel_workers = ParallelWorkers(self.store, policy["workers"])
        self.owns_parallel_workers = True

    def drain_parallel(self, *, read_results=True):
        if self.owns_parallel_workers:
            active = self.state.get("active_parallel", {})
            pending = self.state["frames"][-1].get("pending", {}) if self.state["frames"] else {}
            batch_id = active.get("batch_id", pending.get("batch_id"))
            try:
                advance = (lambda: self.queue_parallel_ready(batch_id)) if batch_id else None
                self.parallel_workers.close(read_results, advance)
            finally:
                self.parallel_workers = None
                self.owns_parallel_workers = False

    def run_ready_child(self, run_id, owner, kind, operation):
        child = self.store.claim_parallel_ready(run_id, owner, kind, operation)
        if child is not None:
            deadline = time.monotonic() + max(0, child["original_deadline"] - time.time())
            self.run_parallel_child(child, deadline, owner)

    def queue_parallel_ready(self, batch_id):
        ready, waiting = self.store.poll_parallel_tree(self.state["run_id"], self.owner, batch_id)
        for run_id, owner, kind, operation in ready:
            self.parallel_workers.submit(run_id, self.run_ready_child, run_id, owner, kind, operation)
        return waiting

    def parallel_child_progress(self, batch_id):
        # Future completion describes a worker, while SQLite owns the child lifecycle.
        workers = self.parallel_workers
        if workers is None:
            return None
        workers.prune()
        waiting = self.queue_parallel_ready(batch_id)
        remaining = self.update_time()
        return remaining if waiting or workers.futures else None

    def delay_parallel_progress(self, remaining):
        if self.host.cancelled():
            raise Fault("RUN_CANCELLED", "Host cancelled the Run during a parallel wait")
        if remaining <= 0:
            raise Fault("BUDGET_EXHAUSTED", "Parallel wait reached the Run deadline")
        self.store.parallel_wake.wait(min(0.1, remaining))

    def wait_parallel_children(self, batch_id):
        with self.store.parallel_wake:
            while (remaining := self.parallel_child_progress(batch_id)) is not None:
                self.delay_parallel_progress(remaining)
        self.drain_parallel()

    def active_parallel(self, handle):
        active = self.state.get("active_parallel")
        if active is None or handle != {"run_id": self.state["run_id"], "batch_id": active["batch_id"]}:
            raise Fault("INVALID_PARALLEL_HANDLE", "Expected the active batch of the current Run")
        return active

    def cancel_parallel_tasks(self, key, handle, task_ids):
        active = self.active_parallel(handle)
        self.require("capability", PARALLEL_CANCEL)
        if PARALLEL_CANCEL not in self.host.grants:
            raise Fault("AUTHORIZATION_DENIED", "Host withdrew parallel cancellation capability")
        cancellation_id = digest([active["batch_id"], key])
        try:
            return self.store.request_parallel_cancel(self.state["run_id"], active["batch_id"], cancellation_id,
                                                      self.runtime._authorize_parallel_cancel, parallel_task_ids(task_ids),
                                                      replay_only=self.parallel_workers is None or
                                                      self.state.get("active_reserved_dispatch", {}).get("batch_id") == active["batch_id"])
        except Fault as fault:
            if fault.error["code"] == "PARALLEL_INCOMPLETE":
                self.join_parallel(active["batch_id"])
            raise

    def next_parallel_event(self, handle):
        active = self.active_parallel(handle)
        run_id, batch_id = self.state["run_id"], active["batch_id"]
        nested = ("parallel_parent" in self.state and self.parallel_workers is not None
                  and not self.owns_parallel_workers)
        while True:
            # Hold the condition across the read to avoid missing an in-process commit notification.
            with self.store.parallel_wake:
                remaining = None if nested else self.parallel_child_progress(batch_id)
                event = self.store.read_parallel_event(run_id, self.owner, batch_id, active["cursor"])
                if event is not None:
                    if (event["kind"] == "all_settled" and event["subject"] not in
                            {batch_id, str(len(self.state["parallel_batches"][batch_id]))}):
                        active["cursor"] = event["sequence"]
                        continue
                    if (event["kind"] == "reserved_dispatch_available" and event["subject"] !=
                            self.state.get("active_reserved_dispatch", {}).get("continuation_id")):
                        active["cursor"] = event["sequence"]
                        continue
                    break
                if nested and not self.store.parallel_batch_terminal(self.state, self.owner):
                    self.state["parallel_schedule"] = "event"
                    self.store.save(self.state, self.owner)
                    raise ParallelBlocked
                if remaining is not None:
                    self.delay_parallel_progress(remaining)
            if remaining is None:
                self.drain_parallel()
                self.join_parallel(batch_id, awaiting_events=True)
                raise Fault("PARALLEL_STATE_INVALID", "Parallel completion event is missing", batch_id=batch_id)
        kind, subject, sequence = event["kind"], event["subject"], event["sequence"]
        value = {"kind": "all_settled" if kind == "all_reconciled" else kind,
                 "event_id": digest([batch_id, sequence]), "sequence": sequence}
        if kind == "steering":
            self.require("capability", STEERING_READ)
            self.runtime._authorize_steering(self.state, "read")
            self.state["steering_cursor"] = event["steering_sequence"]
            value.update(steering_id=subject, text=event["text"])
        elif kind == "task_settled":
            task, = self.store.parallel_results(run_id, self.owner, batch_id, settled_event=subject)[0]
            self.authorize_parallel_results([task])
            outcome = parallel_outcome(task)
            self.check_parallel_result_fault(batch_id, outcome)
            outcome.pop("id")
            value.update(task_id=subject, **outcome)
        elif kind == "task_awaiting_input":
            child_wait = parse(subject)
            tasks, _ = self.store.parallel_results(run_id, self.owner, batch_id, settled_event=child_wait["task_id"])
            if len(tasks) != 1 or tasks[0]["run_id"] != child_wait["run_id"]:
                raise Fault("PARALLEL_STATE_INVALID", "Child input event does not match the original slot", batch_id=batch_id)
            self.authorize_parallel_results(tasks)
            value.update(child_wait)
        elif kind == "reserved_dispatch_available":
            grant = self.state.get("active_reserved_dispatch", {})
            value["task_ids"] = [task["task_id"] for task in grant["tasks"]]
        elif kind in {"all_settled", "all_reconciled"}:
            self.drain_parallel()
            value["results"] = self.join_parallel(batch_id, sequence=sequence)
            del self.state["active_parallel"]
        active["cursor"] = sequence
        self.trace("parallel_event", batch_id=batch_id, kind=kind, event_id=value["event_id"], sequence=sequence)
        return value

    def authorize_parallel_results(self, tasks):
        for task in tasks:
            self.runtime._reauthorize(task["state"], phase="join")
            for category, value in task["state"]["accessed"]:
                self.require(category, value)
        used_tools = {name for task in tasks for name in task["state"].get("parallel_tools_used", [])}
        for name in sorted(used_tools):
            self.check_parallel_tool(name, self.current["grants"])
        self.state["parallel_tools_used"] = sorted(set(self.state.get("parallel_tools_used", [])) | used_tools)
        for field in ("parallel_models_used", "parallel_preflights_used"):
            used = {name for task in tasks for name in task["state"].get(field, [])}
            if used:
                self.state[field] = sorted(set(self.state.get(field, [])) | used)

    @staticmethod
    def check_parallel_result_fault(batch_id, outcome):
        error = outcome.get("error", {})
        if error.get("code") in PARALLEL_BATCH_FAULTS:
            raise Fault(error["code"], error["message"], batch_id=batch_id, child_run_id=outcome["run_id"])

    def join_parallel(self, batch_id, *, sequence=None, awaiting_events=False):
        tasks, cancellation = self.store.parallel_results(self.state["run_id"], self.owner, batch_id)
        outcomes, unresolved, uncertain = parallel_settlement(
            tasks, batch_id, reconcile_uncertain=self.state.get("parallel_settlement", {}).get(batch_id) == "reconcile_uncertain")
        self.authorize_parallel_results(tasks)
        for row in outcomes:
            self.check_parallel_result_fault(batch_id, row)
        abandoned = [row["run_id"] for row in unresolved if row["status"] == "unresolved"]
        checkpoint = {"batch_id": batch_id, "operation": self.current["pending"]["id"]}
        if abandoned and (awaiting_events or "active_parallel" not in self.state):
            checkpoint["takeover_children"] = abandoned
        if uncertain and ("takeover_children" in checkpoint or (
                not unresolved and all(task["result"] is not None for task in tasks)
                and (sequence is not None or "active_parallel" not in self.state))):
            checkpoint["children"] = [row["run_id"] for row in uncertain]
            if sequence is not None:
                checkpoint["sequence"] = sequence
        if "takeover_children" in checkpoint or "children" in checkpoint:
            self.state["parallel_join_checkpoint"] = checkpoint
        if any(task["result"] is None for task in tasks) or unresolved or uncertain:
            raise incomplete_parallel(batch_id, cancellation, outcomes, unresolved + uncertain)
        settled = sorted((task for task in tasks if task["finish_order"] is not None), key=lambda task: task["finish_order"])
        usage = {kind: sum(task["state"][kind] + task["state"].get("parallel_usage", {}).get(kind, 0)
                           for task in tasks) for kind in ("calls", "steps")}
        if "parallel_parent" in self.state:
            previous = self.state.get("parallel_usage", {})
            self.state["parallel_usage"] = {kind: previous.get(kind, 0) + amount for kind, amount in usage.items()}
        self.trace("parallel_joined", batch_id=batch_id, cancellation_id=cancellation,
                   completion_order=[task["id"] for task in settled], usage=usage)
        return outcomes

    def wait(self, key, schema, reason):
        resumed = self.state.get("resume_values", {})
        if key in resumed:
            # Keep input until this instruction commits; replay can reach the same continuation.
            return resumed[key]
        raise Waiting(key, schema, reason)

    def effect(self, key, kind, label, adapter, request, *, adapter_key=None, destination=None, selection=None,
               stream_schemas=None, from_stream=None):
        if "parallel_parent" in self.state and kind in {"model", "preflight"}:
            policy = self.host.parallel_models if kind == "model" else self.host.parallel_preflights
            name = destination if kind == "model" else label
            if name not in policy or adapter.capability not in self.host.grants:
                raise Fault("AUTHORIZATION_DENIED", "External effect is not approved for parallel use", target=name)
            used = self.state.setdefault("parallel_models_used" if kind == "model" else "parallel_preflights_used", [])
            if name not in used:
                used.append(name)
        call_id = digest([self.state["run_id"], key, kind, label])
        pending = self.current["pending"]
        request_hash = digest(request)
        adapter_id = adapter_key if adapter_key is not None else label
        binding = effect_binding(adapter, adapter_id, destination, kind)
        if selection is not None and call_id not in pending["effects"]:
            check_effect_binding(selection["binding"], binding, call_id, "execute")
        new_call = call_id not in pending["effects"]
        if new_call:
            if "reconciled_replay" in self.state:
                raise Fault("CONTINUATION_EFFECT_UNCONFIRMED", "Original instruction replay cannot register an effect",
                            phase="continue", call_id=call_id)
            if self.budget_used("calls") >= self.state["limits"]["calls"]:
                raise Fault("BUDGET_EXHAUSTED", "External call budget exhausted")
            pending["effects"].append(call_id)
            self.state["calls"] += 1
            self.trace(kind, label=label, call_id=call_id, request_hash=request_hash)
        remaining = self.update_time()
        if remaining <= 0:
            raise Fault("BUDGET_EXHAUSTED", "Execution time budget exhausted")
        intent = None
        source = binding["source_id"] if binding is not None else adapter.source
        if isinstance(source, str) and source:
            intent = {"run_id": self.state["run_id"], "call_id": call_id, "kind": kind,
                      "adapter": adapter_id, "capability": adapter.capability, "source": source,
                      "destination": destination, "request": request, "request_hash": request_hash}
            if binding is not None:
                intent.update(binding=binding, binding_fingerprint=digest(binding))
            if selection is not None:
                intent["selection"] = selection
        try:
            if stream_schemas is not None:
                self.state.pop("stream_ready", None)
                self.state["active_stream"] = {"call_id": call_id, "cursor": 0, "ready": {},
                                               "capability": adapter.capability, "destination": destination}
                self.state["stream_tools"] = sorted(stream_schemas)
            if from_stream is None:
                status, result = self.store.effect(self.state, self.owner, call_id, intent)
            else:
                status, result = self.store.effect(self.state, self.owner, call_id, intent, from_stream=from_stream)
        except ParallelCancelled:
            if new_call:
                pending["effects"].pop()
                self.state["calls"] -= 1
                self.state["trace"].pop()
            raise
        if status in {"not_dispatched", "output_limited"}:
            if new_call:
                pending["effects"].pop()
                self.state["calls"] -= 1
                self.state["trace"].pop()
            if status == "output_limited":
                raise Fault("STREAM_TOOL_NOT_READY", "Output-limited model responses cannot authorize tools")
            self.trace("stream_tool_not_dispatched", stream_call_id=from_stream, cancellation_id=result)
            return {"status": "not_dispatched", "reason": "STREAM_CANCELLED", "cancellation_id": result}
        if stream_schemas is not None:
            if status == "new":
                timeout = min(self.update_time(), self.state["limits"]["operation_seconds"])
                deadline = time.monotonic() + timeout
                cancellation = self.store.stream_cancellation(self.state, self.owner, call_id)
                if cancellation is not None:
                    self.interrupt_stream("STREAM_CANCELLED", cancellation_id=cancellation)
                elif timeout <= 0:
                    self.interrupt_stream("STREAM_DEADLINE")
                else:
                    try:
                        transport = adapter.open_stream(copy.deepcopy(request), call_id, timeout)
                        self.live_stream = Stream(transport, stream_schemas, deadline)
                    except (Fault, OSError, TimeoutError):
                        self.interrupt_stream("STREAM_OPEN_FAILED")
            return {"run_id": self.state["run_id"], "call_id": call_id}
        if status == "reconciled":
            self.reconciled_response_read = True
            self.trace("reconciled_response_read", call_id=call_id)
            return result
        if status == "confirmed":
            return result
        if status == "pending" and adapter.recover is None:
            raise Fault("EFFECT_UNCERTAIN", "Previous external effect could not be confirmed", call_id=call_id)
        function = adapter.recover if status == "pending" else adapter.invoke
        if self.deadline is not None:
            remaining = self.update_time()
            if remaining <= 0:
                raise Fault("EFFECT_UNCERTAIN", "Parallel task deadline expired after effect registration", call_id=call_id)
        timeout = min(remaining, self.state["limits"]["operation_seconds"])
        completed = threading.Event()
        outcome = []

        def perform():
            try:
                outcome.append((True, function(copy.deepcopy(request), call_id, timeout)))
            except Exception as exc:
                outcome.append((False, exc))
            finally:
                completed.set()

        started = time.perf_counter()
        threading.Thread(target=perform, daemon=True).start()
        if not completed.wait(timeout):
            raise Fault("EFFECT_UNCERTAIN", "External operation timed out with cancellation unconfirmed",
                        call_id=call_id, reason="EXTERNAL_TIMEOUT", cancelled=False)
        success, result = outcome[0]
        if not success:
            if isinstance(result, Fault):
                raise result
            raise Fault("EXTERNAL_FAILURE", "Adapter failed", adapter_error=type(result).__name__, call_id=call_id)
        self.trace("effect_confirmed", call_id=call_id, milliseconds=(time.perf_counter() - started) * 1000,
                   usage=result.get("usage") if kind == "model" and isinstance(result, dict) else None)
        self.update_time()
        self.store.confirm(self.state, self.owner, call_id, result)
        return result

    def close_stream(self):
        live, self.live_stream = self.live_stream, None
        if live is None:
            return {"transport_closed": False, "provider_stopped": False}
        try:
            outcome = live.transport.close()
            return {"transport_closed": outcome.get("transport_closed") is True,
                    "provider_stopped": outcome.get("provider_stopped") is True}
        except (Fault, OSError, TimeoutError, AttributeError):
            return {"transport_closed": False, "provider_stopped": False}

    def interrupt_stream(self, reason, events=None, *, cancellation_id=None, closed=None):
        events = [] if events is None else events
        active = self.state["active_stream"]
        terminal = {"kind": "interrupted", "reason": reason, "usage": None,
                    "cancellation_requested": reason in {"HOST_CANCELLED", "STREAM_CANCELLED"},
                    **(self.close_stream() if closed is None else closed)}
        if cancellation_id is not None:
            terminal["cancellation_id"] = cancellation_id
        accepted = self.store.append_stream_events(self.state, self.owner, active["call_id"], [*events, terminal])
        if accepted is not None:
            close_status = {name: terminal[name] for name in ("transport_closed", "provider_stopped")}
            return self.interrupt_stream(reason if reason == "HOST_CANCELLED" else "STREAM_CANCELLED",
                                         cancellation_id=accepted, closed=close_status)
        return terminal

    def stream_next(self, handle):
        active = self.state.get("active_stream")
        if active is None or handle != {"run_id": self.state["run_id"], "call_id": active["call_id"]}:
            raise Fault("INVALID_STREAM_HANDLE", "Expected the active model stream of this Run")
        call_id, sequence = active["call_id"], active["cursor"] + 1
        self.runtime._reauthorize(self.state, phase="stream")
        self.require("capability", active["capability"])
        self.require("destination", active["destination"])
        if self.live_stream is not None and self.host.cancelled():
            self.interrupt_stream("HOST_CANCELLED")
        event = self.store.stream_event(self.state, self.owner, call_id, sequence)
        if event is None:
            self.read_stream_batch()
            event = self.store.stream_event(self.state, self.owner, call_id, sequence)
        if self.state.get("reconciled_replay", {}).get("call_id") == call_id:
            self.reconciled_response_read = True
            self.trace("reconciled_journal_read", call_id=call_id, sequence=sequence)
        active["cursor"] = sequence
        if event["kind"] == "tool_call_ready":
            active["ready"][event["id"]] = {"name": event["name"], "arguments": event["arguments"]}
        if event["kind"] in STREAM_TERMINAL:
            if event["kind"] == "completed" and event["response"]["finish"] != "length":
                self.state["stream_ready"] = {key: active[key] for key in ("call_id", "ready", "capability", "destination")}
            del self.state["active_stream"]
            self.state.pop("stream_journal_replay", None)
        self.trace("stream_event", kind=event["kind"], call_id=call_id, sequence=sequence)
        return {**event, "sequence": sequence, "event_id": digest([call_id, sequence]), "call_id": call_id}

    def read_stream_batch(self):
        live = self.live_stream
        active = self.state["active_stream"]
        if live is None:
            self.interrupt_stream("STREAM_CONTINUATION_UNAVAILABLE")
            return
        events = []
        closed = None
        # Read a bounded burst. Idle transports return None and EOF is explicit.
        until = time.monotonic() + 0.02
        while True:
            if self.host.cancelled():
                self.interrupt_stream("HOST_CANCELLED", events)
                return
            if not events:
                cancellation = self.store.stream_cancellation(self.state, self.owner, active["call_id"])
                if cancellation is not None:
                    self.interrupt_stream("STREAM_CANCELLED", cancellation_id=cancellation)
                    return
            remaining = min(self.update_time(), live.deadline - time.monotonic())
            if remaining <= 0:
                self.interrupt_stream("STREAM_DEADLINE", events)
                return
            timeout = min(remaining, 0.1, max(0.0, until - time.monotonic())) if events else min(remaining, 0.1)
            try:
                raw = live.transport.read(timeout)
                if raw is None:
                    if events:
                        break
                    continue
                event = live.normalize(raw)
            except (Fault, OSError, EOFError, TypeError, KeyError, ValueError):
                self.interrupt_stream("STREAM_READ_FAILED", events)
                return
            coalesce(events, event)
            if event["kind"] in STREAM_TERMINAL:
                closed = self.close_stream()
                if event["kind"] == "interrupted":
                    for name, confirmed in closed.items():
                        event[name] = event[name] or confirmed
                else:
                    event["transport_closed"] = closed["transport_closed"]
                break
            if event["kind"] == "tool_call_ready" or len(events) >= 32 or time.monotonic() >= until:
                break
        accepted = self.store.append_stream_events(self.state, self.owner, active["call_id"], events)
        if accepted is not None:
            self.interrupt_stream("STREAM_CANCELLED", cancellation_id=accepted, closed=closed)

    def authorize_stream_tool(self, name, arguments, reference, key):
        stream = self.state.get("active_stream") or self.state.get("stream_ready")
        if (stream is None or not isinstance(reference, dict) or reference.get("kind") != "tool_call_ready"
                or reference.get("call_id") != stream["call_id"] or not isinstance(reference.get("id"), str)):
            raise Fault("STREAM_TOOL_NOT_READY", "Expected a ready event from this Run's model stream")
        call = stream["ready"].get(reference["id"])
        self.runtime._reauthorize(self.state, phase="stream")
        self.require("capability", stream["capability"])
        self.require("destination", stream["destination"])
        if self.state.get("active_stream") is not None and self.live_stream is None:
            terminal = self.store.stream_terminal(self.state, self.owner, stream["call_id"])
            cancellation = self.store.stream_cancellation(self.state, self.owner, stream["call_id"])
            if terminal != "completed" and cancellation is None:
                raise Fault("STREAM_TOOL_NOT_READY", "A recovered or interrupted stream needs confirmed completion before tool use")
        if (call is None or call["name"] != name or canonical(call["arguments"]) != canonical(arguments)
                or call.get("consumed", key) != key):
            raise Fault("STREAM_TOOL_NOT_READY", "Ready call does not match this tool instruction or was already used")
        call["consumed"] = key
        return stream["call_id"]

    def resolve(self, key, operation, *, address=None):
        node = self.graph.node(key)
        if "parallel_parent" in self.state:
            self.check_parallel_data_node(key, node)
        identity = [key, node.get("binding", "first_read"), self.current["binding"]]
        if node["type"] == "ResolvedPool":
            identity.append(address)
        cache_key = digest(identity)
        # The lookup namespace is pinned to this run. Narrowed grants must not refresh a binding.
        cached = self.state["cache"].get(cache_key)
        if cached is not None:
            for category, value in cached["access"]:
                self.require(category, value)
            self.trace("binding_reused", resource=key)
            return cached["value"]
        accesses = []
        self.capture.append(accesses)
        try:
            self.require("capability", node.get("capability"))
            value = (self.expand_pool(key, address or key[2]) if node["type"] == "ResolvedPool"
                     else self._resolve(key, node, operation))
            schema = schema_for(node["type"], self.graph.types)
            if node["type"] not in {"ModelEndpoint", "Procedure"}:
                validate(value, schema, location=key)
            self.state["cache"][cache_key] = {"value": value, "access": accesses, "key": cache_key}
            self.trace("bound", resource=key, policy=node.get("binding", "first_read"), binding_key=cache_key)
            return value
        finally:
            self.capture.pop()

    def model_ref(self, key, endpoint, **metadata):
        handle = uuid.uuid4().hex
        self.state["models"][handle] = {"value": endpoint, "key": key, **metadata}
        return {"$model": handle}

    def expand_pool(self, key, address):
        pool_id = uuid.uuid4().hex
        pool = {"kind": "ResolvedPool", "id": pool_id, "address": address, "resource": list(key), "candidates": []}
        pending = [(key, [], [])]
        while pending:
            if self.update_time() <= 0:
                raise Fault("BUDGET_EXHAUSTED", "Execution time budget exhausted during pool expansion")
            resource, ancestors, grants = pending.pop()
            node = self.graph.node(resource)
            capability = node.get("capability")
            self.require("capability", capability)
            path = [*ancestors, list(resource)]
            if capability is not None:
                grants = [*grants, capability]
            if node["kind"] != "terminal":
                pending.extend((child, path, grants) for child in reversed(self.graph.edges[resource]))
                continue
            endpoint = node["value"]
            destination = f"{endpoint['adapter']}:{endpoint['model']}"
            adapter = self.host.models.get(endpoint["adapter"])
            binding = effect_binding(adapter, endpoint["adapter"], destination, "model") if adapter else None
            source = binding["source_id"] if binding else (adapter.source if adapter else None)
            selection = {"pool_id": pool_id, "address": address, "pool": list(key), "candidate": list(resource),
                         "path": path, "binding": binding}
            ref = self.model_ref(resource, endpoint, capabilities=grants, selection=selection)
            pool["candidates"].append({"ref": ref, "resource": list(resource), "path": path, "model": endpoint["model"],
                                       "destination": destination, "source": source})
        self.state.setdefault("pools", {})[pool_id] = digest(pool)
        self.trace("pool_resolved", address=address, resource=key, pool_id=pool_id, candidates=len(pool["candidates"]))
        return pool

    def _resolve(self, key, node, operation):
        kind = node["kind"]
        module = key[0]
        context = f"{operation}/resource/{digest(key)}"

        def resolve(address):
            return self.resolve(self.graph.lookup(address, module), context)

        if kind == "alias":
            return resolve(node["target"])
        if kind == "terminal":
            value = node["value"]
            if node["type"] == "ModelEndpoint":
                return self.model_ref(key, value)
            if node["type"] == "Procedure":
                return list(key)
            return value
        inputs = {name: resolve(address) for name, address in node.get("inputs", {}).items()}
        if kind == "host":
            adapter = self.host.bindings.get(node.get("adapter"))
            if adapter is None:
                raise Fault("UNKNOWN_ADAPTER", "Host binding is unavailable", resource=key)
            self.require("capability", adapter.capability)
            response = self.effect(context, "binding", node["adapter"], adapter,
                                   {"inputs": inputs, "revision": node.get("source_revision"), "resource": list(key)},
                                   destination=node["adapter"])
            if isinstance(response, dict) and set(response) == {"await"}:
                request = response["await"]
                return self.wait(context, schema_for(request["schema"], self.graph.types), request["reason"])
            if isinstance(response, dict) and set(response) == {"select"}:
                candidate = response["select"]
                if candidate not in node.get("candidates", []):
                    raise Fault("CANDIDATE_NOT_ALLOWED", "Binding selected an undeclared candidate", resource=key)
                return resolve(candidate)
            return response
        resolver = node.get("selector") if kind == "select" else node["resolver"]
        if resolver is None:
            for candidate in node["candidates"]:
                handle = resolve(candidate)
                endpoint = self.model_entry(handle)["value"]
                adapter = self.host.models.get(endpoint["adapter"])
                self.trace("pool_candidate", resource=key, candidate=candidate, model=endpoint["model"])
                if adapter and adapter.available(endpoint["model"]):
                    return handle
            raise Fault("MODEL_UNAVAILABLE", "No configured Pool candidate is available", resource=key)
        inputs.update({name: resolve(address) for name, address in resolver.get("inputs", {}).items()})
        if kind == "select":
            inputs["candidates"] = node["candidates"]
        response = self.model(resolve(resolver.get("model")), resolver["task"], inputs, {}, context)
        value = parse(response["text"]) if resolver.get("format") == "json" else response["text"]
        if kind == "compute":
            return value
        if not isinstance(value, str):
            raise Fault("INVALID_CANDIDATE", "Selector must return a candidate address", resource=key)
        if value not in node["candidates"]:
            raise Fault("CANDIDATE_NOT_ALLOWED", "Selector returned an undeclared candidate", resource=key)
        self.trace("selected", resource=key, candidate=value)
        return resolve(value)

    def model_entry(self, handle):
        token = handle.get("$model") if isinstance(handle, dict) and set(handle) == {"$model"} else None
        found = self.state["models"].get(token) if isinstance(token, str) else None
        if found is None:
            raise Fault("INVALID_MODEL", "Expected a resolved model handle")
        for capability in found.get("capabilities", []):
            self.require("capability", capability)
        return found

    def policy_selection(self, selection, current):
        if selection is None or selection["pool_id"] not in current.get("pools", ()):
            return None
        policy = self.graph.programs[tuple(current["program"])]
        return {**selection, "policy": {"resource": current["program"], "procedure_hash": policy.hash}}

    def model(self, handle, task, arguments, overrides, operation, submit=None, *, return_unavailable=False,
              stream_schemas=None):
        entry = self.model_entry(handle)
        endpoint, selection = entry["value"], entry.get("selection")
        if selection is not None:
            for procedure in reversed(self.state["frames"]):
                policy = self.policy_selection(selection, procedure)
                if policy is not None:
                    selection = policy
                    break
        destination = f"{endpoint['adapter']}:{endpoint['model']}"
        self.require("destination", destination)
        adapter = self.host.models.get(endpoint["adapter"])
        model_call_id = digest([self.state["run_id"], operation, "model", task])
        effects = self.current["pending"]["effects"]
        existing = model_call_id in effects
        if selection is not None and not existing:
            if selection["binding"] is None:
                raise Fault("ORIGINAL_BINDING_UNAVAILABLE", "Pool candidate lacks a pinned adapter identity",
                            call_id=model_call_id)
            if adapter is not None:
                binding = effect_binding(adapter, endpoint["adapter"], destination, "model")
                check_effect_binding(selection["binding"], binding, model_call_id, "execute")
        if adapter is None or (not return_unavailable and not existing and not adapter.available(endpoint["model"])):
            raise Fault("MODEL_UNAVAILABLE", "Selected model is unavailable", target=destination)
        self.require("capability", adapter.capability)
        if stream_schemas is not None:
            if not existing and adapter.open_stream is None:
                raise Fault("MODEL_STREAM_UNSUPPORTED", "Model adapter does not expose live stream records", target=destination)
            if effect_binding(adapter, endpoint["adapter"], destination, "model") is None:
                raise Fault("ORIGINAL_BINDING_UNAVAILABLE", "Streaming requires a pinned effect identity", call_id=model_call_id)
        if not isinstance(overrides, dict) or not overrides.keys() <= set(endpoint.get("allow_overrides", [])):
            raise Fault("MODEL_CONFIG_CONFLICT", "Model call attempted a disallowed override")
        sources = {}
        params = {}
        for label, values in (("endpoint", endpoint.get("defaults", {})),
                              ("route", endpoint.get("route_overrides", {})), ("call", overrides)):
            params.update(values)
            sources.update(dict.fromkeys(values, label))
        validate(params, self.host.request_schema, code="MODEL_CONFIG_CONFLICT")
        ordinal = 1
        for event in self.state["trace"]:
            if event.get("call_id") == model_call_id:
                break
            if event["event"] == "model" and event["label"] == task:
                ordinal += 1
        request = {"model": endpoint["model"], "task": task, "arguments": arguments,
                   "params": params, "submit": submit, "ordinal": ordinal}
        if stream_schemas is not None:
            request.update(stream=True, stream_tools=stream_schemas)
        if not self.host.authorize_model(destination, request):
            raise Fault("AUTHORIZATION_DENIED", "Host rejected model data egress", target=destination)
        hook_name = endpoint.get("preflight")
        hook = None
        if hook_name:
            hook = self.host.preflights.get(hook_name)
            if hook is None:
                raise Fault("PREFLIGHT_UNAVAILABLE", "Configured preflight is not registered")
            self.require("capability", hook.capability)
        if return_unavailable and not effects:
            available = adapter.available(endpoint["model"])
            if available is False:
                self.trace("model_not_sent", target=destination, task=task, selection=selection,
                           reason="MODEL_UNAVAILABLE")
                return {"status": "not_sent", "reason": "MODEL_UNAVAILABLE"}
            if not available:
                raise Fault("MODEL_UNAVAILABLE", "Selected model is unavailable", target=destination)
        provenance = {"call_id": model_call_id, "selection": selection} if selection is not None else {}
        self.trace("model_route", target=destination, task=task, parameter_sources=sources, **provenance)
        if hook is not None:
            result = self.effect(operation, "preflight", hook_name, hook, request, destination=destination)
            if not isinstance(result, dict) or result.get("passed") is not True:
                raise Fault("PREFLIGHT_FAILED", "Preflight rejected the request", hook=hook_name)
            if "params" in result:
                request["params"] = validate(result["params"], self.host.request_schema, code="MODEL_CONFIG_CONFLICT")
                self.trace("preflight_parameters", hook=hook_name, fields=sorted(request["params"]))
        self.require("destination", destination)
        if not self.host.authorize_model(destination, request):
            raise Fault("AUTHORIZATION_DENIED", "Host rejected final model request", target=destination)
        response = self.effect(operation, "model", task, adapter, request,
                               adapter_key=endpoint["adapter"], destination=destination, selection=selection,
                               stream_schemas=stream_schemas)
        if stream_schemas is not None:
            return response
        if not isinstance(response, dict) or not isinstance(response.get("text", ""), str):
            raise Fault("INVALID_MODEL_RESPONSE", "Model response must contain text or submissions")
        if submit is None and "text" not in response:
            raise Fault("INVALID_MODEL_RESPONSE", "Model response is missing text")
        value = self.submitted(response, submit, model_call_id) if submit is not None else response
        return {"status": "completed", "value": value} if return_unavailable else value

    def submitted(self, response, schema, call_id):
        submissions = response.get("submissions", [])
        if not isinstance(submissions, list):
            raise Fault("INVALID_SUBMISSION", "Expected a list of submissions")
        if not submissions:
            raise Fault("SUBMISSION_MISSING", "Model did not call the submit operation")
        recorded = {}
        for entry in submissions:
            if (not isinstance(entry, dict) or set(entry) != {"id", "value"}
                    or not isinstance(entry["id"], str) or not entry["id"]):
                raise Fault("INVALID_SUBMISSION", "Expected submission ID and value")
            validate(entry["value"], schema, code="INVALID_SUBMISSION")
            if entry["id"] in recorded and digest(recorded[entry["id"]]) != digest(entry["value"]):
                raise Fault("SUBMISSION_CONFLICT", "Submission key contains different content")
            recorded[entry["id"]] = entry["value"]
        if len(recorded) != 1:
            raise Fault("SUBMISSION_CONFLICT", "More than one distinct submission")
        self.trace("submitted", call_id=call_id, key=next(iter(recorded)), duplicates=len(submissions) - 1)
        return next(iter(recorded.values()))

    def drive(self):
        try:
            return self._drive()
        except ParallelBlocked:
            return None
        except sqlite3.Error:
            return failed(Fault("PERSISTENCE_FAILED", "Could not read or persist the execution checkpoint"),
                          self.state["run_id"])
        finally:
            self.drain_parallel(read_results=False)
            self.close_stream()

    def _drive(self):
        if "parallel_exit" in self.state:
            return self.finish_batch_exit(self.state.pop("parallel_exit"))
        try:
            if self.state.get("request_id") and self.parallel_workers is not None:
                self.runtime._authorize_continuation(self.state)
            while self.state["frames"]:
                pending_join = self.state["frames"][-1].get("pending", {}).get("batch_id")
                if "parallel_parent" in self.state and not pending_join:
                    self.store.check_parallel_cancel(self.state, self.owner)
                active_stream = self.state.get("active_stream")
                if not pending_join and self.host.cancelled() and not active_stream:
                    raise Fault("RUN_CANCELLED", "Host cancelled the run between instructions")
                if self.update_time() <= 0 and (not (pending_join or active_stream)
                                               or ("continuation_deadline" in self.state and not pending_join)):
                    raise Fault("BUDGET_EXHAUSTED", "Execution time budget exhausted")
                result = self.step()
                if result is not None:
                    break
                self.store.save(self.state, self.owner)
                if self.emission is not None:
                    value, context = self.emission
                    self.emission = None
                    self.runtime._publish(value, context)
        except Waiting as wait:
            checkpoint = self.state.get("checkpoint", 0) + 1
            self.state["checkpoint"] = checkpoint
            expires = time.time() + self.host.wait_seconds
            if "parallel_parent" in self.state:
                expires = min(expires, self.state["original_deadline"])
            self.state["wait"] = {"key": wait.key, "schema": wait.schema, "reason": wait.reason, "expires": expires}
            result = {"status": "awaiting_input", "resume_token": uuid.uuid4().hex,
                      "input_schema": wait.schema, "reason": wait.reason, "checkpoint": checkpoint}
            if "parallel_parent" not in self.state:
                self.trace("awaiting_input", checkpoint=checkpoint)
        except ParallelCancelled as fault:
            result = {"status": "cancelled", "error": fault.error}
            self.trace("parallel_cancelled", **fault.error)
        except Fault as fault:
            result = failed(fault)
            self.trace("failed", error=fault.error)
        except (TypeError, KeyError, AttributeError, ValueError, RecursionError):
            result = failed(Fault("INVALID_EXECUTION_VALUE", "Instruction or adapter produced an invalid value"))
            self.trace("failed", error=result["error"])
        active_stream = self.state.get("active_stream")
        if active_stream is not None:
            call_id = active_stream["call_id"]
            terminal_kind = self.store.stream_terminal(self.state, self.owner, call_id)
            error = result.get("error", {})
            if (terminal_kind == "completed" and error.get("code") == "EFFECT_UNCERTAIN"
                    and error.get("call_id") == call_id):
                self.state["settled_stream"] = {"stream": copy.deepcopy(active_stream),
                                                "operation": self.state["frames"][-1].get("pending", {}).get("id"),
                                                "frames_hash": digest(self.state["frames"])}
            if terminal_kind not in STREAM_TERMINAL:
                terminal = self.interrupt_stream("HOST_CANCELLED" if self.host.cancelled() else "RUN_EXIT")
                self.trace("stream_closed", call_id=call_id, detail=terminal)
            del self.state["active_stream"]
        if "parallel_parent" in self.state and "active_parallel" in self.state and self.parallel_workers is not None:
            if self.owns_parallel_workers:
                try:
                    self.drain_parallel()
                except Fault as fault:
                    result = failed(fault)
                    self.trace("failed", error=fault.error)
            elif not self.store.parallel_batch_terminal(self.state, self.owner, descendants=True):
                # Keep the coordinator's owner live until its subtree can record its own results.
                self.state.update(parallel_schedule="drain", parallel_exit=result)
                self.store.save(self.state, self.owner)
                raise ParallelBlocked
            return self.finish_batch_exit(result)
        return self.finish_execution(result)

    def finish_batch_exit(self, result):
        active = self.state.pop("active_parallel")
        batch_id = active["batch_id"]
        tasks, _ = self.store.parallel_results(self.state["run_id"], self.owner, batch_id)
        outcomes = []
        for task in tasks:
            outcome = parallel_outcome(task)
            outcomes.append({name: outcome[name] for name in ("id", "run_id", "status", "effects")})
        self.trace("parallel_drained", batch_id=batch_id, tasks=outcomes)
        return self.finish_execution(result)

    def finish_execution(self, result):
        try:
            self.drain_parallel()
        except Fault as fault:
            result = failed(fault)
            self.trace("failed", error=fault.error)
        self.update_time()
        result.update(run_id=self.state["run_id"], revision=self.state["revision"],
                      scope_revision=self.state["scope"].get("revision"),
                      metrics={key: self.state[key] for key in ("steps", "calls", "elapsed")})
        self.store.finish(self.state, self.owner, result)
        return result

    def step(self):
        frames = self.state["frames"]
        current = self.current = frames[-1]
        program_key = current["program"]
        program = self.program if program_key is None else self.graph.programs[tuple(program_key)]
        module = self.graph.root if program_key is None else program_key[0]
        values = current["values"]
        block = program.blocks[current["block"]]
        if current["pc"] == len(block):
            return self.end_block(program)
        instruction = block[current["pc"]]
        location = f"{current['block']}/{instruction['id']}"
        op = instruction["op"]
        active_stream = self.state.get("active_stream")
        if active_stream is not None:
            reopening = op == "model_call" and active_stream["call_id"] in current.get("pending", {}).get("effects", [])
            if op not in STREAM_OPS and not reopening:
                raise Fault("STREAM_TERMINAL_REQUIRED", "Consume the terminal stream event before this instruction")
            if op == "tool_call" and (self.host.cancelled() or self.update_time() <= 0):
                raise Fault("STREAM_STOP_REQUESTED", "Read the terminal stream event before a tool call")
        if ("active_parallel" in self.state and op not in PARALLEL_COORDINATION_OPS
                and not (op == "parallel_call" and current.get("pending", {}).get("batch_id") == self.state["active_parallel"]["batch_id"])):
            raise Fault("PARALLEL_JOIN_REQUIRED", "Finish coordinating the active batch before this instruction", location=location)
        if "pending" not in current:
            if self.budget_used("steps") >= self.state["limits"]["steps"]:
                raise Fault("BUDGET_EXHAUSTED", "Interpreter step budget exhausted", location=location)
            self.state["steps"] += 1
            current["pending"] = {"id": f"{self.state['steps']}:{location}", "effects": []}
            self.trace("instruction", location=location, op=instruction["op"])
        key = current["pending"]["id"]
        types = program.source.get("types", {})

        def value(field):
            return evaluate(instruction[field], values, types)

        def resolve(address):
            return self.resolve(self.graph.lookup(address, module), key)

        result = None
        if op == "let":
            result = value("value")
        elif op == "emit":
            self.emission = (copy.deepcopy(value("value")), {
                "run_id": self.state["run_id"], "event_id": digest([self.state["run_id"], key]),
                "location": location, "stream_call_id": active_stream["call_id"] if active_stream else None,
            })
        elif op == "resolve":
            reference = value("ref")["$ref"]
            target = self.graph.lookup(reference["id"], module)
            if target not in self.graph.ranks or self.graph.node(target)["type"] != reference["type"]:
                raise Fault("UNLINKED_REFERENCE", "Reference was not linked with its declared type")
            result = self.resolve(target, key, address=reference["id"])
        elif op == "resolve_model":
            result = resolve(instruction.get("address"))
        elif op == "model_call":
            model = value("model") if "model" in instruction else resolve(None)
            schema = schema_for(instruction["submit"], types) if "submit" in instruction else None
            stream_schemas = ({tool: schema_for(name, types) for tool, name in instruction.get("stream_tools", {}).items()}
                              if instruction.get("stream") else None)
            result = self.model(model, instruction["task"], value("arguments"),
                                value("params") if "params" in instruction else {}, key, schema,
                                return_unavailable=instruction.get("on_unavailable") == "return", stream_schemas=stream_schemas)
            if stream_schemas is None:
                self.state.pop("stream_tools", None)
                self.state.pop("stream_ready", None)
        elif op == "stream_next":
            result = self.stream_next(value("stream"))
        elif op == "parse_json":
            result = validate(parse(value("text")), schema_for(instruction["schema"], types))
        elif op == "tool_call":
            tool = (self.check_parallel_tool(instruction["tool"], current["grants"])
                    if "parallel_parent" in self.state else self.host.tools.get(instruction["tool"]))
            if tool is None:
                raise Fault("UNKNOWN_TOOL", "Tool is not registered", tool=instruction["tool"])
            self.require("capability", tool.capability)
            if tool.capability not in self.host.grants:
                raise Fault("AUTHORIZATION_DENIED", "Host no longer grants this tool", tool=instruction["tool"])
            arguments = value("arguments")
            from_stream = None
            if "stream_call" in instruction:
                from_stream = self.authorize_stream_tool(instruction["tool"], arguments, value("stream_call"), key)
            elif active_stream is not None or instruction["tool"] in self.state.get("stream_tools", []):
                raise Fault("STREAM_TOOL_NOT_READY", "Streaming tools require an explicit ready event")
            if "parallel_parent" in self.state:
                used = self.state.setdefault("parallel_tools_used", [])
                if instruction["tool"] not in used:
                    used.append(instruction["tool"])
            result = self.effect(key, "tool", instruction["tool"], tool, arguments,
                                 destination=instruction["tool"], from_stream=from_stream)
        elif op == "await_input":
            if ("parallel_parent" in self.state and key not in self.state.get("resume_values", {})
                    and any(flag in self.state for flag in ("active_child_takeover", "active_continuation",
                                                           "active_parallel_continuation"))):
                raise Fault("PARALLEL_CHILD_CONTINUATION_DENIED", "Independent child continuation cannot suspend for input")
            result = self.wait(key, schema_for(instruction["schema"], types), instruction["reason"])
        elif op == "poll_steering":
            self.require("capability", STEERING_READ)
            cursor = self.state.get("steering_cursor", 0)
            self.runtime._authorize_steering(self.state, "read")
            result = self.store.read_steering(self.state["run_id"], self.owner, cursor)
            if result:
                # The normal step save commits this cursor with the output and PC.
                self.state["steering_cursor"] = result[-1]["sequence"]
            self.trace("steering_polled", event_ids=[event["event_id"] for event in result])
        elif op == "if":
            branch = "then" if validate(value("condition"), {"type": "boolean"}) else "else"
            frames.append(frame(program_key, values, current["grants"], block=f"{location}:{branch}", role="if", binding=current["binding"]))
        elif op == "while":
            if validate(value("condition"), {"type": "boolean"}):
                frames.append(frame(program_key, values, current["grants"], block=f"{location}:body", role="while", binding=current["binding"]))
                del current["pending"]
                return None
        elif op == "for_each":
            items = validate(value("items"), {"type": "array"})
            if items:
                current["loop"] = {"items": items, "values": [], "index": 0}
                self.add_iteration(current, instruction, location)
            else:
                values[instruction["collect_as"]] = []
        elif op == "call":
            child_key = resolve(instruction["procedure"])
            child = self.graph.programs[tuple(child_key)]
            arguments = validate(value("arguments"), child.input_schema)
            for ref, target_type in self.graph.roots_for(child, child_key[0], arguments):
                if ref not in self.graph.ranks or self.graph.node(ref)["type"] != target_type:
                    raise Fault("UNLINKED_REFERENCE", "Child input reference was not linked before execution")
            grants = set(current["grants"]).intersection(child.source["capabilities"])
            pools = []
            for name, kind in child.source["inputs"].items():
                if kind == "ResolvedPool":
                    pool = arguments[name]
                    if self.state.get("pools", {}).get(pool["id"]) != digest(pool):
                        raise Fault("INVALID_POOL", "Expected a pool resolved by this run")
                    pools.append(pool["id"])
            frames.append(frame(child_key, arguments, grants, role="call", target=instruction["as"], pools=pools))
        elif op == "parallel_call":
            result = self.parallel_call(key, instruction, module, types)
        elif op == "parallel_next":
            result = self.next_parallel_event(value("batch"))
        elif op == "parallel_add":
            result = self.add_parallel_tasks(value("batch"), value("tasks"), types)
        elif op == "parallel_dispatch_reserved":
            result = self.dispatch_reserved_tasks(value("batch"), value("task_ids"), key)
        elif op == "parallel_cancel":
            result = self.cancel_parallel_tasks(key, value("batch"), value("task_ids"))
        elif op == "return":
            returned = validate(value("value"), program.output_schema)
            if "parallel_parent" in self.state and not any(active.get("role") == "call" for active in frames):
                check_parallel_value(returned)
            return self.return_value(returned)
        if "as" in instruction and op != "call":
            values[instruction["as"]] = result
        if "reconciled_replay" in self.state:
            if self.state["reconciled_replay"]["operation"] != key or not self.reconciled_response_read:
                raise Fault("CONTINUATION_PC_MISMATCH", "Original instruction did not consume the reconciled response",
                            phase="continue")
            del self.state["reconciled_replay"]
        current["pc"] += 1
        del current["pending"]
        self.state.pop("resume_values", None)
        return None

    def add_iteration(self, parent, instruction, location):
        loop = parent["loop"]
        local = {**parent["values"], instruction["item"]: loop["items"][loop["index"]]}
        self.state["frames"].append(frame(parent["program"], local, parent["grants"],
                                          block=f"{location}:body", role="iteration", binding=parent["binding"]))

    def end_block(self, program):
        frames = self.state["frames"]
        current = frames.pop()
        role = current.get("role")
        if role not in {"iteration", "while", "if"}:
            raise Fault("RETURN_MISSING", "Procedure completed without a return")
        parent = frames[-1]
        if role in {"while", "if"}:
            parent["values"].update(current["values"])
            return None
        instruction = program.blocks[parent["block"]][parent["pc"] - 1]
        loop = parent["loop"]
        loop["values"].append(evaluate(instruction["yield"], current["values"], program.source.get("types", {})))
        loop["index"] += 1
        if loop["index"] < len(loop["items"]):
            self.add_iteration(parent, instruction, current["block"].removesuffix(":body"))
        else:
            parent["values"][instruction["collect_as"]] = parent.pop("loop")["values"]
        return None

    def return_value(self, value):
        frames = self.state["frames"]
        while frames and frames[-1].get("role") not in {None, "call"}:
            frames.pop()
        current = frames[-1]
        if current.get("pools") and isinstance(value, dict) and set(value) == {"$model"}:
            entry = self.model_entry(value)
            selection = self.policy_selection(entry.get("selection"), current)
            if selection is None:
                raise Fault("INVALID_POOL_SELECTION", "Policy must return a candidate from its input pools")
            value = self.model_ref(entry["key"], entry["value"], capabilities=entry["capabilities"], selection=selection)
            self.trace("pool_selected", **selection)
        frames.pop()
        self.state.pop("resume_values", None)
        if not frames:
            return {"status": "completed", "value": value}
        frames[-1]["values"][current["target"]] = value
        return None
