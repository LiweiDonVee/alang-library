"""Normalize live adapter records at the model effect boundary."""
from .contracts import Fault, parse, validate


TERMINAL = {"completed", "interrupted"}


class Stream:
    """A transport supplies bounded read/close and emits completed only after a terminal receipt."""

    def __init__(self, transport, schemas, deadline):
        self.transport = transport
        self.schemas = schemas
        self.deadline = deadline
        self.ready = {}
        self.pending = set()

    def normalize(self, event):
        kind = event["kind"]
        if kind == "text_delta" and isinstance(event.get("text"), str):
            block = event.get("block", 0)
            if type(block) is int and block >= 0:
                return {"kind": kind, "block": block, "text": event["text"]}
        if kind == "tool_call_delta":
            call, text, name = event.get("id"), event.get("arguments_delta"), event.get("name")
            if isinstance(call, str) and call and call not in self.ready and isinstance(text, str) and (name is None or isinstance(name, str)):
                self.pending.add(call)
                return {"kind": kind, "id": call, "name": name, "arguments_delta": text}
        if kind == "tool_call_ready":
            call, name, raw = event.get("id"), event.get("name"), event.get("arguments")
            if (isinstance(call, str) and call and call not in self.ready and isinstance(name, str)
                    and name in self.schemas and isinstance(raw, str)):
                arguments = validate(parse(raw), self.schemas[name])
                self.ready[call] = {"id": call, "name": name, "arguments": arguments}
                self.pending.discard(call)
                return {"kind": kind, **self.ready[call]}
        if kind == "completed" and isinstance(event.get("response"), dict):
            response = event["response"]
            finish, usage = response.get("finish"), response.get("usage")
            if (isinstance(response.get("text"), str) and (usage is None or isinstance(usage, dict))
                    and finish in {"stop", "tool_calls", "length"}):
                if finish == "length":
                    calls = response.get("tool_calls", list(self.ready.values()))
                    if not isinstance(calls, list) or not all(isinstance(call, dict) for call in calls):
                        raise Fault("INVALID_STREAM_EVENT", "Expected observed tool-call records")
                    return {"kind": kind, "response": {**response, "tool_calls": calls}}
                if self.pending:
                    return self.normalize({"kind": "interrupted", "usage": usage, "reason": "INCOMPLETE_TOOL_CALL"})
                return {"kind": kind, "response": {**response, "tool_calls": list(self.ready.values())}}
        if kind == "interrupted" and isinstance(event.get("reason"), str):
            usage = event.get("usage")
            return {"kind": kind, "reason": event["reason"], "usage": usage if isinstance(usage, dict) else None,
                    "observed": event.get("observed"),
                    "cancellation_requested": False, "transport_closed": event.get("transport_closed") is True,
                    "provider_stopped": event.get("provider_stopped") is True}
        raise Fault("INVALID_STREAM_EVENT", "Adapter produced an invalid stream record", event_kind=kind)


def coalesce(events, event):
    """Combine adjacent deltas from the same block or tool without changing order."""
    if events:
        last = events[-1]
        if last["kind"] == event["kind"] == "text_delta" and last["block"] == event["block"]:
            last["text"] += event["text"]
            return
        if (last["kind"] == event["kind"] == "tool_call_delta" and last["id"] == event["id"]
                and last["name"] == event["name"]):
            last["arguments_delta"] += event["arguments_delta"]
            return
    events.append(event)
