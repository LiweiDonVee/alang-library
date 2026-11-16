"""Local transactional run, effect and Registry state."""
import copy
import sqlite3
import threading
import time
import uuid
import weakref
from contextlib import contextmanager
from pathlib import Path

from .contracts import Fault, canonical, digest, parse, validate
from .effect_identity import check_effect_binding, require_original_binding


class ParallelCancelled(Fault):
    def __init__(self, batch_id, cancellation_id):
        super().__init__("PARALLEL_CANCELLED", "Parallel child stopped at a cancellation boundary",
                         batch_id=batch_id, cancellation_id=cancellation_id)


_event_conditions = weakref.WeakValueDictionary()
_event_conditions_lock = threading.Lock()


def parallel_event_checkpoint(state, batch_id):
    return digest([state["frames"], state.get("active_parallel"), state["parallel_batches"][batch_id]])


def supervised_event_batch(state):
    reserved = state.get("active_reserved_dispatch", {})
    if "settlement" in reserved:
        return reserved["batch_id"]
    return state.get("active_parallel_continuation", {}).get("event_batch")


class Store:
    def __init__(self, path):
        self.path = Path(path)
        with _event_conditions_lock:
            self.parallel_wake = _event_conditions.setdefault(self.path.resolve(), threading.Condition())
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.transaction() as db:
            db.executescript("""
                BEGIN IMMEDIATE;
                CREATE TABLE IF NOT EXISTS runs (
                    id TEXT PRIMARY KEY, state TEXT NOT NULL, result TEXT, token TEXT UNIQUE,
                    owner TEXT, lease REAL NOT NULL DEFAULT 0
                );
                CREATE TABLE IF NOT EXISTS requests (
                    run TEXT, id TEXT, token TEXT, input_hash TEXT, result TEXT,
                    PRIMARY KEY(run, id)
                );
                CREATE TABLE IF NOT EXISTS steering_events (
                    run TEXT NOT NULL, sequence INTEGER NOT NULL, event_id TEXT NOT NULL, text TEXT NOT NULL,
                    PRIMARY KEY(run, sequence), UNIQUE(run, event_id)
                );
                CREATE TABLE IF NOT EXISTS effects (
                    run TEXT, id TEXT, status TEXT NOT NULL, response TEXT,
                    PRIMARY KEY(run, id)
                );
                CREATE TABLE IF NOT EXISTS effect_intents (
                    run TEXT, id TEXT, intent TEXT NOT NULL, PRIMARY KEY(run, id)
                );
                CREATE TABLE IF NOT EXISTS effect_reconciliations (
                    run TEXT, verification_id TEXT, call_id TEXT NOT NULL,
                    record TEXT NOT NULL, recorded_at REAL NOT NULL,
                    PRIMARY KEY(run, verification_id)
                );
                CREATE TABLE IF NOT EXISTS effect_usage (
                    run TEXT, id TEXT, usage TEXT, PRIMARY KEY(run, id)
                );
                CREATE TABLE IF NOT EXISTS reconciled_continuations (
                    run TEXT NOT NULL, id TEXT NOT NULL, call TEXT NOT NULL,
                    previous_result TEXT NOT NULL, result TEXT, started_at REAL NOT NULL,
                    PRIMARY KEY(run, id), UNIQUE(run, call)
                );
                CREATE TABLE IF NOT EXISTS parallel_continuations (
                    run TEXT NOT NULL, id TEXT NOT NULL, batch TEXT NOT NULL,
                    previous_result TEXT NOT NULL, result TEXT, started_at REAL NOT NULL,
                    settlement TEXT, PRIMARY KEY(run, id)
                );
                CREATE TABLE IF NOT EXISTS reserved_dispatch_continuations (
                    run TEXT NOT NULL, id TEXT NOT NULL, authorization TEXT NOT NULL,
                    previous_result TEXT NOT NULL, result TEXT, started_at REAL NOT NULL,
                    PRIMARY KEY(run, id)
                );
                CREATE TABLE IF NOT EXISTS reserved_dispatch_claims (
                    child TEXT PRIMARY KEY, parent TEXT NOT NULL, batch TEXT NOT NULL,
                    continuation TEXT NOT NULL, operation TEXT NOT NULL, owner TEXT NOT NULL, revision TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS parallel_child_takeovers (
                    parent TEXT NOT NULL, id TEXT NOT NULL, batch TEXT NOT NULL, task TEXT NOT NULL,
                    child TEXT NOT NULL, previous_owner TEXT NOT NULL, owner TEXT NOT NULL,
                    checkpoint_hash TEXT NOT NULL, result TEXT, PRIMARY KEY(parent, id)
                );
                CREATE TABLE IF NOT EXISTS parallel_parent_settlements (
                    run TEXT NOT NULL, id TEXT NOT NULL, batch TEXT NOT NULL, previous_owner TEXT NOT NULL,
                    owner TEXT NOT NULL, result TEXT NOT NULL, PRIMARY KEY(run, id)
                );
                CREATE TABLE IF NOT EXISTS versions (
                    name TEXT, revision TEXT, record TEXT NOT NULL, status TEXT NOT NULL,
                    PRIMARY KEY(name, revision)
                );
                CREATE TABLE IF NOT EXISTS defaults (name TEXT PRIMARY KEY, revision TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS parallel_tasks (
                    parent TEXT NOT NULL, batch TEXT NOT NULL, ordinal INTEGER NOT NULL, task TEXT NOT NULL,
                    child TEXT NOT NULL UNIQUE, dispatched INTEGER NOT NULL DEFAULT 0, finish_order INTEGER,
                    PRIMARY KEY(parent, batch, ordinal)
                );
                CREATE TABLE IF NOT EXISTS parallel_cancellations (
                    parent TEXT NOT NULL, batch TEXT NOT NULL, id TEXT NOT NULL, PRIMARY KEY(parent, batch)
                );
                CREATE TABLE IF NOT EXISTS parallel_task_cancellations (
                    parent TEXT NOT NULL, batch TEXT NOT NULL, id TEXT NOT NULL, task TEXT NOT NULL,
                    PRIMARY KEY(parent, batch, id, task)
                );
                CREATE TABLE IF NOT EXISTS parallel_events (
                    parent TEXT NOT NULL, batch TEXT NOT NULL, sequence INTEGER NOT NULL,
                    kind TEXT NOT NULL, subject TEXT NOT NULL, cancellation_id TEXT,
                    PRIMARY KEY(parent, batch, sequence)
                );
                CREATE TABLE IF NOT EXISTS parallel_event_settlements (
                    parent TEXT NOT NULL, batch TEXT NOT NULL, task TEXT NOT NULL, snapshot TEXT NOT NULL,
                    PRIMARY KEY(parent, batch, task)
                );
                CREATE TABLE IF NOT EXISTS stream_events (
                    run TEXT NOT NULL, call TEXT NOT NULL, sequence INTEGER NOT NULL, event TEXT NOT NULL,
                    PRIMARY KEY(run, call, sequence)
                );
                CREATE TABLE IF NOT EXISTS stream_journal_seals (
                    run TEXT NOT NULL, call TEXT NOT NULL, seal TEXT NOT NULL,
                    PRIMARY KEY(run, call)
                );
                CREATE TABLE IF NOT EXISTS stream_cancellations (
                    run TEXT NOT NULL, call TEXT NOT NULL, id TEXT NOT NULL,
                    PRIMARY KEY(run, call), UNIQUE(run, id)
                );
            """)
            self._migrate_parallel_continuations(db)

    @staticmethod
    def _migrate_parallel_continuations(db):
        if "settlement" not in {row["name"] for row in db.execute("PRAGMA table_info(parallel_continuations)")}:
            db.execute("ALTER TABLE parallel_continuations RENAME TO legacy_parallel_continuations")
            db.execute("""CREATE TABLE parallel_continuations (
                          run TEXT NOT NULL, id TEXT NOT NULL, batch TEXT NOT NULL, previous_result TEXT NOT NULL,
                          result TEXT, started_at REAL NOT NULL, settlement TEXT, PRIMARY KEY(run, id))""")
            db.execute("""INSERT INTO parallel_continuations
                          SELECT run,id,batch,previous_result,result,started_at,NULL FROM legacy_parallel_continuations""")
            db.execute("DROP TABLE legacy_parallel_continuations")
        db.execute("""CREATE UNIQUE INDEX IF NOT EXISTS parallel_continuation_batch
                      ON parallel_continuations(run,batch) WHERE settlement IS NULL""")
        db.execute("""CREATE UNIQUE INDEX IF NOT EXISTS parallel_continuation_settlement
                      ON parallel_continuations(run,batch,settlement) WHERE settlement IS NOT NULL""")

    @contextmanager
    def transaction(self):
        db = sqlite3.connect(self.path, timeout=10, isolation_level=None)
        db.row_factory = sqlite3.Row
        try:
            db.execute("BEGIN IMMEDIATE")
            yield db
            db.commit()
        except BaseException:
            db.rollback()
            raise
        finally:
            db.close()

    def create(self, state):
        owner = uuid.uuid4().hex
        with self.transaction() as db:
            db.execute("INSERT INTO runs(id,state,owner,lease) VALUES(?,?,?,?)",
                       (state["run_id"], canonical(state), owner, self.lease(state)))
        return owner

    @staticmethod
    def lease(state):
        if "reconciled_replay" in state:
            # Replaying this checkpoint is read-only, so a short lease is enough
            # to recover a crash before the saved instruction advances.
            return min(time.time() + 5, state["continuation_deadline"])
        return time.time() + max(30, state["limits"]["seconds"] + 10)

    def _save(self, db, state, owner):
        if "parallel_parent" in state:
            self._require_parallel_child_owner(db, state)
        updated = db.execute("UPDATE runs SET state=?,lease=? WHERE id=? AND owner=?",
                             (canonical(state), self.lease(state), state["run_id"], owner))
        if updated.rowcount != 1:
            raise Fault("RUN_OWNERSHIP_LOST", "Execution claim is no longer valid")

    def save(self, state, owner):
        with self.transaction() as db:
            self._save(db, state, owner)

    @staticmethod
    def _require_owner(db, run_id, owner):
        if not db.execute("SELECT 1 FROM runs WHERE id=? AND owner=?", (run_id, owner)).fetchone():
            raise Fault("RUN_OWNERSHIP_LOST", "Execution claim is no longer valid")

    def _require_parallel_child_owner(self, db, state):
        while "parallel_parent" in state:
            if (settlement := state.get("nested_parent_settlement")) and (claim := state.get("active_parallel_continuation")):
                if not db.execute("""SELECT 1 FROM parallel_continuations c JOIN runs r ON r.id=c.run
                                     JOIN parallel_parent_settlements s ON s.run=c.run AND s.batch=c.batch
                                     WHERE c.run=? AND c.id=? AND c.batch=? AND r.owner=? AND c.result IS NULL
                                     AND s.id=? AND s.previous_owner=? AND c.previous_result=s.result
                                     AND c.settlement IS ?""",
                                  (state["run_id"], claim["id"], claim["batch_id"], claim["owner"],
                                   settlement["id"], settlement["previous_owner"], claim.get("settlement"))).fetchone():
                    raise Fault("RUN_OWNERSHIP_LOST", "Nested batch no longer owns its original continuation")
                return self._check_settled_ancestor_slot(db, state, settlement["ancestor"])
            if (grant := state.get("active_reserved_dispatch", {})).get("settlement"):
                settlement = state["nested_parent_settlement"]
                if (grant["settlement"] != settlement["id"] or grant["previous_owner"] != settlement["previous_owner"]
                        or not db.execute("""SELECT 1 FROM reserved_dispatch_continuations c JOIN runs r ON r.id=c.run
                                            JOIN parallel_parent_settlements s ON s.run=c.run AND s.id=? AND s.batch=?
                                            WHERE c.run=? AND c.id=? AND r.owner=? AND c.result IS NULL
                                            AND c.authorization=? AND s.previous_owner=? AND c.previous_result=s.result""",
                                          (settlement["id"], grant["batch_id"], state["run_id"], grant["continuation_id"],
                                           grant["owner"], canonical(grant), settlement["previous_owner"])).fetchone()):
                    raise Fault("RUN_OWNERSHIP_LOST", "Nested batch no longer owns its reserved dispatch")
                return self._check_settled_ancestor_slot(db, state, settlement["ancestor"])
            if "active_child_takeover" in state:
                return self._check_independent_child(db, state, previously_settled=False)
            if "active_continuation" in state:
                return self._check_independent_child(db, state)
            if "child_input_settlement" in state:
                return self._settled_child_input_parent(db, state, state["child_input_settlement"])
            state = self._owned_parallel_parent(db, state["parallel_parent"])

    @staticmethod
    def _owned_parallel_parent(db, parent, *, live=False):
        row = db.execute("SELECT state,lease FROM runs WHERE id=? AND owner=?", (parent["run_id"], parent["owner"])).fetchone()
        if row is None or (live and row["lease"] <= time.time()):
            raise Fault("RUN_OWNERSHIP_LOST", "Execution claim is no longer valid")
        return parse(row["state"])

    def _check_independent_child(self, db, state, *, previously_settled=True, check_cancel=True, nested_takeover=False):
        parent = state["parallel_parent"]
        row = db.execute("""SELECT r.state,r.result,r.owner,t.dispatched,t.finish_order
                            FROM parallel_tasks t JOIN runs r ON r.id=t.parent
                            WHERE t.parent=? AND t.batch=? AND t.child=?""",
                         (parent["run_id"], parent["batch_id"], state["run_id"])).fetchone()
        if (row is None or row["owner"] is not None or not row["dispatched"]
                or (row["finish_order"] is not None) != previously_settled
                or (check_cancel and self._task_cancellation(db, parent, state["run_id"]) is not None)):
            raise Fault("PARALLEL_CHILD_CONTINUATION_DENIED", "Child needs a settled parent and an uncancelled original slot",
                        phase="continue")
        original = parse(row["state"])
        if "parallel_parent" in original:
            if not nested_takeover:
                claim = state.get("active_child_takeover")
                if not db.execute("""SELECT 1 FROM parallel_child_takeovers c JOIN runs r ON r.id=c.child
                                     WHERE c.parent=? AND c.batch=? AND c.id=? AND c.child=?
                                     AND c.owner=r.owner AND c.result IS NULL""",
                                  (parent["run_id"], parent["batch_id"], claim, state["run_id"])).fetchone():
                    raise Fault("PARALLEL_CHILD_CONTINUATION_DENIED", "Nested slots require an original takeover claim", phase="continue")
            self._check_nested_settlement(db, original)
        elif row["result"] is None:
            raise Fault("PARALLEL_CHILD_CONTINUATION_DENIED", "Child needs a settled parent", phase="continue")
        if self._active_batch(state) is not None:
            raise Fault("PARALLEL_CHILD_CONTINUATION_DENIED", "Independent child continuation cannot resume a nested Join",
                        phase="continue")
        origin = state.get("parallel_origin")
        if (origin is None or state["scope_hash"] != original["scope_hash"]
                or any(origin[key] != original[key] for key in ("source", "inputs", "program_hash"))
                or state.get("original_deadline", float("inf")) > original.get("original_deadline", 0)
                or state["frames"] and state["frames"][0]["program"] != origin["target"]):
            raise Fault("CONTINUATION_REVISION_MISMATCH", "Original parent or linked child checkpoint differs", phase="continue")

    def _check_settled_ancestor_slot(self, db, state, settlement_id):
        parent = state["parallel_parent"]
        row = db.execute("""SELECT p.state,p.owner,p.result,t.dispatched,t.finish_order,s.id,s.result AS fact
                            FROM parallel_tasks t JOIN runs p ON p.id=t.parent
                            JOIN parallel_parent_settlements s ON s.run=t.parent AND s.batch=t.batch
                            WHERE t.parent=? AND t.batch=? AND t.child=? AND s.previous_owner=?
                            AND (? IS NULL OR s.id=?)""",
                         (parent["run_id"], parent["batch_id"], state["run_id"], parent["owner"],
                          settlement_id, settlement_id)).fetchone()
        if row is not None and row["owner"] is None and row["dispatched"] and row["finish_order"] is None:
            original = parse(row["state"])
            checkpoint = original.get("parallel_join_checkpoint", {})
            if (checkpoint.get("supervised") is True and checkpoint.get("batch_id") == parent["batch_id"]
                    and self._matches_parallel_checkpoint(original, checkpoint)
                    and state["run_id"] in checkpoint.get("takeover_children", [])
                    and self._parallel_cancellation_scope(db, state) is None):
                if "parallel_parent" in original:
                    binding = original.get("nested_parent_settlement", {})
                    if binding.get("id") == row["id"]:
                        self._check_nested_settlement(db, original)
                        return row["id"]
                elif row["result"] == row["fact"]:
                    return row["id"]
        raise Fault("PARALLEL_CHILD_CONTINUATION_DENIED", "Original ancestor settlement does not authorize this slot", phase="continue")

    @staticmethod
    def _matches_parallel_checkpoint(state, checkpoint):
        if not state["frames"]:
            return False
        if "event_checkpoint" in checkpoint:
            batch = checkpoint["batch_id"]
            return (state.get("active_parallel", {}).get("batch_id") == batch
                    and batch in state.get("parallel_batches", {})
                    and parallel_event_checkpoint(state, batch) == checkpoint["event_checkpoint"])
        return checkpoint.get("operation") == state["frames"][-1].get("pending", {}).get("id")

    def _check_nested_settlement(self, db, state):
        binding = state.get("nested_parent_settlement", {})
        checkpoint = state.get("parallel_join_checkpoint", {})
        row = db.execute("""SELECT s.result FROM parallel_parent_settlements s JOIN runs r ON r.id=s.run
                            WHERE s.run=? AND s.batch=? AND s.id=? AND s.previous_owner=?
                            AND r.owner IS NULL AND r.result IS NULL AND r.token IS NULL""",
                         (state["run_id"], checkpoint.get("batch_id"), binding.get("id"), binding.get("previous_owner"))).fetchone()
        if (row is None or checkpoint.get("supervised") is not True
                or not self._matches_parallel_checkpoint(state, checkpoint)):
            raise Fault("PARALLEL_CHILD_CONTINUATION_DENIED", "Original nested batch has no active supervised settlement", phase="continue")
        self._check_settled_ancestor_slot(db, state, binding["ancestor"])
        return row["result"]

    def reserve_parallel(self, state, owner, batch_id, tasks):
        with self.transaction() as db:
            self._check_parallel_limits(db, state, len(tasks), state["parallel_policy"])
            self._save(db, state, owner)
            for ordinal, (task_id, child) in enumerate(tasks):
                db.execute("INSERT INTO runs(id,state) VALUES(?,?)", (child["run_id"], canonical(child)))
                db.execute("INSERT INTO parallel_tasks(parent,batch,ordinal,task,child) VALUES(?,?,?,?,?)",
                           (state["run_id"], batch_id, ordinal, task_id, child["run_id"]))
            if state.get("active_parallel", {}).get("batch_id") == batch_id:
                queued = db.execute("SELECT event_id FROM steering_events WHERE run=? AND sequence>? ORDER BY sequence",
                                    (state["run_id"], state.get("steering_cursor", 0))).fetchall()
                for event in queued:
                    self._append_parallel_event(db, state["run_id"], batch_id, "steering", event["event_id"])

    def _check_parallel_limits(self, db, state, added, policy):
        root = state
        if "parallel_parent" in state:
            self._check_parallel_cancel(db, state)
            for parent in self._parallel_ancestors(db, state):
                if any(key in root for key in ("active_child_takeover", "active_continuation", "child_input_settlement")):
                    raise Fault("PARALLEL_CHILD_CONTINUATION_DENIED", "Independent child continuation cannot reserve a nested batch")
                if root is not state and supervised_event_batch(root):
                    raise Fault("PARALLEL_CHILD_CONTINUATION_DENIED", "Event continuation only covers its original batch")
                root = parent
                policy = {kind: min(policy[kind], root["parallel_policy"][kind]) for kind in ("active", "cumulative")}
            if "active_reserved_dispatch" in root:
                raise Fault("PARALLEL_CHILD_CONTINUATION_DENIED", "Reserved-dispatch authorization does not cover new nested slots")
        active = db.execute("""SELECT COUNT(*) FROM parallel_tasks t JOIN runs r ON r.id=t.child
                               WHERE t.parent=? AND r.result IS NULL""", (state["run_id"],)).fetchone()[0]
        if active + added > policy["active"]:
            raise Fault("PARALLEL_LIMIT_EXCEEDED", "Parallel tasks exceed the Host limit", limit="active")
        cumulative = db.execute("""WITH RECURSIVE subtree(child) AS (
                                  SELECT child FROM parallel_tasks WHERE parent=?
                                  UNION ALL SELECT t.child FROM parallel_tasks t JOIN subtree s ON t.parent=s.child
                               ) SELECT COUNT(*) FROM subtree""", (root["run_id"],)).fetchone()[0]
        if cumulative + added > policy["cumulative"]:
            raise Fault("PARALLEL_LIMIT_EXCEEDED", "Parallel tasks exceed the Host limit", limit="cumulative")

    def append_parallel(self, state, owner, batch_id, tasks, policy):
        """Commit appended slots, their budgets and the instruction receipt together."""
        with self.transaction() as db:
            self._require_owner(db, state["run_id"], owner)
            saved = parse(db.execute("SELECT state FROM runs WHERE id=?", (state["run_id"],)).fetchone()["state"])
            event_batch = saved.get("active_parallel_continuation", {}).get("event_batch", batch_id)
            if (saved.get("active_parallel", {}).get("batch_id") != batch_id or event_batch != batch_id
                    or self._parallel_cancellation(db, state["run_id"], batch_id) is not None):
                raise Fault("PARALLEL_BATCH_NOT_ACTIVE", "Batch no longer accepts appended tasks")
            manifest = saved["parallel_batches"][batch_id]
            if {row["id"] for row in manifest} & {task_id for task_id, _ in tasks}:
                raise Fault("INVALID_PARALLEL_TASKS", "Appended task IDs must be unique across the batch")
            self._check_parallel_limits(db, state, len(tasks), policy)
            self._save(db, state, owner)
            for ordinal, (task_id, child) in enumerate(tasks, len(manifest)):
                db.execute("INSERT INTO runs(id,state) VALUES(?,?)", (child["run_id"], canonical(child)))
                db.execute("INSERT INTO parallel_tasks(parent,batch,ordinal,task,child) VALUES(?,?,?,?,?)",
                           (state["run_id"], batch_id, ordinal, task_id, child["run_id"]))

    @staticmethod
    def _append_parallel_event(db, run_id, batch_id, kind, subject, cancellation_id=None):
        db.execute("""INSERT INTO parallel_events SELECT ?,?,COALESCE(MAX(sequence),0)+1,?,?,?
                      FROM parallel_events WHERE parent=? AND batch=?""",
                   (run_id, batch_id, kind, subject, cancellation_id, run_id, batch_id))

    def _parallel_task_event(self, db, parent, child_id):
        run_id, batch_id = parent["run_id"], parent["batch_id"]
        state = parse(db.execute("SELECT state FROM runs WHERE id=?", (run_id,)).fetchone()["state"])
        if state.get("active_parallel", {}).get("batch_id") != batch_id:
            return False
        task = db.execute("SELECT task FROM parallel_tasks WHERE child=?", (child_id,)).fetchone()["task"]
        result = db.execute("SELECT result FROM runs WHERE id=?", (child_id,)).fetchone()["result"]
        snapshot = {"result": parse(result), "effects": self._parallel_effects(db, child_id)}
        db.execute("INSERT INTO parallel_event_settlements VALUES(?,?,?,?)", (run_id, batch_id, task, canonical(snapshot)))
        self._append_parallel_event(db, run_id, batch_id, "task_settled", task,
                                    self._task_cancellation(db, parent, child_id))
        if not db.execute("""SELECT 1 FROM parallel_tasks t JOIN runs r ON r.id=t.child
                             WHERE t.parent=? AND t.batch=? AND r.result IS NULL""", (run_id, batch_id)).fetchone():
            count = db.execute("SELECT COUNT(*) FROM parallel_tasks WHERE parent=? AND batch=?", (run_id, batch_id)).fetchone()[0]
            self._append_parallel_event(db, run_id, batch_id, "all_settled", str(count))
        return True

    def _finish_parallel_slot(self, db, parent, child_id):
        sequence = db.execute("SELECT COALESCE(MAX(finish_order),0)+1 FROM parallel_tasks WHERE parent=? AND batch=?",
                              (parent["run_id"], parent["batch_id"])).fetchone()[0]
        db.execute("UPDATE parallel_tasks SET finish_order=? WHERE child=?", (sequence, child_id))
        return self._parallel_task_event(db, parent, child_id)

    def _finish_waiting_child(self, db, state, token, fault):
        child_id, parent = state["run_id"], state["parallel_parent"]
        state.pop("wait")
        status = "cancelled" if isinstance(fault, ParallelCancelled) else "failed"
        state["trace"].append({"event": status, "error": fault.error})
        result = {"status": status, "error": fault.error, "run_id": child_id, "revision": state["revision"],
                  "scope_revision": state["scope"].get("revision"),
                  "metrics": {key: state[key] for key in ("steps", "calls", "elapsed")}}
        updated = db.execute("UPDATE runs SET state=?,result=?,token=NULL WHERE id=? AND token=? AND owner IS NULL",
                             (canonical(state), canonical(result), child_id, token))
        if updated.rowcount != 1:
            raise Fault("RUN_OWNERSHIP_LOST", "Child no longer owns this input checkpoint")
        self._finish_parallel_slot(db, parent, child_id)

    @staticmethod
    def _waiting_children(db, run_id, batch_id):
        return db.execute("""SELECT r.state,r.token,t.task FROM parallel_tasks t JOIN runs r ON r.id=t.child
                             WHERE t.parent=? AND t.batch=? AND r.token IS NOT NULL AND r.owner IS NULL
                             AND r.result IS NULL AND t.dispatched=1 AND t.finish_order IS NULL ORDER BY t.ordinal""",
                          (run_id, batch_id)).fetchall()

    @staticmethod
    def _active_batch(state):
        active = state.get("active_parallel")
        if active is not None:
            return active["batch_id"]
        return state["frames"][-1].get("pending", {}).get("batch_id") if state["frames"] else None

    def _child_input_parent(self, db, state):
        binding = state["parallel_parent"]
        row = db.execute("""SELECT p.state,p.owner,p.lease,p.result,t.task,t.dispatched,t.finish_order
                            FROM parallel_tasks t JOIN runs p ON p.id=t.parent
                            WHERE t.parent=? AND t.batch=? AND t.child=?""",
                         (binding["run_id"], binding["batch_id"], state["run_id"])).fetchone()
        if (row is None or row["owner"] != binding["owner"] or row["lease"] <= time.time() or row["result"] is not None
                or not row["dispatched"]):
            raise Fault("PARALLEL_CHILD_CONTINUATION_DENIED", "Original parent no longer owns the child batch", phase="resume")
        parent = parse(row["state"])
        if self._active_batch(parent) != binding["batch_id"]:
            raise Fault("PARALLEL_CHILD_CONTINUATION_DENIED", "Original parent batch is no longer active", phase="resume")
        try:
            self._require_live_ancestors(db, parent)
        except Fault as fault:
            raise Fault("PARALLEL_CHILD_CONTINUATION_DENIED", "Original ancestor no longer owns the child batch", phase="resume") from fault
        return parent, row["task"], row["finish_order"]

    def _settled_child_input_parent(self, db, child, binding):
        original = child["parallel_parent"]
        record = db.execute("""SELECT p.state,p.result,p.owner,p.token,s.result AS settlement_result,t.task,t.ordinal
                               FROM parallel_parent_settlements s JOIN runs p ON p.id=s.run
                               JOIN parallel_tasks t ON t.parent=s.run AND t.batch=s.batch
                               WHERE s.run=? AND s.batch=? AND s.id=? AND s.previous_owner=?
                                 AND t.child=? AND t.dispatched=1 AND t.finish_order IS NULL""",
                            (original["run_id"], original["batch_id"], binding["id"], binding["previous_owner"],
                             child["run_id"])).fetchone()
        if (record is None or binding["previous_owner"] != original["owner"] or record["owner"] is not None
                or record["token"] is not None or record["result"] != record["settlement_result"]):
            raise Fault("PARALLEL_CHILD_CONTINUATION_DENIED", "Original parent settlement does not authorize this waiting slot",
                        phase="resume")
        parent = parse(record["state"])
        failure = parse(record["result"])
        checkpoint = parent.get("parallel_join_checkpoint", {})
        pending = parent["frames"][-1].get("pending", {}) if parent["frames"] else {}
        batch_id = original["batch_id"]
        manifest = parent.get("parallel_batches", {}).get(batch_id, [])
        ordinal = record["ordinal"]
        if (failure.get("status") != "failed" or failure.get("error", {}).get("code") != "PARALLEL_INCOMPLETE"
                or failure["error"].get("batch_id") != batch_id or checkpoint.get("supervised") is not True
                or checkpoint.get("batch_id") != batch_id or not checkpoint.get("operation")
                or checkpoint["operation"] != pending.get("id") or child["run_id"] not in checkpoint.get("waiting_children", [])
                or ordinal >= len(manifest) or manifest[ordinal] != {"id": record["task"], "run_id": child["run_id"]}
                or child["run_id"] != digest([batch_id, ordinal])):
            raise Fault("PARALLEL_CHILD_CONTINUATION_DENIED", "Original supervised Join does not contain this waiting slot",
                        phase="resume")
        self._check_independent_child(db, child, previously_settled=False, check_cancel=False)
        return parent, record["task"]

    def parallel_child_waits(self, run_id, batch_id, authorize, *, parent_settlement=None):
        with self.transaction() as db:
            waits = []
            for row in self._waiting_children(db, run_id, batch_id):
                child = parse(row["state"])
                try:
                    if parent_settlement is None:
                        parent, task, _ = self._child_input_parent(db, child)
                        if self._parallel_cancellation_scope(db, child):
                            continue
                    else:
                        child["child_input_settlement"] = parent_settlement
                        parent, task = self._settled_child_input_parent(db, child, parent_settlement)
                        if self._task_cancellation(db, child["parallel_parent"], child["run_id"]) is not None:
                            continue
                    authorize(parent, child, task, child["checkpoint"], "read", None)
                except Fault as fault:
                    if fault.error["code"] not in {"AUTHORIZATION_DENIED", "PARALLEL_CHILD_CONTINUATION_DENIED"}:
                        raise
                    continue
                wait = child["wait"]
                if wait["expires"] > time.time():
                    waits.append({"status": "awaiting_input", "run_id": child["run_id"], "task_id": task, "checkpoint": child["checkpoint"],
                                  "input_schema": wait["schema"], "reason": wait["reason"], "expires": wait["expires"],
                                  "resume_token": row["token"]})
            return waits

    def poll_parallel_tree(self, run_id, owner, batch_id):
        """Find persisted continuations under the live owner, and settle expired input waits."""
        with self.transaction() as db:
            self._require_owner(db, run_id, owner)
            rows = db.execute("""WITH RECURSIVE tree(child) AS (
                                     SELECT child FROM parallel_tasks WHERE parent=? AND batch=?
                                     UNION ALL SELECT t.child FROM tree JOIN parallel_tasks t ON t.parent=tree.child
                               ) SELECT r.id,r.owner,r.lease,r.token,r.state FROM tree JOIN runs r ON r.id=tree.child
                               WHERE r.result IS NULL AND (r.token IS NOT NULL
                               OR json_extract(r.state,'$.parallel_schedule') IN ('input','join','event','drain'))""",
                              (run_id, batch_id)).fetchall()
            ready = []
            waiting = False
            for row in rows:
                state = parse(row["state"])
                try:
                    self._require_live_ancestors(db, state)
                except Fault as fault:
                    if fault.error["code"] != "RUN_OWNERSHIP_LOST":
                        raise
                    continue
                if row["token"]:
                    cancelled = self._parallel_cancellation_scope(db, state)
                    if cancelled:
                        self._finish_waiting_child(db, state, row["token"], ParallelCancelled(*cancelled))
                    elif state["wait"]["expires"] <= time.time():
                        self._finish_waiting_child(db, state, row["token"], Fault("RESUME_EXPIRED", "Child input checkpoint has expired"))
                    else:
                        waiting = True
                elif row["owner"] is not None and row["lease"] > time.time():
                    kind = state["parallel_schedule"]
                    if self._parallel_ready(db, state, kind):
                        ready.append((row["id"], row["owner"], kind, self._parallel_resume_key(state, kind)))
                    waiting = True
            return ready, waiting

    def _parallel_batch_terminal(self, db, state):
        batch_id = self._active_batch(state)
        return batch_id in state.get("parallel_batches", {}) and not db.execute("""
            SELECT 1 FROM parallel_tasks t JOIN runs r ON r.id=t.child
            WHERE t.parent=? AND t.batch=? AND r.result IS NULL LIMIT 1
        """, (state["run_id"], batch_id)).fetchone()

    def parallel_batch_terminal(self, state, owner, *, descendants=False):
        with self.transaction() as db:
            self._require_owner(db, state["run_id"], owner)
            self._require_parallel_child_owner(db, state)
            if descendants:
                return self._parallel_subtree_terminal(db, state)
            return self._parallel_batch_terminal(db, state)

    def _parallel_subtree_terminal(self, db, state):
        return not db.execute("""WITH RECURSIVE tree(child) AS (
            SELECT child FROM parallel_tasks WHERE parent=? AND batch=?
            UNION ALL SELECT t.child FROM tree JOIN parallel_tasks t ON t.parent=tree.child
        ) SELECT 1 FROM tree JOIN runs r ON r.id=tree.child WHERE r.result IS NULL OR r.owner IS NOT NULL LIMIT 1
        """, (state["run_id"], self._active_batch(state))).fetchone()

    def _parallel_ready(self, db, state, kind):
        if kind == "input":
            return True
        if kind == "drain":
            return self._parallel_subtree_terminal(db, state)
        if kind == "event":
            active = state["active_parallel"]
            if self._parallel_cancellation_scope(db, state) or time.time() >= state["original_deadline"]:
                return True
            if db.execute("""SELECT 1 FROM parallel_events WHERE parent=? AND batch=? AND sequence>? LIMIT 1""",
                          (state["run_id"], active["batch_id"], active["cursor"])).fetchone():
                return True
        return self._parallel_batch_terminal(db, state)

    @staticmethod
    def _parallel_resume_key(state, kind):
        if kind == "drain":
            return state["active_parallel"]["batch_id"]
        return state["frames"][-1]["pending"]["id"]

    def _parallel_ancestors(self, db, state):
        supervised = False
        child_batch = None
        while "parallel_parent" in state:
            event_batch = supervised_event_batch(state)
            if not supervised and event_batch:
                if child_batch is not None and child_batch != event_batch:
                    raise Fault("RUN_OWNERSHIP_LOST", "Event continuation does not own this child batch")
                self._require_parallel_child_owner(db, state)
                claim = state.get("active_reserved_dispatch") or state["active_parallel_continuation"]
                owner = db.execute("SELECT lease FROM runs WHERE id=? AND owner=?",
                                   (state["run_id"], claim["owner"])).fetchone()
                if owner is None or owner["lease"] <= time.time():
                    raise Fault("RUN_OWNERSHIP_LOST", "Event continuation no longer holds its claim")
                if state["original_deadline"] <= time.time():
                    raise Fault("BUDGET_EXHAUSTED", "Event continuation reached its original deadline")
                supervised = True
            link = state["parallel_parent"]
            child_batch = link["batch_id"]
            if supervised:
                # The continuation's ownership check binds this settled ancestor chain.
                state = parse(db.execute("SELECT state FROM runs WHERE id=?", (link["run_id"],)).fetchone()["state"])
            else:
                state = self._owned_parallel_parent(db, link, live=True)
            yield state

    def _require_live_ancestors(self, db, state):
        for _ in self._parallel_ancestors(db, state):
            pass

    def claim_parallel_ready(self, run_id, owner, kind, operation):
        with self.transaction() as db:
            row = db.execute("""SELECT state FROM runs WHERE id=? AND owner=? AND result IS NULL AND token IS NULL
                                AND lease>? AND json_extract(state,'$.parallel_schedule')=?""",
                             (run_id, owner, time.time(), kind)).fetchone()
            if row is None:
                return None
            state = parse(row["state"])
            if self._parallel_resume_key(state, kind) != operation:
                return None
            self._require_live_ancestors(db, state)
            if not self._parallel_ready(db, state, kind):
                return None
            del state["parallel_schedule"]
            self._save(db, state, owner)
            return state

    def parallel_input_result(self, state, owner):
        with self.transaction() as db:
            row = db.execute("SELECT result FROM requests WHERE run=? AND id=?", (state["run_id"], state["request_id"])).fetchone()
            if row is not None and row["result"] is not None:
                return parse(row["result"])
            self._require_owner(db, state["run_id"], owner)
            self._require_live_ancestors(db, state)
            if time.time() >= state["original_deadline"]:
                raise Fault("BUDGET_EXHAUSTED", "Child input result exceeded the original deadline", phase="resume")
            return None

    def read_parallel_event(self, run_id, owner, batch_id, after):
        with self.transaction() as db:
            self._require_owner(db, run_id, owner)
            state = parse(db.execute("SELECT state FROM runs WHERE id=?", (run_id,)).fetchone()["state"])
            self._require_parallel_child_owner(db, state)
            row = db.execute("""SELECT e.sequence,e.kind,e.subject,e.cancellation_id,s.sequence AS steering_sequence,s.text
                                FROM parallel_events e LEFT JOIN steering_events s
                                ON e.kind='steering' AND s.run=e.parent AND s.event_id=e.subject
                                WHERE e.parent=? AND e.batch=? AND e.sequence>? ORDER BY e.sequence LIMIT 1""",
                             (run_id, batch_id, after)).fetchone()
            return dict(row) if row else None

    def wake_parallel(self):
        with self.parallel_wake:
            self.parallel_wake.notify_all()

    @staticmethod
    def _parallel_cancellation(db, parent, batch):
        row = db.execute("SELECT id FROM parallel_cancellations WHERE parent=? AND batch=?", (parent, batch)).fetchone()
        return row["id"] if row else None

    def request_parallel_cancel(self, run_id, batch_id, cancellation_id, authorize, task_ids=None, *, replay_only=False):
        with self.transaction() as db:
            parent = db.execute("SELECT state,owner,lease FROM runs WHERE id=?", (run_id,)).fetchone()
            if parent is None:
                raise Fault("UNKNOWN_RUN", "Run does not exist", phase="cancel")
            state = parse(parent["state"])
            authorize(state, batch_id)
            whole = self._parallel_cancellation(db, run_id, batch_id)
            targets = [row["task"] for row in db.execute("""
                SELECT task FROM parallel_task_cancellations WHERE parent=? AND batch=? AND id=? ORDER BY task
            """, (run_id, batch_id, cancellation_id))]
            if task_ids is None:
                conflict = whole not in {None, cancellation_id} or bool(targets)
                saved = whole == cancellation_id
            else:
                conflict = whole == cancellation_id or (bool(targets) and targets != task_ids)
                saved = bool(targets)
            if conflict:
                raise Fault("PARALLEL_CANCEL_CONFLICT", "Cancellation ID or scope conflicts with the saved request", phase="cancel")
            if not saved:
                active_batch = self._active_batch(state)
                rows = db.execute("""SELECT t.task,r.result FROM parallel_tasks t JOIN runs r ON r.id=t.child
                                     WHERE t.parent=? AND t.batch=?""", (run_id, batch_id)).fetchall()
                if parent["owner"] is None or parent["lease"] <= time.time() or active_batch != batch_id or not rows:
                    raise Fault("PARALLEL_BATCH_NOT_ACTIVE", "No active parallel batch accepts this request", phase="cancel")
                if "parallel_parent" in state:
                    self._check_parallel_cancel(db, state)
                if task_ids is None:
                    if all(row["result"] is not None for row in rows):
                        raise Fault("PARALLEL_BATCH_NOT_ACTIVE", "Parallel batch is already settled", phase="cancel")
                    db.execute("INSERT INTO parallel_cancellations VALUES(?,?,?)", (run_id, batch_id, cancellation_id))
                else:
                    if not set(task_ids) <= {row["task"] for row in rows}:
                        raise Fault("UNKNOWN_PARALLEL_TASK", "Task is not in the selected batch", phase="cancel")
                    if replay_only:
                        if all(row["result"] is not None for row in rows if row["task"] in task_ids):
                            return {"status": "already_settled", "run_id": run_id, "batch_id": batch_id, "task_ids": task_ids}
                        raise Fault("PARALLEL_INCOMPLETE", "Recovery cannot apply a new cancellation", batch_id=batch_id)
                    db.executemany("INSERT INTO parallel_task_cancellations VALUES(?,?,?,?)",
                                   [(run_id, batch_id, cancellation_id, task) for task in task_ids])
                for row in self._waiting_children(db, run_id, batch_id):
                    if task_ids is None or row["task"] in task_ids:
                        child = parse(row["state"])
                        effective = self._parallel_cancellation_scope(db, child)
                        self._finish_waiting_child(db, child, row["token"], ParallelCancelled(*effective))
        if not saved:
            self.wake_parallel()
        receipt = {"status": "cancel_requested", "run_id": run_id, "batch_id": batch_id, "cancellation_id": cancellation_id}
        if task_ids is not None:
            receipt["task_ids"] = task_ids
        return receipt

    def _task_cancellation(self, db, parent, child_id):
        row = db.execute("""SELECT c.id FROM parallel_tasks t JOIN parallel_task_cancellations c
                            ON c.parent=t.parent AND c.batch=t.batch AND c.task=t.task
                            WHERE t.child=? ORDER BY c.rowid LIMIT 1""", (child_id,)).fetchone()
        return row["id"] if row else self._parallel_cancellation(db, parent["run_id"], parent["batch_id"])

    def check_parallel_cancel(self, state, owner):
        with self.transaction() as db:
            self._require_owner(db, state["run_id"], owner)
            self._check_parallel_cancel(db, state)

    def _check_parallel_cancel(self, db, state):
        self._require_parallel_child_owner(db, state)
        if cancellation := self._parallel_cancellation_scope(db, state):
            raise ParallelCancelled(*cancellation)

    def _parallel_cancellation_scope(self, db, state):
        effective = None
        while "parallel_parent" in state:
            parent = state["parallel_parent"]
            cancellation = self._task_cancellation(db, parent, state["run_id"])
            if cancellation is not None:
                effective = (parent["batch_id"], cancellation)
            state = parse(db.execute("SELECT state FROM runs WHERE id=?", (parent["run_id"],)).fetchone()["state"])
        return effective

    def dispatch_parallel(self, state):
        parent = state["parallel_parent"]
        child_id = state["run_id"]
        owner = uuid.uuid4().hex
        wake = False
        with self.transaction() as db:
            self._require_parallel_child_owner(db, state)
            self._require_live_ancestors(db, state)
            cancellation = self._parallel_cancellation_scope(db, state)
            if cancellation is not None:
                receipt = {"status": "not_dispatched", "cancellation_id": cancellation[1]}
                updated = db.execute("UPDATE runs SET result=? WHERE id=? AND owner IS NULL AND result IS NULL",
                                     (canonical(receipt), child_id))
                if updated.rowcount != 1:
                    raise Fault("RUN_OWNERSHIP_LOST", "Parallel task was already dispatched or settled")
                wake = self._parallel_task_event(db, parent, child_id)
            else:
                updated = db.execute("UPDATE parallel_tasks SET dispatched=1 WHERE child=? AND dispatched=0", (child_id,))
                if updated.rowcount != 1:
                    raise Fault("RUN_OWNERSHIP_LOST", "Parallel task was already dispatched")
                db.execute("UPDATE runs SET owner=?,lease=? WHERE id=?", (owner, self.lease(state), child_id))
        if wake:
            self.wake_parallel()
        return None if cancellation is not None else owner

    def _pristine_reserved_slots(self, db, state, batch_id, task_ids):
        run_id = state["run_id"]
        manifest = state.get("parallel_batches", {}).get(batch_id, [])
        rows = db.execute("""SELECT t.*,r.state,r.result,r.token,r.owner,r.lease FROM parallel_tasks t
                             JOIN runs r ON r.id=t.child WHERE t.parent=? AND t.batch=? ORDER BY t.ordinal""",
                          (run_id, batch_id)).fetchall()
        if ([{"id": row["task"], "run_id": row["child"]} for row in rows] != manifest
                or [row["ordinal"] for row in rows] != list(range(len(rows)))):
            raise Fault("PARALLEL_STATE_INVALID", "Original batch manifest differs", batch_id=batch_id)
        selected = [row for row in rows if row["task"] in task_ids]
        if len(selected) != len(task_ids):
            raise Fault("UNKNOWN_PARALLEL_TASK", "Requested slot is not in the original batch", batch_id=batch_id)
        slots = []
        for row in selected:
            child_id = row["child"]
            parent = {"run_id": run_id, "batch_id": batch_id}
            if (row["dispatched"] != 0 or row["finish_order"] is not None or row["owner"] is not None
                    or row["lease"] != 0 or row["token"] is not None or row["result"] is not None
                    or self._task_cancellation(db, parent, child_id) is not None or db.execute("""
                        SELECT 1 FROM effects WHERE run=? UNION ALL SELECT 1 FROM effect_intents WHERE run=?
                        UNION ALL SELECT 1 FROM reserved_dispatch_claims WHERE child=? LIMIT 1
                    """, (child_id, child_id, child_id)).fetchone()):
                raise Fault("PARALLEL_SLOT_NOT_DISPATCHABLE", "Original slot is no longer a pristine reservation",
                            batch_id=batch_id, task_id=row["task"])
            child = parse(row["state"])
            origin = child.get("parallel_origin", {})
            binding = child.get("parallel_parent", {})
            # Reservation creates a child row and an initial frame before any owner exists.
            pristine = {"run_id", "source", "scope", "inputs", "program_hash", "revision", "scope_hash", "environment",
                        "limits", "frames", "root_grants", "destinations", "accessed", "cache", "models", "steps", "calls",
                        "elapsed", "original_deadline", "trace", "parallel_parent", "parallel_origin"}
            original_link = state.get("parallel_link", state.get("parallel_origin"))
            if original_link is not None:
                pristine.add("parallel_link")
            frames = child["frames"]
            if (set(child) != pristine or child_id != child["run_id"] or child_id != digest([batch_id, row["ordinal"]])
                    or any(child[name] for name in ("accessed", "cache", "models", "steps", "calls", "elapsed", "trace"))
                    or binding.get("run_id") != run_id or binding.get("batch_id") != batch_id
                    or child["scope_hash"] != state["scope_hash"] or digest(child["scope"]) != state["scope_hash"]
                    or child["revision"] != child["program_hash"] or digest(child["source"]) != child["revision"]
                    or child.get("parallel_link") != original_link
                    or any(origin.get(key) != state[key] for key in ("source", "inputs", "program_hash"))
                    or child["original_deadline"] > state["original_deadline"]
                    or len(frames) != 1 or frames[0] != {
                        "program": origin.get("target"), "block": "$", "pc": 0, "values": child["inputs"],
                        "grants": child["root_grants"],
                        "binding": digest([origin.get("target"), child["inputs"], child["root_grants"]]),
                    }):
                raise Fault("PARALLEL_SLOT_NOT_DISPATCHABLE", "Child record differs from a reservation checkpoint",
                            batch_id=batch_id, task_id=row["task"])
            slots.append((row, child))
        return slots

    @staticmethod
    def _reserved_task_bindings(slots):
        return [{"task_id": row["task"], "run_id": row["child"], "revision": child["revision"],
                 "target": child["parallel_origin"]["target"], "reservation_hash": digest(child)} for row, child in slots]

    def dispatch_reserved(self, state, owner, batch_id, task_ids, operation, authorize, prepare, active_limit):
        with self.transaction() as db:
            run_id = state["run_id"]
            self._require_owner(db, run_id, owner)
            current = parse(db.execute("SELECT state FROM runs WHERE id=?", (run_id,)).fetchone()["state"])
            grant = state.get("active_reserved_dispatch")
            if (grant is None or current.get("active_reserved_dispatch") != grant or grant["batch_id"] != batch_id
                    or current.get("active_parallel", {}).get("batch_id") != batch_id
                    or not set(task_ids) <= {task["task_id"] for task in grant["tasks"]}):
                raise Fault("AUTHORIZATION_DENIED", "Original continuation does not authorize these reserved slots")
            slots = self._pristine_reserved_slots(db, state, batch_id, task_ids)
            running = db.execute("""SELECT count(*) FROM parallel_tasks t JOIN runs r ON r.id=t.child
                                    WHERE t.parent=? AND t.dispatched=1 AND r.result IS NULL""", (run_id,)).fetchone()[0]
            if running + len(slots) > active_limit:
                raise Fault("PARALLEL_LIMIT_EXCEEDED", "Reserved dispatch exceeds the current Host active limit", limit="active")
            bindings = self._reserved_task_bindings(slots)
            if bindings != [task for task in grant["tasks"] if task["task_id"] in task_ids]:
                raise Fault("CONTINUATION_REVISION_MISMATCH", "Reserved slot binding differs", phase="continue")
            authorize(state, {**grant, "tasks": bindings})
            self._check_continuation_budget(state, allow_current_step=True)
            claims = []
            for row, child in slots:
                prepare(child)
                child_owner = uuid.uuid4().hex
                child["parallel_parent"]["owner"] = owner
                child["continuation_deadline"] = child["original_deadline"]
                if db.execute("UPDATE parallel_tasks SET dispatched=1 WHERE child=? AND dispatched=0",
                              (row["child"],)).rowcount != 1:
                    raise Fault("RUN_OWNERSHIP_LOST", "Reserved slot was already claimed")
                if db.execute("""UPDATE runs SET state=?,owner=?,lease=? WHERE id=? AND owner IS NULL
                                 AND lease=0 AND result IS NULL AND token IS NULL""",
                              (canonical(child), child_owner, self.lease(child), row["child"])).rowcount != 1:
                    raise Fault("RUN_OWNERSHIP_LOST", "Reserved child was already claimed")
                db.execute("INSERT INTO reserved_dispatch_claims VALUES(?,?,?,?,?,?,?)",
                           (row["child"], run_id, batch_id, grant["continuation_id"], operation, child_owner, child["revision"]))
                claims.append((child, child_owner))
            receipt = {"status": "claimed", "batch_id": batch_id, "tasks": bindings}
            # Replaying this instruction may read the receipt, but it must never submit the worker again.
            checkpoint = parse(canonical(state))
            checkpoint["frames"][-1]["pending"]["reserved_dispatch"] = receipt
            checkpoint["trace"].append({"event": "reserved_dispatch_claimed", "batch_id": batch_id,
                                        "tasks": bindings, "operation": operation})
            self._save(db, checkpoint, owner)
            return receipt, claims

    def parallel_results(self, run_id, owner, batch_id, *, settled_event=None):
        with self.transaction() as db:
            self._require_owner(db, run_id, owner)
            parent = parse(db.execute("SELECT state FROM runs WHERE id=?", (run_id,)).fetchone()["state"])
            if "parallel_parent" in parent:
                self._require_parallel_child_owner(db, parent)
            return self._parallel_results(db, parent, batch_id, settled_event=settled_event)

    def _parallel_results(self, db, parent, batch_id, *, settled_event=None):
        run_id = parent["run_id"]
        manifest = parent.get("parallel_batches", {}).get(batch_id)
        rows = db.execute("""
            SELECT t.ordinal,t.task,t.child,t.dispatched,t.finish_order,r.state,r.result,r.token IS NOT NULL AS waiting,e.sequence,e.cancellation_id
            FROM parallel_tasks t JOIN runs r ON r.id=t.child
            LEFT JOIN parallel_events e ON e.parent=t.parent AND e.batch=t.batch
                AND e.kind='task_settled' AND e.subject=t.task
            WHERE t.parent=? AND t.batch=? ORDER BY t.ordinal
        """, (run_id, batch_id)).fetchall()
        observed = [{"id": row["task"], "run_id": row["child"]} for row in rows]
        # Old two-task checkpoints predate the durable batch manifest.
        if ((manifest is None and len(rows) != 2) or (manifest is not None and observed != manifest)
                or [row["ordinal"] for row in rows] != list(range(len(rows)))):
            raise Fault("PARALLEL_STATE_INVALID", "Parallel task reservations are missing", batch_id=batch_id)
        tasks = []
        inherited = self._parallel_cancellation_scope(db, parent) if "parallel_parent" in parent else None
        for row in rows:
            if settled_event is not None and row["task"] != settled_event:
                continue
            cancellation = row["cancellation_id"] if row["sequence"] is not None else self._task_cancellation(
                db, {"run_id": run_id, "batch_id": batch_id}, row["child"])
            snapshot = db.execute("SELECT snapshot FROM parallel_event_settlements WHERE parent=? AND batch=? AND task=?",
                                  (run_id, batch_id, settled_event)).fetchone() if settled_event else None
            settlement = parse(snapshot["snapshot"]) if snapshot else {
                "result": parse(row["result"]) if row["result"] is not None else None,
                "effects": self._parallel_effects(db, row["child"]),
            }
            tasks.append({"id": row["task"], "run_id": row["child"], "dispatched": bool(row["dispatched"]),
                          "finish_order": row["finish_order"], "state": parse(row["state"]),
                          "waiting": bool(row["waiting"]), "cancellation_id": inherited[1] if inherited else cancellation,
                          **settlement})
        return tasks, self._parallel_cancellation(db, run_id, batch_id)

    @staticmethod
    def _parallel_effects(db, child_id):
        effects = db.execute("""
            SELECT e.id AS call_id,e.status,e.response IS NOT NULL AS has_receipt,
                   EXISTS(SELECT 1 FROM effect_reconciliations a WHERE a.run=e.run AND a.call_id=e.id
                          AND json_extract(a.record,'$.status')='confirmed') AS has_reconciled_usage
            FROM effects e WHERE e.run=? ORDER BY e.rowid
        """, (child_id,)).fetchall()
        return [{"call_id": effect["call_id"], "status": effect["status"], "has_receipt": bool(effect["has_receipt"]),
                 "has_reconciled_usage": bool(effect["has_reconciled_usage"])} for effect in effects]

    def enqueue_steering(self, run_id, event_id, text, authorize):
        wake = False
        with self.transaction() as db:
            run = db.execute("SELECT state,owner,token FROM runs WHERE id=?", (run_id,)).fetchone()
            if run is None:
                raise Fault("UNKNOWN_RUN", "Run does not exist", phase="steering")
            state = parse(run["state"])
            authorize(state, "enqueue")
            if run["owner"] is None and run["token"] is None:
                raise Fault("RUN_SETTLED", "Run has already settled", phase="steering")
            event = db.execute("SELECT sequence,text FROM steering_events WHERE run=? AND event_id=?",
                               (run_id, event_id)).fetchone()
            if event is not None:
                if event["text"] != text:
                    raise Fault("STEERING_EVENT_CONFLICT", "Event ID already has different text", phase="steering")
                sequence = event["sequence"]
            else:
                sequence = db.execute("SELECT COALESCE(MAX(sequence),0)+1 FROM steering_events WHERE run=?",
                                      (run_id,)).fetchone()[0]
                db.execute("INSERT INTO steering_events VALUES(?,?,?,?)", (run_id, sequence, event_id, text))
                active = state.get("active_parallel")
                if active is not None:
                    self._append_parallel_event(db, run_id, active["batch_id"], "steering", event_id)
                    wake = True
        if wake:
            self.wake_parallel()
        return {"status": "enqueued", "run_id": run_id, "event_id": event_id, "sequence": sequence}

    def read_steering(self, run_id, owner, after):
        with self.transaction() as db:
            run = db.execute("SELECT 1 FROM runs WHERE id=? AND owner=?", (run_id, owner)).fetchone()
            if run is None:
                raise Fault("RUN_OWNERSHIP_LOST", "Execution claim is no longer valid")
            events = db.execute("""
                SELECT sequence,event_id,text FROM steering_events WHERE run=? AND sequence>? ORDER BY sequence
            """, (run_id, after)).fetchall()
            return [dict(event) for event in events]

    def effect(self, state, owner, call_id, intent=None, *, from_stream=None):
        with self.transaction() as db:
            run_id = state["run_id"]
            self._require_owner(db, run_id, owner)
            row = db.execute("""
                SELECT e.status,e.response,i.intent FROM effects e LEFT JOIN effect_intents i
                ON i.run=e.run AND i.id=e.id WHERE e.run=? AND e.id=?
            """, (run_id, call_id)).fetchone()
            replay = state.get("reconciled_replay")
            if replay and (row is None or row["status"] != "confirmed"):
                raise Fault("CONTINUATION_EFFECT_UNCONFIRMED", "Replaying the original instruction can only read confirmed effects",
                            phase="continue", call_id=call_id)
            if from_stream is not None and row is None:
                cancellation = self._stream_cancellation(db, run_id, from_stream)
                if cancellation is not None:
                    return "not_dispatched", cancellation
                latest = self._last_stream_event(db, run_id, from_stream)
                terminal = parse(latest["event"]) if latest else None
                if (terminal and terminal["kind"] == "completed"
                        and terminal["response"]["finish"] == "length"):
                    return "output_limited", None
            self._save(db, state, owner)
            if row:
                if row["status"] == "pending" or replay:
                    original = parse(row["intent"]) if row["intent"] is not None else None
                    binding = require_original_binding(original, state["run_id"], call_id, "resume")
                    check_effect_binding(binding, intent.get("binding") if intent else None, call_id, "resume")
                    if original["request_hash"] != intent["request_hash"]:
                        raise Fault("REQUEST_BINDING_MISMATCH", "Current request differs from the original",
                                    phase="resume", call_id=call_id)
                    if original["kind"] != intent["kind"] or original["capability"] != intent["capability"]:
                        raise Fault("ADAPTER_BINDING_MISMATCH", "Current adapter differs from the original",
                                    phase="resume", call_id=call_id)
                if replay and replay["call_id"] == call_id:
                    _, envelope = self._confirmed_receipt(db, run_id, call_id)
                    return "reconciled", envelope["raw_response"]
                return row["status"], parse(row["response"]) if row["response"] is not None else None
            if "parallel_parent" in state:
                # Cancellation and first effect admission contend for the same write transaction.
                if not any(key in state for key in ("active_child_takeover", "active_continuation", "child_input_settlement",
                                                     "active_parallel_continuation")):
                    self._require_live_ancestors(db, state)
                self._check_parallel_cancel(db, state)
            db.execute("INSERT INTO effects VALUES(?,?,'pending',NULL)", (state["run_id"], call_id))
            if intent is not None:
                db.execute("INSERT INTO effect_intents VALUES(?,?,?)", (state["run_id"], call_id, canonical(intent)))
        return "new", None

    def confirm(self, state, owner, call_id, response):
        with self.transaction() as db:
            self._save(db, state, owner)
            db.execute("UPDATE effects SET status='confirmed',response=? WHERE run=? AND id=?",
                       (canonical(response), state["run_id"], call_id))

    @staticmethod
    def _stream_cancellation(db, run_id, call_id):
        row = db.execute("SELECT id FROM stream_cancellations WHERE run=? AND call=?", (run_id, call_id)).fetchone()
        return row["id"] if row else None

    def stream_cancellation(self, state, owner, call_id):
        with self.transaction() as db:
            self._require_owner(db, state["run_id"], owner)
            return self._stream_cancellation(db, state["run_id"], call_id)

    def request_stream_cancel(self, run_id, call_id, cancellation_id, authorize):
        with self.transaction() as db:
            run = db.execute("SELECT state,owner,lease FROM runs WHERE id=?", (run_id,)).fetchone()
            if run is None:
                raise Fault("UNKNOWN_RUN", "Run does not exist", phase="cancel")
            intent = db.execute("SELECT intent FROM effect_intents WHERE run=? AND id=?", (run_id, call_id)).fetchone()
            state = parse(run["state"])
            authorize(state, parse(intent["intent"]) if intent else None)
            saved = self._stream_cancellation(db, run_id, call_id)
            if saved != cancellation_id:
                if saved is not None or db.execute("SELECT 1 FROM stream_cancellations WHERE run=? AND id=?",
                                                   (run_id, cancellation_id)).fetchone():
                    raise Fault("STREAM_CANCEL_CONFLICT", "Cancellation ID or target conflicts with the saved request", phase="cancel")
                latest = self._last_stream_event(db, run_id, call_id)
                if (run["owner"] is None or run["lease"] <= time.time()
                        or state.get("active_stream", {}).get("call_id") != call_id
                        or (latest and parse(latest["event"])["kind"] in {"completed", "interrupted"})):
                    raise Fault("STREAM_NOT_ACTIVE", "No active stream accepts this request", phase="cancel")
                db.execute("INSERT INTO stream_cancellations VALUES(?,?,?)", (run_id, call_id, cancellation_id))
        return {"status": "cancel_requested", "run_id": run_id, "call_id": call_id, "cancellation_id": cancellation_id}

    @staticmethod
    def _last_stream_event(db, run_id, call_id):
        return db.execute("SELECT sequence,event FROM stream_events WHERE run=? AND call=? ORDER BY sequence DESC LIMIT 1",
                          (run_id, call_id)).fetchone()

    def append_stream_events(self, state, owner, call_id, events):
        with self.transaction() as db:
            run_id = state["run_id"]
            self._require_owner(db, run_id, owner)
            if "parallel_parent" in state:
                self._require_parallel_child_owner(db, state)
            latest = self._last_stream_event(db, run_id, call_id)
            if latest and parse(latest["event"])["kind"] in {"completed", "interrupted"}:
                raise Fault("STREAM_SETTLED", "Stream already has a terminal event", call_id=call_id)
            cancellation = self._stream_cancellation(db, run_id, call_id)
            if cancellation is not None:
                terminal = events[-1]
                if terminal.get("cancellation_id") != cancellation:
                    return cancellation
                # Preserve text already in the journal. A batch that lost the cancellation race is not visible.
                prior = (parse(row["event"]) for row in db.execute(
                    "SELECT event FROM stream_events WHERE run=? AND call=? ORDER BY sequence", (run_id, call_id)))
                terminal["partial_text"] = "".join(event["text"] for event in prior if event["kind"] == "text_delta")
            sequence = latest["sequence"] if latest else 0
            db.executemany("INSERT INTO stream_events VALUES(?,?,?,?)",
                           [(run_id, call_id, sequence + i, canonical(event)) for i, event in enumerate(events, 1)])
            if events[-1]["kind"] == "completed":
                response = events[-1]["response"]
                db.execute("UPDATE effects SET status='confirmed',response=? WHERE run=? AND id=?",
                           (canonical(response), run_id, call_id))
                db.execute("INSERT INTO effect_usage VALUES(?,?,?)",
                           (run_id, call_id, canonical(response["usage"]) if response.get("usage") is not None else None))
                intent_row = db.execute("SELECT intent FROM effect_intents WHERE run=? AND id=?",
                                        (run_id, call_id)).fetchone()
                intent = parse(intent_row["intent"]) if intent_row else None
                require_original_binding(intent, run_id, call_id, "stream")
                journal = [(row["sequence"], parse(row["event"])) for row in db.execute(
                    "SELECT sequence,event FROM stream_events WHERE run=? AND call=? ORDER BY sequence",
                    (run_id, call_id))]
                if [entry[0] for entry in journal] != list(range(1, len(journal) + 1)):
                    raise Fault("STREAM_JOURNAL_INVALID", "Completed stream journal has a sequence gap", call_id=call_id)
                seal = {key: intent[key] for key in ("run_id", "call_id", "source", "destination",
                                                     "request_hash", "binding_fingerprint")}
                seal.update(final=True, response_hash=digest(response), journal_hash=digest(journal),
                            event_count=len(journal), usage_hash=digest(response.get("usage")))
                db.execute("INSERT INTO stream_journal_seals VALUES(?,?,?)", (run_id, call_id, canonical(seal)))

    @staticmethod
    def _sealed_stream_receipt(db, run_id, call_id):
        row = db.execute("""SELECT e.response,i.intent,u.usage,s.seal FROM effects e
                            JOIN effect_intents i ON i.run=e.run AND i.id=e.id
                            JOIN effect_usage u ON u.run=e.run AND u.id=e.id
                            JOIN stream_journal_seals s ON s.run=e.run AND s.call=e.id
                            WHERE e.run=? AND e.id=? AND e.status='confirmed'""", (run_id, call_id)).fetchone()
        if row is None:
            raise Fault("CONTINUATION_JOURNAL_UNAVAILABLE", "Stream lacks a sealed complete journal", phase="continue")
        intent, response, seal = parse(row["intent"]), parse(row["response"]), parse(row["seal"])
        require_original_binding(intent, run_id, call_id, "continue")
        usage = parse(row["usage"]) if row["usage"] is not None else None
        journal = [(item["sequence"], parse(item["event"])) for item in db.execute(
            "SELECT sequence,event FROM stream_events WHERE run=? AND call=? ORDER BY sequence", (run_id, call_id))]
        fields = {key: intent[key] for key in ("run_id", "call_id", "source", "destination",
                                             "request_hash", "binding_fingerprint")}
        fields.update(final=True, response_hash=digest(response), journal_hash=digest(journal),
                      event_count=len(journal), usage_hash=digest(usage))
        if (intent.get("request", {}).get("stream") is not True or not journal
                or [item[0] for item in journal] != list(range(1, len(journal) + 1))
                or journal[-1][1].get("kind") != "completed" or journal[-1][1].get("response") != response
                or any(item[1].get("kind") in {"completed", "interrupted"} for item in journal[:-1])
                or response.get("usage") != usage or seal != fields):
            raise Fault("CONTINUATION_JOURNAL_MISMATCH", "Complete stream journal, receipt or usage differs", phase="continue")
        return intent, {"receipt": seal, "raw_response": response, "usage": usage, "journal": journal}

    def stream_event(self, state, owner, call_id, sequence):
        with self.transaction() as db:
            run_id = state["run_id"]
            self._require_owner(db, run_id, owner)
            if state.get("stream_journal_replay") == call_id:
                self._sealed_stream_receipt(db, run_id, call_id)
            row = db.execute("SELECT event FROM stream_events WHERE run=? AND call=? AND sequence=?",
                             (run_id, call_id, sequence)).fetchone()
            return parse(row["event"]) if row else None

    def stream_terminal(self, state, owner, call_id):
        with self.transaction() as db:
            self._require_owner(db, state["run_id"], owner)
            row = self._last_stream_event(db, state["run_id"], call_id)
            return parse(row["event"])["kind"] if row else None

    def finish(self, state, owner, result):
        wake = False
        with self.transaction() as db:
            parent = state.get("parallel_parent")
            state.pop("parallel_schedule", None)
            suspended = parent is not None and result["status"] == "awaiting_input"
            if suspended:
                try:
                    self._check_parallel_cancel(db, state)
                except ParallelCancelled as fault:
                    state.pop("wait")
                    state["trace"].append({"event": "parallel_cancelled", **fault.error})
                    for key in ("resume_token", "input_schema", "reason", "checkpoint"):
                        del result[key]
                    result.update(status="cancelled", error=fault.error)
                    suspended = False
                else:
                    state["trace"].append({"event": "awaiting_input", "checkpoint": state["checkpoint"]})
            continuation = state.get("active_continuation")
            parent_continuation = state.get("active_parallel_continuation")
            reserved_dispatch = state.get("active_reserved_dispatch")
            state.pop("reconciled_replay", None)
            self._save(db, state, owner)
            state.pop("active_reserved_dispatch", None)
            state.pop("active_parallel_continuation", None)
            state.pop("active_continuation", None)
            if not suspended:
                state.pop("child_input_settlement", None)
            child_takeover = state.pop("active_child_takeover", None)
            db.execute("UPDATE runs SET state=?,result=?,token=?,owner=NULL,lease=0 WHERE id=?",
                       (canonical(state), None if suspended else canonical(result), result.get("resume_token"), state["run_id"]))
            if suspended:
                del result["resume_token"]
                if db.execute("SELECT 1 FROM runs WHERE id=? AND json_extract(state,'$.active_parallel.batch_id')=?",
                              (parent["run_id"], parent["batch_id"])).fetchone():
                    task = db.execute("SELECT task FROM parallel_tasks WHERE child=?", (state["run_id"],)).fetchone()["task"]
                    self._append_parallel_event(db, parent["run_id"], parent["batch_id"], "task_awaiting_input",
                                                canonical({"task_id": task, "run_id": state["run_id"],
                                                           "checkpoint": state["checkpoint"], "expires": state["wait"]["expires"],
                                                           "input_schema": result["input_schema"], "reason": result["reason"]}))
            if state.get("request_id"):
                db.execute("UPDATE requests SET result=? WHERE run=? AND id=?",
                           (canonical(result), state["run_id"], state["request_id"]))
            if continuation is not None:
                db.execute("UPDATE reconciled_continuations SET result=? WHERE run=? AND id=?",
                           (canonical(result), state["run_id"], continuation["id"]))
            if parent_continuation is not None:
                db.execute("UPDATE parallel_continuations SET result=? WHERE run=? AND id=?",
                           (canonical(result), state["run_id"], parent_continuation["id"]))
            if reserved_dispatch is not None:
                db.execute("UPDATE reserved_dispatch_continuations SET result=? WHERE run=? AND id=?",
                           (canonical(result), state["run_id"], reserved_dispatch["continuation_id"]))
            if child_takeover is not None:
                updated = db.execute("""UPDATE parallel_child_takeovers SET result=?
                                        WHERE parent=? AND id=? AND child=? AND owner=? AND result IS NULL""",
                                     (canonical(result), state["parallel_parent"]["run_id"], child_takeover, state["run_id"], owner))
                if updated.rowcount != 1:
                    raise Fault("RUN_OWNERSHIP_LOST", "Child takeover result no longer belongs to this claim")
            if suspended:
                wake = True
            elif parent is not None and continuation is None:
                wake = self._finish_parallel_slot(db, parent, state["run_id"])
        if wake:
            self.wake_parallel()

    def claim(self, token, inputs, request_id, reauthorize, authorize_child_input=None, *, parent_settlement=None):
        input_hash = digest(inputs)
        with self.transaction() as db:
            row = db.execute("""
                SELECT * FROM runs WHERE token=? OR id IN
                (SELECT run FROM requests WHERE token=? AND id=?)
            """, (token, token, request_id)).fetchone()
            if not row:
                raise Fault("RESUME_CONFLICT", "Unknown or consumed checkpoint", phase="resume")
            state = parse(row["state"])
            child = "parallel_parent" in state
            if parent_settlement is not None and not child:
                raise Fault("INVALID_RESUME_REQUEST", "Parent settlement requires a child input checkpoint", phase="resume")
            reauthorize(state)
            saved = db.execute("SELECT * FROM requests WHERE run=? AND id=?",
                               (row["id"], request_id)).fetchone()
            if saved:
                if child:
                    self._authorize_child_input_replay(db, state, request_id, authorize_child_input, inputs, parent_settlement)
                if saved["token"] != token or saved["input_hash"] != input_hash:
                    raise Fault("REQUEST_CONFLICT", "Request key already has different input or checkpoint", phase="resume")
                if saved["result"]:
                    return None, parse(saved["result"])
                raise Fault("RESUME_CONFLICT", "Request is already in progress", phase="resume")
            wait = state.get("wait")
            if row["token"] != token or row["owner"] or not wait:
                raise Fault("RESUME_CONFLICT", "Checkpoint has been consumed", phase="resume")
            if child:
                if parent_settlement is None:
                    parent, task, finish_order = self._child_input_parent(db, state)
                else:
                    state["child_input_settlement"] = parent_settlement
                    parent, task = self._settled_child_input_parent(db, state, parent_settlement)
                    finish_order = None
                if finish_order is not None or authorize_child_input is None:
                    raise Fault("PARALLEL_CHILD_CONTINUATION_DENIED", "Child input checkpoint cannot be claimed", phase="resume")
                self._check_parallel_cancel(db, state)
                authorize_child_input(parent, state, task, state["checkpoint"], "resume", inputs)
            if time.time() >= wait["expires"]:
                raise Fault("RESUME_EXPIRED", "Checkpoint has expired", phase="resume")
            validate(inputs, wait["schema"], code="INVALID_RESUME_INPUT", phase="resume")
            if child:
                state["continuation_deadline"] = state["original_deadline"]
                self._check_continuation_budget(state, allow_current_step=True)
                if parent_settlement is not None:
                    self._check_takeover_effects(db, state)
                if parent_settlement is None:
                    state["parallel_schedule"] = "input"
                state.setdefault("child_input_requests", {})[request_id] = {
                    "checkpoint": state["checkpoint"], "parent_settlement": parent_settlement}
            state["request_id"] = request_id
            state.setdefault("resume_values", {})[wait["key"]] = inputs
            del state["wait"]
            owner = uuid.uuid4().hex
            db.execute("INSERT INTO requests VALUES(?,?,?,?,NULL)", (row["id"], request_id, token, input_hash))
            updated = db.execute("""UPDATE runs SET token=NULL,result=NULL,owner=?,lease=?
                                    WHERE id=? AND token=? AND owner IS NULL""",
                                 (owner, self.lease(state), row["id"], token))
            if updated.rowcount != 1:
                raise Fault("RESUME_CONFLICT", "Checkpoint has been consumed", phase="resume")
            self._save(db, state, owner)
            return owner, state

    def _authorize_child_input_replay(self, db, state, request_id, authorize, inputs, parent_settlement):
        binding = state["parallel_parent"]
        row = db.execute("""SELECT p.state,t.task FROM parallel_tasks t JOIN runs p ON p.id=t.parent
                            WHERE t.parent=? AND t.batch=? AND t.child=?""",
                         (binding["run_id"], binding["batch_id"], state["run_id"])).fetchone()
        saved = state.get("child_input_requests", {}).get(request_id)
        if row is None or authorize is None or saved is None:
            raise Fault("PARALLEL_CHILD_CONTINUATION_DENIED", "Original child input result cannot be read", phase="resume")
        checkpoint = saved["checkpoint"] if isinstance(saved, dict) else saved
        original_settlement = saved.get("parent_settlement") if isinstance(saved, dict) else None
        if original_settlement != parent_settlement:
            raise Fault("REQUEST_CONFLICT", "Request key belongs to a different parent settlement", phase="resume")
        if original_settlement is None:
            state.pop("child_input_settlement", None)
        else:
            state["child_input_settlement"] = original_settlement
        authorize(parse(row["state"]), state, row["task"], checkpoint, "replay", inputs)

    def recover(self, run_id, reauthorize, authorize_pending, authorize_reconciled=None, authorize_parallel=None,
                authorize_reserved=None):
        with self.transaction() as db:
            row = db.execute("SELECT * FROM runs WHERE id=?", (run_id,)).fetchone()
            if row is None:
                raise Fault("RUN_ACTIVE_OR_SETTLED", "Run is active, settled or missing", phase="resume")
            state = parse(row["state"])
            if "parallel_parent" in state and not state.get("active_continuation"):
                raise Fault("PARALLEL_CHILD_CONTINUATION_DENIED", "Only the parent coordinates parallel tasks", phase="resume")
            if not row["owner"] or row["lease"] > time.time():
                raise Fault("RUN_ACTIVE_OR_SETTLED", "Run is active, settled or missing", phase="resume")
            reauthorize(state)
            if continuation := state.get("active_continuation"):
                if authorize_reconciled is None:
                    raise Fault("AUTHORIZATION_DENIED", "Host rejected reconciled continuation recovery", phase="continue")
                self._check_reconciled_claim(db, state, continuation["call_id"], authorize_reconciled,
                                             check_pc="reconciled_replay" in state)
                self._check_continuation_budget(state)
            if continuation := state.get("active_parallel_continuation"):
                if authorize_parallel is None:
                    raise Fault("AUTHORIZATION_DENIED", "Host rejected parent continuation recovery", phase="continue")
                self._check_fixed_revision(db, state)
                authorize_parallel(state, continuation["batch_id"])
                self._check_continuation_budget(state)
            if authorization := state.get("active_reserved_dispatch"):
                if authorize_reserved is None:
                    raise Fault("AUTHORIZATION_DENIED", "Host rejected reserved-dispatch recovery", phase="continue")
                self._check_fixed_revision(db, state)
                authorize_reserved(state, authorization)
                self._check_continuation_budget(state, allow_current_step=True)
            return self._take_expired_run(db, state, authorize_pending)

    def _take_expired_run(self, db, state, authorize_pending):
        run_id = state["run_id"]
        active_child = db.execute("""
                WITH RECURSIVE subtree(child) AS (
                    SELECT child FROM parallel_tasks WHERE parent=?
                    UNION ALL SELECT t.child FROM parallel_tasks t JOIN subtree s ON t.parent=s.child
                ) SELECT 1 FROM subtree s JOIN runs r ON r.id=s.child
                WHERE r.owner IS NOT NULL AND r.lease>? LIMIT 1
            """, (run_id, time.time())).fetchone()
        if active_child:
            raise Fault("RUN_ACTIVE_OR_SETTLED", "A parallel child still holds an active lease", phase="resume")
        pending = db.execute("""
                SELECT e.id,i.intent FROM effects e LEFT JOIN effect_intents i
                ON i.run=e.run AND i.id=e.id WHERE e.run=? AND e.status='pending'
            """, (run_id,)).fetchall()
        for effect in pending:
            intent = parse(effect["intent"]) if effect["intent"] is not None else None
            require_original_binding(intent, run_id, effect["id"], "resume")
            authorize_pending(intent)
        owner = uuid.uuid4().hex
        db.execute("UPDATE runs SET owner=? WHERE id=?", (owner, run_id))
        self._save(db, state, owner)
        return owner, state

    def settle_abandoned_parallel_parent(self, run_id, batch_id, takeover_id, expected_owner, authorize, verify_exit, settle):
        with self.transaction() as db:
            saved = db.execute("SELECT * FROM parallel_parent_settlements WHERE run=? AND id=?",
                               (run_id, takeover_id)).fetchone()
            if saved is not None and (saved["batch"], saved["previous_owner"]) != (batch_id, expected_owner):
                raise Fault("CONTINUATION_CONFLICT", "Parent settlement ID belongs to another batch or owner", phase="continue")
            row = db.execute("SELECT * FROM runs WHERE id=?", (run_id,)).fetchone()
            if row is None:
                raise Fault("RUN_NOT_CONTINUABLE", "Original parent Run is missing", phase="continue")
            state = parse(row["state"])
            binding = {"parent_run_id": run_id, "batch_id": batch_id, "takeover_id": takeover_id,
                       "previous_owner": expected_owner, "parent_revision": state["revision"]}
            self._check_fixed_revision(db, state)
            authorize(state, binding)
            if saved is not None:
                return parse(saved["result"])
            nested = "parallel_parent" in state
            event_claim = supervised_event_batch(state) == batch_id
            if (row["owner"] != expected_owner or row["result"] is not None or row["token"] is not None
                    or not state["frames"] or not state.get("parallel_batches", {}).get(batch_id)
                    or (nested and state.get("parallel_schedule") != "join"
                        and not (state.get("active_parallel", {}).get("batch_id") == batch_id
                                 and state.get("parallel_schedule") in {None, "event"}))
                    or any(key in state for key in ("active_stream", "active_continuation"))
                    or ("active_parallel_continuation" in state and not event_claim)
                    or (nested and "active_reserved_dispatch" in state and not event_claim)):
                raise Fault("RUN_NOT_CONTINUABLE", "Parent is not the original live batch owner", phase="continue")
            ancestor = None
            if nested:
                ancestor = (self._require_parallel_child_owner(db, state) if event_claim
                            else self._check_settled_ancestor_slot(db, state, None))
            tasks, cancellation = self._parallel_results(db, state, batch_id)
            if receipt := state["frames"][-1].get("pending", {}).get("reserved_dispatch"):
                operation = state["frames"][-1]["pending"]["id"]
                selected = {(task["task_id"], task["run_id"], task["revision"]) for task in receipt["tasks"]}
                claims = db.execute("""SELECT t.task,c.child,c.revision FROM reserved_dispatch_claims c
                                       JOIN parallel_tasks t ON t.child=c.child
                                       WHERE c.parent=? AND c.batch=? AND c.operation=? AND c.continuation=?""",
                                    (run_id, batch_id, operation,
                                     state.get("active_reserved_dispatch", {}).get("continuation_id"))).fetchall()
                if not selected or selected != {tuple(claim) for claim in claims}:
                    raise Fault("CONTINUATION_PC_MISMATCH", "Original dispatch receipt does not match its claims", phase="continue")
            child_writers = db.execute("""WITH RECURSIVE subtree(child, parent, batch, ordinal) AS (
                                              SELECT child,parent,batch,ordinal FROM parallel_tasks WHERE parent=?
                                              UNION ALL SELECT t.child,t.parent,t.batch,t.ordinal
                                              FROM parallel_tasks t JOIN subtree s ON t.parent=s.child
                                          ) SELECT r.id AS child_run_id,r.owner FROM subtree s JOIN runs r ON r.id=s.child
                                          WHERE r.owner IS NOT NULL ORDER BY s.parent,s.batch,s.ordinal""",
                                       (run_id,)).fetchall()
            if verify_exit({**binding, "child_writers": [dict(writer) for writer in child_writers]}) is not True:
                raise Fault("PARENT_EXIT_UNVERIFIED", "Host could not verify that all original local writers exited", phase="continue")
            authorize(state, binding)
            result = settle(state, batch_id, tasks, cancellation)
            owner = uuid.uuid4().hex
            changed = db.execute("""UPDATE runs SET owner=? WHERE id=? AND owner=? AND result IS NULL AND token IS NULL""",
                                 (owner, run_id, expected_owner))
            if changed.rowcount != 1:
                raise Fault("RUN_OWNERSHIP_LOST", "Parent settlement claim was lost", phase="continue")
            state["trace"].append({"event": "parallel_parent_settlement", "takeover_id": takeover_id, "batch_id": batch_id})
            encoded = canonical(result)
            if continuation := state.pop("active_parallel_continuation", None):
                if db.execute("""UPDATE parallel_continuations SET result=? WHERE run=? AND id=? AND batch=?
                                 AND result IS NULL""", (encoded, run_id, continuation["id"], batch_id)).rowcount != 1:
                    raise Fault("PARALLEL_STATE_INVALID", "Original batch continuation history is missing", phase="continue")
            reserved = state.pop("active_reserved_dispatch", None)
            if reserved is not None:
                updated = db.execute("""UPDATE reserved_dispatch_continuations SET result=?
                                        WHERE run=? AND id=? AND result IS NULL""",
                                     (encoded, run_id, reserved["continuation_id"]))
                if updated.rowcount != 1:
                    raise Fault("PARALLEL_STATE_INVALID", "Original reserved dispatch history is missing", phase="continue")
            if nested:
                state["nested_parent_settlement"] = {"id": takeover_id, "previous_owner": expected_owner, "ancestor": ancestor}
                state.pop("parallel_schedule", None)
            elif request_id := state.get("request_id"):
                db.execute("UPDATE requests SET result=? WHERE run=? AND id=?", (encoded, run_id, request_id))
            db.execute("INSERT INTO parallel_parent_settlements VALUES(?,?,?,?,?,?)",
                       (run_id, takeover_id, batch_id, expected_owner, owner, encoded))
            db.execute("UPDATE runs SET state=?,result=?,owner=NULL,lease=0 WHERE id=? AND owner=?",
                       (canonical(state), None if nested else encoded, run_id, owner))
            return result

    def claim_child_takeover(self, parent_id, batch_id, task_id, takeover_id, expected_owner, authorize, verify_exit):
        with self.transaction() as db:
            saved = db.execute("SELECT * FROM parallel_child_takeovers WHERE parent=? AND id=?",
                               (parent_id, takeover_id)).fetchone()
            if saved is not None and (saved["batch"], saved["task"], saved["previous_owner"]) != (batch_id, task_id, expected_owner):
                raise Fault("CONTINUATION_CONFLICT", "Takeover ID belongs to another child or owner", phase="continue")
            parent = db.execute("SELECT * FROM runs WHERE id=?", (parent_id,)).fetchone()
            child = db.execute("""SELECT r.*,t.ordinal,t.dispatched,t.finish_order
                                  FROM parallel_tasks t JOIN runs r ON r.id=t.child
                                  WHERE t.parent=? AND t.batch=? AND t.task=?""", (parent_id, batch_id, task_id)).fetchone()
            if parent is None or child is None:
                raise Fault("RUN_NOT_CONTINUABLE", "Original parent or slot is missing", phase="continue")
            parent_state, state = parse(parent["state"]), parse(child["state"])
            binding = {"parent_run_id": parent_id, "batch_id": batch_id, "task_id": task_id,
                       "child_run_id": child["id"], "takeover_id": takeover_id, "previous_owner": expected_owner,
                       "parent_revision": parent_state["revision"], "child_revision": state["revision"]}
            request_id = state.get("request_id")
            input_claim = state.get("child_input_requests", {}).get(request_id)
            self._check_fixed_revision(db, parent_state)
            self._check_fixed_revision(db, state)
            authorize(parent_state, state, binding)
            if saved is not None:
                if saved["child"] != child["id"]:
                    raise Fault("CONTINUATION_CONFLICT", "Takeover ID belongs to another child", phase="continue")
                if saved["result"] is not None:
                    return None, parse(saved["result"])
                if child["owner"] != saved["owner"]:
                    raise Fault("CONTINUATION_SUPERSEDED", "A different claim now owns this child", phase="continue")
                raise Fault("CONTINUATION_IN_PROGRESS", "Child takeover already holds a claim", phase="continue")

            nested = "parallel_parent" in parent_state
            previous = parse(self._check_nested_settlement(db, parent_state)) if nested else (
                parse(parent["result"]) if parent["result"] is not None else {})
            failure = previous.get("error", {})
            checkpoint = parent_state.get("parallel_join_checkpoint", {})
            manifest = parent_state.get("parallel_batches", {}).get(batch_id, [])
            ordinal = child["ordinal"]
            waiting_slot = child["id"] in checkpoint.get("waiting_children", [])
            if (parent["owner"] is not None or parent["token"] is not None or previous.get("status") != "failed"
                    or failure.get("code") != "PARALLEL_INCOMPLETE"
                    or failure.get("batch_id") != batch_id or checkpoint.get("batch_id") != batch_id
                    or not (checkpoint.get("operation") or checkpoint.get("event_checkpoint"))
                    or not self._matches_parallel_checkpoint(parent_state, checkpoint)
                    or (not waiting_slot and child["id"] not in checkpoint.get("takeover_children", [])) or not child["dispatched"]
                    or child["finish_order"] is not None or child["result"] is not None or child["token"] is not None
                    or child["owner"] != expected_owner or not state["frames"] or ordinal >= len(manifest)
                    or manifest[ordinal] != {"id": task_id, "run_id": child["id"]}
                    or child["id"] != state["run_id"] or child["id"] != digest([batch_id, ordinal])
                    or state.get("parallel_parent", {}).get("run_id") != parent_id
                    or state.get("parallel_parent", {}).get("batch_id") != batch_id):
                raise Fault("RUN_NOT_CONTINUABLE", "Child is not an eligible unsettled original slot", phase="continue")
            self._check_independent_child(db, state, previously_settled=False, nested_takeover=nested)
            if waiting_slot or isinstance(input_claim, dict) or state.get("parallel_schedule") == "input":
                self._check_accepted_child_input(db, state, input_claim, waiting_slot=waiting_slot)
            if claim := db.execute("SELECT * FROM reserved_dispatch_claims WHERE child=?", (child["id"],)).fetchone():
                original = db.execute("""SELECT authorization FROM reserved_dispatch_continuations WHERE run=? AND id=?""",
                                      (parent_id, claim["continuation"])).fetchone()
                grant = parse(original["authorization"]) if original is not None else {}
                if (claim["parent"] != parent_id or claim["batch"] != batch_id or claim["revision"] != state["revision"]
                        or grant.get("parent_revision") != parent_state["revision"] or grant.get("batch_id") != batch_id
                        or not any(task["task_id"] == task_id and task["run_id"] == child["id"]
                                   for task in grant.get("tasks", []))):
                    raise Fault("CONTINUATION_REVISION_MISMATCH", "Original reserved dispatch binding differs", phase="continue")
            if "original_deadline" not in state:
                raise Fault("CONTINUATION_DEADLINE_UNKNOWN", "Original child lacks a persisted deadline", phase="continue")
            if verify_exit(copy.deepcopy(binding)) is not True:
                raise Fault("CHILD_EXIT_UNVERIFIED", "Host could not verify the original execution domain exited", phase="continue")
            authorize(parent_state, state, binding)
            state["continuation_deadline"] = state["original_deadline"]
            self._check_continuation_budget(state, allow_current_step=bool(state["frames"][-1].get("pending")))
            self._check_takeover_effects(db, state)
            checkpoint_hash = digest(state["frames"])
            if state.get("parallel_schedule") == "input":
                del state["parallel_schedule"]
            state["active_child_takeover"] = takeover_id
            state["trace"].append({"event": "parallel_child_takeover", "takeover_id": takeover_id, "batch_id": batch_id,
                                   "task_id": task_id, "checkpoint_hash": checkpoint_hash})
            owner = uuid.uuid4().hex
            if db.execute("""UPDATE runs SET owner=? WHERE id=? AND owner=? AND result IS NULL AND token IS NULL""",
                          (owner, child["id"], expected_owner)).rowcount != 1:
                raise Fault("RUN_OWNERSHIP_LOST", "Child takeover claim was lost", phase="continue")
            db.execute("INSERT INTO parallel_child_takeovers VALUES(?,?,?,?,?,?,?,?,NULL)",
                       (parent_id, takeover_id, batch_id, task_id, child["id"], expected_owner, owner, checkpoint_hash))
            self._save(db, state, owner)
            return owner, state

    def _check_accepted_child_input(self, db, state, input_claim, *, waiting_slot):
        row = db.execute("SELECT input_hash,result FROM requests WHERE run=? AND id=?",
                         (state["run_id"], state.get("request_id"))).fetchone()
        values = state.get("resume_values", {})
        operation = state["frames"][-1].get("pending", {}).get("id")
        settlement = state.get("child_input_settlement")
        if (row is None or row["result"] is not None or not isinstance(input_claim, dict) or state.get("wait") is not None
                or input_claim["checkpoint"] != state.get("checkpoint")
                or input_claim.get("parent_settlement") != settlement
                or (values and (set(values) != {operation} or digest(values[operation]) != row["input_hash"]))
                or (state.get("parallel_schedule") == "input" and not values)
                or waiting_slot != (settlement is not None)):
            raise Fault("RUN_NOT_CONTINUABLE", "Child has no matching accepted input checkpoint", phase="continue")
        if waiting_slot:
            self._settled_child_input_parent(db, state, settlement)

    @staticmethod
    def _check_confirmed_effects(db, run_id):
        unknown = db.execute("""SELECT id FROM effects WHERE run=? AND (status!='confirmed' OR response IS NULL)
                                UNION ALL SELECT i.id FROM effect_intents i LEFT JOIN effects e ON e.run=i.run AND e.id=i.id
                                WHERE i.run=? AND e.id IS NULL LIMIT 1""", (run_id, run_id)).fetchone()
        if unknown is not None:
            raise Fault("EFFECT_UNCERTAIN", "Child still has an unconfirmed external effect", phase="continue", call_id=unknown["id"])

    def _check_takeover_effects(self, db, state):
        run_id = state["run_id"]
        # A local exit does not establish the outcome of a registered external effect.
        self._check_confirmed_effects(db, run_id)
        for frame in state["frames"]:
            for call_id in frame.get("pending", {}).get("effects", []):
                if db.execute("SELECT 1 FROM effects WHERE run=? AND id=?", (run_id, call_id)).fetchone() is None:
                    raise Fault("EFFECT_UNCERTAIN", "Child checkpoint has an effect without a receipt", phase="continue", call_id=call_id)
        if stream := state.get("active_stream"):
            self._sealed_stream_receipt(db, run_id, stream["call_id"])
            state["stream_journal_replay"] = stream["call_id"]

    def claim_reconciled(self, run_id, call_id, continuation_id, authorize, authorize_pending):
        with self.transaction() as db:
            row = db.execute("SELECT * FROM runs WHERE id=?", (run_id,)).fetchone()
            if row is None:
                raise Fault("RUN_NOT_CONTINUABLE", "No matching Run", phase="continue")
            state = parse(row["state"])
            saved = db.execute("SELECT call,result FROM reconciled_continuations WHERE run=? AND id=?",
                               (run_id, continuation_id)).fetchone()
            if saved is not None:
                if saved["call"] != call_id:
                    raise Fault("CONTINUATION_CONFLICT", "Continuation ID belongs to another effect", phase="continue")
                self._check_reconciled_claim(db, state, call_id, authorize,
                                             check_pc="reconciled_replay" in state and saved["result"] is None)
                if saved["result"] is not None:
                    return None, parse(saved["result"])
                if (state.get("active_continuation") != {"id": continuation_id, "call_id": call_id}
                        or row["owner"] is None or row["lease"] > time.time()):
                    raise Fault("CONTINUATION_IN_PROGRESS", "Continuation already holds an execution claim", phase="continue")
                self._check_continuation_budget(state)
                return self._take_expired_run(db, state, authorize_pending)

            previous = parse(row["result"]) if row["result"] is not None else {}
            frames = state["frames"]
            pending = frames[-1].get("pending", {}) if frames else {}
            stream_checkpoint = state.get("settled_stream")
            sealed_stream = (isinstance(stream_checkpoint, dict)
                             and stream_checkpoint.get("stream", {}).get("call_id") == call_id
                             and stream_checkpoint.get("operation") == pending.get("id"))
            if (row["owner"] is not None or row["token"] is not None or previous.get("status") != "failed"
                    or previous.get("error", {}).get("code") != "EFFECT_UNCERTAIN"
                    or previous["error"].get("call_id") != call_id
                    or (not sealed_stream and pending.get("effects", [])[-1:] != [call_id])
                    or any(key in state for key in ("active_parallel", "active_stream"))
                    or "batch_id" in pending or db.execute(
                        "SELECT 1 FROM reconciled_continuations WHERE run=? AND call=?", (run_id, call_id)).fetchone()):
                raise Fault("RUN_NOT_CONTINUABLE", "Run is not settled at the requested uncertain effect", phase="continue")
            if "parallel_parent" in state:
                self._check_independent_child(db, state)
            self._check_reconciled_claim(db, state, call_id, authorize, check_pc=True)
            if "original_deadline" not in state:
                raise Fault("CONTINUATION_DEADLINE_UNKNOWN", "Original Run lacks a persisted deadline", phase="continue")
            state["continuation_deadline"] = state["original_deadline"]
            self._check_continuation_budget(state)
            state.pop("request_id", None)
            state["active_continuation"] = {"id": continuation_id, "call_id": call_id}
            state["reconciled_replay"] = {"operation": pending["id"], "call_id": call_id}
            if sealed_stream:
                state["active_stream"] = parse(canonical(stream_checkpoint["stream"]))
                state["stream_journal_replay"] = call_id
            state["trace"].append({"event": "reconciled_continuation", "continuation_id": continuation_id,
                                   "call_id": call_id, "operation": pending["id"]})
            owner = uuid.uuid4().hex
            db.execute("INSERT INTO reconciled_continuations VALUES(?,?,?,?,NULL,?)",
                       (run_id, continuation_id, call_id, row["result"], time.time()))
            db.execute("UPDATE runs SET result=NULL,owner=? WHERE id=?", (owner, run_id))
            self._save(db, state, owner)
            return owner, state

    def claim_parallel_continuation(self, run_id, batch_id, continuation_id, authorize, authorize_pending):
        with self.transaction() as db:
            row = db.execute("SELECT * FROM runs WHERE id=?", (run_id,)).fetchone()
            if row is None:
                raise Fault("RUN_NOT_CONTINUABLE", "No matching Run", phase="continue")
            state = parse(row["state"])
            saved = db.execute("SELECT batch,result FROM parallel_continuations WHERE run=? AND id=?",
                               (run_id, continuation_id)).fetchone()
            if saved is not None and saved["batch"] != batch_id:
                raise Fault("CONTINUATION_CONFLICT", "Continuation ID belongs to another batch", phase="continue")
            self._check_fixed_revision(db, state)
            authorize(state, batch_id)
            claim = {"id": continuation_id, "batch_id": batch_id}
            nested = "parallel_parent" in state
            if saved is not None:
                if saved["result"] is not None:
                    return None, parse(saved["result"])
                if (nested or state.get("active_parallel_continuation") != claim
                        or row["owner"] is None or row["lease"] > time.time()):
                    raise Fault("CONTINUATION_IN_PROGRESS", "Parent continuation already holds a claim", phase="continue")
                self._check_continuation_budget(state)
                return self._take_expired_run(db, state, authorize_pending)
            checkpoint = state.get("parallel_join_checkpoint", {})
            reconciled = checkpoint.get("children", [])
            taken_over = checkpoint.get("takeover_children", [])
            resumed_waits = checkpoint.get("waiting_children", [])
            previous_result = self._check_nested_settlement(db, state) if nested else row["result"]
            settlement = state["nested_parent_settlement"]["id"] if nested and "event_checkpoint" in checkpoint else None
            previous = parse(previous_result) if previous_result is not None else {}
            if (row["owner"] is not None or row["token"] is not None or previous.get("status") != "failed"
                    or previous.get("error", {}).get("code") != "PARALLEL_INCOMPLETE"
                    or previous.get("error", {}).get("batch_id") != batch_id
                    or checkpoint.get("batch_id") != batch_id or not self._matches_parallel_checkpoint(state, checkpoint)
                    or (not reconciled and not taken_over and checkpoint.get("supervised") is not True)
                    or (reconciled and state.get("parallel_settlement", {}).get(batch_id) != "reconcile_uncertain")
                    or db.execute("SELECT 1 FROM parallel_continuations WHERE run=? AND batch=? AND settlement IS ?",
                                  (run_id, batch_id, settlement)).fetchone()):
                    raise Fault("RUN_NOT_CONTINUABLE", "Parent has no eligible settled batch checkpoint", phase="continue")
            if db.execute("""SELECT 1 FROM parallel_tasks t JOIN runs r ON r.id=t.child
                             WHERE t.parent=? AND t.batch=? AND (r.result IS NULL OR r.owner IS NOT NULL)""",
                          (run_id, batch_id)).fetchone():
                raise Fault("PARALLEL_INCOMPLETE", "Original batch still has unsettled slots", batch_id=batch_id, phase="continue")
            if "event_checkpoint" in checkpoint:
                for child in db.execute("SELECT child FROM parallel_tasks WHERE parent=? AND batch=?", (run_id, batch_id)):
                    self._check_confirmed_effects(db, child["child"])
            for child_id in reconciled + taken_over + resumed_waits:
                child = db.execute("""SELECT r.result,r.state,
                                        EXISTS (SELECT 1 FROM reconciled_continuations c
                                                WHERE c.run=r.id AND c.result=r.result) AS reconciled,
                                        EXISTS (SELECT 1 FROM parallel_child_takeovers c
                                                WHERE c.parent=t.parent AND c.batch=t.batch AND c.task=t.task
                                                  AND c.child=r.id AND c.result=r.result) AS takeover,
                                        EXISTS (SELECT 1 FROM requests q JOIN json_each(r.state,'$.child_input_requests') AS a ON a.key=q.id
                                                JOIN parallel_parent_settlements s ON s.run=t.parent AND s.batch=t.batch
                                                WHERE q.run=r.id AND q.result=r.result AND s.result=?
                                                  AND s.id=json_extract(a.value,'$.parent_settlement.id')
                                                  AND s.previous_owner=json_extract(a.value,'$.parent_settlement.previous_owner')) AS resumed_wait
                                      FROM parallel_tasks t JOIN runs r ON r.id=t.child
                                      WHERE t.parent=? AND t.batch=? AND t.child=?""",
                           (previous_result, run_id, batch_id, child_id)).fetchone()
                receipt = "reconciled" if child_id in reconciled else "takeover" if child_id in taken_over else "resumed_wait"
                if (child is None or parse(child["result"])["status"] != "completed"
                        or not (child[receipt] or (receipt == "takeover" and
                                self._nested_continuation_receipt(db, child, run_id, batch_id, child_id, previous_result)))):
                    raise Fault("PARALLEL_INCOMPLETE", "Original child continuation has not completed", batch_id=batch_id,
                                child_run_id=child_id, phase="continue")
            state["continuation_deadline"] = state["original_deadline"]
            self._check_continuation_budget(state, allow_current_step=bool(taken_over) or checkpoint.get("supervised") is True)
            if "sequence" in checkpoint:
                active = state["active_parallel"]
                active["cursor"] = checkpoint["sequence"]
                self._append_parallel_event(db, run_id, batch_id, "all_reconciled",
                                            str(len(state["parallel_batches"][batch_id])))
            del state["parallel_join_checkpoint"]
            if nested:
                self._check_takeover_effects(db, state)
            else:
                state.pop("request_id", None)
            state["active_parallel_continuation"] = claim
            state["trace"].append({"event": "parallel_continuation", "continuation_id": continuation_id,
                                   "batch_id": batch_id, "child_run_ids": reconciled + taken_over + resumed_waits})
            owner = uuid.uuid4().hex
            if nested:
                claim["owner"] = owner
                if "event_checkpoint" in checkpoint:
                    claim["event_batch"] = batch_id
                    claim["settlement"] = settlement
            db.execute("INSERT INTO parallel_continuations VALUES(?,?,?,?,NULL,?,?)",
                       (run_id, continuation_id, batch_id, previous_result, time.time(), settlement))
            claimed = db.execute("UPDATE runs SET result=NULL,owner=? WHERE id=? AND owner IS NULL", (owner, run_id))
            if claimed.rowcount != 1:
                raise Fault("RUN_OWNERSHIP_LOST", "Original Join claim was lost", phase="continue")
            self._save(db, state, owner)
            return owner, state

    @staticmethod
    def _nested_continuation_receipt(db, child, parent_id, batch_id, child_id, parent_result):
        state = parse(child["state"])
        binding = state.get("nested_parent_settlement")
        parent = state.get("parallel_parent", {})
        if binding is None or (parent.get("run_id"), parent.get("batch_id")) != (parent_id, batch_id):
            return False
        return db.execute("""SELECT 1 FROM parallel_parent_settlements outer_fact
                             JOIN parallel_parent_settlements inner_fact ON inner_fact.run=? AND inner_fact.id=?
                             WHERE outer_fact.run=? AND outer_fact.batch=? AND outer_fact.id=?
                             AND outer_fact.previous_owner=? AND outer_fact.result=? AND inner_fact.previous_owner=?
                             AND (EXISTS(SELECT 1 FROM parallel_continuations c WHERE c.run=inner_fact.run
                                 AND c.batch=inner_fact.batch AND (c.settlement=inner_fact.id OR c.settlement IS NULL)
                                 AND c.previous_result=inner_fact.result AND c.result=?)
                             OR EXISTS(SELECT 1 FROM reserved_dispatch_continuations c WHERE c.run=inner_fact.run
                                 AND json_extract(c.authorization,'$.settlement')=inner_fact.id
                                 AND json_extract(c.authorization,'$.batch_id')=inner_fact.batch
                                 AND json_extract(c.authorization,'$.previous_owner')=inner_fact.previous_owner
                                 AND c.previous_result=inner_fact.result AND c.result=?))""",
                          (child_id, binding["id"], parent_id, batch_id, binding["ancestor"], parent["owner"],
                           parent_result, binding["previous_owner"], child["result"], child["result"])).fetchone() is not None

    def claim_reserved_dispatch(self, run_id, batch_id, task_ids, continuation_id, authorize, authorize_pending):
        with self.transaction() as db:
            row = db.execute("SELECT * FROM runs WHERE id=?", (run_id,)).fetchone()
            if row is None:
                raise Fault("RUN_NOT_CONTINUABLE", "No matching parent Run", phase="continue")
            state = parse(row["state"])
            saved = db.execute("SELECT authorization,result FROM reserved_dispatch_continuations WHERE run=? AND id=?",
                               (run_id, continuation_id)).fetchone()
            self._check_fixed_revision(db, state)
            if saved is not None:
                authorization = parse(saved["authorization"])
                if authorization["batch_id"] != batch_id or sorted(task["task_id"] for task in authorization["tasks"]) != task_ids:
                    raise Fault("CONTINUATION_CONFLICT", "Continuation ID belongs to another batch or task set", phase="continue")
                authorize(state, authorization)
                if saved["result"] is not None:
                    return None, parse(saved["result"])
                if ("settlement" in authorization or state.get("active_reserved_dispatch") != authorization or row["owner"] is None
                        or row["lease"] > time.time()):
                    raise Fault("CONTINUATION_IN_PROGRESS", "Reserved-dispatch continuation already holds a claim",
                                phase="continue")
                self._check_continuation_budget(state, allow_current_step=True)
                return self._take_expired_run(db, state, authorize_pending)
            nested = "parallel_parent" in state
            previous_result = self._check_nested_settlement(db, state) if nested else row["result"]
            previous = parse(previous_result) if previous_result is not None else {}
            pending = state["frames"][-1].get("pending", {}) if state["frames"] else {}
            reported = {task["id"] for task in previous.get("error", {}).get("not_dispatched_tasks", [])}
            if (row["owner"] is not None or row["token"] is not None or previous.get("status") != "failed"
                    or previous.get("error", {}).get("code") != "PARALLEL_INCOMPLETE"
                    or previous["error"].get("batch_id") != batch_id
                    or state.get("active_parallel", {}).get("batch_id") != batch_id
                    or (nested and "event_checkpoint" not in state.get("parallel_join_checkpoint", {}))
                    or not pending.get("id") or not set(task_ids) <= reported):
                raise Fault("RUN_NOT_CONTINUABLE", "Parent has no eligible reserved-dispatch checkpoint", phase="continue")
            slots = self._pristine_reserved_slots(db, state, batch_id, task_ids)
            if nested:
                others = db.execute("""SELECT t.task,t.child,r.result,r.owner FROM parallel_tasks t
                                       JOIN runs r ON r.id=t.child WHERE t.parent=? AND t.batch=?""",
                                    (run_id, batch_id)).fetchall()
                for slot in others:
                    if slot["task"] in task_ids:
                        continue
                    if slot["owner"] is not None or slot["result"] is None:
                        raise Fault("PARALLEL_INCOMPLETE", "Other slots need an independent outcome before dispatch",
                                    batch_id=batch_id, phase="continue")
                    self._check_confirmed_effects(db, slot["child"])
            authorization = {"parent_run_id": run_id, "batch_id": batch_id, "parent_revision": state["revision"],
                             "continuation_id": continuation_id, "tasks": self._reserved_task_bindings(slots)}
            owner = uuid.uuid4().hex
            if nested:
                settlement = state["nested_parent_settlement"]
                authorization.update(settlement=settlement["id"], previous_owner=settlement["previous_owner"], owner=owner)
            authorize(state, authorization, check_pc=True)
            state["continuation_deadline"] = state["original_deadline"]
            self._check_continuation_budget(state, allow_current_step=True)
            state["active_reserved_dispatch"] = authorization
            state.pop("request_id", None)
            state["trace"].append({"event": "reserved_dispatch_continuation", **authorization})
            if not nested:
                self._append_parallel_event(db, run_id, batch_id, "reserved_dispatch_available", continuation_id)
            db.execute("INSERT INTO reserved_dispatch_continuations VALUES(?,?,?,?,NULL,?)",
                       (run_id, continuation_id, canonical(authorization), previous_result, time.time()))
            if db.execute("""UPDATE runs SET result=NULL,owner=? WHERE id=? AND result IS ?
                             AND owner IS NULL AND token IS NULL""", (owner, run_id, row["result"])).rowcount != 1:
                raise Fault("RUN_OWNERSHIP_LOST", "Parent reserved-dispatch claim was lost", phase="continue")
            if nested:
                del state["parallel_join_checkpoint"]
            self._save(db, state, owner)
            return owner, state

    @staticmethod
    def _check_continuation_budget(state, *, allow_current_step=False):
        reserved = state.get("parallel_reserved", {})
        steps = state["steps"] + reserved.get("steps", 0)
        if (state["continuation_deadline"] <= time.time() or state["elapsed"] >= state["limits"]["seconds"]
                or state["calls"] + reserved.get("calls", 0) > state["limits"]["calls"]
                or steps > state["limits"]["steps"] or (steps == state["limits"]["steps"] and not allow_current_step)):
            raise Fault("BUDGET_EXHAUSTED", "Original Run has no remaining continuation budget", phase="continue")

    def _check_reconciled_claim(self, db, state, call_id, authorize, *, check_pc=False):
        self._check_fixed_revision(db, state)
        intent_row = db.execute("SELECT intent FROM effect_intents WHERE run=? AND id=?",
                                (state["run_id"], call_id)).fetchone()
        original = parse(intent_row["intent"]) if intent_row else {}
        reader = self._sealed_stream_receipt if original.get("request", {}).get("stream") is True else self._confirmed_receipt
        intent, envelope = reader(db, state["run_id"], call_id)
        authorize(state, intent, envelope, check_pc=check_pc)

    @staticmethod
    def _check_fixed_revision(db, state):
        if state["revision"] != state["program_hash"]:
            row = db.execute("SELECT record,status FROM versions WHERE name=? AND revision=?",
                             (state["source"]["name"], state["revision"])).fetchone()
            if row is None or row["status"] != "published" or parse(row["record"])["hash"] != state["program_hash"]:
                raise Fault("CONTINUATION_REVISION_MISMATCH", "Fixed Procedure revision is unavailable", phase="continue")

    @staticmethod
    def _confirmed_receipt(db, run_id, call_id):
        row = db.execute("""
            SELECT e.response,i.intent,u.usage FROM effects e
            JOIN effect_intents i ON i.run=e.run AND i.id=e.id
            JOIN effect_usage u ON u.run=e.run AND u.id=e.id
            WHERE e.run=? AND e.id=? AND e.status='confirmed'
        """, (run_id, call_id)).fetchone()
        if row is None or row["response"] is None:
            raise Fault("CONTINUATION_RECEIPT_UNAVAILABLE", "Effect has no confirmed reconciliation receipt", phase="continue")
        intent, envelope = parse(row["intent"]), parse(row["response"])
        require_original_binding(intent, run_id, call_id, "continue")
        receipt = envelope.get("receipt") if isinstance(envelope, dict) else None
        if not isinstance(receipt, dict):
            raise Fault("CONTINUATION_RECEIPT_MISMATCH", "Stored receipt is not a verified envelope", phase="continue")
        fields = {key: intent[key] for key in ("run_id", "call_id", "source", "destination", "request_hash", "binding_fingerprint")}
        fields["response_hash"] = digest(envelope.get("raw_response"))
        usage = parse(row["usage"]) if row["usage"] is not None else None
        audits = db.execute("SELECT record FROM effect_reconciliations WHERE run=? AND call_id=?", (run_id, call_id))
        if ("raw_response" not in envelope or receipt.get("final") is not True or envelope.get("usage") != usage
                or any(receipt.get(key) != value for key, value in fields.items())
                or not any(audit.get("status") == "confirmed" and audit.get("receipt") == receipt and audit.get("usage") == usage
                           for audit in (parse(row["record"]) for row in audits))):
            raise Fault("CONTINUATION_RECEIPT_MISMATCH", "Stored receipt, response or usage differs", phase="continue")
        return intent, envelope

    def reconciliation_target(self, run_id, call_id, verification_id):
        with self.transaction() as db:
            return self._reconciliation_target(db, run_id, call_id, verification_id)

    def _reconciliation_target(self, db, run_id, call_id, verification_id):
        row = db.execute("SELECT state,result,owner,token FROM runs WHERE id=?", (run_id,)).fetchone()
        if row is None:
            return None
        state = parse(row["state"])
        result = parse(row["result"]) if row["result"] is not None else {}
        error = result.get("error", {})
        frames = state["frames"]
        pending = frames[-1].get("pending", {}).get("effects", []) if frames else []
        # The last effect of the failed instruction identifies the operation in
        # flight even if its adapter raised a diagnostic without a call_id.
        eligible = (row["owner"] is None and row["token"] is None and result.get("status") == "failed"
                    and pending[-1:] == [call_id] and error.get("call_id", call_id) == call_id)
        effect = db.execute("""
            SELECT e.status,e.response,i.intent FROM effects e LEFT JOIN effect_intents i
            ON i.run=e.run AND i.id=e.id WHERE e.run=? AND e.id=?
        """, (run_id, call_id)).fetchone()
        audit = db.execute("SELECT call_id,record FROM effect_reconciliations WHERE run=? AND verification_id=?",
                           (run_id, verification_id)).fetchone()
        reconciled = db.execute("""SELECT 1 FROM effect_reconciliations WHERE run=? AND call_id=?
                                    AND json_extract(record,'$.status')='confirmed'""", (run_id, call_id)).fetchone()
        return {
            "state": state, "eligible": eligible and effect is not None and (
                effect["status"] == "pending" or (effect["status"] == "confirmed" and reconciled is not None)),
            "effect_status": effect["status"] if effect else None,
            "intent": parse(effect["intent"]) if effect and effect["intent"] is not None else None,
            "response": parse(effect["response"]) if effect and effect["response"] is not None else None,
            "audit_call_id": audit["call_id"] if audit else None,
            "audit": parse(audit["record"]) if audit else None,
        }

    def commit_reconciliation(self, run_id, call_id, verification_id, intent, authorize, result, response=None):
        with self.transaction() as db:
            target = self._reconciliation_target(db, run_id, call_id, verification_id)
            if target is None or not target["eligible"] or target["intent"] != intent:
                raise Fault("RECONCILIATION_TARGET_CHANGED", "The settled effect binding changed", phase="reconcile")
            authorize(target["state"], target["intent"])
            saved = target["audit"]
            if saved is not None and target["audit_call_id"] != call_id:
                raise Fault("VERIFICATION_ID_CONFLICT", "Verification ID belongs to another effect", phase="reconcile")
            if (response is not None and target["effect_status"] == "confirmed"
                    and canonical(target["response"]) != canonical(response)):
                if saved is not None:
                    raise Fault("RECEIPT_CONFLICT", "A different receipt is already confirmed", phase="reconcile")
                result = {**result, "status": "conflict", "reason": "RECEIPT_CONFLICT", "receipt": None,
                          "usage": None, "observed_response_hash": digest(response)}
            if saved is not None:
                return saved
            if response is not None and target["effect_status"] == "pending":
                db.execute("UPDATE effects SET status='confirmed',response=? WHERE run=? AND id=?",
                           (canonical(response), run_id, call_id))
                usage = response["usage"]
                db.execute("INSERT INTO effect_usage VALUES(?,?,?)",
                           (run_id, call_id, canonical(usage) if usage is not None else None))
            db.execute("INSERT INTO effect_reconciliations VALUES(?,?,?,?,?)",
                       (run_id, verification_id, call_id, canonical(result), time.time()))
            return result

    def inspect(self, run_id):
        with self.transaction() as db:
            row = db.execute("SELECT state,result FROM runs WHERE id=?", (run_id,)).fetchone()
            if not row:
                raise Fault("UNKNOWN_RUN", "Run does not exist")
            return parse(row["state"]), parse(row["result"]) if row["result"] else None
