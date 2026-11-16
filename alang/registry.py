"""Immutable procedure revisions and atomic host-managed defaults."""
from .compiler import compile_program
from .contracts import Fault, canonical, digest, parse
from .runtime import failed


class Registry:
    def __init__(self, runtime):
        self.runtime, self.store = runtime, runtime.store

    def _manage(self):
        if not self.runtime.host.manage_registry:
            raise Fault("AUTHORIZATION_DENIED", "Registry management is not authorized", phase="registry")

    def _read(self, db, name, revision):
        if revision is None:
            pointer = db.execute("SELECT revision FROM defaults WHERE name=?", (name,)).fetchone()
            if pointer:
                revision = pointer["revision"]
        row = db.execute("SELECT * FROM versions WHERE name=? AND revision=?", (name, revision)).fetchone()
        if not row:
            raise Fault("UNKNOWN_PROCEDURE", "No matching procedure revision", phase="registry", name=name)
        return {**parse(row["record"]), "status": row["status"]}

    def save_candidate(self, source, parent=None, validation=None):
        self._manage()
        compiled = self.runtime.compile(source)
        name = source["name"]
        revision = digest({"hash": compiled.hash, "parent": parent})
        record = {
            "name": name, "revision": revision, "parent": parent, "hash": compiled.hash, "source": source,
            "description": source.get("description", ""), "input_schema": compiled.input_schema,
            "output_schema": compiled.output_schema, "capabilities": source["capabilities"],
            "validation": validation,
        }
        with self.store.transaction() as db:
            if parent is not None:
                self._read(db, name, parent)
            db.execute("INSERT OR IGNORE INTO versions VALUES(?,?,?,'candidate')",
                       (name, revision, canonical(record)))
            return self._read(db, name, revision)

    def publish(self, name, revision, validation):
        self._manage()
        if not isinstance(validation, str) or not validation.strip():
            raise Fault("VALIDATION_REQUIRED", "Publication requires a validation evidence reference", phase="registry")
        with self.store.transaction() as db:
            record = self._read(db, name, revision)
            if record["status"] != "candidate":
                raise Fault("INVALID_VERSION_STATE", "Only a candidate can be published", phase="registry")
            del record["status"]
            record["validation"] = validation
            db.execute("UPDATE versions SET status='published',record=? WHERE name=? AND revision=?",
                       (canonical(record), name, revision))
            return self._read(db, name, revision)

    def set_default(self, name, revision, *, expected):
        self._manage()
        with self.store.transaction() as db:
            record = self._read(db, name, revision)
            if record["status"] != "published":
                raise Fault("INVALID_VERSION_STATE", "Default must point to a published revision", phase="registry")
            pointer = db.execute("SELECT revision FROM defaults WHERE name=?", (name,)).fetchone()
            current = pointer["revision"] if pointer else None
            if current != expected:
                raise Fault("REGISTRY_CONFLICT", "Default revision changed", phase="registry", current=current)
            db.execute("INSERT INTO defaults VALUES(?,?) ON CONFLICT(name) DO UPDATE SET revision=excluded.revision",
                       (name, revision))
            return {"name": name, "previous": current, "revision": revision}

    def deactivate(self, name, revision):
        self._manage()
        with self.store.transaction() as db:
            record = self._read(db, name, revision)
            if record["status"] != "published":
                raise Fault("INVALID_VERSION_STATE", "Only published revisions can be deactivated", phase="registry")
            pointer = db.execute("SELECT revision FROM defaults WHERE name=?", (name,)).fetchone()
            if pointer and pointer["revision"] == revision:
                raise Fault("DEFAULT_VERSION_ACTIVE", "Move the default before deactivating this revision", phase="registry")
            db.execute("UPDATE versions SET status='inactive' WHERE name=? AND revision=?", (name, revision))

    def _visible(self, record):
        host = self.runtime.host
        return record["status"] == "published" and (host.procedures is None or record["name"] in host.procedures)

    @staticmethod
    def _description(record):
        return {key: value for key, value in record.items() if key != "source"}

    def describe(self, name, revision=None):
        with self.store.transaction() as db:
            record = self._read(db, name, revision)
        if not self._visible(record):
            raise Fault("UNKNOWN_PROCEDURE", "Procedure revision is not visible", phase="registry", name=name)
        return self._description(record)

    def list(self):
        with self.store.transaction() as db:
            rows = db.execute("SELECT name,revision FROM defaults ORDER BY name").fetchall()
            records = [self._read(db, row["name"], row["revision"]) for row in rows]
        return [self._description(record) for record in records if self._visible(record)]

    def run(self, name, inputs, limits=None, *, revision=None):
        try:
            with self.store.transaction() as db:
                record = self._read(db, name, revision)
            if not self._visible(record):
                raise Fault("UNKNOWN_PROCEDURE", "Procedure revision is not visible", phase="registry", name=name)
            return self.runtime.run_inline(record["source"], inputs, limits, revision=record["revision"])
        except Fault as fault:
            return failed(fault)
