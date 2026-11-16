# ALang Library

ALang runs versioned JSON procedures with typed context, explicit model and tool
calls, and SQLite checkpoints. Procedures decide what to call, how to process
results, and when to return or wait for input. The host supplies resources,
permissions, and adapters.

## Quickstart

From the source directory, use Python 3.12 or later in an activated virtual
environment.

```sh
python -m pip install .
alang --host examples/host.json tool alang_run_inline examples/run.json
```

The example resolves a scoped greeting and returns a result with
`"status": "completed"` and `"value": "Hello, ALang"`. The run makes no model
or network calls.

## Library and MCP

Python hosts can construct `Runtime` with a `Host` and a SQLite path, then call
`run_inline(source, inputs)`. Register `Model` or `Operation` adapters on the
host to make model or tool calls available to a procedure.

A local stdio MCP client can start the same example host with
`alang-mcp --host examples/host.json` and call `alang_run_inline` with the
contents of `examples/run.json`.
