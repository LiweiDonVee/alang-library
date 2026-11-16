"""Standard MCP SDK stdio bridge."""
import argparse

import anyio
from mcp.server.lowlevel import Server
from mcp.server.stdio import stdio_server
from mcp.types import CallToolResult, TextContent, Tool, ToolAnnotations

from .adapters import read_json
from .cli import runtime_from_config
from .contracts import canonical, validate
from .tools import PINNED_SCHEMAS, SCHEMAS, PinnedTools, Tools


def server_for(tools, *, allowed_tools=None, annotations=None, schemas=SCHEMAS):
    server = Server("alang-library", version="0.1.0")
    allowed_tools = schemas.keys() if allowed_tools is None else allowed_tools
    annotations = annotations or {}

    @server.list_tools()
    async def discover():
        return [
            Tool(name=name, description=description, inputSchema=input_schema, outputSchema=output_schema,
                 annotations=annotations.get(name))
            for name, (description, input_schema, output_schema) in schemas.items()
            if name in allowed_tools
        ]

    @server.call_tool()
    async def call(name, arguments):
        if name not in allowed_tools:
            return CallToolResult(content=[TextContent(type="text", text="Tool is not exposed by this host")], isError=True)
        result = await anyio.to_thread.run_sync(tools.call, name, arguments)
        return CallToolResult(
            content=[TextContent(type="text", text=canonical(result))],
            structuredContent=result, isError=False,
        )

    return server


def pinned_server(runtime, revisions, *, read_only=False):
    annotations = {name: ToolAnnotations(readOnlyHint=name != "alang_run_procedure" or read_only)
                   for name in PINNED_SCHEMAS}
    return server_for(PinnedTools(runtime, revisions), schemas=PINNED_SCHEMAS, annotations=annotations)


async def serve_stdio(server):
    async with stdio_server() as (incoming, outgoing):
        await server.run(incoming, outgoing, server.create_initialization_options())


async def serve(config, db, pins=None, read_only=False):
    runtime = runtime_from_config(config, db)
    if pins is None:
        server = server_for(Tools(runtime))
    else:
        revisions = validate(read_json(pins), {"type": "object", "minProperties": 1,
                                               "additionalProperties": {"type": "string", "minLength": 1}})
        server = pinned_server(runtime, revisions, read_only=read_only)
    await serve_stdio(server)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", required=True)
    parser.add_argument("--db")
    parser.add_argument("--pins", help="Trusted JSON map of Procedure names to allowed Registry revisions")
    parser.add_argument("--read-only", action="store_true", help="Declare that every pinned Procedure has a read-only Host")
    args = parser.parse_args()
    if args.read_only and not args.pins:
        parser.error("--read-only requires --pins")
    anyio.run(serve, args.host, args.db, args.pins, args.read_only)


if __name__ == "__main__":
    main()
