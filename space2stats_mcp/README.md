# Space2Stats MCP Server

An [MCP (Model Context Protocol)](https://modelcontextprotocol.io/) server that exposes the Space2Stats API as tools for AI assistants. This allows LLMs in Claude Desktop, Claude Code, Cursor, and other MCP-compatible clients to directly query World Bank spatial statistics.

## Tools

| Tool | Description |
|------|-------------|
| `list_fields` | List all available field names |
| `list_timeseries_fields` | List available timeseries fields |
| `list_topics` | List dataset topics/themes from the STAC catalog |
| `get_topic_fields` | Get field descriptions for a specific topic |
| `fetch_admin_boundaries` | Fetch country boundaries (World Bank or GeoBoundaries) |
| `get_summary` | Get H3 hex-level statistics for an AOI |
| `get_summary_by_hexids` | Get statistics for specific H3 hex IDs |
| `get_aggregate` | Aggregate statistics for an AOI |
| `get_aggregate_by_hexids` | Aggregate statistics for specific hex IDs |
| `get_timeseries` | Get timeseries data for an AOI |
| `get_timeseries_by_hexids` | Get timeseries data for specific hex IDs |

## Installation

```bash
cd space2stats_mcp
pip install -e .
```

## Configuration

By default the server connects to the production API at `https://space2stats.ds.io`. Override with:

```bash
export SPACE2STATS_BASE_URL="http://localhost:8000"
```

### Claude Desktop

Add to `~/Library/Application Support/Claude/claude_desktop_config.json` (macOS) or `%APPDATA%\Claude\claude_desktop_config.json` (Windows):

```json
{
  "mcpServers": {
    "space2stats": {
      "command": "space2stats-mcp"
    }
  }
}
```

### Claude Code

Add to `.claude/settings.json` in your project or `~/.claude/settings.json` globally:

```json
{
  "mcpServers": {
    "space2stats": {
      "command": "space2stats-mcp"
    }
  }
}
```

### Cursor

Add to `.cursor/mcp.json` in your project root:

```json
{
  "mcpServers": {
    "space2stats": {
      "command": "space2stats-mcp"
    }
  }
}
```

### VS Code (GitHub Copilot)

Add to `.vscode/mcp.json` in your project or user settings:

```json
{
  "servers": {
    "space2stats": {
      "command": "space2stats-mcp"
    }
  }
}
```

### Windsurf

Add to `~/.codeium/windsurf/mcp_config.json`:

```json
{
  "mcpServers": {
    "space2stats": {
      "command": "space2stats-mcp"
    }
  }
}
```

### ChatGPT / Gemini / Other LLM chat interfaces

These platforms **do not currently support MCP** (including their desktop apps). They have their own plugin/extension systems:

- **ChatGPT / ChatGPT Desktop**: Uses "Actions" based on OpenAPI specs — not compatible with MCP
- **Gemini**: Uses Google-specific extensions — not compatible with MCP
- **OpenAI Agents SDK**: Has [experimental MCP support](https://openai.github.io/openai-agents-python/mcp/) for connecting to MCP servers programmatically

MCP is an open standard and adoption is growing. As more platforms add support, this server will work with them without changes.

## Testing

### Quick smoke test

Run the server directly to verify it starts without errors:

```bash
space2stats-mcp
```

The server communicates over stdio (JSON-RPC), so it will sit waiting for input. Press `Ctrl+C` to stop.

### Test with the MCP Inspector

The [MCP Inspector](https://modelcontextprotocol.io/docs/tools/inspector) provides a web UI for interacting with MCP servers:

```bash
npx @modelcontextprotocol/inspector space2stats-mcp
```

This opens a browser where you can:

1. See all 11 tools and their schemas
2. Call `list_fields` to verify API connectivity
3. Call `fetch_admin_boundaries` with `{"iso3": "KEN", "adm": "ADM0"}` to test boundary fetching
4. Call `get_aggregate` with a small AOI to test data queries

### Test with a manual JSON-RPC request

You can pipe a JSON-RPC message directly to the server:

```bash
echo '{"jsonrpc": "2.0", "id": 1, "method": "tools/call", "params": {"name": "list_fields", "arguments": {}}}' | space2stats-mcp
```

### End-to-end test in Claude Desktop

1. Install the server and add the config (see Configuration above)
2. Restart Claude Desktop
3. Look for the hammer icon — it should show "space2stats" with 11 tools
4. Try asking: "What population data fields are available in Space2Stats?"
5. Then try: "What is the total population of Andorra?" (small country = fast query)
