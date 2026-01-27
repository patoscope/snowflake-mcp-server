# Snowflake MCP Server for GitHub Copilot

This MCP server allows GitHub Copilot to execute SQL queries against your Snowflake instances using your local configuration.

## Setup

1.  **Prerequisites**:
    *   Python 3.10+
    *   `pip install -r requirements.txt`

2.  **Configuration**:
    The server reads connection profiles from `~/.snowflake/connections.toml`.
    It automatically overrides the `user` field with your current active Windows username (e.g., `li23nw`), as requested.

3.  **Registering with VS Code / GitHub Copilot**:
    Add the following to your MCP settings configuration (typically `~/.vscode/mcp.json` or configured via the Copilot extension settings):

    ```json
    {
      "mcpServers": {
        "snowflake": {
          "command": "python",
          "args": [
            "c:/CDI/dev/snowflake-mcp-server/server.py"
          ]
        }
      }
    }
    ```
    *Note: Ensure the path to `server.py` is correct and absolute. If executing from a specific environment, use the absolute path to the python executable.*

## Tools

*   **list_connections**: Lists the available connection profiles from your TOML file.
*   **execute_query**: Executes a SQL query against a specified connection. 
    *   Arguments: `query` (string), `connection_name` (string)

## Troubleshooting

*   The server uses `authenticator='externalbrowser'`. The first time you run a query, a browser window should open to authenticate you.
*   Logs are printed to stderr, which should be visible in the MCP server logs in VS Code.
