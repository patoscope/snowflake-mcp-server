from mcp.server.fastmcp import FastMCP
import snowflake.connector
import toml
import os
import json
import logging
import csv

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

mcp = FastMCP("snowflake-mcp-server")

CONNECTIONS_FILE = os.path.expanduser("~/.snowflake/connections.toml")

# Global cache for active connections to avoid re-authentication (browser popups)
ACTIVE_CONNECTIONS = {}

def get_connection_config(connection_name: str):
    if not os.path.exists(CONNECTIONS_FILE):
        raise FileNotFoundError(f"Connections file not found at {CONNECTIONS_FILE}")
    
    try:
        with open(CONNECTIONS_FILE, "r") as f:
            config = toml.load(f)
    except Exception as e:
        raise ValueError(f"Failed to parse connections.toml: {e}")

    if connection_name not in config:
        # Try to find a partial match or case-insensitive match
        for key in config.keys():
            if connection_name.lower() == key.lower():
                connection_name = key
                break
        else:
            available = list(config.keys())
            raise ValueError(f"Connection '{connection_name}' not found. Available: {available}")
        
    profile = config[connection_name]
    
    # We use the user defined in the toml file (e.g. email) instead of the windows session user,
    # because Snowflake IDP often requires the full email address.
    current_user = profile.get("user")
    windows_user = os.environ.get("USERNAME") or os.getlogin()
    logger.info(f"Configuration loaded. User from TOML: '{current_user}'. (Windows session user: '{windows_user}')")
    
    return profile

def get_snowflake_connection(connection_name: str):
    """
    Retrieves an existing open connection or creates a new one.
    """
    global ACTIVE_CONNECTIONS
    
    # Check if we have a cached connection
    if connection_name in ACTIVE_CONNECTIONS:
        conn = ACTIVE_CONNECTIONS[connection_name]
        # Verify if it's still open
        if not conn.is_closed():
            logger.info(f"Reusing existing connection for: {connection_name}")
            return conn
        else:
            logger.info(f"Cached connection for {connection_name} is closed. Removing from cache.")
            del ACTIVE_CONNECTIONS[connection_name]

    # Create new connection
    config = get_connection_config(connection_name)
    logger.info(f"Connecting to Snowflake connection: {connection_name}")
    logger.info(f"Connection parameters: Account={config.get('account')}, User={config.get('user')}, Role={config.get('role')}, Warehouse={config.get('warehouse')}, Authenticator={config.get('authenticator')}")
    
    ctx = snowflake.connector.connect(
        user=config.get("user"),
        password=config.get("password"),
        account=config.get("account"),
        warehouse=config.get("warehouse"),
        database=config.get("database"),
        schema=config.get("schema"),
        role=config.get("role"),
        authenticator=config.get("authenticator"),
        client_session_keep_alive=True,
        client_store_temporary_credential=True
    )
    
    # Store in cache
    ACTIVE_CONNECTIONS[connection_name] = ctx
    return ctx

@mcp.tool()
def list_connections() -> str:
    """List available connection names from connections.toml"""
    if not os.path.exists(CONNECTIONS_FILE):
        return "Connections file not found."
    with open(CONNECTIONS_FILE, "r") as f:
        config = toml.load(f)
    return json.dumps(list(config.keys()))

@mcp.tool()
def execute_query(query: str, connection_name: str, export_to_csv: str = None) -> str:
    """
    Execute a SQL query against Snowflake.
    
    Args:
        query: The SQL query to execute.
        connection_name: The name of the connection profile to use (e.g. from list_connections).
        export_to_csv: Optional absolute path to save the result as a CSV file instead of returning JSON.
                       Useful for large datasets.
    """
    # SAFETY CHECK: Prevent execution in PROD
    try:
        # Check connection name
        if "PROD" in connection_name.upper():
            return f"🚫 AUTHORIZATION DENIED: Executing queries in PROD environment '{connection_name}' is not allowed via MCP."

        # Check warehouse in configuration
        # We need to load config to check the warehouse
        config = get_connection_config(connection_name)
        warehouse = config.get("warehouse", "").upper()
        if "PROD" in warehouse:
             return f"🚫 AUTHORIZATION DENIED: Executing queries on PROD warehouse '{warehouse}' is not allowed via MCP."

        # SAFETY CHECK: Allow only SELECT-like queries
        # We allow SELECT, WITH (Common Table Expressions), SHOW (Metadata), DESC/DESCRIBE, EXPLAIN, LIST (Stages)
        # Strip whitespace and remove SQL comments before checking
        query_check = query.strip()
        # Remove leading SQL comments (-- style)
        while query_check.upper().startswith(('--', '/*')):
            if query_check.startswith('--'):
                # Remove single-line comment
                next_line = query_check.find('\n')
                if next_line > 0:
                    query_check = query_check[next_line+1:].strip()
                else:
                    return "🚫 AUTHORIZATION DENIED: Query contains only comments."
            elif query_check.startswith('/*'):
                # Remove multi-line comment
                end_comment = query_check.find('*/')
                if end_comment > 0:
                    query_check = query_check[end_comment+2:].strip()
                else:
                    return "🚫 AUTHORIZATION DENIED: Malformed multi-line comment."
        
        if not query_check.upper().startswith(("SELECT", "WITH", "SHOW", "DESC", "EXPLAIN", "LIST")):
            return "🚫 AUTHORIZATION DENIED: Only SELECT/Read-Only statements are allowed."
             
    except Exception as e:
        return f"Configuration Error during safety check: {str(e)}"

    try:
        ctx = get_snowflake_connection(connection_name)
        
        cs = ctx.cursor()
        try:
            cs.execute(query)
            
            # Handle CSV Export
            if export_to_csv:
                try:
                    # Check if result has rows
                    if not cs.description:
                        return json.dumps({"status": "success", "message": "Query executed but returned no rows (DDL/DML?). No CSV created."})
                    
                    columns = [col[0] for col in cs.description]
                    
                    # Ensure directory exists if path is absolute
                    output_path = os.path.abspath(export_to_csv)
                    os.makedirs(os.path.dirname(output_path), exist_ok=True)
                    
                    row_count = 0
                    with open(output_path, 'w', newline='', encoding='utf-8') as f:
                        writer = csv.writer(f)
                        writer.writerow(columns)  # Header
                        
                        # Fetch in chunks to save memory
                        while True:
                            rows = cs.fetchmany(1000)
                            if not rows:
                                break
                            writer.writerows(rows)
                            row_count += len(rows)
                            
                    return json.dumps({
                        "status": "success", 
                        "message": f"Query result exported to CSV.",
                        "file_path": output_path,
                        "rows_count": row_count
                    })
                    
                except Exception as csv_err:
                    return f"Error exporting to CSV: {str(csv_err)}"
            
            # Standard JSON Return
            result = cs.fetchall()
            
            if cs.description:
                columns = [col[0] for col in cs.description]
                data = [dict(zip(columns, row)) for row in result]
                return json.dumps(data, default=str)
            else:
                return json.dumps({"status": "success", "rows_affected": cs.rowcount})
                
        finally:
            cs.close()
            # DO NOT close the connection here, so we can reuse it
            pass
            
    except Exception as e:
        # If an error occurs, it might be due to a connection drop.
        # We can invalidate the cache for next time.
        if connection_name in ACTIVE_CONNECTIONS:
            try:
                ACTIVE_CONNECTIONS[connection_name].close()
            except:
                pass
            if connection_name in ACTIVE_CONNECTIONS:
                del ACTIVE_CONNECTIONS[connection_name]
            
        return f"Error executing query: {str(e)}"

if __name__ == "__main__":
    mcp.run()
