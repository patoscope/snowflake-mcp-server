from mcp.server.fastmcp import FastMCP
import snowflake.connector
import toml
import os
import json
import logging

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
        authenticator=config.get("authenticator")
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
def execute_query(query: str, connection_name: str) -> str:
    """
    Execute a SQL query against Snowflake.
    
    Args:
        query: The SQL query to execute.
        connection_name: The name of the connection profile to use (e.g. from list_connections).
    """
    try:
        ctx = get_snowflake_connection(connection_name)
        
        cs = ctx.cursor()
        try:
            cs.execute(query)
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
