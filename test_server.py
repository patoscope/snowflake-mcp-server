import json
import logging
from server import list_connections, execute_query

# Configure minimal logging to see what's happening
logging.basicConfig(level=logging.INFO)

print("--- Testing List Connections ---")
try:
    connections = list_connections()
    print("Connections found:", connections)
except Exception as e:
    print(f"Error listing connections: {e}")

print("\n--- Testing Execution (Dry Run logic check) ---")
# specific connection name from user's toml context
TEST_CONN = "CDI li SANDBOX & INTG iafg.canada-central.azure" 

try:
    # We will try to execute a simple query. 
    # NOTE: This might trigger the browser popup for authentication if the token is not cached.
    # We'll use a very safe, lightweight query.
    print(f"Attempting query on: {TEST_CONN}")
    # result = execute_query("SELECT CURRENT_USER(), CURRENT_ACCOUNT(), CURRENT_VERSION()", TEST_CONN)
    result = execute_query("SELECT * FROM DB_CDI_DEV_DWH.RDV_SUREX_SANDBOX.HUB_SUREX_SOURCE_CLIENT LIMIT 10;", TEST_CONN)
    print("Query Result:", result)
except Exception as e:
    print(f"Error executing query: {e}")
